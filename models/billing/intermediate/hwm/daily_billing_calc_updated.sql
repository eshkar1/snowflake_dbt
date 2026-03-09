WITH

-- ============================================================
-- SOURCES
-- ============================================================
meta AS (
    SELECT * FROM {{ ref('ltp_account_meta') }}
),

hwm AS (
    SELECT * FROM {{ ref('current_global_tenant_ltp_hwm') }}
),

dmarc_hwm AS (
    SELECT * FROM {{ ref('current_month_ltp_dmarc_domain_hwm') }}
),

-- Deduplicate: unpivot table contains duplicate rows per tenant/sku/tier
pricing AS (
    SELECT DISTINCT
        TENANT_GLOBAL_ID,
        SKU,
        IS_NFR,
        TIER_MIN,
        TIER_MAX,
        PRICE,
        SNAPSHOT_DATE,
        PRICE_TYPE
    FROM {{ ref('ltp_pricing_tbl_unpivot') }}
),

-- ============================================================
-- QUANTITIES: aggregate to LTP (ROOT) grain
--
-- combined_quantity: sum across all roots sharing the same ACCOUNT_MASTER_ID
--   → for single-root LTPs: combined_quantity = quantity (same value)
--   → for multi-region LTPs (e.g. Pax8 US-733 + EU-25): pools both roots
--
-- effective_is_nfr: TRUE only when PARTNER_PRICING=TRUE
--   AND NOT_NFR_PARTNER is not overriding it (FALSE or NULL)
-- ============================================================
quantities AS (
    SELECT
        h.DATE_RECORDED,
        h.ROOT                                      AS ltp,
        m.ACCOUNT_MASTER_ID,
        h.PLAN_NAME,
        h.PARTNER_PRICING                           AS is_nfr,
        -- effective_is_nfr: TRUE only when partner-priced AND NOT_NFR_PARTNER is not overriding it
        (h.PARTNER_PRICING = TRUE AND COALESCE(h.NOT_NFR_PARTNER, FALSE) = FALSE)
                                                    AS effective_is_nfr,
        h.PREMIUM_NAME,
        h.INCIDENT_MANAGEMENT,
        h.SIMULATION_AND_TRAINING_BUNDLE_PLUS,
        h.ATO,
        h.DMARC_IRONSCALES_PLAN,
        m.PROFILE_TYPE,
        -- This LTP's own seat count
        CASE m.PROFILE_TYPE
            WHEN 'active'  THEN SUM(h.ACTIVE_PROFILES)
            WHEN 'license' THEN SUM(h.LICENSED_PROFILES)
            WHEN 'shared'  THEN CASE
                                    WHEN SUM(h.SHARED_PROFILES) IS NULL THEN SUM(h.ACTIVE_PROFILES)
                                    ELSE SUM(h.ACTIVE_PROFILES) - SUM(h.SHARED_PROFILES)
                                END
        END                                         AS quantity,
        -- Pooled seat count across all roots sharing the same ACCOUNT_MASTER_ID
        -- Drives the tier waterfall calculation
        SUM(
            CASE m.PROFILE_TYPE
                WHEN 'active'  THEN SUM(h.ACTIVE_PROFILES)
                WHEN 'license' THEN SUM(h.LICENSED_PROFILES)
                WHEN 'shared'  THEN CASE
                                        WHEN SUM(h.SHARED_PROFILES) IS NULL THEN SUM(h.ACTIVE_PROFILES)
                                        ELSE SUM(h.ACTIVE_PROFILES) - SUM(h.SHARED_PROFILES)
                                    END
            END
        ) OVER (
            PARTITION BY
                h.DATE_RECORDED,
                m.ACCOUNT_MASTER_ID,
                h.PLAN_NAME,
                (h.PARTNER_PRICING = TRUE AND COALESCE(h.NOT_NFR_PARTNER, FALSE) = FALSE),
                m.PROFILE_TYPE
        )                                           AS combined_quantity
    FROM hwm h
    JOIN meta m ON h.ROOT = m.TENANT_GLOBAL_ID
    WHERE
        h.APPROVED = TRUE
        AND h.BILLING_STATUS IN ('Active', 'Active-POC')
        AND m.PROFILE_TYPE IS NOT NULL
        AND h.LICENSED_PROFILES IS NOT NULL
    GROUP BY
        h.DATE_RECORDED, h.ROOT, m.ACCOUNT_MASTER_ID, h.PLAN_NAME, h.PARTNER_PRICING, h.NOT_NFR_PARTNER,
        h.PREMIUM_NAME, h.INCIDENT_MANAGEMENT,
        h.SIMULATION_AND_TRAINING_BUNDLE_PLUS, h.ATO,
        h.DMARC_IRONSCALES_PLAN, m.PROFILE_TYPE
),

-- ============================================================
-- CANONICAL LTP: for multi-region accounts (same ACCOUNT_MASTER_ID),
-- prefer US- root as the pricing anchor; fall back to MIN if no US root.
-- Ensures one consistent price grid for the combined waterfall.
-- ============================================================
canonical_ltp AS (
    SELECT
        ACCOUNT_MASTER_ID,
        COALESCE(
            MIN(CASE WHEN TENANT_GLOBAL_ID LIKE 'US-%' THEN TENANT_GLOBAL_ID END),
            MIN(TENANT_GLOBAL_ID)
        )                                           AS pricing_ltp
    FROM meta
    GROUP BY ACCOUNT_MASTER_ID
),

-- ============================================================
-- PLANS: blended tier waterfall
--
-- Step 1 (tier_calc_combined): run the waterfall ONCE per ACCOUNT_MASTER_ID
--   using combined_quantity → produces total_tier_amount for the group
--
-- Step 2 (plans): each LTP's amount = its proportion of total_tier_amount
--   amount = (quantity / combined_quantity) * total_tier_amount
--   → this gives each LTP the blended rate applied to its own seat count
--
-- Pricing always uses the canonical root (US- preferred) for consistency.
-- ============================================================
tier_calc_combined AS (
    SELECT
        q.DATE_RECORDED,
        q.ACCOUNT_MASTER_ID,
        q.PLAN_NAME,
        q.effective_is_nfr,
        q.PROFILE_TYPE,
        q.combined_quantity,
        SUM(
            GREATEST(0, LEAST(q.combined_quantity, p.TIER_MAX) - (p.TIER_MIN - 1))
            * p.PRICE
        )                                           AS total_tier_amount
    FROM (
        -- one row per account_master_id/plan/nfr/profile_type/date
        SELECT DISTINCT
            DATE_RECORDED,
            ACCOUNT_MASTER_ID,
            PLAN_NAME,
            effective_is_nfr,
            PROFILE_TYPE,
            combined_quantity
        FROM quantities
        WHERE PLAN_NAME IN (
            'Email Protect', 'Complete Protect', 'Core',
            'IRONSCALES Protect', 'SAT Suite', 'Starter'
        )
    ) q
    JOIN canonical_ltp c
        ON  q.ACCOUNT_MASTER_ID = c.ACCOUNT_MASTER_ID
    JOIN pricing p
        ON  c.pricing_ltp = p.TENANT_GLOBAL_ID     -- always price off canonical root
        AND p.IS_NFR      = q.effective_is_nfr
        AND p.SKU         = CASE q.PLAN_NAME
                                WHEN 'Email Protect'      THEN 'Email Protect'
                                WHEN 'Complete Protect'   THEN 'Complete Protect'
                                WHEN 'Core'               THEN 'CORE'
                                WHEN 'IRONSCALES Protect' THEN 'IRONSCALES Protect'
                                WHEN 'SAT Suite'          THEN 'SAT_SUITE'
                                WHEN 'Starter'            THEN 'STARTER'
                            END
    GROUP BY
        q.DATE_RECORDED, q.ACCOUNT_MASTER_ID, q.PLAN_NAME,
        q.effective_is_nfr, q.PROFILE_TYPE, q.combined_quantity
),

plans AS (
    SELECT
        q.DATE_RECORDED,
        q.ltp,
        q.PLAN_NAME                                 AS item,
        CASE WHEN q.effective_is_nfr = FALSE THEN
            CASE q.PLAN_NAME
                WHEN 'Email Protect'      THEN 'IS-LTP-EP'
                WHEN 'Complete Protect'   THEN 'IS-LTP-CP'
                WHEN 'Core'               THEN 'IS-LTP-CORE'
                WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IP'
                WHEN 'SAT Suite'          THEN 'IS-SAT_SUITE_1'
                WHEN 'Starter'            THEN 'IS-LTP-STARTER'
            END
        ELSE
            CASE q.PLAN_NAME
                WHEN 'Email Protect'      THEN 'IS-LTP-EPNFR'
                WHEN 'Complete Protect'   THEN 'IS-LTP-CPNFR'
                WHEN 'Core'               THEN 'IS-LTP-CORENFR'
                WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IPNFR'
                WHEN 'SAT Suite'          THEN 'IS-SAT_SUITENFR_1'
                WHEN 'Starter'            THEN 'IS-LTP-STARTERNFR'
            END
        END                                         AS sku,
        q.is_nfr                                    AS partner_pricing,
        q.quantity,
        -- Each LTP's amount = its share of the blended waterfall total
        -- = (this LTP's seats / all seats in the group) * total waterfall amount
        CASE
            WHEN t.combined_quantity = 0 OR t.combined_quantity IS NULL THEN 0
            ELSE (q.quantity / t.combined_quantity) * t.total_tier_amount
        END                                         AS amount
    FROM quantities q
    JOIN tier_calc_combined t
        ON  q.DATE_RECORDED     = t.DATE_RECORDED
        AND q.ACCOUNT_MASTER_ID = t.ACCOUNT_MASTER_ID
        AND q.PLAN_NAME         = t.PLAN_NAME
        AND q.effective_is_nfr  = t.effective_is_nfr
        AND q.PROFILE_TYPE      = t.PROFILE_TYPE
    WHERE q.PLAN_NAME IN (
        'Email Protect', 'Complete Protect', 'Core',
        'IRONSCALES Protect', 'SAT Suite', 'Starter'
    )
),

-- ============================================================
-- PREMIUM (flat rate)
-- Aggregate to LTP grain: SUM across all plan rows per LTP.
-- No IS_NFR filter — all tenants billed regardless of partner status.
-- ============================================================
premium AS (
    SELECT
        q.DATE_RECORDED,
        q.ltp,
        q.PREMIUM_NAME                              AS item,
        'IS-LTP-PSCP'                               AS sku,
        NULL                                        AS partner_pricing,
        SUM(q.quantity)                             AS quantity,
        SUM(q.quantity) * p.PRICE                   AS amount
    FROM quantities q
    JOIN pricing p
        ON  q.ltp      = p.TENANT_GLOBAL_ID
        AND p.SKU      = 'PSCP'
        AND p.TIER_MIN = 1
    WHERE q.PREMIUM_NAME NOT IN ('No Premium', '')
      AND q.PREMIUM_NAME IS NOT NULL
    GROUP BY
        q.DATE_RECORDED, q.ltp, q.PREMIUM_NAME,
        q.PROFILE_TYPE, p.PRICE
),

-- ============================================================
-- INCIDENT MANAGEMENT (incremental tiers)
-- Aggregate to LTP grain: SUM quantity across all plan rows first,
-- then apply tier waterfall on the LTP total.
-- No IS_NFR filter — all tenants billed regardless of partner status.
-- ============================================================
incident_mgmt AS (
    SELECT
        q.DATE_RECORDED,
        q.ltp,
        'Incident Management'                       AS item,
        'IS-LTP-IM'                                 AS sku,
        NULL                                        AS partner_pricing,
        SUM(q.quantity)                             AS quantity,
        SUM(
            GREATEST(0, LEAST(q.quantity, p.TIER_MAX) - (p.TIER_MIN - 1)) * p.PRICE
        )                                           AS amount
    FROM quantities q
    JOIN pricing p
        ON  q.ltp      = p.TENANT_GLOBAL_ID
        AND p.SKU      = 'Incident Management'
    WHERE q.INCIDENT_MANAGEMENT = TRUE
    GROUP BY
        q.DATE_RECORDED, q.ltp,
        q.PROFILE_TYPE, q.quantity, p.TIER_MIN, p.TIER_MAX, p.PRICE
),

-- ============================================================
-- S&T BUNDLE PLUS (flat rate)
-- Aggregate to LTP grain: exclude CP/SAT plan rows first (bundled),
-- then SUM remaining quantity.
-- No IS_NFR filter — all tenants billed regardless of partner status.
-- ============================================================
stbp AS (
    SELECT
        q.DATE_RECORDED,
        q.ltp,
        'S&T Bundle Plus'                           AS item,
        'IS-LTP-STBP'                               AS sku,
        NULL                                        AS partner_pricing,
        SUM(q.quantity)                             AS quantity,
        SUM(q.quantity) * p.PRICE                   AS amount
    FROM quantities q
    JOIN pricing p
        ON  q.ltp      = p.TENANT_GLOBAL_ID
        AND p.SKU      = 'STBP'
        AND p.TIER_MIN = 1
    WHERE q.SIMULATION_AND_TRAINING_BUNDLE_PLUS = TRUE
      AND q.PLAN_NAME NOT IN ('Complete Protect', 'SAT Suite')
    GROUP BY
        q.DATE_RECORDED, q.ltp, q.PROFILE_TYPE, p.PRICE
),

-- ============================================================
-- ACCOUNT TAKEOVER (flat rate)
-- Aggregate to LTP grain: exclude CP plan rows first (bundled),
-- then SUM remaining quantity.
-- No IS_NFR filter — all tenants billed regardless of partner status.
-- ============================================================
ato AS (
    SELECT
        q.DATE_RECORDED,
        q.ltp,
        'Account Takeover'                          AS item,
        'IS-LTP-ATO'                                AS sku,
        NULL                                        AS partner_pricing,
        SUM(q.quantity)                             AS quantity,
        SUM(q.quantity) * p.PRICE                   AS amount
    FROM quantities q
    JOIN pricing p
        ON  q.ltp      = p.TENANT_GLOBAL_ID
        AND p.SKU      = 'ATO'
        AND p.TIER_MIN = 1
    WHERE q.ATO = TRUE
      AND q.PLAN_NAME != 'Complete Protect'
    GROUP BY
        q.DATE_RECORDED, q.ltp, q.PROFILE_TYPE, p.PRICE
),

-- ============================================================
-- DMARC: join dmarc_hwm on SUB-TENANT (h.TENANT_GLOBAL_ID),
-- aggregate domain counts up to ROOT, price from unpivot on ROOT.
-- No IS_NFR filter — all tenants billed regardless of partner status.
-- DMARC_MANAGEMENT=TRUE required: excludes plan-set but inactive roots.
-- ============================================================
dmarc AS (
    SELECT
        h.DATE_RECORDED,
        h.ROOT                                      AS ltp,
        CASE h.DMARC_IRONSCALES_PLAN
            WHEN 1 THEN 'DMARC Core Management'
            WHEN 2 THEN 'DMARC Pro'
            WHEN 3 THEN 'DMARC Premium'
        END                                         AS item,
        CASE h.DMARC_IRONSCALES_PLAN
            WHEN 1 THEN 'IS-LTP-DMARC'
            WHEN 2 THEN 'IS-LTP-DMARC_PRO'
            WHEN 3 THEN 'IS-LTP-DMARC_PREMIUM'
        END                                         AS sku,
        NULL                                        AS partner_pricing,
        SUM(d.DMARC_DOMAINS_NUMBER)                 AS quantity,
        SUM(d.DMARC_DOMAINS_NUMBER) * p.PRICE       AS amount
    FROM hwm h
    JOIN dmarc_hwm d
        ON  h.TENANT_GLOBAL_ID = d.TENANT_GLOBAL_ID
    JOIN pricing p
        ON  h.ROOT             = p.TENANT_GLOBAL_ID
        AND p.TIER_MIN         = 1
        AND p.SKU              = CASE h.DMARC_IRONSCALES_PLAN
                                    WHEN 1 THEN 'DMARC'
                                    WHEN 2 THEN 'DMARC_PRO'
                                    WHEN 3 THEN 'DMARC'
                                 END
    WHERE
        h.APPROVED = TRUE
        AND h.BILLING_STATUS IN ('Active', 'Active-POC')
        AND h.DMARC_IRONSCALES_PLAN IS NOT NULL
        AND h.DMARC_MANAGEMENT = TRUE
    GROUP BY
        h.DATE_RECORDED, h.ROOT,
        h.DMARC_IRONSCALES_PLAN, p.PRICE
    HAVING SUM(d.DMARC_DOMAINS_NUMBER) > 0
)

-- ============================================================
-- FINAL OUTPUT
-- ============================================================
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount FROM plans
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount FROM premium
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount FROM incident_mgmt
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount FROM stbp
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount FROM ato
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount FROM dmarc
