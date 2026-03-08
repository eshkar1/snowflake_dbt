WITH

-- ============================================================
-- SOURCES (all new tables)
-- ============================================================
meta AS (
    SELECT * FROM {{ ref('ltp_account_meta') }}
    -- WHERE SNAPSHOT_DATE = CURRENT_DATE()
),

hwm AS (
    SELECT * FROM {{ ref('current_global_tenant_ltp_hwm') }}
),

dmarc_hwm AS (
    SELECT * FROM {{ ref('current_month_ltp_dmarc_domain_hwm') }}
),

pricing AS (
    SELECT * FROM {{ ref('ltp_pricing_tbl_unpivot') }}
    -- WHERE SNAPSHOT_DATE = CURRENT_DATE()
),

-- ============================================================
-- QUANTITIES
-- ROOT = the LTP account, join to meta on ROOT = TENANT_GLOBAL_ID
-- profile_type (active/license/shared) drives how we count seats
-- ============================================================
quantities AS (
    SELECT
        h.DATE_RECORDED,
        h.ROOT                                      AS ltp,
        h.PLAN_NAME,
        h.PARTNER_PRICING                           AS is_nfr,
        h.PREMIUM_NAME,
        h.INCIDENT_MANAGEMENT,
        h.SIMULATION_AND_TRAINING_BUNDLE_PLUS,
        h.ATO,
        h.DMARC_IRONSCALES_PLAN,
        m.PROFILE_TYPE,
        -- per-sub-tenant quantity
        CASE m.PROFILE_TYPE
            WHEN 'active'  THEN SUM(h.ACTIVE_PROFILES)
            WHEN 'license' THEN SUM(h.LICENSED_PROFILES)
            WHEN 'shared'  THEN CASE
                                    WHEN SUM(h.SHARED_PROFILES) IS NULL THEN SUM(h.ACTIVE_PROFILES)
                                    ELSE SUM(h.ACTIVE_PROFILES) - SUM(h.SHARED_PROFILES)
                                END
        END                                         AS quantity,
        -- combined_quantity = total across all sub-tenants under this LTP × plan
        -- used for tier routing in MSP waterfall scenarios
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
            PARTITION BY h.DATE_RECORDED, h.ROOT, h.PLAN_NAME, h.PARTNER_PRICING, m.PROFILE_TYPE
        )                                           AS combined_quantity
    FROM hwm h
    JOIN meta m
        ON  h.ROOT          = m.TENANT_GLOBAL_ID
    WHERE
        h.APPROVED          = TRUE
        AND h.BILLING_STATUS IN ('Active', 'Active-POC')
        AND m.PROFILE_TYPE  IS NOT NULL
        AND h.LICENSED_PROFILES IS NOT NULL
    GROUP BY
        h.DATE_RECORDED, h.ROOT, h.PLAN_NAME, h.PARTNER_PRICING,
        h.PREMIUM_NAME, h.INCIDENT_MANAGEMENT,
        h.SIMULATION_AND_TRAINING_BUNDLE_PLUS, h.ATO,
        h.DMARC_IRONSCALES_PLAN, m.PROFILE_TYPE
),

-- ============================================================
-- PLANS: incremental waterfall using unpivot tiers
-- Each tier row contributes: GREATEST(0, LEAST(combined_qty, TIER_MAX) - (TIER_MIN-1)) * PRICE
-- Sub-tenant amount = its share of root total (quantity / combined_quantity)
-- ============================================================
tier_calc AS (
    SELECT
        q.DATE_RECORDED,
        q.ltp,
        q.PLAN_NAME,
        q.is_nfr,
        q.PROFILE_TYPE,
        q.quantity,
        q.combined_quantity,
        GREATEST(0, LEAST(q.combined_quantity, p.TIER_MAX) - (p.TIER_MIN - 1))
            * p.PRICE                               AS tier_contribution
    FROM quantities q
    JOIN pricing p
        ON  q.ltp       = p.TENANT_GLOBAL_ID
        AND p.IS_NFR    = q.is_nfr
        AND p.SKU       = CASE q.PLAN_NAME
                            WHEN 'Email Protect'      THEN 'Email Protect'
                            WHEN 'Complete Protect'   THEN 'Complete Protect'
                            WHEN 'Core'               THEN 'CORE'
                            WHEN 'IRONSCALES Protect' THEN 'IRONSCALES Protect'
                            WHEN 'SAT Suite'          THEN 'SAT_SUITE'
                            WHEN 'Starter'            THEN 'STARTER'
                          END
    WHERE q.PLAN_NAME IN (
        'Email Protect', 'Complete Protect', 'Core',
        'IRONSCALES Protect', 'SAT Suite', 'Starter'
    )
),

plans AS (
    SELECT
        DATE_RECORDED,
        ltp,
        PLAN_NAME                                   AS item,
        CASE WHEN is_nfr = FALSE THEN
            CASE PLAN_NAME
                WHEN 'Email Protect'      THEN 'IS-LTP-EP'
                WHEN 'Complete Protect'   THEN 'IS-LTP-CP'
                WHEN 'Core'               THEN 'IS-LTP-CORE'
                WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IP'
                WHEN 'SAT Suite'          THEN 'IS-SAT_SUITE_1'
                WHEN 'Starter'            THEN 'IS-LTP-STARTER'
            END
        ELSE
            CASE PLAN_NAME
                WHEN 'Email Protect'      THEN 'IS-LTP-EPNFR'
                WHEN 'Complete Protect'   THEN 'IS-LTP-CPNFR'
                WHEN 'Core'               THEN 'IS-LTP-CORENFR'
                WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IPNFR'
                WHEN 'SAT Suite'          THEN 'IS-SAT_SUITENFR_1'
                WHEN 'Starter'            THEN 'IS-LTP-STARTERNFR'
            END
        END                                         AS sku,
        is_nfr                                      AS partner_pricing,
        quantity,
        CASE
            WHEN combined_quantity = 0 OR combined_quantity IS NULL THEN 0
            ELSE (quantity / combined_quantity) * SUM(tier_contribution)
        END                                         AS amount
    FROM tier_calc
    GROUP BY
        DATE_RECORDED, ltp, PLAN_NAME, is_nfr,
        PROFILE_TYPE, quantity, combined_quantity
),

-- ============================================================
-- PREMIUM (flat rate, PSCP)
-- ============================================================
premium AS (
    SELECT DISTINCT
        q.DATE_RECORDED,
        q.ltp,
        q.PREMIUM_NAME                              AS item,
        'IS-LTP-PSCP'                               AS sku,
        FALSE                                       AS partner_pricing,
        q.quantity,
        q.quantity * p.PRICE                        AS amount
    FROM quantities q
    JOIN pricing p
        ON  q.ltp       = p.TENANT_GLOBAL_ID
        AND p.SKU       = 'PSCP'
        AND p.IS_NFR    = FALSE
        AND p.TIER_MIN  = 1
    WHERE q.PREMIUM_NAME NOT IN ('No Premium', '')
      AND q.PREMIUM_NAME IS NOT NULL
),

-- ============================================================
-- INCIDENT MANAGEMENT (incremental tiers, quantity NOT combined)
-- IM is per-tenant flat or tiered — no MSP cross-tenant pooling
-- ============================================================
incident_mgmt AS (
    SELECT
        q.DATE_RECORDED,
        q.ltp,
        'Incident Management'                       AS item,
        'IS-LTP-IM'                                 AS sku,
        FALSE                                       AS partner_pricing,
        q.quantity,
        SUM(
            GREATEST(0, LEAST(q.quantity, p.TIER_MAX) - (p.TIER_MIN - 1)) * p.PRICE
        )                                           AS amount
    FROM quantities q
    JOIN pricing p
        ON  q.ltp       = p.TENANT_GLOBAL_ID
        AND p.SKU       = 'Incident Management'
        AND p.IS_NFR    = FALSE
    WHERE q.INCIDENT_MANAGEMENT = TRUE
    GROUP BY
        q.DATE_RECORDED, q.ltp, q.PROFILE_TYPE, q.quantity
),

-- ============================================================
-- S&T BUNDLE PLUS (flat rate)
-- ============================================================
stbp AS (
    SELECT DISTINCT
        q.DATE_RECORDED,
        q.ltp,
        'S&T Bundle Plus'                           AS item,
        'IS-LTP-STBP'                               AS sku,
        FALSE                                       AS partner_pricing,
        q.quantity,
        q.quantity * p.PRICE                        AS amount
    FROM quantities q
    JOIN pricing p
        ON  q.ltp       = p.TENANT_GLOBAL_ID
        AND p.SKU       = 'STBP'
        AND p.IS_NFR    = FALSE
        AND p.TIER_MIN  = 1
    WHERE q.SIMULATION_AND_TRAINING_BUNDLE_PLUS = TRUE
      AND q.PLAN_NAME NOT IN ('Complete Protect', 'SAT Suite')
      AND q.is_nfr = FALSE
),

-- ============================================================
-- ACCOUNT TAKEOVER (flat rate)
-- ============================================================
ato AS (
    SELECT DISTINCT
        q.DATE_RECORDED,
        q.ltp,
        'Account Takeover'                          AS item,
        'IS-LTP-ATO'                                AS sku,
        FALSE                                       AS partner_pricing,
        q.quantity,
        q.quantity * p.PRICE                        AS amount
    FROM quantities q
    JOIN pricing p
        ON  q.ltp       = p.TENANT_GLOBAL_ID
        AND p.SKU       = 'ATO'
        AND p.IS_NFR    = FALSE
        AND p.TIER_MIN  = 1
    WHERE q.ATO = TRUE
      AND q.PLAN_NAME != 'Complete Protect'
      AND q.is_nfr = FALSE
),

-- ============================================================
-- DMARC (flat rate per domain, quantity from dmarc_hwm)
-- ============================================================
dmarc AS (
    SELECT
        q.DATE_RECORDED,
        q.ltp,
        CASE q.DMARC_IRONSCALES_PLAN
            WHEN 1 THEN 'DMARC Core Management'
            WHEN 2 THEN 'DMARC Pro'
            WHEN 3 THEN 'DMARC Premium'
        END                                         AS item,
        CASE q.DMARC_IRONSCALES_PLAN
            WHEN 1 THEN 'IS-LTP-DMARC'
            WHEN 2 THEN 'IS-LTP-DMARC_PRO'
            WHEN 3 THEN 'IS-LTP-DMARC_PREMIUM'
        END                                         AS sku,
        FALSE                                       AS partner_pricing,
        SUM(d.DMARC_DOMAINS_NUMBER)                 AS quantity,
        SUM(d.DMARC_DOMAINS_NUMBER) * p.PRICE       AS amount
    FROM quantities q
    JOIN dmarc_hwm d
        ON  q.ltp               = d.TENANT_GLOBAL_ID
    JOIN pricing p
        ON  q.ltp               = p.TENANT_GLOBAL_ID
        AND p.IS_NFR            = FALSE
        AND p.TIER_MIN          = 1
        AND p.SKU               = CASE q.DMARC_IRONSCALES_PLAN
                                    WHEN 1 THEN 'DMARC'
                                    WHEN 2 THEN 'DMARC_PRO'
                                    WHEN 3 THEN 'DMARC'
                                  END
    WHERE q.DMARC_IRONSCALES_PLAN IS NOT NULL
    GROUP BY
        q.DATE_RECORDED, q.ltp,
        q.DMARC_IRONSCALES_PLAN, p.PRICE
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