WITH

-- ============================================================
-- SOURCES
-- ============================================================
hwm AS (
    SELECT * FROM {{ ref('current_global_tenant_ltp_hwm') }}
),

dmarc_hwm AS (
    SELECT * FROM {{ ref('current_month_ltp_dmarc_domain_hwm') }}
),

meta AS (
    SELECT * FROM {{ ref('ltp_account_meta') }}
),

-- Deduplicate unpivot table
pricing AS (
    SELECT DISTINCT
        TENANT_GLOBAL_ID,
        SKU,
        IS_NFR,
        TIER_MIN,
        TIER_MAX,
        PRICE,
        SNAPSHOT_DATE
    FROM {{ ref('ltp_pricing_tbl_unpivot') }}
    WHERE SNAPSHOT_DATE = CURRENT_DATE()
),

-- ============================================================
-- BASE: aggregate HWM to ROOT/PLAN grain — used for PLANS only.
-- Excludes US-733, EU-25 (handled separately via pax8_* CTEs)
-- and other special accounts.
-- ============================================================
base AS (
    SELECT
        h.DATE_RECORDED,
        h.ROOT                                                                     AS ltp,
        h.PLAN_NAME,
        -- effective_partner_pricing: if NOT_NFR_PARTNER=TRUE, override to FALSE
        CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END AS PARTNER_PRICING,
        m.PROFILE_TYPE,
        CASE m.PROFILE_TYPE
            WHEN 'active'  THEN SUM(h.ACTIVE_PROFILES)
            WHEN 'license' THEN SUM(h.LICENSED_PROFILES)
            WHEN 'shared'  THEN COALESCE(
                                    SUM(h.ACTIVE_PROFILES) - SUM(h.SHARED_PROFILES),
                                    SUM(h.ACTIVE_PROFILES)
                                )
        END                                                                        AS quantity
    FROM hwm h
    JOIN meta m ON h.ROOT = m.TENANT_GLOBAL_ID
    WHERE
        h.APPROVED = TRUE
        AND h.BILLING_STATUS IN ('Active', 'Active-POC')
        AND m.PROFILE_TYPE IS NOT NULL
        AND h.LICENSED_PROFILES IS NOT NULL
        AND h.ROOT NOT IN ('US-733','EU-25')
    GROUP BY
        h.DATE_RECORDED, h.ROOT, h.PLAN_NAME,
        CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END, m.PROFILE_TYPE
),

-- ============================================================
-- BASE ADDONS: aggregate HWM to ROOT/PLAN/FLAG grain.
-- Used for IM, STBP, ATO, and PREMIUM.
-- Add-on flags and PREMIUM_NAME kept in GROUP BY — no MAX() —
-- so quantities are correct per flag combination with no bleeding.
-- DMARC_IRONSCALES_PLAN excluded from GROUP BY (handled in dmarc CTE).
-- US-733 and EU-25 included — billed for add-ons per account separately.
-- ============================================================
base_addons AS (
    SELECT
        h.DATE_RECORDED,
        h.ROOT                                                                     AS ltp,
        h.PLAN_NAME,
        -- effective_partner_pricing: if NOT_NFR_PARTNER=TRUE, override to FALSE
        CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END AS PARTNER_PRICING,
        h.INCIDENT_MANAGEMENT,
        h.SIMULATION_AND_TRAINING_BUNDLE_PLUS,
        h.ATO,
        h.PREMIUM_NAME,
        m.PROFILE_TYPE,
        CASE m.PROFILE_TYPE
            WHEN 'active'  THEN SUM(h.ACTIVE_PROFILES)
            WHEN 'license' THEN SUM(h.LICENSED_PROFILES)
            WHEN 'shared'  THEN COALESCE(
                                    SUM(h.ACTIVE_PROFILES) - SUM(h.SHARED_PROFILES),
                                    SUM(h.ACTIVE_PROFILES)
                                )
        END                                                                        AS quantity
    FROM hwm h
    JOIN meta m ON h.ROOT = m.TENANT_GLOBAL_ID
    WHERE
        h.APPROVED = TRUE
        AND h.BILLING_STATUS IN ('Active', 'Active-POC')
        AND m.PROFILE_TYPE IS NOT NULL
        AND h.LICENSED_PROFILES IS NOT NULL
    GROUP BY
        h.DATE_RECORDED, h.ROOT, h.PLAN_NAME,
        CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END,
        h.INCIDENT_MANAGEMENT, h.SIMULATION_AND_TRAINING_BUNDLE_PLUS,
        h.ATO, h.PREMIUM_NAME, m.PROFILE_TYPE
),

-- ============================================================
-- PAX8 POOLING: US-733 + EU-25 plans only
--
-- Step 1 (pax8_base):    individual quantities per account
-- Step 2 (pax8_combined): combined quantity across both accounts
-- Step 3 (pax8_tier_calc): tier waterfall once on combined quantity,
--                           priced off US-733 (canonical root)
-- Step 4 (pax8_plans):   spread total amount back proportionally
--                         amount = (account_qty / combined_qty) * total_tier_amount
--
-- Add-ons and DMARC: not billed for US-733 / EU-25.
-- ============================================================
pax8_base AS (
    SELECT
        h.DATE_RECORDED,
        h.ROOT                                                                     AS ltp,
        h.PLAN_NAME,
        -- effective_partner_pricing: if NOT_NFR_PARTNER=TRUE, override to FALSE
        CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END AS PARTNER_PRICING,
        m.PROFILE_TYPE,
        CASE m.PROFILE_TYPE
            WHEN 'active'  THEN SUM(h.ACTIVE_PROFILES)
            WHEN 'license' THEN SUM(h.LICENSED_PROFILES)
            WHEN 'shared'  THEN COALESCE(
                                    SUM(h.ACTIVE_PROFILES) - SUM(h.SHARED_PROFILES),
                                    SUM(h.ACTIVE_PROFILES)
                                )
        END                                                                        AS quantity
    FROM hwm h
    JOIN meta m ON h.ROOT = m.TENANT_GLOBAL_ID
    WHERE
        h.APPROVED = TRUE
        AND h.BILLING_STATUS IN ('Active', 'Active-POC')
        AND m.PROFILE_TYPE IS NOT NULL
        AND h.LICENSED_PROFILES IS NOT NULL
        AND h.ROOT IN ('US-733', 'EU-25')
    GROUP BY
        h.DATE_RECORDED, h.ROOT, h.PLAN_NAME,
        CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END, m.PROFILE_TYPE
),

pax8_combined AS (
    SELECT
        DATE_RECORDED,
        PLAN_NAME,
        PARTNER_PRICING,
        PROFILE_TYPE,
        SUM(quantity)                               AS combined_quantity
    FROM pax8_base
    WHERE PLAN_NAME IN (
        'Email Protect', 'Complete Protect', 'Core',
        'IRONSCALES Protect', 'SAT Suite', 'Starter'
    )
    GROUP BY DATE_RECORDED, PLAN_NAME, PARTNER_PRICING, PROFILE_TYPE
),

pax8_tier_calc AS (
    SELECT
        c.DATE_RECORDED,
        c.PLAN_NAME,
        c.PARTNER_PRICING,
        c.PROFILE_TYPE,
        c.combined_quantity,
        SUM(
            GREATEST(0, LEAST(c.combined_quantity, p.TIER_MAX) - (p.TIER_MIN - 1)) * p.PRICE
        )                                           AS total_tier_amount
    FROM pax8_combined c
    JOIN pricing p
        ON  p.TENANT_GLOBAL_ID = 'US-733'
        AND p.IS_NFR           = c.PARTNER_PRICING
        AND p.SKU              = CASE c.PLAN_NAME
                                    WHEN 'Email Protect'      THEN 'Email Protect'
                                    WHEN 'Complete Protect'   THEN 'Complete Protect'
                                    WHEN 'Core'               THEN 'CORE'
                                    WHEN 'IRONSCALES Protect' THEN 'IRONSCALES Protect'
                                    WHEN 'SAT Suite'          THEN 'SAT_SUITE'
                                    WHEN 'Starter'            THEN 'STARTER'
                                END
    GROUP BY
        c.DATE_RECORDED, c.PLAN_NAME, c.PARTNER_PRICING,
        c.PROFILE_TYPE, c.combined_quantity
),

pax8_plans AS (
    SELECT
        b.DATE_RECORDED,
        b.ltp,
        b.PLAN_NAME                                 AS item,
        CASE WHEN b.PARTNER_PRICING = FALSE THEN
            CASE b.PLAN_NAME
                WHEN 'Email Protect'      THEN 'IS-LTP-EP'
                WHEN 'Complete Protect'   THEN 'IS-LTP-CP'
                WHEN 'Core'               THEN 'IS-LTP-CORE'
                WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IP'
                WHEN 'SAT Suite'          THEN 'IS-SAT_SUITE_1'
                WHEN 'Starter'            THEN 'IS-LTP-STARTER'
            END
        ELSE
            CASE b.PLAN_NAME
                WHEN 'Email Protect'      THEN 'IS-LTP-EPNFR'
                WHEN 'Complete Protect'   THEN 'IS-LTP-CPNFR'
                WHEN 'Core'               THEN 'IS-LTP-CORENFR'
                WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IPNFR'
                WHEN 'SAT Suite'          THEN 'IS-SAT_SUITENFR_1'
                WHEN 'Starter'            THEN 'IS-LTP-STARTERNFR'
            END
        END                                         AS sku,
        b.PARTNER_PRICING                           AS partner_pricing,
        b.quantity,
        CASE
            WHEN t.combined_quantity = 0 OR t.combined_quantity IS NULL THEN 0
            ELSE (b.quantity / t.combined_quantity) * t.total_tier_amount
        END                                         AS amount
    FROM pax8_base b
    JOIN pax8_tier_calc t
        ON  b.DATE_RECORDED   = t.DATE_RECORDED
        AND b.PLAN_NAME       = t.PLAN_NAME
        AND b.PARTNER_PRICING = t.PARTNER_PRICING
        AND b.PROFILE_TYPE    = t.PROFILE_TYPE
    WHERE b.PLAN_NAME IN (
        'Email Protect', 'Complete Protect', 'Core',
        'IRONSCALES Protect', 'SAT Suite', 'Starter'
    )
),

----------------------------------------------------------------------------------------
-- PLANS (tier waterfall) — all accounts except US-733, EU-25
----------------------------------------------------------------------------------------
plans AS (
    SELECT
        b.DATE_RECORDED,
        b.ltp,
        b.PLAN_NAME                                 AS item,
        CASE WHEN b.PARTNER_PRICING = FALSE THEN
            CASE b.PLAN_NAME
                WHEN 'Email Protect'      THEN 'IS-LTP-EP'
                WHEN 'Complete Protect'   THEN 'IS-LTP-CP'
                WHEN 'Core'               THEN 'IS-LTP-CORE'
                WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IP'
                WHEN 'SAT Suite'          THEN 'IS-SAT_SUITE_1'
                WHEN 'Starter'            THEN 'IS-LTP-STARTER'
            END
        ELSE
            CASE b.PLAN_NAME
                WHEN 'Email Protect'      THEN 'IS-LTP-EPNFR'
                WHEN 'Complete Protect'   THEN 'IS-LTP-CPNFR'
                WHEN 'Core'               THEN 'IS-LTP-CORENFR'
                WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IPNFR'
                WHEN 'SAT Suite'          THEN 'IS-SAT_SUITENFR_1'
                WHEN 'Starter'            THEN 'IS-LTP-STARTERNFR'
            END
        END                                         AS sku,
        b.PARTNER_PRICING                           AS partner_pricing,
        b.quantity,
        SUM(
            GREATEST(0, LEAST(b.quantity, p.TIER_MAX) - (p.TIER_MIN - 1)) * p.PRICE
        )                                           AS amount
    FROM base b
    JOIN pricing p
        ON  b.ltp         = p.TENANT_GLOBAL_ID
        AND p.IS_NFR      = b.PARTNER_PRICING
        AND p.SKU         = CASE b.PLAN_NAME
                                WHEN 'Email Protect'      THEN 'Email Protect'
                                WHEN 'Complete Protect'   THEN 'Complete Protect'
                                WHEN 'Core'               THEN 'CORE'
                                WHEN 'IRONSCALES Protect' THEN 'IRONSCALES Protect'
                                WHEN 'SAT Suite'          THEN 'SAT_SUITE'
                                WHEN 'Starter'            THEN 'STARTER'
                            END
    WHERE b.PLAN_NAME IN (
        'Email Protect', 'Complete Protect', 'Core',
        'IRONSCALES Protect', 'SAT Suite', 'Starter'
    )
    GROUP BY
        b.DATE_RECORDED, b.ltp, b.PLAN_NAME,
        b.PARTNER_PRICING, b.PROFILE_TYPE, b.quantity
),

----------------------------------------------------------------------------------------
-- PREMIUM (flat rate): NINJIO, Cybermaniacs Videos, Habitu8
-- Uses base_addons — PREMIUM_NAME in GROUP BY so multiple premium
-- types per ROOT are each billed correctly.
----------------------------------------------------------------------------------------
premium AS (
    SELECT
        b.DATE_RECORDED,
        b.ltp,
        b.PREMIUM_NAME                              AS item,
        'IS-LTP-PSCP'                               AS sku,
        NULL                                        AS partner_pricing,
        SUM(b.quantity)                             AS quantity,
        SUM(b.quantity) * p.PRICE                   AS amount
    FROM base_addons b
    JOIN pricing p
        ON  b.ltp      = p.TENANT_GLOBAL_ID
        AND p.SKU      = 'PSCP'
        AND p.TIER_MIN = 1
    WHERE b.PREMIUM_NAME IN ('NINJIO', 'Cybermaniacs Videos', 'Habitu8')
    GROUP BY
        b.DATE_RECORDED, b.ltp, b.PREMIUM_NAME,
        b.PROFILE_TYPE, p.PRICE
),

----------------------------------------------------------------------------------------
-- INCIDENT MANAGEMENT (tiered)
-- Uses base_addons — IM flag in GROUP BY so only seats genuinely
-- flagged IM=TRUE are included. SUM across plans first, then tier waterfall.
----------------------------------------------------------------------------------------
incident_mgmt AS (
    WITH im_totals AS (
        SELECT
            DATE_RECORDED,
            ltp,
            PROFILE_TYPE,
            SUM(quantity)                           AS total_quantity
        FROM base_addons
        WHERE INCIDENT_MANAGEMENT = TRUE
        GROUP BY DATE_RECORDED, ltp, PROFILE_TYPE
    )
    SELECT
        t.DATE_RECORDED,
        t.ltp,
        'Incident Management'                       AS item,
        'IS-LTP-IM'                                 AS sku,
        NULL                                        AS partner_pricing,
        t.total_quantity                            AS quantity,
        SUM(
            GREATEST(0, LEAST(t.total_quantity, p.TIER_MAX) - (p.TIER_MIN - 1)) * p.PRICE
        )                                           AS amount
    FROM im_totals t
    JOIN pricing p
        ON  t.ltp  = p.TENANT_GLOBAL_ID
        AND p.SKU  = 'Incident Management'
    GROUP BY
        t.DATE_RECORDED, t.ltp, t.PROFILE_TYPE, t.total_quantity
),

----------------------------------------------------------------------------------------
-- S&T BUNDLE PLUS (flat rate)
-- Uses base_addons — STBP flag in GROUP BY so no bleeding from other plans.
-- Excludes CP and SAT Suite (bundled). Only non-NFR.
----------------------------------------------------------------------------------------
stbp AS (
    SELECT
        b.DATE_RECORDED,
        b.ltp,
        'S&T Bundle Plus'                           AS item,
        'IS-LTP-STBP'                               AS sku,
        NULL                                        AS partner_pricing,
        SUM(b.quantity)                             AS quantity,
        SUM(b.quantity) * p.PRICE                   AS amount
    FROM base_addons b
    JOIN pricing p
        ON  b.ltp      = p.TENANT_GLOBAL_ID
        AND p.SKU      = 'STBP'
        AND p.TIER_MIN = 1
    WHERE b.SIMULATION_AND_TRAINING_BUNDLE_PLUS = TRUE
      AND b.PLAN_NAME NOT IN ('Complete Protect', 'SAT Suite')
      AND b.PARTNER_PRICING = FALSE
    GROUP BY
        b.DATE_RECORDED, b.ltp, b.PROFILE_TYPE, p.PRICE
),

----------------------------------------------------------------------------------------
-- ACCOUNT TAKEOVER (flat rate)
-- Uses base_addons — ATO flag in GROUP BY so no bleeding from other plans.
-- Excludes CP (bundled). Only non-NFR.
----------------------------------------------------------------------------------------
ato AS (
    SELECT
        b.DATE_RECORDED,
        b.ltp,
        'Account Takeover'                          AS item,
        'IS-LTP-ATO'                                AS sku,
        NULL                                        AS partner_pricing,
        SUM(b.quantity)                             AS quantity,
        SUM(b.quantity) * p.PRICE                   AS amount
    FROM base_addons b
    JOIN pricing p
        ON  b.ltp      = p.TENANT_GLOBAL_ID
        AND p.SKU      = 'ATO'
        AND p.TIER_MIN = 1
    WHERE b.ATO = TRUE
      AND b.PLAN_NAME != 'Complete Protect'
      AND b.PARTNER_PRICING = FALSE
    GROUP BY
        b.DATE_RECORDED, b.ltp, b.PROFILE_TYPE, p.PRICE
),

----------------------------------------------------------------------------------------
-- DMARC (flat rate per domain)
-- Joins hwm directly — uses domain count not seat count.
-- US-733 and EU-25 included — billed for DMARC per account separately.
----------------------------------------------------------------------------------------
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
        ON  h.ROOT     = p.TENANT_GLOBAL_ID
        AND p.TIER_MIN = 1
        AND p.SKU      = CASE h.DMARC_IRONSCALES_PLAN
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
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount FROM pax8_plans
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
