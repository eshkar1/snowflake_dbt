WITH

-- ============================================================
-- SOURCES
-- ============================================================
hwm AS (
    SELECT * FROM {{ ref('current_global_tenant_ltp_hwm') }}
),

dmarc_hwm AS (
    SELECT * FROM
    {{ ref('global_tenant_history_sltp_daily_billing_DMARC_tbl') }}
),

meta AS (
    SELECT * FROM {{ ref('ltp_account_meta') }}
),

-- Deduplicate unpivot table — includes START_DATE, END_DATE, PRICE_TYPE
pricing AS (
    SELECT DISTINCT
        TENANT_GLOBAL_ID,
        SKU,
        IS_NFR,
        TIER_MIN,
        TIER_MAX,
        PRICE,
        START_DATE,
        END_DATE,
        PRICE_TYPE
    FROM {{ ref('ltp_pricing_tbl_unpivot') }}
),

-- ============================================================
-- PRICING RESOLVED: apply override priority logic
-- Priority 1: Exceptional Price (manual_excep_tbl, tenant-level)
-- Priority 2: Promo Price
-- Priority 3: Standard (fallback)
-- Note: Exceptional pricing for sub-tenants is handled via
-- exceptional_base/exceptional_plans CTEs below, NOT here.
-- pricing_resolved is used only for LTP-level pricing.
-- ============================================================
pricing_resolved AS (
    SELECT
        TENANT_GLOBAL_ID,
        SKU,
        IS_NFR,
        TIER_MIN,
        TIER_MAX,
        PRICE,
        PRICE_TYPE
    FROM (
        SELECT
            p.*,
            MAX(
                CASE
                    WHEN p.PRICE_TYPE = 'Promo Price'
                     AND CURRENT_DATE() BETWEEN p.START_DATE AND p.END_DATE
                    THEN 1 ELSE 0
                END
            ) OVER (PARTITION BY p.TENANT_GLOBAL_ID, p.SKU, p.IS_NFR) AS has_active_promo
        FROM pricing p
    ) t
    WHERE
        (has_active_promo = 1
            AND PRICE_TYPE = 'Promo Price'
            AND CURRENT_DATE() BETWEEN START_DATE AND END_DATE)
        OR
        (has_active_promo = 0
            AND PRICE_TYPE != 'Promo Price')
),

-- ============================================================
-- UNIFIED: map every active tenant to its master_tenant_id.
-- Tenants not in ltp_unified_tbl are treated as self-master.
-- ============================================================
unified AS (
    SELECT
        m.TENANT_GLOBAL_ID                                          AS global_tenant_id,
        COALESCE(u.MASTER_TENANT_ID, m.TENANT_GLOBAL_ID)           AS master_tenant_id
    FROM meta m
    LEFT JOIN {{ ref('ltp_unified_tbl') }} u
        ON m.TENANT_GLOBAL_ID = u.GLOBAL_TENANT_ID
),

-- ============================================================
-- EXCEPTIONAL RULES: load all active Tenant-level exceptions.
-- Each row = one sub-tenant with a specific rate override.
-- TENANT_ID  = sub-tenant identified by TENANT_GLOBAL_ID in HWM
-- PARENT_ID  = the LTP (ROOT in HWM)
-- Scalable: any number of sub-tenants across any number of LTPs.
-- ============================================================
excep_rules AS (
    SELECT
        TENANT_ID                                                   AS sub_tenant_id,
        PARENT_ID                                                   AS ltp_id,
        ITEM                                                        AS plan_name,
        PARTNER_PRICING                                             AS is_nfr,
        TIER_MIN,
        TIER_MAX,
        TIER_RATE,
        START_DATE,
        END_DATE
    FROM {{ ref('manual_excep_tbl') }}
    WHERE TENANT_MSP = 'Tenant'
      AND CURRENT_DATE() BETWEEN START_DATE AND END_DATE
),

-- ============================================================
-- EXCEPTIONAL BASE: seats belonging to exceptional sub-tenants.
-- Joins HWM to excep_rules on sub_tenant_id + ltp_id + plan + nfr.
-- Kept at sub_tenant_id grain so each sub-tenant's tier waterfall
-- is calculated independently — critical for scalability.
-- These rows are EXCLUDED from base below to prevent double-counting.
-- ============================================================
exceptional_base AS (
    SELECT
        h.DATE_RECORDED,
        h.ROOT                                                      AS ltp,
        h.TENANT_GLOBAL_ID                                          AS sub_tenant_id,
        h.PLAN_NAME,
        CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END AS PARTNER_PRICING,
        m.PROFILE_TYPE,
        e.TIER_MIN,
        e.TIER_MAX,
        e.TIER_RATE,
        CASE m.PROFILE_TYPE
            WHEN 'active'  THEN SUM(h.ACTIVE_PROFILES)
            WHEN 'license' THEN SUM(h.LICENSED_PROFILES)
            WHEN 'shared'  THEN COALESCE(
                                    SUM(h.ACTIVE_PROFILES) - SUM(h.SHARED_PROFILES),
                                    SUM(h.ACTIVE_PROFILES)
                                )
        END                                                         AS quantity
    FROM hwm h
    JOIN meta m ON h.ROOT = m.TENANT_GLOBAL_ID
    JOIN excep_rules e
        ON  h.TENANT_GLOBAL_ID = e.sub_tenant_id
        AND h.ROOT             = e.ltp_id
        AND h.PLAN_NAME        = e.plan_name
        AND CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END = e.is_nfr
    WHERE
        h.APPROVED = TRUE
        AND h.BILLING_STATUS IN ('Active', 'Active-POC')
        AND m.PROFILE_TYPE IS NOT NULL
    GROUP BY
        h.DATE_RECORDED, h.ROOT, h.TENANT_GLOBAL_ID, h.PLAN_NAME,
        CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END,
        m.PROFILE_TYPE, e.TIER_MIN, e.TIER_MAX, e.TIER_RATE
),

-- ============================================================
-- EXCEPTIONAL PLANS: tier waterfall per sub-tenant.
-- Grouped at sub_tenant_id + TIER_RATE grain so multiple sub-tenants
-- under the same LTP with different rates are each calculated
-- independently. Output ltp = the LTP (ROOT), not the sub-tenant.
-- ============================================================
exceptional_plans AS (
    SELECT
        DATE_RECORDED,
        ltp,
        sub_tenant_id,
        PLAN_NAME                                                   AS item,
        CASE WHEN PARTNER_PRICING = FALSE THEN
            CASE PLAN_NAME
                WHEN 'Email Protect'                    THEN 'IS-LTP-EP'
                WHEN 'Complete Protect'                 THEN 'IS-LTP-CP'
                WHEN 'Core'                             THEN 'IS-LTP-CORE'
                WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IP'
                WHEN 'SAT Suite'                        THEN 'IS-SAT_SUITE'
                WHEN 'Starter'                          THEN 'IS-LTP-STARTER'
                WHEN 'Phishing Simulation and Training' THEN 'IS-LTP-PST'
            END
        ELSE
            CASE PLAN_NAME
                WHEN 'Email Protect'                    THEN 'IS-LTP-EPNFR'
                WHEN 'Complete Protect'                 THEN 'IS-LTP-CPNFR'
                WHEN 'Core'                             THEN 'IS-LTP-CORENFR'
                WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IPNFR'
                WHEN 'SAT Suite'                        THEN 'IS-SAT_SUITENFR'
                WHEN 'Starter'                          THEN 'IS-LTP-STARTERNFR'
            END
        END                                                         AS sku,
        PARTNER_PRICING                                             AS partner_pricing,
        SUM(quantity)                                               AS quantity,
        SUM(
            GREATEST(0, LEAST(quantity, TIER_MAX) - (TIER_MIN - 1)) * TIER_RATE
        )                                                           AS amount,
        'Exceptional Price'                                         AS price_type
    FROM exceptional_base
    GROUP BY
        DATE_RECORDED, ltp, sub_tenant_id, PLAN_NAME,
        PARTNER_PRICING, PROFILE_TYPE, TIER_MIN, TIER_MAX, TIER_RATE
),

-- ============================================================
-- BASE: aggregate HWM to MASTER_TENANT_ID/PLAN grain.
-- All tenants included — non-unified tenants are their own master.
-- Both master_tenant_id (for pricing/tier calc) and
-- global_tenant_id (for final output spread) are carried through.
-- EXCLUDES rows belonging to exceptional sub-tenants via NOT EXISTS
-- to prevent double-counting with exceptional_plans above.
-- ============================================================
base AS (
    SELECT
        h.DATE_RECORDED,
        u.master_tenant_id                                          AS ltp,
        h.ROOT                                                      AS global_tenant_id,
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
        END                                                         AS quantity
    FROM hwm h
    JOIN meta m ON h.ROOT = m.TENANT_GLOBAL_ID
    JOIN unified u ON h.ROOT = u.global_tenant_id
    WHERE
        h.APPROVED = TRUE
        AND h.BILLING_STATUS IN ('Active', 'Active-POC')
        AND m.PROFILE_TYPE IS NOT NULL
        -- EXCLUDE sub-tenants with an active exceptional rule
        AND NOT EXISTS (
            SELECT 1 FROM excep_rules e
            WHERE e.sub_tenant_id = h.TENANT_GLOBAL_ID
              AND e.ltp_id        = h.ROOT
              AND e.plan_name     = h.PLAN_NAME
              AND e.is_nfr        = CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END
        )
    GROUP BY
        h.DATE_RECORDED, u.master_tenant_id, h.ROOT, h.PLAN_NAME,
        CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END,
        m.PROFILE_TYPE
),

-- ============================================================
-- BASE ADDONS: same as base but includes addon flags.
-- Used for IM, STBP, ATO, and PREMIUM.
-- Same NOT EXISTS exclusion as base — exceptional sub-tenant
-- addon seats are excluded to avoid double-counting.
-- ============================================================
base_addons AS (
    SELECT
        h.DATE_RECORDED,
        u.master_tenant_id                                          AS ltp,
        h.ROOT                                                      AS global_tenant_id,
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
        END                                                         AS quantity
    FROM hwm h
    JOIN meta m ON h.ROOT = m.TENANT_GLOBAL_ID
    JOIN unified u ON h.ROOT = u.global_tenant_id
    WHERE
        h.APPROVED = TRUE
        AND h.BILLING_STATUS IN ('Active', 'Active-POC')
        AND m.PROFILE_TYPE IS NOT NULL
        -- EXCLUDE sub-tenants with an active exceptional rule
        AND NOT EXISTS (
            SELECT 1 FROM excep_rules e
            WHERE e.sub_tenant_id = h.TENANT_GLOBAL_ID
              AND e.ltp_id        = h.ROOT
              AND e.plan_name     = h.PLAN_NAME
              AND e.is_nfr        = CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END
        )
    GROUP BY
        h.DATE_RECORDED, u.master_tenant_id, h.ROOT, h.PLAN_NAME,
        CASE WHEN h.NOT_NFR_PARTNER = TRUE THEN FALSE ELSE h.PARTNER_PRICING END,
        h.INCIDENT_MANAGEMENT, h.SIMULATION_AND_TRAINING_BUNDLE_PLUS,
        h.ATO, h.PREMIUM_NAME, m.PROFILE_TYPE
),

-- ============================================================
-- MASTER BASE: combined quantity per master_tenant_id/plan grain.
-- Used as input to tier waterfall calculation.
-- ============================================================
master_base AS (
    SELECT
        DATE_RECORDED,
        ltp                                                         AS master_tenant_id,
        PLAN_NAME,
        PARTNER_PRICING,
        PROFILE_TYPE,
        SUM(quantity)                                               AS combined_quantity
    FROM base
    GROUP BY DATE_RECORDED, ltp, PLAN_NAME, PARTNER_PRICING, PROFILE_TYPE
),

-- ============================================================
-- MASTER TIER CALC: tier waterfall on combined quantity
-- using master tenant's pricing (via pricing_resolved).
-- ============================================================
master_tier_calc AS (
    SELECT
        c.DATE_RECORDED,
        c.master_tenant_id,
        c.PLAN_NAME,
        c.PARTNER_PRICING,
        c.PROFILE_TYPE,
        c.combined_quantity,
        SUM(
            GREATEST(0, LEAST(c.combined_quantity, p.TIER_MAX) - (p.TIER_MIN - 1)) * p.PRICE
        )                                                           AS total_tier_amount,
        MAX(p.PRICE_TYPE)                                           AS price_type
    FROM master_base c
    JOIN pricing_resolved p
        ON  p.TENANT_GLOBAL_ID = c.master_tenant_id
        AND p.IS_NFR           = c.PARTNER_PRICING
        AND p.SKU              = CASE c.PLAN_NAME
                                    WHEN 'Email Protect'                    THEN 'Email Protect'
                                    WHEN 'Complete Protect'                 THEN 'Complete Protect'
                                    WHEN 'Core'                             THEN 'CORE'
                                    WHEN 'IRONSCALES Protect'               THEN 'IRONSCALES Protect'
                                    WHEN 'SAT Suite'                        THEN 'SAT_SUITE'
                                    WHEN 'Starter'                          THEN 'STARTER'
                                    WHEN 'Phishing Simulation and Training' THEN 'PST'
                                END
    WHERE c.PLAN_NAME IN (
        'Email Protect', 'Complete Protect', 'Core',
        'IRONSCALES Protect', 'SAT Suite', 'Starter',
        'Phishing Simulation and Training'
    )
    GROUP BY
        c.DATE_RECORDED, c.master_tenant_id, c.PLAN_NAME,
        c.PARTNER_PRICING, c.PROFILE_TYPE, c.combined_quantity
),

-- ============================================================
-- PLANS: spread total_tier_amount back to each global_tenant_id
-- proportionally by their individual quantity share.
-- Standard seats only — exceptional seats handled separately.
-- ============================================================
plans AS (
    SELECT
        b.DATE_RECORDED,
        b.global_tenant_id                                          AS ltp,
        b.PLAN_NAME                                                 AS item,
        CASE WHEN b.PARTNER_PRICING = FALSE THEN
            CASE b.PLAN_NAME
                WHEN 'Email Protect'                    THEN 'IS-LTP-EP'
                WHEN 'Complete Protect'                 THEN 'IS-LTP-CP'
                WHEN 'Core'                             THEN 'IS-LTP-CORE'
                WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IP'
                WHEN 'SAT Suite'                        THEN 'IS-SAT_SUITE'
                WHEN 'Starter'                          THEN 'IS-LTP-STARTER'
                WHEN 'Phishing Simulation and Training' THEN 'IS-LTP-PST'
            END
        ELSE
            CASE b.PLAN_NAME
                WHEN 'Email Protect'                    THEN 'IS-LTP-EPNFR'
                WHEN 'Complete Protect'                 THEN 'IS-LTP-CPNFR'
                WHEN 'Core'                             THEN 'IS-LTP-CORENFR'
                WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IPNFR'
                WHEN 'SAT Suite'                        THEN 'IS-SAT_SUITENFR'
                WHEN 'Starter'                          THEN 'IS-LTP-STARTERNFR'
            END
        END                                                         AS sku,
        b.PARTNER_PRICING                                           AS partner_pricing,
        b.quantity,
        CASE
            WHEN t.combined_quantity = 0 OR t.combined_quantity IS NULL THEN 0
            ELSE (b.quantity / t.combined_quantity) * t.total_tier_amount
        END                                                         AS amount,
        t.price_type
    FROM base b
    JOIN master_tier_calc t
        ON  b.DATE_RECORDED   = t.DATE_RECORDED
        AND b.ltp             = t.master_tenant_id
        AND b.PLAN_NAME       = t.PLAN_NAME
        AND b.PARTNER_PRICING = t.PARTNER_PRICING
        AND b.PROFILE_TYPE    = t.PROFILE_TYPE
),

-- ============================================================
-- PREMIUM (flat rate): NINJIO, Cybermaniacs Videos, Habitu8
-- Combined at master level then spread back by each tenant's quantity.
-- ============================================================
premium_master AS (
    SELECT
        b.DATE_RECORDED,
        b.ltp                                                       AS master_tenant_id,
        b.PREMIUM_NAME,
        b.PROFILE_TYPE,
        SUM(b.quantity)                                             AS combined_quantity,
        SUM(b.quantity) * MAX(p.PRICE)                              AS total_amount,
        MAX(p.PRICE_TYPE)                                           AS price_type
    FROM base_addons b
    JOIN pricing_resolved p
        ON  b.ltp      = p.TENANT_GLOBAL_ID
        AND p.SKU      = 'PSCP'
        AND p.TIER_MIN = 1
    WHERE b.PREMIUM_NAME IN ('NINJIO', 'Cybermaniacs Videos', 'Habitu8')
    GROUP BY b.DATE_RECORDED, b.ltp, b.PREMIUM_NAME, b.PROFILE_TYPE
),

premium AS (
    SELECT
        b.DATE_RECORDED,
        b.global_tenant_id                                          AS ltp,
        b.PREMIUM_NAME                                              AS item,
        'IS-LTP-PSCP'                                               AS sku,
        NULL                                                        AS partner_pricing,
        SUM(b.quantity)                                             AS quantity,
        CASE
            WHEN pm.combined_quantity = 0 OR pm.combined_quantity IS NULL THEN 0
            ELSE (SUM(b.quantity) / pm.combined_quantity) * pm.total_amount
        END                                                         AS amount,
        pm.price_type
    FROM base_addons b
    JOIN premium_master pm
        ON  b.DATE_RECORDED  = pm.DATE_RECORDED
        AND b.ltp            = pm.master_tenant_id
        AND b.PREMIUM_NAME   = pm.PREMIUM_NAME
        AND b.PROFILE_TYPE   = pm.PROFILE_TYPE
    WHERE b.PREMIUM_NAME IN ('NINJIO', 'Cybermaniacs Videos', 'Habitu8')
    GROUP BY
        b.DATE_RECORDED, b.global_tenant_id, b.PREMIUM_NAME,
        b.PROFILE_TYPE, pm.combined_quantity, pm.total_amount, pm.price_type
),

-- ============================================================
-- INCIDENT MANAGEMENT (tiered)
-- Combined at master level using IM-flagged quantity,
-- tier waterfall on combined, spread back by each tenant's IM quantity.
-- ============================================================
im_master AS (
    SELECT
        DATE_RECORDED,
        ltp                                                         AS master_tenant_id,
        global_tenant_id,
        PROFILE_TYPE,
        SUM(quantity)                                               AS tenant_im_quantity
    FROM base_addons
    WHERE INCIDENT_MANAGEMENT = TRUE
    GROUP BY DATE_RECORDED, ltp, global_tenant_id, PROFILE_TYPE
),

im_combined AS (
    SELECT
        DATE_RECORDED,
        master_tenant_id,
        PROFILE_TYPE,
        SUM(tenant_im_quantity)                                     AS combined_quantity
    FROM im_master
    GROUP BY DATE_RECORDED, master_tenant_id, PROFILE_TYPE
),

im_tier_calc AS (
    SELECT
        c.DATE_RECORDED,
        c.master_tenant_id,
        c.PROFILE_TYPE,
        c.combined_quantity,
        SUM(
            GREATEST(0, LEAST(c.combined_quantity, p.TIER_MAX) - (p.TIER_MIN - 1)) * p.PRICE
        )                                                           AS total_tier_amount,
        MAX(p.PRICE_TYPE)                                           AS price_type
    FROM im_combined c
    JOIN pricing_resolved p
        ON  c.master_tenant_id = p.TENANT_GLOBAL_ID
        AND p.SKU              = 'Incident Management'
    GROUP BY c.DATE_RECORDED, c.master_tenant_id, c.PROFILE_TYPE, c.combined_quantity
),

incident_mgmt AS (
    SELECT
        im.DATE_RECORDED,
        im.global_tenant_id                                         AS ltp,
        'Incident Management'                                       AS item,
        'IS-LTP-IM'                                                 AS sku,
        NULL                                                        AS partner_pricing,
        im.tenant_im_quantity                                       AS quantity,
        CASE
            WHEN t.combined_quantity = 0 OR t.combined_quantity IS NULL THEN 0
            ELSE (im.tenant_im_quantity / t.combined_quantity) * t.total_tier_amount
        END                                                         AS amount,
        t.price_type
    FROM im_master im
    JOIN im_tier_calc t
        ON  im.DATE_RECORDED    = t.DATE_RECORDED
        AND im.master_tenant_id = t.master_tenant_id
        AND im.PROFILE_TYPE     = t.PROFILE_TYPE
),

-- ============================================================
-- S&T BUNDLE PLUS (flat rate)
-- Combined at master level using STBP-flagged quantity,
-- spread back by each tenant's STBP quantity.
-- Excludes CP and SAT Suite (bundled). Only non-NFR.
-- ============================================================
stbp_master AS (
    SELECT
        b.DATE_RECORDED,
        b.ltp                                                       AS master_tenant_id,
        b.global_tenant_id,
        b.PROFILE_TYPE,
        SUM(b.quantity)                                             AS tenant_stbp_quantity,
        MAX(p.PRICE)                                                AS price,
        MAX(p.PRICE_TYPE)                                           AS price_type
    FROM base_addons b
    JOIN pricing_resolved p
        ON  b.ltp      = p.TENANT_GLOBAL_ID
        AND p.SKU      = 'STBP'
        AND p.TIER_MIN = 1
    WHERE b.SIMULATION_AND_TRAINING_BUNDLE_PLUS = TRUE
      AND b.PLAN_NAME NOT IN ('Complete Protect', 'SAT Suite')
      AND b.PARTNER_PRICING = FALSE
    GROUP BY b.DATE_RECORDED, b.ltp, b.global_tenant_id, b.PROFILE_TYPE
),

stbp_combined AS (
    SELECT
        DATE_RECORDED,
        master_tenant_id,
        PROFILE_TYPE,
        SUM(tenant_stbp_quantity)                                   AS combined_quantity,
        MAX(price)                                                  AS price,
        MAX(price_type)                                             AS price_type
    FROM stbp_master
    GROUP BY DATE_RECORDED, master_tenant_id, PROFILE_TYPE
),

stbp AS (
    SELECT
        s.DATE_RECORDED,
        s.global_tenant_id                                          AS ltp,
        'S&T Bundle Plus'                                           AS item,
        'IS-LTP-STBP'                                               AS sku,
        NULL                                                        AS partner_pricing,
        s.tenant_stbp_quantity                                      AS quantity,
        CASE
            WHEN c.combined_quantity = 0 OR c.combined_quantity IS NULL THEN 0
            ELSE (s.tenant_stbp_quantity / c.combined_quantity) * (c.combined_quantity * c.price)
        END                                                         AS amount,
        c.price_type
    FROM stbp_master s
    JOIN stbp_combined c
        ON  s.DATE_RECORDED    = c.DATE_RECORDED
        AND s.master_tenant_id = c.master_tenant_id
        AND s.PROFILE_TYPE     = c.PROFILE_TYPE
),

-- ============================================================
-- ACCOUNT TAKEOVER (flat rate)
-- Combined at master level using ATO-flagged quantity,
-- spread back by each tenant's ATO quantity.
-- Excludes CP (bundled). Only non-NFR.
-- ============================================================
ato_master AS (
    SELECT
        b.DATE_RECORDED,
        b.ltp                                                       AS master_tenant_id,
        b.global_tenant_id,
        b.PROFILE_TYPE,
        SUM(b.quantity)                                             AS tenant_ato_quantity,
        MAX(p.PRICE)                                                AS price,
        MAX(p.PRICE_TYPE)                                           AS price_type
    FROM base_addons b
    JOIN pricing_resolved p
        ON  b.ltp      = p.TENANT_GLOBAL_ID
        AND p.SKU      = 'ATO'
        AND p.TIER_MIN = 1
    WHERE b.ATO = TRUE
      AND b.PLAN_NAME != 'Complete Protect'
      AND b.PARTNER_PRICING = FALSE
    GROUP BY b.DATE_RECORDED, b.ltp, b.global_tenant_id, b.PROFILE_TYPE
),

ato_combined AS (
    SELECT
        DATE_RECORDED,
        master_tenant_id,
        PROFILE_TYPE,
        SUM(tenant_ato_quantity)                                    AS combined_quantity,
        MAX(price)                                                  AS price,
        MAX(price_type)                                             AS price_type
    FROM ato_master
    GROUP BY DATE_RECORDED, master_tenant_id, PROFILE_TYPE
),

ato AS (
    SELECT
        a.DATE_RECORDED,
        a.global_tenant_id                                          AS ltp,
        'Account Takeover'                                          AS item,
        'IS-LTP-ATO'                                                AS sku,
        NULL                                                        AS partner_pricing,
        a.tenant_ato_quantity                                       AS quantity,
        CASE
            WHEN c.combined_quantity = 0 OR c.combined_quantity IS NULL THEN 0
            ELSE (a.tenant_ato_quantity / c.combined_quantity) * (c.combined_quantity * c.price)
        END                                                         AS amount,
        c.price_type
    FROM ato_master a
    JOIN ato_combined c
        ON  a.DATE_RECORDED    = c.DATE_RECORDED
        AND a.master_tenant_id = c.master_tenant_id
        AND a.PROFILE_TYPE     = c.PROFILE_TYPE
),

-- ============================================================
-- DMARC (flat rate per domain)
-- Combined at master level using domain count,
-- spread back by each tenant's own domain count.
-- ============================================================
dmarc_base AS (
    SELECT
        g.date_recorded,
        u.master_tenant_id,
        g.root                                                      AS global_tenant_id,
        g.dmarc_ironscales_plan,
        SUM(g.dmarc_domains_number)                                 AS tenant_domains
    FROM dmarc_hwm g
    JOIN unified u ON g.root = u.global_tenant_id
    GROUP BY g.date_recorded, u.master_tenant_id, g.root, g.dmarc_ironscales_plan
    HAVING SUM(g.dmarc_domains_number) > 0
),

dmarc_combined AS (
    SELECT
        date_recorded,
        master_tenant_id,
        dmarc_ironscales_plan,
        SUM(tenant_domains)                                         AS combined_domains
    FROM dmarc_base
    GROUP BY date_recorded, master_tenant_id, dmarc_ironscales_plan
),

dmarc_tier_calc AS (
    SELECT
        c.date_recorded,
        c.master_tenant_id,
        c.dmarc_ironscales_plan,
        c.combined_domains,
        c.combined_domains * p.PRICE                                AS total_amount,
        MAX(p.PRICE_TYPE)                                           AS price_type
    FROM dmarc_combined c
    JOIN pricing_resolved p
        ON  c.master_tenant_id = p.TENANT_GLOBAL_ID
        AND p.TIER_MIN         = 1
        AND p.SKU              = CASE c.dmarc_ironscales_plan
                                    WHEN 1 THEN 'DMARC'
                                    WHEN 2 THEN 'DMARC_PRO'
                                    WHEN 3 THEN 'DMARC'
                                 END
    GROUP BY c.date_recorded, c.master_tenant_id, c.dmarc_ironscales_plan,
             c.combined_domains, p.PRICE
),

dmarc AS (
    SELECT
        d.date_recorded,
        d.global_tenant_id                                          AS ltp,
        CASE d.dmarc_ironscales_plan
            WHEN 1 THEN 'DMARC Core Management'
            WHEN 2 THEN 'DMARC Pro'
            WHEN 3 THEN 'DMARC Premium'
        END                                                         AS item,
        CASE d.dmarc_ironscales_plan
            WHEN 1 THEN 'IS-LTP-DMARC'
            WHEN 2 THEN 'IS-LTP-DMARC_PRO'
            WHEN 3 THEN 'IS-LTP-DMARC_PREMIUM'
        END                                                         AS sku,
        NULL                                                        AS partner_pricing,
        d.tenant_domains                                            AS quantity,
        CASE
            WHEN t.combined_domains = 0 OR t.combined_domains IS NULL THEN 0
            ELSE (d.tenant_domains / t.combined_domains) * t.total_amount
        END                                                         AS amount,
        t.price_type
    FROM dmarc_base d
    JOIN dmarc_tier_calc t
        ON  d.date_recorded         = t.date_recorded
        AND d.master_tenant_id      = t.master_tenant_id
        AND d.dmarc_ironscales_plan = t.dmarc_ironscales_plan
)

-- ============================================================
-- FINAL OUTPUT
-- plans        — standard tier waterfall, all tenants except exceptional sub-tenants
-- exceptional_plans — exceptional sub-tenant seats at their specific rate
-- premium      — flat rate addons
-- incident_mgmt — tiered addon
-- stbp         — flat rate addon
-- ato          — flat rate addon
-- dmarc        — flat rate per domain
-- ============================================================
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount,
       CASE WHEN price_type = 'Promo Price'       THEN 'Promo'
            WHEN price_type = 'Exceptional Price' THEN 'Exceptional'
            ELSE 'Standard'
       END AS price_type
FROM plans
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount,
       'Exceptional'   AS price_type
FROM exceptional_plans
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount,
       CASE WHEN price_type = 'Promo Price'       THEN 'Promo'
            WHEN price_type = 'Exceptional Price' THEN 'Exceptional'
            ELSE 'Standard'
       END AS price_type
FROM premium
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount,
       CASE WHEN price_type = 'Promo Price'       THEN 'Promo'
            WHEN price_type = 'Exceptional Price' THEN 'Exceptional'
            ELSE 'Standard'
       END AS price_type
FROM incident_mgmt
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount,
       CASE WHEN price_type = 'Promo Price'       THEN 'Promo'
            WHEN price_type = 'Exceptional Price' THEN 'Exceptional'
            ELSE 'Standard'
       END AS price_type
FROM stbp
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount,
       CASE WHEN price_type = 'Promo Price'       THEN 'Promo'
            WHEN price_type = 'Exceptional Price' THEN 'Exceptional'
            ELSE 'Standard'
       END AS price_type
FROM ato
UNION ALL
SELECT DATE_RECORDED, ltp, item, sku, partner_pricing, quantity, amount,
       CASE WHEN price_type = 'Promo Price'       THEN 'Promo'
            WHEN price_type = 'Exceptional Price' THEN 'Exceptional'
            ELSE 'Standard'
       END AS price_type
FROM dmarc