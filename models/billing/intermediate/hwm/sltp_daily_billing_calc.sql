WITH

-- ============================================================
-- SOURCES
-- ============================================================
sltp AS (
    SELECT * FROM {{ ref('current_global_tenant_sltp_hwm_by_layer') }}
),

sltp_dmarc AS (
    SELECT * FROM {{ ref('current_global_tenant_sltp_hwm_by_layer_DMARC') }}
),

-- Blended per-unit rate for non-NFR plans, pre-calculated at LTP level
itemized AS (
    SELECT * FROM {{ ref('ltp_daily_itemized_new_billing_tbl') }}
    WHERE billing_date = current_date
),

meta AS (
    SELECT * FROM {{ ref('ltp_account_meta') }}
),

-- Deduplicated unpivot table — used for NFR rates and add-on flat rates
pricing AS (
    SELECT DISTINCT
        tenant_global_id,
        sku,
        is_nfr,
        tier_min,
        tier_max,
        price,
        snapshot_date
    FROM {{ ref('ltp_pricing_tbl_unpivot') }}
    WHERE snapshot_date = current_date
),

dmarc_hwm AS (
    SELECT * FROM {{ ref('current_month_ltp_dmarc_domain_hwm') }}
),

-- ============================================================
-- BASE: one row per tenant per plan — no aggregation needed
-- as current_global_tenant_sltp_by_layer is already at tenant grain.
-- FIRST_LAYER_ID = LTP root; billing quantity derived from profile_type.
-- All LTPs go through this unified model (no exclusions).
-- ============================================================
base AS (
    SELECT
        g.date_recorded                                                         AS billing_date,
        g.first_layer_id,
        g.second_layer_id,
        g.third_layer_id,
        g.fourth_layer_id,
        g.fifth_layer_id,
        g.plan_name,
        g.premium_name,
        CASE WHEN g.not_nfr_partner = TRUE THEN FALSE ELSE g.partner_pricing END AS partner_pricing,
        g.incident_management,
        g.simulation_and_training_bundle_plus,
        g.ato,
        g.billing_status,
        g.approved,
        m.profile_type,
        CASE m.profile_type
            WHEN 'active'  THEN g.active_profiles
            WHEN 'license' THEN g.licensed_profiles
            WHEN 'shared'  THEN COALESCE(
                                    g.active_profiles - g.shared_profiles,
                                    g.active_profiles
                                )
        END                                                                     AS billable_quantity
    FROM sltp g
    LEFT JOIN meta m ON g.first_layer_id = m.tenant_global_id
    WHERE
        g.approved = TRUE
        AND g.billing_status IN ('Active', 'Active-POC')
        AND m.profile_type IS NOT NULL
),

-- ============================================================
-- PLANS (non-NFR): blended rate from itemized billing table
-- amount = billable_quantity * (i.amount / i.quantity)
-- price_type: normalized from itemized — handles NULL and empty string.
--   'Promo Price'       → 'Promo'
--   'Exceptional Price' → 'Exceptional'
--   anything else       → 'Standard'
-- ============================================================
plans_non_nfr AS (
    SELECT
        b.billing_date,
        b.first_layer_id,
        b.second_layer_id,
        b.third_layer_id,
        b.fourth_layer_id,
        b.fifth_layer_id,
        b.plan_name                                                             AS item,
        CASE b.plan_name
            WHEN 'Email Protect'                    THEN 'IS-LTP-EP'
            WHEN 'Complete Protect'                 THEN 'IS-LTP-CP'
            WHEN 'Core'                             THEN 'IS-LTP-CORE'
            WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IP'
            WHEN 'SAT Suite'                        THEN 'IS-SAT_SUITE'
            WHEN 'Starter'                          THEN 'IS-LTP-STARTER'
            WHEN 'Phishing Simulation and Training' THEN 'IS-LTP-PST'
        END                                                                     AS sku,
        b.partner_pricing,
        b.billable_quantity,
        b.billable_quantity * (i.amount / NULLIF(i.quantity, 0))               AS amount,
        CASE COALESCE(NULLIF(i.price_type, ''), 'Standard')
            WHEN 'Promo Price'       THEN 'Promo'
            WHEN 'Exceptional Price' THEN 'Exceptional'
            ELSE                          'Standard'
        END                                                                     AS price_type
    FROM base b
    LEFT JOIN itemized i
        ON  b.first_layer_id  = i.ltp
        AND b.plan_name       = i.item
        AND b.partner_pricing = i.partner_pricing
    WHERE
        b.partner_pricing = FALSE
        AND b.plan_name IN (
            'Email Protect', 'Complete Protect', 'Core',
            'IRONSCALES Protect', 'SAT Suite', 'Starter',
            'Phishing Simulation and Training'
        )
),

-- ============================================================
-- PLANS (NFR): flat rate from pricing unpivot
-- price_type from itemized
-- ============================================================
plans_nfr AS (
    SELECT
        b.billing_date,
        b.first_layer_id,
        b.second_layer_id,
        b.third_layer_id,
        b.fourth_layer_id,
        b.fifth_layer_id,
        b.plan_name                                                             AS item,
        CASE b.plan_name
            WHEN 'Email Protect'      THEN 'IS-LTP-EPNFR'
            WHEN 'Complete Protect'   THEN 'IS-LTP-CPNFR'
            WHEN 'Core'               THEN 'IS-LTP-CORENFR'
            WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IPNFR'
            WHEN 'SAT Suite'          THEN 'IS-SAT_SUITENFR'
            WHEN 'Starter'            THEN 'IS-LTP-STARTERNFR'
        END                                                                     AS sku,
        b.partner_pricing,
        b.billable_quantity,
        b.billable_quantity * p.price                                           AS amount,
        CASE COALESCE(NULLIF(i.price_type, ''), 'Standard')
            WHEN 'Promo Price'       THEN 'Promo'
            WHEN 'Exceptional Price' THEN 'Exceptional'
            ELSE                          'Standard'
        END                                                                     AS price_type
    FROM base b
    JOIN pricing p
        ON  b.first_layer_id = p.tenant_global_id
        AND p.is_nfr         = TRUE
        AND p.tier_min       = 1
        AND p.sku            = CASE b.plan_name
                                    WHEN 'Email Protect'      THEN 'Email Protect'
                                    WHEN 'Complete Protect'   THEN 'Complete Protect'
                                    WHEN 'Core'               THEN 'CORE'
                                    WHEN 'IRONSCALES Protect' THEN 'IRONSCALES Protect'
                                    WHEN 'SAT Suite'          THEN 'SAT_SUITE'
                                    WHEN 'Starter'            THEN 'STARTER'
                               END
    LEFT JOIN itemized i
        ON  b.first_layer_id  = i.ltp
        AND b.plan_name       = i.item
        AND b.partner_pricing = i.partner_pricing
    WHERE
        b.partner_pricing = TRUE
        AND b.plan_name IN (
            'Email Protect', 'Complete Protect', 'Core',
            'IRONSCALES Protect', 'SAT Suite', 'Starter'
        )
),

-- ============================================================
-- PREMIUM: NINJIO, Cybermaniacs Videos, Habitu8 (flat rate)
-- price_type from itemized
-- ============================================================
premium AS (
    SELECT
        b.billing_date,
        b.first_layer_id,
        b.second_layer_id,
        b.third_layer_id,
        b.fourth_layer_id,
        b.fifth_layer_id,
        b.premium_name                                                          AS item,
        'IS-LTP-PSCP'                                                           AS sku,
        NULL::boolean                                                           AS partner_pricing,
        b.billable_quantity,
        b.billable_quantity * p.price                                           AS amount,
        CASE COALESCE(NULLIF(i.price_type, ''), 'Standard')
            WHEN 'Promo Price'       THEN 'Promo'
            WHEN 'Exceptional Price' THEN 'Exceptional'
            ELSE                          'Standard'
        END                                                                     AS price_type
    FROM base b
    JOIN pricing p
        ON  b.first_layer_id = p.tenant_global_id
        AND p.sku            = 'PSCP'
        AND p.tier_min       = 1
    LEFT JOIN itemized i
        ON  b.first_layer_id = i.ltp
        AND b.premium_name   = i.item
        AND i.partner_pricing IS NULL
    WHERE b.premium_name IN ('NINJIO', 'Cybermaniacs Videos', 'Habitu8')
),

-- ============================================================
-- INCIDENT MANAGEMENT (flat rate per tenant)
-- price_type from itemized
-- ============================================================
incident_mgmt AS (
    SELECT
        b.billing_date,
        b.first_layer_id,
        b.second_layer_id,
        b.third_layer_id,
        b.fourth_layer_id,
        b.fifth_layer_id,
        'Incident Management'                                                   AS item,
        'IS-LTP-IM'                                                             AS sku,
        NULL::boolean                                                           AS partner_pricing,
        b.billable_quantity,
        b.billable_quantity * p.price                                           AS amount,
        CASE COALESCE(NULLIF(i.price_type, ''), 'Standard')
            WHEN 'Promo Price'       THEN 'Promo'
            WHEN 'Exceptional Price' THEN 'Exceptional'
            ELSE                          'Standard'
        END                                                                     AS price_type
    FROM base b
    JOIN pricing p
        ON  b.first_layer_id = p.tenant_global_id
        AND p.sku            = 'Incident Management'
        AND p.tier_min       = 1
    LEFT JOIN itemized i
        ON  b.first_layer_id = i.ltp
        AND i.item           = 'Incident Management'
        AND i.partner_pricing IS NULL
    WHERE b.incident_management = TRUE
),

-- ============================================================
-- S&T BUNDLE PLUS (flat rate per tenant)
-- Excludes Complete Protect and SAT Suite (bundled).
-- Non-NFR only.
-- price_type from itemized
-- ============================================================
stbp AS (
    SELECT
        b.billing_date,
        b.first_layer_id,
        b.second_layer_id,
        b.third_layer_id,
        b.fourth_layer_id,
        b.fifth_layer_id,
        'S&T Bundle Plus'                                                       AS item,
        'IS-LTP-STBP'                                                           AS sku,
        NULL::boolean                                                           AS partner_pricing,
        b.billable_quantity,
        b.billable_quantity * p.price                                           AS amount,
        CASE COALESCE(NULLIF(i.price_type, ''), 'Standard')
            WHEN 'Promo Price'       THEN 'Promo'
            WHEN 'Exceptional Price' THEN 'Exceptional'
            ELSE                          'Standard'
        END                                                                     AS price_type
    FROM base b
    JOIN pricing p
        ON  b.first_layer_id = p.tenant_global_id
        AND p.sku            = 'STBP'
        AND p.tier_min       = 1
    LEFT JOIN itemized i
        ON  b.first_layer_id = i.ltp
        AND i.item           = 'S&T Bundle Plus'
        AND i.partner_pricing IS NULL
    WHERE
        b.simulation_and_training_bundle_plus = TRUE
        AND b.plan_name NOT IN ('Complete Protect', 'SAT Suite')
        AND b.partner_pricing = FALSE
),

-- ============================================================
-- ACCOUNT TAKEOVER (flat rate per tenant)
-- Excludes Complete Protect (bundled). Non-NFR only.
-- price_type from itemized
-- ============================================================
ato AS (
    SELECT
        b.billing_date,
        b.first_layer_id,
        b.second_layer_id,
        b.third_layer_id,
        b.fourth_layer_id,
        b.fifth_layer_id,
        'Account Takeover'                                                      AS item,
        'IS-LTP-ATO'                                                            AS sku,
        NULL::boolean                                                           AS partner_pricing,
        b.billable_quantity,
        b.billable_quantity * p.price                                           AS amount,
        CASE COALESCE(NULLIF(i.price_type, ''), 'Standard')
            WHEN 'Promo Price'       THEN 'Promo'
            WHEN 'Exceptional Price' THEN 'Exceptional'
            ELSE                          'Standard'
        END                                                                     AS price_type
    FROM base b
    JOIN pricing p
        ON  b.first_layer_id = p.tenant_global_id
        AND p.sku            = 'ATO'
        AND p.tier_min       = 1
    LEFT JOIN itemized i
        ON  b.first_layer_id = i.ltp
        AND i.item           = 'Account Takeover'
        AND i.partner_pricing IS NULL
    WHERE
        b.ato = TRUE
        AND b.plan_name != 'Complete Protect'
        AND b.partner_pricing = FALSE
),

-- ============================================================
-- DMARC (flat rate per domain, per individual tenant)
-- Grain: individual SLTP tenant, not rolled up to LTP root.
-- price_type from itemized
-- ============================================================
dmarc AS (
    SELECT
        g.date_recorded                                                         AS billing_date,
        g.first_layer_id,
        g.second_layer_id,
        g.third_layer_id,
        g.fourth_layer_id,
        g.fifth_layer_id,
        CASE g.dmarc_ironscales_plan
            WHEN 1 THEN 'DMARC Core Management'
            WHEN 2 THEN 'DMARC Pro'
            WHEN 3 THEN 'DMARC Premium'
        END                                                                     AS item,
        CASE g.dmarc_ironscales_plan
            WHEN 1 THEN 'IS-LTP-DMARC'
            WHEN 2 THEN 'IS-LTP-DMARC_PRO'
            WHEN 3 THEN 'IS-LTP-DMARC_PREMIUM'
        END                                                                     AS sku,
        NULL::boolean                                                           AS partner_pricing,
        d.dmarc_domains_number                                                  AS billable_quantity,
        d.dmarc_domains_number * p.price                                        AS amount,
        CASE COALESCE(NULLIF(i.price_type, ''), 'Standard')
            WHEN 'Promo Price'       THEN 'Promo'
            WHEN 'Exceptional Price' THEN 'Exceptional'
            ELSE                          'Standard'
        END                                                                     AS price_type
    FROM sltp_dmarc g
    JOIN dmarc_hwm d
        ON  COALESCE(
                NULLIF(TRIM(g.fifth_layer_id),  ''),
                NULLIF(TRIM(g.fourth_layer_id), ''),
                NULLIF(TRIM(g.third_layer_id),  ''),
                NULLIF(TRIM(g.second_layer_id), ''),
                NULLIF(TRIM(g.first_layer_id),  '')
            ) = d.tenant_global_id
    JOIN pricing p
        ON  g.first_layer_id = p.tenant_global_id
        AND p.tier_min       = 1
        AND p.sku            = CASE g.dmarc_ironscales_plan
                                    WHEN 1 THEN 'DMARC'
                                    WHEN 2 THEN 'DMARC_PRO'
                                    WHEN 3 THEN 'DMARC'
                               END
    LEFT JOIN itemized i
        ON  g.first_layer_id = i.ltp
        AND i.item           = CASE g.dmarc_ironscales_plan
                                    WHEN 1 THEN 'DMARC Core Management'
                                    WHEN 2 THEN 'DMARC Pro'
                                    WHEN 3 THEN 'DMARC Premium'
                               END
        AND i.partner_pricing IS NULL
    WHERE
        g.approved = TRUE
        AND g.billing_status IN ('Active', 'Active-POC')
        AND g.dmarc_ironscales_plan IS NOT NULL
    HAVING
        (d.dmarc_domains_number > 0 AND d.dmarc_domains_number IS NOT NULL)
)

-- ============================================================
-- FINAL OUTPUT
-- ============================================================
SELECT billing_date, first_layer_id, second_layer_id, third_layer_id, fourth_layer_id, fifth_layer_id, item, sku, partner_pricing, billable_quantity, amount, price_type FROM plans_non_nfr
UNION ALL
SELECT billing_date, first_layer_id, second_layer_id, third_layer_id, fourth_layer_id, fifth_layer_id, item, sku, partner_pricing, billable_quantity, amount, price_type FROM plans_nfr
UNION ALL
SELECT billing_date, first_layer_id, second_layer_id, third_layer_id, fourth_layer_id, fifth_layer_id, item, sku, partner_pricing, billable_quantity, amount, price_type FROM premium
UNION ALL
SELECT billing_date, first_layer_id, second_layer_id, third_layer_id, fourth_layer_id, fifth_layer_id, item, sku, partner_pricing, billable_quantity, amount, price_type FROM incident_mgmt
UNION ALL
SELECT billing_date, first_layer_id, second_layer_id, third_layer_id, fourth_layer_id, fifth_layer_id, item, sku, partner_pricing, billable_quantity, amount, price_type FROM stbp
UNION ALL
SELECT billing_date, first_layer_id, second_layer_id, third_layer_id, fourth_layer_id, fifth_layer_id, item, sku, partner_pricing, billable_quantity, amount, price_type FROM ato
UNION ALL
SELECT billing_date, first_layer_id, second_layer_id, third_layer_id, fourth_layer_id, fifth_layer_id, item, sku, partner_pricing, billable_quantity, amount, price_type FROM dmarc