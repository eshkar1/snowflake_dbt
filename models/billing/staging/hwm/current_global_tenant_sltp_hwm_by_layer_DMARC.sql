WITH

-- ============================================================
-- SOURCES
-- ============================================================
global_tenant_history AS (
    SELECT * FROM {{ ref('global_tenant_history_sltp_daily_billing_DMARC_tbl') }}
),

ltp_account_meta AS (
    SELECT * FROM {{ ref('ltp_account_meta') }}
),

-- ============================================================
-- EXTRACTED: parse layer IDs from tree_key, join meta for is_ltp flag.
-- Filters to current date, active/approved tenants only.
-- ============================================================
extracted AS (
    SELECT
        CASE
            WHEN l.tenant_global_id IS NULL THEN 'not_ltp'
            ELSE 'ltp'
        END                                                                          AS is_ltp,

        LEFT(g.root, 2)                                                              AS prefix,
        COALESCE(LEFT(g.root, 2) || '-' || REGEXP_SUBSTR(g.tree_key, '[0-9]{1,}', 1, 1), '') AS first_layer,
        COALESCE(LEFT(g.root, 2) || '-' || REGEXP_SUBSTR(g.tree_key, '[0-9]{1,}', 1, 2), '') AS second_layer,
        COALESCE(LEFT(g.root, 2) || '-' || REGEXP_SUBSTR(g.tree_key, '[0-9]{1,}', 1, 3), '') AS third_layer,
        COALESCE(LEFT(g.root, 2) || '-' || REGEXP_SUBSTR(g.tree_key, '[0-9]{1,}', 1, 4), '') AS fourth_layer,
        COALESCE(LEFT(g.root, 2) || '-' || REGEXP_SUBSTR(g.tree_key, '[0-9]{1,}', 1, 5), '') AS fifth_layer,

        -- from global_tenant_history
        g.tenant_global_id,
        g.approved,
        g.billing_status,
        g.domain,
        g.partner_pricing,
        g.plan_id,
        g.plan_name,
        g.premium_id,
        g.premium_name,
        g.trial_plan_id,
        g.trial_plan_name,
        g.trial_premium_id,
        g.trial_premium_name,
        g.trial_plan_expiry_date,
        g.licensed_profiles,
        g.active_profiles,
        -- g.record_date,
        g.hwm_date,
        g.tree_key,
        g.incident_management,
        g.security_awareness_training,
        g.ato,
        g.multi_tenancy,
        g.parent_name,
        g.simulation_and_training_bundle,
        g.simulation_and_training_bundle_plus,
        g.ai_empower_bundle,
        g.themis_co_pilot,
        g.teams_protection,
        g.file_scanning,
        g.link_scanning,
        g.shared_profiles,
        g.date_recorded,
        g.dmarc_management,
        g.dmarc_domains_number,
        g.dmarc_ironscales_plan,
        g.dmarc_ironscales_plan_name

    FROM global_tenant_history g
    LEFT JOIN ltp_account_meta l ON g.root = l.tenant_global_id
    WHERE
        g.date_recorded = CURRENT_DATE
        AND g.billing_status IN ('Active', 'Active-POC')
        AND g.approved = TRUE
)

-- ============================================================
-- FINAL OUTPUT: resolve layer IDs to names via self-joins
-- ============================================================
SELECT
    a.is_ltp,
    a.first_layer                        AS first_layer_id,
    COALESCE(b.tenant_name, '')          AS first_layer_name,
    a.second_layer                       AS second_layer_id,
    COALESCE(c.tenant_name, '')          AS second_layer_name,
    a.third_layer                        AS third_layer_id,
    COALESCE(d.tenant_name, '')          AS third_layer_name,
    a.fourth_layer                       AS fourth_layer_id,
    COALESCE(e.tenant_name, '')          AS fourth_layer_name,
    a.fifth_layer                        AS fifth_layer_id,
    COALESCE(f.tenant_name, '')          AS fifth_layer_name,
    a.approved,
    a.billing_status,
    a.domain,
    a.partner_pricing,
    a.plan_id,
    a.plan_name,
    a.premium_id,
    a.premium_name,
    a.trial_plan_id,
    a.trial_plan_name,
    a.trial_premium_id,
    a.trial_premium_name,
    a.trial_plan_expiry_date,
    a.licensed_profiles,
    a.active_profiles,
    -- a.record_date,
    a.hwm_date,
    a.tree_key,
    a.incident_management,
    a.security_awareness_training,
    a.ato,
    a.multi_tenancy,
    a.parent_name,
    a.simulation_and_training_bundle,
    a.simulation_and_training_bundle_plus,
    a.ai_empower_bundle,
    a.themis_co_pilot,
    a.teams_protection,
    a.file_scanning,
    a.link_scanning,
    a.shared_profiles,
    a.date_recorded,
    a.dmarc_management,
    a.dmarc_domains_number,
    a.dmarc_ironscales_plan,
    a.dmarc_ironscales_plan_name

FROM extracted a
LEFT JOIN global_tenant_history b ON a.hwm_date = b.hwm_date AND a.first_layer  = b.tenant_global_id
LEFT JOIN global_tenant_history c ON a.hwm_date = c.hwm_date AND a.second_layer = c.tenant_global_id
LEFT JOIN global_tenant_history d ON a.hwm_date = d.hwm_date AND a.third_layer  = d.tenant_global_id
LEFT JOIN global_tenant_history e ON a.hwm_date = e.hwm_date AND a.fourth_layer = e.tenant_global_id
LEFT JOIN global_tenant_history f ON a.hwm_date = f.hwm_date AND a.fifth_layer  = f.tenant_global_id
