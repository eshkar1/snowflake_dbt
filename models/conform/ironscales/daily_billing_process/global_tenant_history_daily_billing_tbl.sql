
with current_month_ltp_roundup_tbl as (
    select * from {{ ref('current_month_ltp_roundup_tbl_updated_new')}} 
),

global_tenant_history as (
    select * from 
    prod_mart.operation.global_tenant_history
    {{ ref('global_tenant_history')}}
)

SELECT
    current_date() as date_recorded,
    g.root,
    g.parent_global_id,
    g.parent_name,
    g.tenant_global_id,
    g.tenant_name,
    g.registration_date,
    g.domain,
    g.partner_pricing,
    g.plan_id,
    g.plan_name,
    g.premium_id,
    g.premium_name,
    g.incident_management,
    g.security_awareness_training,
    g.simulation_and_training_bundle,
    g.simulation_and_training_bundle_plus,
    g.ai_empower_bundle,
    g.themis_co_pilot,
    g.ato,
    g.teams_protection,
    g.file_scanning,
    g.link_scanning,
    g.multi_tenancy,
    g.licensed_profiles,
    g.active_profiles,
    g.shared_profiles,
    g.trial_plan_id,
    g.trial_plan_name,
    g.trial_premium_id,
    g.trial_premium_name,
    g.trial_plan_expiry_date,
    g.depth,
    g.approved,
    g.tree_key,
    g.record_date,
    g.billing_status,
    g.DMARC_MANAGEMENT
FROM
    current_month_ltp_roundup_tbl r
    JOIN global_tenant_history g ON g.tenant_global_id = r.tenant_global_id AND g.record_date = r.record_date