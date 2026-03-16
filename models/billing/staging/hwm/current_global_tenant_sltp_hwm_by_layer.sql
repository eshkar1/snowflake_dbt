with global_tenant_history as (
    select * from {{ ref('current_global_tenant_ltp_hwm') }}
),

ltp_account_meta as (
    select * from {{ ref('ltp_account_meta') }}
),

extracted as (
    select
        case
            when l.tenant_global_id is null then 'not_ltp'
            else 'ltp'
        end as is_ltp,

        left(g.root, 2)                                                              as prefix,
        coalesce(left(g.root, 2) || '-' || regexp_substr(g.tree_key, '[0-9]{1,}', 1, 1), '') as first_layer,
        coalesce(left(g.root, 2) || '-' || regexp_substr(g.tree_key, '[0-9]{1,}', 1, 2), '') as second_layer,
        coalesce(left(g.root, 2) || '-' || regexp_substr(g.tree_key, '[0-9]{1,}', 1, 3), '') as third_layer,
        coalesce(left(g.root, 2) || '-' || regexp_substr(g.tree_key, '[0-9]{1,}', 1, 4), '') as fourth_layer,
        coalesce(left(g.root, 2) || '-' || regexp_substr(g.tree_key, '[0-9]{1,}', 1, 5), '') as fifth_layer,

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
        g.NOT_NFR_PARTNER

    from global_tenant_history g
    left join ltp_account_meta l on g.root = l.tenant_global_id
    where
        g.date_recorded = current_date
        and g.billing_status in ('Active', 'Active-POC')
        and g.approved = true
)

select
    a.is_ltp,
    a.first_layer                        as first_layer_id,
    coalesce(b.tenant_name, '')          as first_layer_name,
    a.second_layer                       as second_layer_id,
    coalesce(c.tenant_name, '')          as second_layer_name,
    a.third_layer                        as third_layer_id,
    coalesce(d.tenant_name, '')          as third_layer_name,
    a.fourth_layer                       as fourth_layer_id,
    coalesce(e.tenant_name, '')          as fourth_layer_name,
    a.fifth_layer                        as fifth_layer_id,
    coalesce(f.tenant_name, '')          as fifth_layer_name,
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
    a.not_nfr_partner
    -- a.prefix,
    -- a.tenant_global_id

from extracted a
left join global_tenant_history b on a.hwm_date = b.hwm_date and a.first_layer  = b.tenant_global_id
left join global_tenant_history c on a.hwm_date = c.hwm_date and a.second_layer = c.tenant_global_id
left join global_tenant_history d on a.hwm_date = d.hwm_date and a.third_layer  = d.tenant_global_id
left join global_tenant_history e on a.hwm_date = e.hwm_date and a.fourth_layer = e.tenant_global_id
left join global_tenant_history f on a.hwm_date = f.hwm_date and a.fifth_layer  = f.tenant_global_id