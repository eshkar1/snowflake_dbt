with global_tenant_history_daily_agg_billing_tbl as (
    select * from {{ ref('global_tenant_history_monthly_billing_tbl')}} 
),

global_tenant_history as (
    select * from {{ ref('global_tenant_history')}}
),

ltp_pricing_list as (
    select * from {{ ref('ltp_pricing_tbl')}}
)


select
current_date as date_of_report,
g.date_recorded as date_of_billing,
g.tenant_global_id as customer_id,
g.tenant_name as customer_name,
g.parent_global_id as parent_id,
g.parent_name as parent_name,
g.licensed_profiles as licensed_mailboxes,
g.active_profiles as enabled_mailboxes,
g.shared_profiles as shared_mailboxes,
CASE profile_type
    when 'active' then g.active_profiles
    when 'license' then g.licensed_profiles
    when 'shared' then 
                    case 
                        when g.shared_profiles is null then g.active_profiles
                        else g.active_profiles - g.shared_profiles
                    end
end as billable_mailboxes,
g.plan_id as plan_id,
g.plan_name as plan_name,
gh.plan_expiry_date as plan_expiration,
gh.trial_plan_expiry_date as trial_expiration,
gh.registration_date as registration_date,
null as parent_type,
OBJECT_CONSTRUCT(
  'INCIDENT_MANAGEMENT', g.INCIDENT_MANAGEMENT,
  'SECURITY_AWARENESS_TRAINING', g.SECURITY_AWARENESS_TRAINING,
  'SIMULATION_AND_TRAINING_BUNDLE', g.SIMULATION_AND_TRAINING_BUNDLE,
  'SIMULATION_AND_TRAINING_BUNDLE_PLUS', g.SIMULATION_AND_TRAINING_BUNDLE_PLUS,
  'AI_EMPOWER_BUNDLE', g.AI_EMPOWER_BUNDLE,
  'THEMIS_CO_PILOT', g.THEMIS_CO_PILOT,
  'ATO', g.ATO,
  'TEAMS_PROTECTION', g.TEAMS_PROTECTION,
  'FILE_SCANNING', g.FILE_SCANNING,
  'LINK_SCANNING', g.LINK_SCANNING,
  'MULTI_TENANCY', g.MULTI_TENANCY
                ) as activated_addons,
p.is_highwatermark as highwater_mark,
null as sled_and_non_profit,
g.partner_pricing as nfr,
null as price_per_mailbox
from global_tenant_history_daily_agg_billing_tbl g
left join global_tenant_history gh on g.record_date = gh.record_date 
                                                        and g.tenant_global_id = gh.tenant_global_id
                                                        
left join ltp_pricing_list p on g.root = p.tenant_global_id