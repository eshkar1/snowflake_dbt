with global_tenant_history_daily_agg_billing_tbl as (
    select * from {{ ref('global_tenant_history_monthly_billing_tbl')}} 
),

global_tenant_history as (
    select * from 
    PROD_MART.OPERATION.GLOBAL_TENANT_HISTORY
    -- {{ ref('global_tenant_history')}}
    
),

ltp_pricing_list as (
    select * from {{ ref('ltp_pricing_tbl')}}
)


select
current_date as record_date,
g.date_recorded as billing_date,
REGEXP_REPLACE(g.tenant_global_id, '[^0-9]', '') as tenant_global_id,
g.tenant_name as tenant_name,
REGEXP_REPLACE(g.parent_global_id, '[^0-9]', '')  as parent_global_id,
g.parent_name as parent_name,
ifnull(g.licensed_profiles,0) as licensed_profiles,
ifnull(g.active_profiles,0) as active_profiles,
ifnull(g.shared_profiles,0) as shared_profiles,
CASE profile_type
    when 'active' then ifnull(g.active_profiles,0)
    when 'license' then ifnull(g.licensed_profiles,0)
    when 'shared' then 
                    case 
                        when g.shared_profiles is null then ifnull(g.active_profiles,0)
                        else ifnull(g.active_profiles - g.shared_profiles,0)
                    end
end as billable_profiles,
g.plan_id as plan_id,
-- g.plan_name as plan_name,
gh.plan_expiry_date as plan_expiry_date,
gh.trial_plan_expiry_date as trial_plan_expiry_date,
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
  'MULTI_TENANCY', g.MULTI_TENANCY,
  'SAT_CONTENT_PACK', g.premium_name,
  'DMARC', g.DMARC_MANAGEMENT
                ) as active_add_ons,
p.is_highwatermark as high_water_mark,
null as non_profit_flag,
g.partner_pricing as not_for_resale_flag,
null as price_per_mailbox,
gh.tree_key
from global_tenant_history_daily_agg_billing_tbl g
left join global_tenant_history gh on g.record_date = gh.record_date 
                                                        and g.tenant_global_id = gh.tenant_global_id
                                                        
left join ltp_pricing_list p on g.root = p.tenant_global_id

WHERE
REGEXP_SUBSTR(g.tenant_global_id, '[A-Za-z]+') = 'EU'
and g.billing_status = 'Active'
and g.approved = true