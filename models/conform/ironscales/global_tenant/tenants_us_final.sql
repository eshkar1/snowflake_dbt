

with tenants_us as (
    select * from {{ ref('tenants_us_tree')}}
),

campaigns_companylicense as (
    select * from {{ ref('stg_ironscales_campaigns_companylicense_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()

),

campaigns_company as (
    select * from {{ ref('stg_ironscales_campaigns_company_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()

),

auth_user as (
    select * from {{ ref('stg_ironscales_auth_user_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
),

active_profiles as (
    select * from {{ ref('tenants_us_active_profile')}}
),

shared_profiles as (
    select * from {{ ref('tenants_us_shared_profile')}}
)

select
    t.root as root,
    t.parent_global_id as parent_global_id,
    t.parent_name as parent_name,
    t.tenant_global_id as tenant_global_id,
    t.tenant_name as tenant_name,
    t.registration_date as registration_timestamp,
    t.domain as domain,
    concat(u.first_name,' ',u.last_name) as owner_name,
    u.email as owner_email,
    l.is_partner as partner_pricing,
    l.plan_type as plan_id,
    l.ironschool_premium_type as premium_id,
    l.plan_expiry_date as plan_expiry,
    l.incident_service_management_enabled as incident_management,
    l.security_awareness_training as security_awareness_training,
    l.simulation_and_training_bundle as simulation_and_training_bundle,
    l.simulation_and_training_bundle_plus as simulation_and_training_bundle_plus,
    l.ai_empower_bundle as ai_empower_bundle,
    l.themis_co_pilot as themis_co_pilot,  
    --l.ato_enabled as ato_enabled,
    l.paid_ato_enabled as ato,
    l.teams_enabled as teams_protection,
    l.file_scanning as file_scanning,
    l.link_scanning as link_scanning,    
    l.multi_tenancy_enabled as multi_tenancy,
    l.profiles_limit as licensed_profiles,
    a.active_profiles as active_profiles,
    s.shared_profiles as shared_profiles,
    l.trial_plan_type as trial_plan_id,
    l.trial_premium_vendor as trial_premium_id,
    l.trial_plan_expiry_date as trial_plan_expiry,
    l.DMARC_MANAGEMENT,
    l.dmarc_domains_number,
    l.dmarc_plan_name,
    t.depth as depth,
    t.approved as approved,
    t.tree_key as tree_key,
    c.business_pillar as pillar,
    c.affiliation_type as type,
    c.business_type as business_type,
    l._rivery_last_update as roundup_timestamp,

from
    tenants_us t
    left join active_profiles a on t.tenant_global_id = a.tenant_global_id 
    left join shared_profiles s on t.tenant_global_id = s.tenant_global_id
    left join campaigns_companylicense l on t.tenant_id = l.company_id
    left join campaigns_company c on t.TENANT_GLOBAL_ID = c.tenant_global_id
    left join auth_user u on c.owner_id = u.id