with tenants_us_final as (
    select * from {{ ref('tenants_us_final')}}
),

tenants_eu_final as (
    select * from {{ ref('stg_ironscales_tenants_eu')}}
)

-- select
-- *
-- from PROD_CONFORM.DBT_PROD_DB.GLOBAL_TENANT_HISTORY

-- union 


select
    root,
    parent_global_id,
    parent_name,
    tenant_global_id,
    tenant_name,
    date(registration_timestamp) as registration_date,
    domain,
    OWNER_NAME,
	OWNER_EMAIL,
    partner_pricing,
    plan_id,
    -- finance_db.billing_sch.plan_id2name_fn(plan_id) as plan_name,
    case plan_id
        when 1 then 'Phishing Simulation and Training'
        when 2 then 'Starter'
        when 3 then 'Core'
        when 4 then 'Email Protect'
        when 5 then 'Complete Protect'
        when 6 then 'IRONSCALES Protect'
        else ifnull(to_varchar(plan_id), 'No_Plan')
    end as plan_name,
    premium_id,
    -- finance_db.billing_sch.premium_id2name_fn(premium_id) as premium_name,
    case premium_id
        when 1 then 'NINJIO'
        when 3 then 'Habitu8'
        when 4 then 'Cybermaniacs Videos'
        when 5 then 'Wizer'
        when 6 then 'IRONSCALES'
        else ifnull(to_varchar(premium_id), 'No Premium')
    end as premium_name,
    date(plan_expiry) as plan_expiry_date,
    incident_management,
    security_awareness_training,
    simulation_and_training_bundle,
    simulation_and_training_bundle_plus,
    ai_empower_bundle,
    themis_co_pilot,  
    ato,
    teams_protection,
    file_scanning,
    link_scanning,    
    multi_tenancy,
    licensed_profiles,
    active_profiles,
    shared_profiles,
    trial_plan_id,
    -- finance_db.billing_sch.plan_id2name_fn(trial_plan_id) as trial_plan_name,
    case trial_plan_id
        when 1 then 'Phishing Simulation and Training'
        when 2 then 'Starter'
        when 3 then 'Core'
        when 4 then 'Email Protect'
        when 5 then 'Complete Protect'
        when 6 then 'IRONSCALES Protect'
        else ifnull(to_varchar(trial_plan_id), 'No_Plan')
    end as trial_plan_name,
    trial_premium_id,
    -- finance_db.billing_sch.premium_id2name_fn(trial_premium_id) as trial_premium_name,
    case trial_premium_id
        when 1 then 'NINJIO'
        when 3 then 'Habitu8'
        when 4 then 'Cybermaniacs Videos'
        when 5 then 'Wizer'
        when 6 then 'IRONSCALES'
        else ifnull(to_varchar(trial_premium_id), 'No Premium')
    end as trial_premium_name,
    date(trial_plan_expiry) as trial_plan_expiry_date ,
    DMARC_MANAGEMENT,
    depth,
    approved,
    tree_key,
    date(roundup_timestamp) as record_date,
    -- finance_db.billing_sch.billing_status_fn(plan_id, trial_plan_id, plan_expiry, trial_plan_expiry, roundup_timestamp) as billing_status,
    -- iff(plan_id is not null and plan_expiry >= roundup_timestamp ,'Active',
    --     iff(trial_plan_id is not null and trial_plan_expiry >= roundup_timestamp, 'POC', 'Inactive')
    --     ) as billing_status,

    CASE
        -- Check for POC first (active trial plan takes precedence)
        WHEN trial_plan_id IS NOT NULL AND date(trial_plan_expiry) >= date(roundup_timestamp) THEN 'POC'
        
        -- Then check for active regular plan
        WHEN plan_id IS NOT NULL AND date(plan_expiry) >= date(roundup_timestamp) THEN 'Active'
        
        -- Everyone else is inactive
        ELSE 'Inactive'
    END AS billing_status
from
    tenants_us_final
    -- ironscales_us_db.rr_prod_sch.tenants_vw
union
select
    root,
    parent_global_id,
    parent_name,
    tenant_global_id,
    tenant_name,
    date(registration_timestamp) as registration_date,
    domain,
    OWNER_NAME,
	OWNER_EMAIL,
    partner_pricing,
    plan_id,
    -- finance_db.billing_sch.plan_id2name_fn(plan_id) as plan_name,
    case plan_id
        when 1 then 'Phishing Simulation and Training'
        when 2 then 'Starter'
        when 3 then 'Core'
        when 4 then 'Email Protect'
        when 5 then 'Complete Protect'
        when 6 then 'IRONSCALES Protect'
        else ifnull(to_varchar(plan_id), 'No_Plan')
    end as plan_name,
    premium_id,
    -- finance_db.billing_sch.premium_id2name_fn(premium_id) as premium_name,
    case premium_id
        when 1 then 'NINJIO'
        when 3 then 'Habitu8'
        when 4 then 'Cybermaniacs Videos'
        when 5 then 'Wizer'
        when 6 then 'IRONSCALES'
        else ifnull(to_varchar(premium_id), 'No Premium')
    end as premium_name,
    date(plan_expiry) as plan_expiry_date,
    incident_management,
    security_awareness_training,
    simulation_and_training_bundle,
    simulation_and_training_bundle_plus,
    ai_empower_bundle,
    themis_co_pilot,  
    ato,
    teams_protection,
    file_scanning,
    link_scanning,    
    multi_tenancy,
    licensed_profiles,
    active_profiles,
    shared_profiles,
    trial_plan_id,
    -- finance_db.billing_sch.plan_id2name_fn(trial_plan_id) as trial_plan_name,
    case trial_plan_id
        when 1 then 'Phishing Simulation and Training'
        when 2 then 'Starter'
        when 3 then 'Core'
        when 4 then 'Email Protect'
        when 5 then 'Complete Protect'
        when 6 then 'IRONSCALES Protect'
        else ifnull(to_varchar(trial_plan_id), 'No_Plan')
    end as trial_plan_name,
    trial_premium_id,
    -- finance_db.billing_sch.premium_id2name_fn(trial_premium_id) as trial_premium_name,
    case trial_premium_id
        when 1 then 'NINJIO'
        when 3 then 'Habitu8'
        when 4 then 'Cybermaniacs Videos'
        when 5 then 'Wizer'
        when 6 then 'IRONSCALES'
        else ifnull(to_varchar(trial_premium_id), 'No Premium')
    end as trial_premium_name,
    date(trial_plan_expiry) as trial_plan_expiry_date,
    DMARC_MANAGEMENT,
    depth,
    approved,
    tree_key,
    date(roundup_timestamp) as record_date,
    -- finance_db.billing_sch.billing_status_fn(plan_id, trial_plan_id, plan_expiry, trial_plan_expiry, roundup_timestamp) as billing_status,
    -- iff(plan_id is not null and plan_expiry >= roundup_timestamp,'Active',
    --     iff(trial_plan_id is not null and trial_plan_expiry >= roundup_timestamp, 'POC', 'Inactive')
    --     ) as billing_status
    CASE
        -- Check for POC first (active trial plan takes precedence)
        WHEN trial_plan_id IS NOT NULL AND date(trial_plan_expiry) >= date(roundup_timestamp) THEN 'POC'
        
        -- Then check for active regular plan
        WHEN plan_id IS NOT NULL AND date(plan_expiry) >= date(roundup_timestamp) THEN 'Active'
        
        -- Everyone else is inactive
        ELSE 'Inactive'
    END AS billing_status
from
    -- secondary_eu_db.tenants_sch.tenants_tbl
    tenants_eu_final