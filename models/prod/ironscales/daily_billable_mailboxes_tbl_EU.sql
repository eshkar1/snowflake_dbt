with global_tenant_history_daily_agg_billing_tbl as (
    select * from 
    -- prod_conform.dbt_prod_db.global_tenant_history_daily_agg_billing_tbl
    {{ ref('global_tenant_history_daily_agg_billing_tbl')}} 
    WHERE
    DATE_RECORDED = current_date
),

global_tenant_history as (
    select * from 
    -- prod_mart.operation.global_tenant_history
    {{ ref('global_tenant_history')}}
    WHERE
    record_date = current_date
),

ltp_pricing_list as (
    select * from 
    -- prod_mart.upload_tables.ltp_pricing_list_today
    {{ ref('ltp_pricing_tbl')}}
),

billing_base AS (
  SELECT
    CASE
        WHEN FIFTH_LAYER_ID IS NOT NULL AND FIFTH_LAYER_ID <> '' THEN FIFTH_LAYER_ID
        WHEN FOURTH_LAYER_ID IS NOT NULL AND FOURTH_LAYER_ID <> '' THEN FOURTH_LAYER_ID
        WHEN THIRD_LAYER_ID IS NOT NULL AND THIRD_LAYER_ID <> '' THEN THIRD_LAYER_ID
        WHEN SECOND_LAYER_ID IS NOT NULL AND SECOND_LAYER_ID <> '' THEN SECOND_LAYER_ID
        ELSE FIRST_LAYER_ID
    END as tenant_global_id,
    item,
    partner_pricing,
    billable_quantity
  FROM prod_conform.dbt_prod_db.sltp_daily_itemized_billing_tbl
  WHERE billing_date = CURRENT_DATE
),

sltp_bill AS (

    SELECT
      tenant_global_id,
    
      -- Email Protect
      MAX(CASE WHEN item = 'Email Protect' AND partner_pricing = false THEN true ELSE false END) as Email_Protect,
      MAX(CASE WHEN item = 'Email Protect' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as Email_Protect_of_Licenses,

      MAX(CASE WHEN item = 'Email Protect' AND partner_pricing = true THEN true ELSE false END) as Email_Protect_NFR,
      MAX(CASE WHEN item = 'Email Protect' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as Email_Protect_NFR_of_Licenses,
    
      -- Complete Protect
      MAX(CASE WHEN item = 'Complete Protect' AND partner_pricing = false THEN true ELSE false END) as Complete_Protect,
      MAX(CASE WHEN item = 'Complete Protect' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as Complete_Protect_of_Licenses,

      MAX(CASE WHEN item = 'Complete Protect' AND partner_pricing = true THEN true ELSE false END) as Complete_Protect_NFR,
      MAX(CASE WHEN item = 'Complete Protect' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as Complete_Protect_NFR_of_Licenses,
      
      -- Core
      MAX(CASE WHEN item = 'Core' AND partner_pricing = false THEN true ELSE false END) as Core,
      MAX(CASE WHEN item = 'Core' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as Core_of_Licenses,

      MAX(CASE WHEN item = 'Core' AND partner_pricing = true THEN true ELSE false END) as Core_NFR,
      MAX(CASE WHEN item = 'Core' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as Core_NFR_of_Licenses,
      
      -- IRONSCALES Protect
      MAX(CASE WHEN item = 'IRONSCALES Protect' AND partner_pricing = false THEN true ELSE false END) as Ironscales_Protect,
      MAX(CASE WHEN item = 'IRONSCALES Protect' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as Ironscales_Protect_of_Licenses,

      MAX(CASE WHEN item = 'IRONSCALES Protect' AND partner_pricing = true THEN true ELSE false END) as Ironscales_Protect_NFR,
      MAX(CASE WHEN item = 'IRONSCALES Protect' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as Ironscales_Protect_NFR_of_Licenses,
      
      -- Phishing Simulation and Training
      MAX(CASE WHEN item = 'Phishing Simulation and Training' AND partner_pricing = false THEN true ELSE false END) as Phishing_Simulation_and_Training,
      MAX(CASE WHEN item = 'Phishing Simulation and Training' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as PST_of_Licenses,

      MAX(CASE WHEN item = 'Phishing Simulation and Training' AND partner_pricing = true THEN true ELSE false END) as Phishing_Simulation_and_Training_NFR,
      MAX(CASE WHEN item = 'Phishing Simulation and Training' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as PST_NFR_of_Licenses,
    
      -- Starter
      MAX(CASE WHEN item = 'Starter' THEN true ELSE false END) as Starter,
      MAX(CASE WHEN item = 'Starter' THEN billable_quantity ELSE 0 END) as Starter_of_Licenses,
    
      -- Premium Content
      MAX(CASE WHEN item IN ('Habitu8') THEN true ELSE false END) as Premium_Content_Habitu8,
      MAX(CASE WHEN item IN ('Habitu8') THEN billable_quantity ELSE 0 END) as Premium_Content_Habitu8_of_Licenses,

      MAX(CASE WHEN item IN ('NINJIO') THEN true ELSE false END) as Premium_Content_NINJIO,
      MAX(CASE WHEN item IN ('NINJIO') THEN billable_quantity ELSE 0 END) as Premium_Content_NINJIO_of_Licenses,

      MAX(CASE WHEN item IN ('Cybermaniacs Videos') THEN true ELSE false END) as Premium_Content_Cybermaniacs,
      MAX(CASE WHEN item IN ('Cybermaniacs Videos') THEN billable_quantity ELSE 0 END) as Premium_Content_Cybermaniacs_of_Licenses,
      
      -- Account Takeover
      MAX(CASE WHEN item = 'Account Takeover' THEN true ELSE false END) as Account_Takeover,
      MAX(CASE WHEN item = 'Account Takeover' THEN billable_quantity ELSE 0 END) as Account_Takeover_of_Licenses,
    
      -- Incident Management
      MAX(CASE WHEN item = 'Incident Management' THEN true ELSE false END) as Incident_Management,
      MAX(CASE WHEN item = 'Incident Management' THEN billable_quantity ELSE 0 END) as Incident_Management_of_Licenses,
    
      -- Multi Tenant
      MAX(CASE WHEN item = 'Multi Tenant' THEN true ELSE false END) as Multi_Tenant,
      MAX(CASE WHEN item = 'Multi Tenant' THEN billable_quantity ELSE 0 END) as Multi_Tenant_of_Licenses,
    
      -- S&T Bundle
      MAX(CASE WHEN item = 'S&T Bundle' THEN true ELSE false END) as ST_Bundle,
      MAX(CASE WHEN item = 'S&T Bundle' THEN billable_quantity ELSE 0 END) as ST_Bundle_of_Licenses,
    
      -- S&T Plus Bundle
      MAX(CASE WHEN item = 'S&T Plus Bundle' THEN true ELSE false END) as ST_Plus_Bundle,
      MAX(CASE WHEN item = 'S&T Plus Bundle' THEN billable_quantity ELSE 0 END) as ST_Plus_Bundle_of_Licenses,
    
      -- Security Awareness Training
      MAX(CASE WHEN item = 'Security Awareness Training' THEN true ELSE false END) as Security_Awareness_Training,
      MAX(CASE WHEN item = 'Security Awareness Training' THEN billable_quantity ELSE 0 END) as SAT_of_Licenses,

      -- Security Awareness Training
      MAX(CASE WHEN item = 'SAT Suite' AND partner_pricing = false THEN true ELSE false END) as Security_Awareness_Training_Suite,
      MAX(CASE WHEN item = 'SAT Suite' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as SAT_Suite_of_Licenses,

      MAX(CASE WHEN item = 'SAT Suite' AND partner_pricing = true THEN true ELSE false END) as Security_Awareness_Training_Suite_NFR,
      MAX(CASE WHEN item = 'SAT Suite' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as SAT_Suite_NFR_of_Licenses,  
    
    FROM billing_base
    GROUP BY tenant_global_id
)

-- select
-- *
-- from PROD_CONFORM.DBT_PROD_DB.DAILY_BILLABLE_MAILBOXES_TBL_US

-- union 

select
current_date as record_date,
g.date_recorded-1 as billing_date,
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
    -- Email Protect
    CASE WHEN sltp_bill.Email_Protect THEN 'Email_Protect' ELSE NULL END, CASE WHEN sltp_bill.Email_Protect THEN sltp_bill.Email_Protect ELSE NULL END,
    CASE WHEN sltp_bill.Email_Protect THEN 'Email_Protect_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Email_Protect THEN sltp_bill.Email_Protect_of_Licenses ELSE NULL END,

    CASE WHEN sltp_bill.Email_Protect_NFR THEN 'Email_Protect_NFR' ELSE NULL END, CASE WHEN sltp_bill.Email_Protect_NFR THEN sltp_bill.Email_Protect_NFR ELSE NULL END,
    CASE WHEN sltp_bill.Email_Protect_NFR THEN 'Email_Protect_NFR_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Email_Protect_NFR THEN sltp_bill.Email_Protect_NFR_of_Licenses ELSE NULL END,
    
    -- Complete Protect
    CASE WHEN sltp_bill.Complete_Protect THEN 'Complete_Protect' ELSE NULL END, CASE WHEN sltp_bill.Complete_Protect THEN sltp_bill.Complete_Protect ELSE NULL END,
    CASE WHEN sltp_bill.Complete_Protect THEN 'Complete_Protect_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Complete_Protect THEN sltp_bill.Complete_Protect_of_Licenses ELSE NULL END,

    CASE WHEN sltp_bill.Complete_Protect_NFR THEN 'Complete_Protect_NFR' ELSE NULL END, CASE WHEN sltp_bill.Complete_Protect_NFR THEN sltp_bill.Complete_Protect_NFR ELSE NULL END,
    CASE WHEN sltp_bill.Complete_Protect_NFR THEN 'Complete_Protect_NFR_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Complete_Protect_NFR THEN sltp_bill.Complete_Protect_NFR_of_Licenses ELSE NULL END,
    
    -- Core
    CASE WHEN sltp_bill.Core THEN 'Core' ELSE NULL END, CASE WHEN sltp_bill.Core THEN sltp_bill.Core ELSE NULL END,
    CASE WHEN sltp_bill.Core THEN 'Core_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Core THEN sltp_bill.Core_of_Licenses ELSE NULL END,

    CASE WHEN sltp_bill.Core_NFR THEN 'Core_NFR' ELSE NULL END, CASE WHEN sltp_bill.Core_NFR THEN sltp_bill.Core_NFR ELSE NULL END,
    CASE WHEN sltp_bill.Core_NFR THEN 'Core_NFR_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Core_NFR THEN sltp_bill.Core_NFR_of_Licenses ELSE NULL END,
    
    -- IRONSCALES Protect
    CASE WHEN sltp_bill.Ironscales_Protect THEN 'IRONSCALES_Protect' ELSE NULL END, CASE WHEN sltp_bill.Ironscales_Protect THEN sltp_bill.Ironscales_Protect ELSE NULL END,
    CASE WHEN sltp_bill.Ironscales_Protect THEN 'IRONSCALES_Protect_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Ironscales_Protect THEN sltp_bill.Ironscales_Protect_of_Licenses ELSE NULL END,

    CASE WHEN sltp_bill.Ironscales_Protect_NFR THEN 'IRONSCALES_Protect_NFR' ELSE NULL END, CASE WHEN sltp_bill.Ironscales_Protect_NFR THEN sltp_bill.Ironscales_Protect_NFR ELSE NULL END,
    CASE WHEN sltp_bill.Ironscales_Protect_NFR THEN 'IRONSCALES_Protect_NFR_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Ironscales_Protect_NFR THEN sltp_bill.Ironscales_Protect_NFR_of_Licenses ELSE NULL END,

    -- Phishing Simulation and Training
    CASE WHEN sltp_bill.Phishing_Simulation_and_Training THEN 'Phishing_Simulation_and_Training_NFR' ELSE NULL END, CASE WHEN sltp_bill.Phishing_Simulation_and_Training THEN sltp_bill.Phishing_Simulation_and_Training ELSE NULL END,
    CASE WHEN sltp_bill.Phishing_Simulation_and_Training THEN 'Phishing_Simulation_and_Training_NFR_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Phishing_Simulation_and_Training THEN sltp_bill.PST_of_Licenses ELSE NULL END,

    CASE WHEN sltp_bill.Phishing_Simulation_and_Training_NFR THEN 'Phishing_Simulation_and_Training_NFR' ELSE NULL END, CASE WHEN sltp_bill.Phishing_Simulation_and_Training_NFR THEN sltp_bill.Phishing_Simulation_and_Training_NFR ELSE NULL END,
    CASE WHEN sltp_bill.Phishing_Simulation_and_Training_NFR THEN 'Phishing_Simulation_and_Training_NFR_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Phishing_Simulation_and_Training_NFR THEN sltp_bill.PST_NFR_of_Licenses ELSE NULL END,
    
    -- Starter
    CASE WHEN sltp_bill.Starter THEN 'Starter' ELSE NULL END, CASE WHEN sltp_bill.Starter THEN sltp_bill.Starter ELSE NULL END,
    CASE WHEN sltp_bill.Starter THEN 'Starter_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Starter THEN sltp_bill.Starter_of_Licenses ELSE NULL END,

    -- Premium SAT Pack - Habitu8
    CASE WHEN sltp_bill.Premium_Content_Habitu8 THEN 'PREMIUM_SAT_PACK' ELSE NULL END, CASE WHEN sltp_bill.Premium_Content_Habitu8 THEN sltp_bill.Premium_Content_Habitu8 ELSE NULL END,
    CASE WHEN sltp_bill.Premium_Content_Habitu8 THEN 'PREMIUM_SAT_PACK_QUANTITY' ELSE NULL END, CASE WHEN sltp_bill.Premium_Content_Habitu8 THEN sltp_bill.Premium_Content_Habitu8_of_Licenses ELSE NULL END,
    CASE WHEN sltp_bill.Premium_Content_Habitu8 THEN 'PREMIUM_SAT_PACK_NAME' ELSE NULL END, CASE WHEN sltp_bill.Premium_Content_Habitu8 THEN 'Habitu8' ELSE NULL END,

    -- Premium SAT Pack - NINJIO
    CASE WHEN sltp_bill.Premium_Content_NINJIO THEN 'PREMIUM_SAT_PACK' ELSE NULL END, CASE WHEN sltp_bill.Premium_Content_NINJIO THEN sltp_bill.Premium_Content_NINJIO ELSE NULL END,
    CASE WHEN sltp_bill.Premium_Content_NINJIO THEN 'PREMIUM_SAT_PACK_QUANTITY' ELSE NULL END, CASE WHEN sltp_bill.Premium_Content_NINJIO THEN sltp_bill.Premium_Content_NINJIO_of_Licenses ELSE NULL END,
    CASE WHEN sltp_bill.Premium_Content_NINJIO THEN 'PREMIUM_SAT_PACK_NAME' ELSE NULL END, CASE WHEN sltp_bill.Premium_Content_NINJIO THEN 'NINJIO' ELSE NULL END,

    -- Premium SAT Pack - Cybermaniacs
    CASE WHEN sltp_bill.Premium_Content_Cybermaniacs THEN 'PREMIUM_SAT_PACK' ELSE NULL END, CASE WHEN sltp_bill.Premium_Content_Cybermaniacs THEN sltp_bill.Premium_Content_Cybermaniacs ELSE NULL END,
    CASE WHEN sltp_bill.Premium_Content_Cybermaniacs THEN 'PREMIUM_SAT_PACK_QUANTITY' ELSE NULL END, CASE WHEN sltp_bill.Premium_Content_Cybermaniacs THEN sltp_bill.Premium_Content_Cybermaniacs_of_Licenses ELSE NULL END,
    CASE WHEN sltp_bill.Premium_Content_Cybermaniacs THEN 'PREMIUM_SAT_PACK_NAME' ELSE NULL END, CASE WHEN sltp_bill.Premium_Content_Cybermaniacs THEN 'Cybermaniacs Videos' ELSE NULL END,

    -- Account Takeover
    CASE WHEN sltp_bill.Account_Takeover THEN 'Account_Takeover' ELSE NULL END, CASE WHEN sltp_bill.Account_Takeover THEN sltp_bill.Account_Takeover ELSE NULL END,
    CASE WHEN sltp_bill.Account_Takeover THEN 'Account_Takeover_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Account_Takeover THEN sltp_bill.Account_Takeover_of_Licenses ELSE NULL END,

    -- Incident Management
    CASE WHEN sltp_bill.Incident_Management THEN 'Incident_Management' ELSE NULL END, CASE WHEN sltp_bill.Incident_Management THEN sltp_bill.Incident_Management ELSE NULL END,
    CASE WHEN sltp_bill.Incident_Management THEN 'Incident_Management_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Incident_Management THEN Incident_Management_of_Licenses ELSE NULL END,

    -- Multi-Tenant
    CASE WHEN sltp_bill.Multi_Tenant THEN 'Multi_Tenant' ELSE NULL END, CASE WHEN sltp_bill.Multi_Tenant THEN sltp_bill.Multi_Tenant ELSE NULL END,
    CASE WHEN sltp_bill.Multi_Tenant THEN 'Multi_Tenant_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Multi_Tenant THEN sltp_bill.Multi_Tenant_of_Licenses ELSE NULL END,

    -- S&T Bundle
    CASE WHEN sltp_bill.ST_Bundle THEN 'S&T_Bundle' ELSE NULL END, CASE WHEN sltp_bill.ST_Bundle THEN sltp_bill.ST_Bundle ELSE NULL END,
    CASE WHEN sltp_bill.ST_Bundle THEN 'S&T_Bundle_Quantity' ELSE NULL END, CASE WHEN sltp_bill.ST_Bundle THEN sltp_bill.ST_Bundle_of_Licenses ELSE NULL END,

    -- S&T Plus Bundle
    CASE WHEN sltp_bill.ST_Plus_Bundle THEN 'S&T_Plus_Bundle' ELSE NULL END, CASE WHEN sltp_bill.ST_Plus_Bundle THEN ST_Plus_Bundle ELSE NULL END,
    CASE WHEN sltp_bill.ST_Plus_Bundle THEN 'S&T_Plus_Bundle_Quantity' ELSE NULL END, CASE WHEN sltp_bill.ST_Plus_Bundle THEN ST_Plus_Bundle_of_Licenses ELSE NULL END,

    -- Security Awareness Training
    CASE WHEN sltp_bill.Security_Awareness_Training THEN 'Security_Awareness_Training' ELSE NULL END, CASE WHEN sltp_bill.Security_Awareness_Training THEN sltp_bill.Security_Awareness_Training ELSE NULL END,
    CASE WHEN sltp_bill.Security_Awareness_Training THEN 'Security_Awareness_Training_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Security_Awareness_Training THEN sltp_bill.SAT_of_Licenses ELSE NULL END,

    -- Security Awareness Training Suite
    CASE WHEN sltp_bill.Security_Awareness_Training_Suite THEN 'Secuity_Awareness_Training_Suite' ELSE NULL END, CASE WHEN sltp_bill.Security_Awareness_Training_Suite THEN sltp_bill.Security_Awareness_Training_Suite ELSE NULL END,
    CASE WHEN sltp_bill.Security_Awareness_Training_Suite THEN 'Secuity_Awareness_Training_Suite_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Security_Awareness_Training_Suite THEN sltp_bill.SAT_Suite_of_Licenses ELSE NULL END,


    CASE WHEN sltp_bill.Security_Awareness_Training_Suite_NFR THEN 'Secuity_Awareness_Training_Suite_NFR' ELSE NULL END, CASE WHEN sltp_bill.Security_Awareness_Training_Suite_NFR THEN sltp_bill.Security_Awareness_Training_Suite_NFR ELSE NULL END,
    CASE WHEN sltp_bill.Security_Awareness_Training_Suite_NFR THEN 'Secuity_Awareness_Training_Suite_NFR_Quantity' ELSE NULL END, CASE WHEN sltp_bill.Security_Awareness_Training_Suite_NFR THEN sltp_bill.SAT_Suite_NFR_of_Licenses ELSE NULL END
) as billable_items,


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
left join global_tenant_history gh on g.date_recorded = gh.record_date 
                                                        and g.tenant_global_id = gh.tenant_global_id
                                                        
left join ltp_pricing_list p on g.root = p.tenant_global_id
left join sltp_bill on g.tenant_global_id = sltp_bill.tenant_global_id
WHERE
REGEXP_SUBSTR(g.tenant_global_id, '[A-Za-z]+') = 'EU'
and gh.billing_status = 'Active'
and gh.approved = true
-- and g.record_date = current_date

-- with global_tenant_history_daily_agg_billing_tbl as (
--     select * from {{ ref('global_tenant_history_daily_agg_billing_tbl')}} 
--     WHERE
--     DATE_RECORDED = current_date
-- ),

-- global_tenant_history as (
--     select * from {{ ref('global_tenant_history')}}
--     WHERE
--     record_date = current_date
-- ),

-- ltp_pricing_list as (
--     select * from {{ ref('ltp_pricing_tbl')}}
-- )

-- -- select
-- -- *
-- -- from PROD_CONFORM.DBT_PROD_DB.DAILY_BILLABLE_MAILBOXES_TBL_EU

-- -- union 


-- select
-- current_date as record_date,
-- g.date_recorded-1 as billing_date,
-- REGEXP_REPLACE(g.tenant_global_id, '[^0-9]', '') as tenant_global_id,
-- g.tenant_name as tenant_name,
-- REGEXP_REPLACE(g.parent_global_id, '[^0-9]', '')  as parent_global_id,
-- g.parent_name as parent_name,
-- ifnull(g.licensed_profiles,0) as licensed_profiles,
-- ifnull(g.active_profiles,0) as active_profiles,
-- ifnull(g.shared_profiles,0) as shared_profiles,
-- CASE profile_type
--     when 'active' then ifnull(g.active_profiles,0)
--     when 'license' then ifnull(g.licensed_profiles,0)
--     when 'shared' then 
--                     case 
--                         when g.shared_profiles is null then ifnull(g.active_profiles,0)
--                         else ifnull(g.active_profiles - g.shared_profiles,0)
--                     end
-- end as billable_profiles,
-- g.plan_id as plan_id,
-- -- g.plan_name as plan_name,
-- gh.plan_expiry_date as plan_expiry_date,
-- gh.trial_plan_expiry_date as trial_plan_expiry_date,
-- gh.registration_date as registration_date,
-- null as parent_type,
-- OBJECT_CONSTRUCT(
--   'INCIDENT_MANAGEMENT', g.INCIDENT_MANAGEMENT,
--   'SECURITY_AWARENESS_TRAINING', g.SECURITY_AWARENESS_TRAINING,
--   'SIMULATION_AND_TRAINING_BUNDLE', g.SIMULATION_AND_TRAINING_BUNDLE,
--   'SIMULATION_AND_TRAINING_BUNDLE_PLUS', g.SIMULATION_AND_TRAINING_BUNDLE_PLUS,
--   'AI_EMPOWER_BUNDLE', g.AI_EMPOWER_BUNDLE,
--   'THEMIS_CO_PILOT', g.THEMIS_CO_PILOT,
--   'ATO', g.ATO,
--   'TEAMS_PROTECTION', g.TEAMS_PROTECTION,
--   'FILE_SCANNING', g.FILE_SCANNING,
--   'LINK_SCANNING', g.LINK_SCANNING,
--   'MULTI_TENANCY', g.MULTI_TENANCY,
--   'SAT_CONTENT_PACK', g.premium_name,
--   'DMARC', g.DMARC_MANAGEMENT
--                 ) as active_add_ons,
-- p.is_highwatermark as high_water_mark,
-- null as non_profit_flag,
-- g.partner_pricing as not_for_resale_flag,
-- null as price_per_mailbox,
-- gh.tree_key
-- from global_tenant_history_daily_agg_billing_tbl g
-- left join global_tenant_history gh on g.DATE_RECORDED = gh.record_date 
--                                                         and g.tenant_global_id = gh.tenant_global_id
                                                        
-- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- WHERE
-- REGEXP_SUBSTR(g.tenant_global_id, '[A-Za-z]+') = 'EU'
-- and gh.billing_status = 'Active'
-- and gh.approved = true
-- -- and g.record_date = current_date