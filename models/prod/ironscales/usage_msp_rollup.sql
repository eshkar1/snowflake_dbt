
with global_tenant_history as (
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

sltp_daily_itemized_billing_tbl as (
    select * from 
    -- prod_conform.dbt_prod_db.sltp_daily_itemized_billing_tbl
    {{ ref('sltp_daily_itemized_billing_tbl')}}
    WHERE
    billing_date = current_date
),

billing_base AS (
  SELECT
  FIRST_LAYER_ID,
  SECOND_LAYER_ID,
  THIRD_LAYER_ID,
  FOURTH_LAYER_ID,
  FIFTH_LAYER_ID,
    CASE
        WHEN FIFTH_LAYER_ID IS NOT NULL AND FIFTH_LAYER_ID <> '' THEN FIFTH_LAYER_ID
        WHEN FOURTH_LAYER_ID IS NOT NULL AND FOURTH_LAYER_ID <> '' THEN FOURTH_LAYER_ID
        WHEN THIRD_LAYER_ID IS NOT NULL AND THIRD_LAYER_ID <> '' THEN THIRD_LAYER_ID
        WHEN SECOND_LAYER_ID IS NOT NULL AND SECOND_LAYER_ID <> '' THEN SECOND_LAYER_ID
        ELSE FIRST_LAYER_ID
    END as tenant_global_id,
    item,
    partner_pricing,
    billable_quantity,
    l.ltp_type
  FROM sltp_daily_itemized_billing_tbl s
  left join ltp_pricing_list l on s.FIRST_LAYER_ID = l.tenant_global_id and l.snapshot_date = current_date
--   WHERE billing_date = CURRENT_DATE
),

sltp_bill AS (

    SELECT
      FIRST_LAYER_ID,
      SECOND_LAYER_ID,
      THIRD_LAYER_ID,
      FOURTH_LAYER_ID,
      FIFTH_LAYER_ID,
      tenant_global_id,
      ltp_type,
      partner_pricing,
    
      -- Email Protect
      MAX(CASE WHEN item = 'Email Protect' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as Email_Protect_of_Licenses,

      MAX(CASE WHEN item = 'Email Protect' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as Email_Protect_NFR_of_Licenses,
    
      -- Complete Protect
      MAX(CASE WHEN item = 'Complete Protect' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as Complete_Protect_of_Licenses,

      MAX(CASE WHEN item = 'Complete Protect' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as Complete_Protect_NFR_of_Licenses,
      
      -- Core
      MAX(CASE WHEN item = 'Core' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as Core_of_Licenses,

      MAX(CASE WHEN item = 'Core' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as Core_NFR_of_Licenses,
      
      -- IRONSCALES Protect
      MAX(CASE WHEN item = 'IRONSCALES Protect' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as Ironscales_Protect_of_Licenses,

      MAX(CASE WHEN item = 'IRONSCALES Protect' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as Ironscales_Protect_NFR_of_Licenses,
      
      -- Phishing Simulation and Training
      MAX(CASE WHEN item = 'Phishing Simulation and Training' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as PST_of_Licenses,

      MAX(CASE WHEN item = 'Phishing Simulation and Training' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as PST_NFR_of_Licenses,
    
      -- Starter
      MAX(CASE WHEN item = 'Starter' THEN billable_quantity ELSE 0 END) as Starter_of_Licenses,
    
      -- Premium Content
      MAX(CASE WHEN item IN ('Habitu8','NINJIO','Cybermaniacs Videos') THEN billable_quantity ELSE 0 END) as Premium_Content_of_Licenses,

    --   MAX(CASE WHEN item IN ('NINJIO') THEN billable_quantity ELSE 0 END) as Premium_Content_NINJIO_of_Licenses,

    --   MAX(CASE WHEN item IN ('Cybermaniacs Videos') THEN billable_quantity ELSE 0 END) as Premium_Content_Cybermaniacs_of_Licenses,
      
      -- Account Takeover
      MAX(CASE WHEN item = 'Account Takeover' THEN billable_quantity ELSE 0 END) as Account_Takeover_of_Licenses,
    
      -- Incident Management
      MAX(CASE WHEN item = 'Incident Management' THEN billable_quantity ELSE 0 END) as Incident_Management_of_Licenses,
    
      -- Multi Tenant
      MAX(CASE WHEN item = 'Multi Tenant' THEN billable_quantity ELSE 0 END) as Multi_Tenant_of_Licenses,
    
      -- S&T Bundle
      MAX(CASE WHEN item = 'S&T Bundle' THEN billable_quantity ELSE 0 END) as ST_Bundle_of_Licenses,
    
      -- S&T Plus Bundle
      MAX(CASE WHEN item = 'S&T Plus Bundle' THEN billable_quantity ELSE 0 END) as ST_Plus_Bundle_of_Licenses,
    
      -- Security Awareness Training
      MAX(CASE WHEN item = 'Security Awareness Training' THEN billable_quantity ELSE 0 END) as SAT_of_Licenses,

      -- Security Awareness Training
      MAX(CASE WHEN item = 'SAT Suite' AND partner_pricing = false THEN billable_quantity ELSE 0 END) as SAT_Suite_of_Licenses,

      MAX(CASE WHEN item = 'SAT Suite' AND partner_pricing = true THEN billable_quantity ELSE 0 END) as SAT_Suite_NFR_of_Licenses, 

            -- DMARC ---
      MAX(CASE WHEN item = 'DMARC' THEN billable_quantity ELSE 0 END) as DMARC_Management_of_Licenses 
    
    FROM billing_base
    GROUP BY 1,2,3,4,5,6,7,8
    ),


main_with_revenue as (
    select
    FIRST_LAYER_ID,
    SECOND_LAYER_ID,
    sum(Email_Protect_of_Licenses),
    sum(Email_Protect_NFR_of_Licenses),
    sum(Complete_Protect_of_Licenses),
    sum(Complete_Protect_NFR_of_Licenses),
    sum(Core_of_Licenses),
    sum(Core_NFR_of_Licenses),
    sum(Ironscales_Protect_of_Licenses),
    sum(Ironscales_Protect_NFR_of_Licenses),
    sum(PST_of_Licenses),
    sum(PST_NFR_of_Licenses),
    sum(Starter_of_Licenses),
    sum(Premium_Content_of_Licenses),
    -- sum(Premium_Content_NINJIO_of_Licenses),
    -- sum(Premium_Content_Cybermaniacs_of_Licenses),
    sum(Account_Takeover_of_Licenses),
    sum(Incident_Management_of_Licenses),
    sum(Multi_Tenant_of_Licenses),
    sum(ST_Bundle_of_Licenses),
    sum(ST_Plus_Bundle_of_Licenses),
    sum(SAT_of_Licenses),
    sum(SAT_Suite_of_Licenses),
    sum(SAT_Suite_NFR_of_Licenses),
    sum(DMARC_Management_of_Licenses)
    from sltp_bill
    group by 1,2
    ),

main_with_revenue_direct as (
        select
    FIRST_LAYER_ID,
    sum(Email_Protect_of_Licenses),
    sum(Email_Protect_NFR_of_Licenses),
    sum(Complete_Protect_of_Licenses),
    sum(Complete_Protect_NFR_of_Licenses),
    sum(Core_of_Licenses),
    sum(Core_NFR_of_Licenses),
    sum(Ironscales_Protect_of_Licenses),
    sum(Ironscales_Protect_NFR_of_Licenses),
    sum(PST_of_Licenses),
    sum(PST_NFR_of_Licenses),
    sum(Starter_of_Licenses),
    sum(Premium_Content_of_Licenses),
    -- sum(Premium_Content_NINJIO_of_Licenses),
    -- sum(Premium_Content_Cybermaniacs_of_Licenses),
    sum(Account_Takeover_of_Licenses),
    sum(Incident_Management_of_Licenses),
    sum(Multi_Tenant_of_Licenses),
    sum(ST_Bundle_of_Licenses),
    sum(ST_Plus_Bundle_of_Licenses),
    sum(SAT_of_Licenses),
    sum(SAT_Suite_of_Licenses),
    sum(SAT_Suite_NFR_of_Licenses),
    sum(DMARC_Management_of_Licenses)
    from sltp_bill
    group by 1
),

second_tier_msp as (
    select
    FIRST_LAYER_ID as msp_parent_id,
    g.tenant_name as msp_parent_name,
    SECOND_LAYER_ID as msp_id,
    gt.tenant_name as msp_name,
    sum(Email_Protect_of_Licenses)              as Email_Protect_of_Licenses,
    sum(Email_Protect_NFR_of_Licenses)          as Email_Protect_NFR_of_Licenses,
    sum(Complete_Protect_of_Licenses)           as Complete_Protect_of_Licenses,
    sum(Complete_Protect_NFR_of_Licenses)       as Complete_Protect_NFR_of_Licenses,
    sum(Core_of_Licenses)                        as Core_of_Licenses,
    sum(Core_NFR_of_Licenses)                    as Core_NFR_of_Licenses,
    sum(Ironscales_Protect_of_Licenses)          as Ironscales_Protect_of_Licenses,
    sum(Ironscales_Protect_NFR_of_Licenses)      as Ironscales_Protect_NFR_of_Licenses,
    sum(PST_of_Licenses)                          as PST_of_Licenses,
    sum(PST_NFR_of_Licenses)                      as PST_NFR_of_Licenses,
    sum(Starter_of_Licenses)                      as Starter_of_Licenses,
    sum(Premium_Content_of_Licenses)             as Premium_Content_of_Licenses,
    -- sum(Premium_Content_NINJIO_of_Licenses)       as Premium_Content_NINJIO_of_Licenses,
    -- sum(Premium_Content_Cybermaniacs_of_Licenses) as Premium_Content_Cybermaniacs_of_Licenses,
    sum(Account_Takeover_of_Licenses)             as Account_Takeover_of_Licenses,
    sum(Incident_Management_of_Licenses)          as Incident_Management_of_Licenses,
    sum(Multi_Tenant_of_Licenses)                 as Multi_Tenant_of_Licenses,
    sum(ST_Bundle_of_Licenses)                    as ST_Bundle_of_Licenses,
    sum(ST_Plus_Bundle_of_Licenses)               as ST_Plus_Bundle_of_Licenses,
    sum(SAT_of_Licenses)                          as SAT_of_Licenses,
    sum(SAT_Suite_of_Licenses)                    as SAT_Suite_of_Licenses,
    sum(SAT_Suite_NFR_of_Licenses)                as SAT_Suite_NFR_of_Licenses,
    sum(DMARC_Management_of_Licenses)             as DMARC_Management_of_Licenses
    from sltp_bill s
    left join global_tenant_history g on s.FIRST_LAYER_ID = g.tenant_global_id
    left join global_tenant_history gt on s.SECOND_LAYER_ID = gt.tenant_global_id
    where ltp_type in ('disti','oem')
    and s.partner_pricing = true
    group by 1,2,3,4
),

direct_msp as (
    select
    FIRST_LAYER_ID as msp_parent_id,
    g.tenant_name as msp_parent_name,
    FIRST_LAYER_ID as msp_id,
    g.tenant_name as msp_name,
    sum(Email_Protect_of_Licenses)              as Email_Protect_of_Licenses,
    sum(Email_Protect_NFR_of_Licenses)          as Email_Protect_NFR_of_Licenses,
    sum(Complete_Protect_of_Licenses)           as Complete_Protect_of_Licenses,
    sum(Complete_Protect_NFR_of_Licenses)       as Complete_Protect_NFR_of_Licenses,
    sum(Core_of_Licenses)                        as Core_of_Licenses,
    sum(Core_NFR_of_Licenses)                    as Core_NFR_of_Licenses,
    sum(Ironscales_Protect_of_Licenses)          as Ironscales_Protect_of_Licenses,
    sum(Ironscales_Protect_NFR_of_Licenses)      as Ironscales_Protect_NFR_of_Licenses,
    sum(PST_of_Licenses)                          as PST_of_Licenses,
    sum(PST_NFR_of_Licenses)                      as PST_NFR_of_Licenses,
    sum(Starter_of_Licenses)                      as Starter_of_Licenses,
    sum(Premium_Content_of_Licenses)                as Premium_Content_of_Licenses,
    -- sum(Premium_Content_NINJIO_of_Licenses)       as Premium_Content_NINJIO_of_Licenses,
    -- sum(Premium_Content_Cybermaniacs_of_Licenses) as Premium_Content_Cybermaniacs_of_Licenses,
    sum(Account_Takeover_of_Licenses)             as Account_Takeover_of_Licenses,
    sum(Incident_Management_of_Licenses)          as Incident_Management_of_Licenses,
    sum(Multi_Tenant_of_Licenses)                 as Multi_Tenant_of_Licenses,
    sum(ST_Bundle_of_Licenses)                    as ST_Bundle_of_Licenses,
    sum(ST_Plus_Bundle_of_Licenses)               as ST_Plus_Bundle_of_Licenses,
    sum(SAT_of_Licenses)                          as SAT_of_Licenses,
    sum(SAT_Suite_of_Licenses)                    as SAT_Suite_of_Licenses,
    sum(SAT_Suite_NFR_of_Licenses)                as SAT_Suite_NFR_of_Licenses,
    sum(DMARC_Management_of_Licenses)             as DMARC_Management_of_Licenses
    from sltp_bill s
    left join global_tenant_history g on s.FIRST_LAYER_ID = g.tenant_global_id
    where ltp_type = 'msp' 
    and s.partner_pricing = true
    group by 1,2,3,4
)

select
  'second_tier' as msp_type,
  *
from second_tier_msp

union all

select
  'direct' as msp_type,
  *
from direct_msp