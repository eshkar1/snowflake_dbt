with current_global_tenant_by_layer as (
    select * from {{ ref('current_global_tenant_by_layer')}} 
),

ltp_pricing_list as (
    select * from {{ ref('ltp_pricing_tbl')}}
    where
    tenant_global_id in ('US-211815')
    and IS_TRACKED = true
),

-- hwm_dmarc_count as (
--     select * from {{ ref('current_month_hwm_dmarc_domains_number')}}
-- ),

LTP_DAILY_ITEMIZED_BILLING_TBL as (
    select * from {{ ref('ltp_daily_itemized_billing_tbl')}}
    where
    billing_date = current_date
)


----------------------------------------------------------------------------------------
                                    -- Plans --
----------------------------------------------------------------------------------------

select
g.date_recorded,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
g.plan_name as item,
CASE g.partner_pricing
    WHEN FALSE then 
        CASE plan_name
            WHEN 'Starter'                          THEN 'IS-LTP-STARTER'
            WHEN 'Email Protect'                    THEN 'IS-LTP-EP'
            WHEN 'Complete Protect'                 THEN 'IS-LTP-CP'
            WHEN 'Core'                             THEN 'IS-LTP-CORE'
            WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IP'
            WHEN 'SAT Suite'                        THEN 'IS-SAT_SUITE_1'
            
        end
    WHEN TRUE THEN
        CASE plan_name
            WHEN 'Starter'                          THEN 'IS-LTP-STARTERNFR'
            WHEN 'Email Protect'                    THEN 'IS-LTP-EPNFR'
            WHEN 'Complete Protect'                 THEN 'IS-LTP-CPNFR'
            WHEN 'Core'                             THEN 'IS-LTP-CORENFR'
            WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IPNFR'
            WHEN 'SAT Suite'                        THEN 'IS-SAT_SUITENFR_1' 
        end    
else null
end as sku, 
g.partner_pricing,   
-- p.profile_type,
CASE p.profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else (Active_profiles - SHARED_PROFILES)
                    end
end as billable_quantity,

-- Non NFR Plans --
CASE 

    -- WHEN FIRST_LAYER_ID in ('EU-49000','EU-51541') and g.partner_pricing = FALSE and plan_name = 'Email Protect' then billable_quantity * i.amount/i.quantity
    WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' then billable_quantity * i.amount/i.quantity

    WHEN g.partner_pricing = FALSE and plan_name = 'Core' then billable_quantity * i.amount/i.quantity

    WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' then billable_quantity * i.amount/i.quantity

    WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' then billable_quantity * i.amount/i.quantity

    WHEN g.partner_pricing = FALSE and plan_name = 'SAT Suite' then billable_quantity * i.amount/i.quantity
    
    WHEN g.partner_pricing = FALSE and plan_name = 'Starter' then billable_quantity * i.amount/i.quantity

    -- NFR Plans Only --

    WHEN g.partner_pricing = True and plan_name = 'Email Protect' then billable_quantity * EPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'Core' then billable_quantity * CORENFR_1
    WHEN g.partner_pricing = True and plan_name = 'IRONSCALES Protect' then billable_quantity * IPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'Complete Protect' then billable_quantity * CPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'SAT Suite' then billable_quantity * SAT_SUITENFR_1
    WHEN g.partner_pricing = True and plan_name = 'Starter' then billable_quantity * STARTERNFR_1

end as amount        
-- my_record_date as record_date
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
left join ltp_daily_itemized_billing_tbl i on g.FIRST_LAYER_ID = i.ltp 
                                            and g.plan_name = i.item
                                            and g.partner_pricing = i.partner_pricing
where
    approved = true
    and billing_status = 'Active'
    and profile_type is not NULL
    and g.FIRST_LAYER_ID in ('US-211815')
    and licensed_profiles is not NULL

----------------------------------------------------------------------------------------
                                    -- Add Ons --
----------------------------------------------------------------------------------------

-------------
-- premium --
-------------

UNION

select
g.date_recorded,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
premium_name as item,
case
    premium_name
    when 'NINJIO'              then 'IS-LTP-PSCP'
    when 'Cybermaniacs Videos' then 'IS-LTP-PSCP'
    when 'Habitu8'             then 'IS-LTP-PSCP'
end as sku,
null as partner_pricing, 
CASE p.profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else (Active_profiles - SHARED_PROFILES)
                    end
end as billable_quantity,
case
    premium_name
    when 'NINJIO' then billable_quantity * PSCP_1
    when 'Cybermaniacs Videos' then billable_quantity * PSCP_1
    when 'Habitu8' then billable_quantity * PSCP_1
end as amount

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
left join ltp_daily_itemized_billing_tbl i on g.FIRST_LAYER_ID = i.ltp 
                                            and g.plan_name = i.item
                                            and g.partner_pricing = i.partner_pricing
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('US-211815')
    and premium_name != 'No Premium'


-------------------------
-- incident management --
-------------------------

UNION

select
g.date_recorded,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Incident Management' as item,
'IS-LTP-IM' as sku,
null as partner_pricing,
CASE p.profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else (Active_profiles - SHARED_PROFILES)
                    end
end as billable_quantity,
-- billable_quantity * i.amount/i.quantity as amount
billable_quantity * IM_1
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
left join ltp_daily_itemized_billing_tbl i on g.FIRST_LAYER_ID = i.ltp 
                                            and g.plan_name = i.item
                                            and g.partner_pricing = i.partner_pricing
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('US-211815')
    and incident_management = true


-----------------------------------------
---------- S&T Bundle Plus --------------
-----------------------------------------

UNION 

select
g.date_recorded,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'S&T Bundle Plus' as item,
'IS-LTP-STBP' as sku,
null as partner_pricing,
CASE p.profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else (Active_profiles - SHARED_PROFILES)
                    end
end as billable_quantity,

billable_quantity * STBP_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('US-211815')
    and SIMULATION_AND_TRAINING_BUNDLE_PLUS = true
    and plan_name != 'Complete Protect'


-----------------------------------------
---------- Account Takeover -------------
-----------------------------------------

UNION 

select
g.date_recorded,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Account Takeover' as item,
'IS-LTP-ATO' as sku,
null as partner_pricing,
CASE p.profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else (Active_profiles - SHARED_PROFILES)
                    end
end as billable_quantity,

billable_quantity * ATO_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('US-211815')
    and ATO = true
    and plan_name != 'Complete Protect'


-----------------------------------------
----------------- DMARC -----------------
-----------------------------------------

union

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'DMARC' as item,
'IS-LTP-DMARC' as sku,
null as partner_pricing,
g.dmarc_domains_number as billable_quantity,
billable_quantity * DMARC_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
-- left join hwm_dmarc_count d on COALESCE(NULLIF(TRIM(fifth_layer_id), ''),NULLIF(TRIM(fourth_layer_id), '') , NULLIF(TRIM(third_layer_id), ''), NULLIF(TRIM(second_layer_id), ''), NULLIF(TRIM(first_layer_id), '')) = d.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('US-211815')
    -- and DMARC_MANAGEMENT = true
having
    billable_quantity is not null