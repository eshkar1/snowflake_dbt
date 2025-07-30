with current_global_tenant_by_layer as (
    select * from {{ ref('current_global_tenant_by_layer')}} 
),

ltp_pricing_list as (
    select * from {{ ref('ltp_pricing_tbl')}}
    where
    tenant_global_id not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815')
    and IS_TRACKED = true
),

hwm_dmarc_count as (
    select * from {{ ref('current_month_hwm_dmarc_domains_number')}}
),

LTP_DAILY_ITEMIZED_BILLING_TBL as (
    select * from {{ ref('ltp_daily_itemized_billing_tbl')}}
    where
    billing_date = current_date
)



----------------------------------------------------------------------------------------
                                    -- Plans --
----------------------------------------------------------------------------------------
select
g.DATE_RECORDED,
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
            WHEN 'Phishing Simulation and Training' THEN 'IS-LTP-PST'
        end
    WHEN TRUE THEN
        CASE plan_name
            WHEN 'Starter'                          THEN 'IS-LTP-STARTERNFR'
            WHEN 'Email Protect'                    THEN 'IS-LTP-EPNFR'
            WHEN 'Complete Protect'                 THEN 'IS-LTP-CPNFR'
            WHEN 'Core'                             THEN 'IS-LTP-CORENFR'
            WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IPNFR'
            WHEN 'Phishing Simulation and Training' THEN 'IS-LTP-PSTNFR'
        end    
else null
end as sku,
g.partner_pricing,   
-- p.profile_type,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,


----------------------------------------------------------------------------------------
                                    -- Plans --
----------------------------------------------------------------------------------------
-- Non NFR Plans --
case 
-- WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and i.quantity >= 7500 then billable_quantity * EP_7500
-- WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and i.quantity >= 3500 then billable_quantity * EP_3500
-- WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and i.quantity >= 1000 then billable_quantity * EP_1000
-- WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and i.quantity < 1000 then billable_quantity * EP_1

-- WHEN g.partner_pricing = FALSE and plan_name = 'Core' and i.quantity >= 7500 then billable_quantity * CORE_7500
-- WHEN g.partner_pricing = FALSE and plan_name = 'Core' and i.quantity >= 3500 then billable_quantity * CORE_3500
-- WHEN g.partner_pricing = FALSE and plan_name = 'Core' and i.quantity >= 1000 then billable_quantity * CORE_1000
-- WHEN g.partner_pricing = FALSE and plan_name = 'Core' and i.quantity < 1000 then billable_quantity * CORE_1
-- -- end
-- WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and i.quantity >= 7500 then billable_quantity * IP_7500
-- WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and i.quantity >= 3500 then billable_quantity * IP_3500
-- WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and i.quantity >= 1000 then billable_quantity * IP_1000
-- WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and i.quantity < 1000 then billable_quantity * IP_1

-- WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and i.quantity >= 7500 then billable_quantity * CP_7500
-- WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and i.quantity >= 3500 then billable_quantity * CP_3500
-- WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and i.quantity >= 1000 then billable_quantity * CP_1000
-- WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and i.quantity < 1000 then billable_quantity * CP_1

-- WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and i.quantity >= 7500 then billable_quantity * PST_7500
-- WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and i.quantity >= 3500 then billable_quantity * PST_3500
-- WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and i.quantity >= 1000 then billable_quantity * PST_1000
-- WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and i.quantity < 1000 then billable_quantity * PST_1
    
    WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' then billable_quantity * i.amount/i.quantity
    WHEN g.partner_pricing = FALSE and plan_name = 'Core' then billable_quantity * i.amount/i.quantity
    WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' then billable_quantity * i.amount/i.quantity
    WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' then billable_quantity * i.amount/i.quantity
    WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' then billable_quantity * i.amount/i.quantity    
    WHEN g.partner_pricing = FALSE and plan_name = 'Starter' then billable_quantity * i.amount/i.quantity

-- NFR Plans Only --

    WHEN g.partner_pricing = True and plan_name = 'Email Protect' then billable_quantity * EPNFR_1 
    WHEN g.partner_pricing = True and plan_name = 'Core' then billable_quantity * CORENFR_1
    WHEN g.partner_pricing = True and plan_name = 'IRONSCALES Protect' then billable_quantity * IPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'Complete Protect' then billable_quantity * CPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'Phishing Simulation and Training' then billable_quantity * PSTNFR_1   

END as amount

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id 
left join ltp_daily_itemized_billing_tbl i on g.FIRST_LAYER_ID = i.ltp 
                                            and g.plan_name = i.item
                                            and g.partner_pricing = i.partner_pricing

where
    approved = true
    and billing_status = 'Active'
    and profile_type is not NULL
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8


        ----------------------------------------------------------------------------------------
                                            -- Add Ons --
        ----------------------------------------------------------------------------------------

-------------
-- premium --
-------------

UNION

select
g.DATE_RECORDED,
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
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

case
    premium_name
    when 'NINJIO' then billable_quantity * PSCP_1
    when 'Cybermaniacs Videos' then billable_quantity * PSCP_1
    when 'Habitu8' then billable_quantity * PSCP_1
end as amount,

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
approved = true
and billing_status = 'Active'
and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
and premium_name != 'No Premium'


-------------------------
-- incident management --
-------------------------

UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Incident Management' as item,
'IS-LTP-IM' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * IM_1 as amount,

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and incident_management = true


-------------------------
---- themis co-pilot ----
-------------------------

UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'themis co-pilot' as item,
'IS-LTP-THEMIS' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * THEMIS_1 as amount,

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and themis_co_pilot = true
    and AI_EMPOWER_BUNDLE = false
    and simulation_and_training_bundle_plus = false
    and plan_name != 'Complete Protect'


-------------------------
------ url scans --------
-------------------------

UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'url scans' as item,
'IS-LTP-URL' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * URL_1 as amount,

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and link_scanning = true
    and plan_name != 'Complete Protect'
    and plan_name != 'Core'
    and plan_name != 'Email Protect'


--------------------------------
------ attachment scans --------
--------------------------------

UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'attachment scans' as item,
'IS-LTP-AS' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * AS_1 as amount,

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and file_scanning = true
    and plan_name != 'Complete Protect'
    and plan_name != 'Core'
    and plan_name != 'Email Protect'


-----------------------------------------
------ Security Awareness Training ------
-----------------------------------------

-- plan name is 'Phishing Simulation and Training' --
UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Security Awareness Training' as item,
'IS-LTP-PSTSAT' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * PSTSAT_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and security_awareness_training = true
    and simulation_and_training_bundle = false
    and simulation_and_training_bundle_plus = false
    and plan_name = 'Phishing Simulation and Training'

    
-- plan name is not 'Phishing Simulation and Training' --    
UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Security Awareness Training' as item,
'IS-LTP-SAT' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * SAT_1
 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and security_awareness_training = true
    and simulation_and_training_bundle = false
    and simulation_and_training_bundle_plus = false
    and plan_name != 'Complete Protect'
    and plan_name != 'Phishing Simulation and Training'


-----------------------------------------
------ S&T Bundle ------
-----------------------------------------

-- plan name is 'Phishing Simulation and Training' --
UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'S&T Bundle' as item,
'IS-LTP-PSTSTB' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * PSTSTB_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and simulation_and_training_bundle = true
    and simulation_and_training_bundle_plus = false
    and plan_name = 'Phishing Simulation and Training'
    and partner_pricing = false


    
-- plan name is not 'Phishing Simulation and Training' --    
UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'S&T Bundle' as item,
'IS-LTP-STB' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * STB_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and simulation_and_training_bundle = true
    and simulation_and_training_bundle_plus = false
    and plan_name != 'Complete Protect'
    and plan_name != 'Phishing Simulation and Training'
    and partner_pricing = false


--------------------------------
------ AI Empower Bundle -------
--------------------------------

UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'AI Empower Bundle' as item,
'IS-LTP-AIEB' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * AIEB_1
 as amount,

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and AI_EMPOWER_BUNDLE = true
    and SIMULATION_AND_TRAINING_BUNDLE_PLUS = false
    and plan_name != 'Complete Protect'
    and partner_pricing = false


--------------------------------
------- S&T Plus Bundle --------
--------------------------------

UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'S&T Plus Bundle' as item,
'IS-LTP-STBP' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * STBP_1 as amount,

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and SIMULATION_AND_TRAINING_BUNDLE_PLUS = true
    and plan_name != 'Complete Protect'
    and partner_pricing = false


--------------------------------
------- Account Takeover -------
--------------------------------

UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Account Takeover' as item,
'IS-LTP-ATO' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * ATO_1 as amount,

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and ATO = true
    and plan_name != 'Complete Protect'
    and partner_pricing = false


--------------------------------
--------- Multi Tenant ---------
--------------------------------

UNION

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Multi Tenant' as item,
'IS-LTP-MT' as sku,
null as partner_pricing,
CASE profile_type
    when 'active' then Active_profiles
    when 'license' then licensed_profiles
    when 'shared' then 
                    case 
                        when SHARED_PROFILES is null then Active_profiles
                        else Active_profiles - SHARED_PROFILES
                    end
end as billable_quantity,

billable_quantity * MT_1 as amount,

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and multi_tenancy = true
    and plan_name != 'Complete Protect'
    and partner_pricing = false

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
g.dmarc_domains_number as quantity,
null as partner_pricing,
quantity * DMARC_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
left join hwm_dmarc_count d on COALESCE(NULLIF(TRIM(fifth_layer_id), ''),NULLIF(TRIM(fourth_layer_id), '') , NULLIF(TRIM(third_layer_id), ''), NULLIF(TRIM(second_layer_id), ''), NULLIF(TRIM(first_layer_id), '')) = d.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and FIRST_LAYER_ID not in ('US-11100','US-733','EU-25','EU-49000','EU-51541','US-211815') -- exclude ofek & pax8
    and DMARC_MANAGEMENT = true

