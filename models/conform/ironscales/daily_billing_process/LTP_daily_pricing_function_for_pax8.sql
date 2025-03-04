with global_tenant_history_daily as (
    select * from {{ ref('global_tenant_history_daily_billing_tbl')}} 
),

ltp_pricing_list as (
    select * from {{ ref('ltp_pricing_tbl')}}
    where
    tenant_global_id in ('US-733','EU-25')
)


----------------------------------------------------------------------------------------
                                    -- Plans --
----------------------------------------------------------------------------------------

select
g.DATE_RECORDED,
g.root as ltp,
g.plan_name as item,  
CASE partner_pricing
    WHEN FALSE then 
        CASE plan_name
            WHEN 'Starter'                          THEN 'IS-LTP-STARTER'
            WHEN 'Email Protect'                    THEN 'IS-LTP-EP'
            WHEN 'Complete Protect'                 THEN 'IS-LTP-CP'
            WHEN 'Core'                             THEN 'IS-LTP-CORE'
            WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IP'
        end
    WHEN TRUE THEN
        CASE plan_name
            WHEN 'Starter'                          THEN 'IS-LTP-STARTERNFR'
            WHEN 'Email Protect'                    THEN 'IS-LTP-EPNFR'
            WHEN 'Complete Protect'                 THEN 'IS-LTP-CPNFR'
            WHEN 'Core'                             THEN 'IS-LTP-CORENFR'
            WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IPNFR'
        end    
else null
end as sku,
sum(licensed_profiles) as quantity,
g.partner_pricing,


-- Non NFR Plans --
CASE 

    WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 160000 then (150000 * EP_1) + ((160000-150000) * EP_1000) + (quantity - 160000) * EP_3500
    WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 150000 then (150000 * EP_1) + (quantity-150000) * EP_1000
    WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity < 150000 then quantity * EP_1
    
    WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 50000 then (5000 * CORE_1) + ((10000-5000) * CORE_1000) + ((25000-10000) * CORE_3500) + ((50000-25000) * CORE_7500) + (quantity-50000) * CORE_10000
    WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 25000 then (5000 * CORE_1) + ((10000-5000) * CORE_1000) + ((25000-10000) * CORE_3500) + (quantity-25000) * CORE_7500
    WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 10000 then (5000 * CORE_1) + ((10000-5000) * CORE_1000) + (quantity-10000) * CORE_3500
    WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 5000 then (5000 * CORE_1) + (quantity-5000) * CORE_1000
    WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity < 5000 then quantity * CORE_1
    -- end
    WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' then quantity * IP_1

    WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' then quantity * CP_1

    WHEN g.partner_pricing = FALSE and plan_name = 'Starter' then quantity * STARTER_1


    -- WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and premium_name = 'No Premium' then quantity * PST_1

    -- NFR Plans Only --

    WHEN g.partner_pricing = True and plan_name = 'Email Protect' then quantity * EPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'Core' then quantity * CORENFR_1
    WHEN g.partner_pricing = True and plan_name = 'IRONSCALES Protect' then quantity * IPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'Complete Protect' then quantity * CPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'Starter' then quantity * STARTERNFR_1

    -- WHEN g.partner_pricing = True and plan_name = 'Phishing Simulation and Training' and premium_name = 'No Premium' then quantity * PSTNFR_1    
                     
end as amount        
-- my_record_date as record_date
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and profile_type is not NULL
    and ltp in ('US-733','EU-25') 
    and plan_name != 'Phishing Simulation and Training'
    and licensed_profiles is not NULL

group by
g.DATE_RECORDED,
root,   
plan_name,
sku,
profile_type,
-- premium_name,
g.partner_pricing,
p.EP_3500,
p.EP_1000,
p.EP_1,
p.Core_10000,
p.CORE_7500,
p.CORE_3500,
p.CORE_1000,
p.CORE_1,
p.IP_1,
p.CP_1,
STARTER_1,
p.EPNFR_1,
p.CORENFR_1,
IPNFR_1,
CPNFR_1,
STARTERNFR_1


-------------------------------------------
---- Phishing Simulation and Training -----
-------------------------------------------
UNION

select
g.DATE_RECORDED,
g.root as ltp,
g.plan_name as item,    
CASE
    WHEN g.partner_pricing = True then  'IS-LTP-PSTNFR'
    WHEN g.partner_pricing = False then  'IS-LTP-PST'
end as sku,
sum(licensed_profiles) as quantity,
g.partner_pricing,
CASE
    WHEN g.partner_pricing = True then  quantity * PSTNFR_1
    WHEN g.partner_pricing = False then  quantity * PST_1
end as amount,
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and profile_type is not NULL
    and ltp in ('US-733','EU-25')  
    and plan_name = 'Phishing Simulation and Training'
    and premium_name = 'No Premium'
group by 
g.DATE_RECORDED,
root,   
plan_name,
sku,
g.partner_pricing,
PSTNFR_1,
PST_1

----------------------------------------------------------------------------------------
                                    -- Add Ons --
----------------------------------------------------------------------------------------

-------------
-- premium --
-------------

UNION

select
g.DATE_RECORDED,
g.root as ltp,
premium_name as item,
case
    premium_name
    when 'NINJIO'              then 'IS-LTP-PSCP'
    when 'Cybermaniacs Videos' then 'IS-LTP-PSCP'
    when 'Habitu8'             then 'IS-LTP-PSCP'
end as sku, 
sum(licensed_profiles) as quantity,
null as partner_pricing,
case
    premium_name
    when 'NINJIO' then quantity * PSCP_1
    when 'Cybermaniacs Videos' then quantity * PSCP_1
    when 'Habitu8' then quantity * PSCP_1
end as amount,

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and premium_name != 'No Premium'
group by                              
g.DATE_RECORDED,
root,   
item,
sku,
profile_type,
-- g.partner_pricing,
premium_name,
PSCP_1

-------------------------
-- incident management --
-------------------------

UNION

select
g.DATE_RECORDED,
g.root as ltp,
'Incident Management' as item,
'IS-LTP-IM' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * IM_1 as amount

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and incident_management = true
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    IM_1


-------------------------
------ S&T Bundle -------
-------------------------

-- plan name is 'Phishing Simulation and Training' --
UNION

select
g.DATE_RECORDED,
g.root as ltp,
'S&T Bundle' as item,
'IS-LTP-PSTSTB' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * PSTSTB_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and simulation_and_training_bundle = true
    and simulation_and_training_bundle_plus = false
    and plan_name = 'Phishing Simulation and Training'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    PSTSTB_1

-- plan name is not 'Phishing Simulation and Training' --
UNION

select
g.DATE_RECORDED,
g.root as ltp,
'S&T Bundle' as item,
'IS-LTP-STB' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * STB_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and simulation_and_training_bundle = true
    and simulation_and_training_bundle_plus = false
    and plan_name != 'Complete Protect'
    and plan_name != 'Phishing Simulation and Training'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    STB_1

-----------------------------------------
------ Security Awareness Training ------
-----------------------------------------

-- plan name is 'Phishing Simulation and Training' --
UNION

select
g.DATE_RECORDED,
g.root as ltp,
'Security Awareness Training' as item,
'IS-LTP-PSTSAT' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * PSTSAT_1

 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and security_awareness_training = true
    and simulation_and_training_bundle = false
    and simulation_and_training_bundle_plus = false
    and plan_name = 'Phishing Simulation and Training'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    PSTSAT_1
    
-- plan name is not 'Phishing Simulation and Training' --    
UNION

select
g.DATE_RECORDED,
g.root as ltp,
'Security Awareness Training' as item,
'IS-LTP-SAT' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * SAT_1
 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and security_awareness_training = true
    and simulation_and_training_bundle = false
    and simulation_and_training_bundle_plus = false
    and plan_name != 'Complete Protect'
    and plan_name != 'Phishing Simulation and Training'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    SAT_1

-----------------------------------------
------------ themis co-pilot ------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
g.root as ltp,
'Themis Co-Pilot' as item,
'IS-LTP-THEMIS' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * THEMIS_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and themis_co_pilot = true
    and AI_EMPOWER_BUNDLE = false
    and simulation_and_training_bundle_plus = false
    and plan_name != 'Complete Protect'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    THEMIS_1

-----------------------------------------
--------------- url scans ---------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
g.root as ltp,
'URL Scans' as item,
'IS-LTP-URL' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * URL_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and link_scanning = true
    and plan_name != 'Complete Protect'
    and plan_name != 'Core'
    and plan_name != 'Email Protect'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    URL_1

    
-----------------------------------------
------------ attachment scans -----------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
g.root as ltp,
'Attachment Scans' as item,
'IS-LTP-AS' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * AS_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and file_scanning = true
    and plan_name != 'Complete Protect'
    and plan_name != 'Core'
    and plan_name != 'Email Protect'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    AS_1

        
-----------------------------------------
---------- -AI Empower Bundle -----------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
g.root as ltp,
'AI Empower Bundle' as item,
'IS-LTP-AIEB' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * AIEB_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and AI_EMPOWER_BUNDLE = true
    and SIMULATION_AND_TRAINING_BUNDLE_PLUS = false
    and plan_name != 'Complete Protect'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    AIEB_1

-----------------------------------------
---------- S&T Plus Bundle --------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
g.root as ltp,
'S&T Plus Bundle' as item,
'IS-LTP-STBP' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * STBP_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and SIMULATION_AND_TRAINING_BUNDLE_PLUS = true
    and plan_name != 'Complete Protect'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    STBP_1

-----------------------------------------
---------- Account Takeover -------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
g.root as ltp,
'Account Takeover' as item,
'IS-LTP-ATO' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * ATO_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and ATO = true
    and plan_name != 'Complete Protect'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    ATO_1

-----------------------------------------
---------- Multi Tenant -------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
g.root as ltp,
'Multi Tenant' as item,
'IS-LTP-MT' as sku,
sum(licensed_profiles) as quantity,
null as partner_pricing,
quantity * MT_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp in ('US-733','EU-25') 
    and multi_tenancy = true
    and plan_name != 'Complete Protect'
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    MT_1