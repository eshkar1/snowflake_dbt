with global_tenant_history_daily as (
    select * from {{ ref('global_tenant_history_daily_billing_tbl')}} 
),

global_tenant_history_daily_dmarc as (
    select * from {{ ref('global_tenant_history_daily_billing_DMARC_tbl')}} 
),

ltp_pricing_list as (
    select * from {{ ref('ltp_pricing_tbl')}}
    where
    tenant_global_id in ('EU-49000','EU-51541','US-11100')
    and IS_TRACKED = true
),

hwm_dmarc_count as (
    select * from {{ ref('current_month_hwm_dmarc_domains_number')}}
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
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as quantity,
g.partner_pricing,
CASE 

    WHEN ltp in ('EU-49000','EU-51541') and g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 25000 then (15000 * EP_1) + ((25000-15000) * EP_1000) + (quantity - 25000) * EP_3500
    WHEN ltp in ('EU-49000','EU-51541') and g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 15000 then (15000 * EP_1) + (quantity-15000) * EP_1000
    WHEN ltp in ('EU-49000','EU-51541') and g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity < 15000 then quantity * EP_1
    
    -- WHEN ltp in ('US-11100') and g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 25000 then (11000 * EP_1) + ((25000-11000) * EP_1000) + (quantity - 25000) * EP_3500
    -- WHEN ltp in ('US-11100') and g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 11000 then (11000 * EP_1) + (quantity-11000) * EP_1000  once 
    -- WHEN ltp in ('US-11100') and g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity < 11000 then quantity * EP_1
    WHEN ltp in ('US-11100') and g.partner_pricing = FALSE and plan_name = 'Email Protect' then GREATEST(quantity,13750) * EP_1

    WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 25000 then (15000 * CORE_1) + ((25000-15000) * CORE_1000) + (quantity - 25000) * CORE_3500
    WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 15000 then (15000 * CORE_1) + (quantity-15000) * CORE_1000
    WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity < 15000 then quantity * CORE_1
    
        -- end
    -- WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' then quantity * IP_1
    WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and quantity >= 25000 then (15000 * IP_1) + ((25000-15000) * IP_1000) + (quantity - 25000) * IP_3500
    WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and quantity >= 15000 then (15000 * IP_1) + (quantity-15000) * IP_1000
    WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and quantity < 15000 then quantity * IP_1

    -- WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' then quantity * CP_1
    WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and quantity >= 25000 then (15000 * CP_1) + ((25000-15000) * CP_1000) + (quantity - 25000) * CP_3500
    WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and quantity >= 15000 then (15000 * CP_1) + (quantity-15000) * CP_1000
    WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and quantity < 15000 then quantity * CP_1

    
    -- WHEN g.partner_pricing = FALSE and plan_name = 'SAT Suite' then quantity * SAT_SUITE_1
    WHEN g.partner_pricing = FALSE and plan_name = 'SAT Suite' and quantity >= 25000 then (15000 * SAT_SUITE_1) + ((25000-15000) * SAT_SUITE_1000) + (quantity - 25000) * SAT_SUITE_3500
    WHEN g.partner_pricing = FALSE and plan_name = 'SAT Suite' and quantity >= 15000 then (15000 * SAT_SUITE_1) + (quantity-15000) * SAT_SUITE_1000
    WHEN g.partner_pricing = FALSE and plan_name = 'SAT Suite' and quantity < 15000 then quantity * SAT_SUITE_1
    
    WHEN g.partner_pricing = FALSE and plan_name = 'Starter' then quantity * STARTER_1


    -- WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and premium_name = 'No Premium' then quantity * PST_1

    -- NFR Plans Only --

    WHEN g.partner_pricing = True and plan_name = 'Email Protect' then quantity * EPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'Core' then quantity * CORENFR_1
    WHEN g.partner_pricing = True and plan_name = 'IRONSCALES Protect' then quantity * IPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'Complete Protect' then quantity * CPNFR_1
    WHEN g.partner_pricing = True and plan_name = 'Starter' then quantity * STARTERNFR_1
    WHEN g.partner_pricing = True and plan_name = 'SAT Suite' then quantity * SAT_SUITENFR_1

    -- WHEN g.partner_pricing = True and plan_name = 'Phishing Simulation and Training' and premium_name = 'No Premium' then quantity * PSTNFR_1    
                     
end as amount        
-- my_record_date as record_date
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status in ('Active','Active-POC')
    and profile_type is not NULL
    and ltp in ('EU-49000','EU-51541','US-11100')


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
-- p.Core_10000,
-- p.CORE_7500,
p.CORE_3500,
p.CORE_1000,
p.CORE_1,
p.IP_3500,
p.IP_1000,
p.IP_1,
p.CP_3500,
p.CP_1000,
p.CP_1,
STARTER_1,
p.EPNFR_1,
p.CORENFR_1,
IPNFR_1,
CPNFR_1,
STARTERNFR_1,
SAT_SUITE_1,
SAT_SUITENFR_1,
SAT_SUITE_1000,
SAT_SUITE_3500,
SAT_SUITE_7500



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
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as quantity,
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
    and billing_status in ('Active','Active-POC')
    and ltp in ('EU-49000','EU-51541','US-11100')
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
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as quantity,
null as partner_pricing,
-- quantity * IM_1 as amount,
CASE
    WHEN quantity >= 25000 then (15000 * IM_1) + ((25000-15000) * IM_1000) + (quantity - 25000) * IM_3500
    WHEN quantity >= 15000 then (15000 * IM_1) + (quantity-15000) * IM_1000
    WHEN quantity < 15000 then quantity * IM_1
end as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status in ('Active','Active-POC')
    and ltp in ('EU-49000','EU-51541','US-11100')
    and incident_management = true
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    IM_1,
    IM_1000,
    IM_3500



-----------------------------------------
---------- SAT Bundle Plus --------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
g.root as ltp,
'SAT Bundle Plus' as item,
'IS-LTP-STBP' as sku,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as quantity,
null as partner_pricing,
quantity * STBP_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status in ('Active','Active-POC')
    and ltp in ('EU-49000','EU-51541','US-11100')
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
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as quantity,
null as partner_pricing,
quantity * ATO_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status in ('Active','Active-POC')
    and ltp in ('EU-49000','EU-51541','US-11100')
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
----------------- DMARC -----------------
-----------------------------------------

union

-- select
-- g.DATE_RECORDED,
-- g.root as ltp,
-- 'DMARC' as item,
-- 'IS-LTP-DMARC' as sku,
-- sum(d.dmarc_domains_number) as quantity,
-- null as partner_pricing,
-- quantity * DMARC_1 as amount
-- from global_tenant_history_daily g
-- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- left join hwm_dmarc_count d on g.tenant_global_id = d.tenant_global_id
-- where
--     approved = true
--     and billing_status in ('Active','Active-POC')
--     and ltp in ('EU-49000','EU-51541','US-11100')
--     -- and DMARC_MANAGEMENT = true

-- group by
--     g.DATE_RECORDED,
--     root,   
--     item,
--     sku,
--     profile_type,
--     -- g.partner_pricing,
--     DMARC_1
-- having
--     quantity is not null

select
gd.date_recorded,
gd.root as ltp,
case 
    when dmarc_ironscales_plan = 1 then 'DMARC Core Management'
    when dmarc_ironscales_plan = 2 then 'DMARC Pro'
    when dmarc_ironscales_plan = 3 then 'DMARC Premium'
end as item,
case 
    when dmarc_ironscales_plan = 1 then 'IS-LTP-DMARC'
    when dmarc_ironscales_plan = 2 then 'IS-LTP-DMARC_PRO'
    when dmarc_ironscales_plan = 3 then 'IS-LTP-DMARC_PREMIUM'
-- when dmarc_ironscales_plan = 2 then 'DMARC Premium'
end as sku,
sum(d.dmarc_domains_number) as quantity,
null as partner_pricing,
case 
    when dmarc_ironscales_plan = 1 then quantity * DMARC_1 
    when dmarc_ironscales_plan = 2 then quantity * DMARC_PRO_1 
    when dmarc_ironscales_plan = 3 then quantity * DMARC_1 
end as amount

from global_tenant_history_daily_dmarc gd
left join ltp_pricing_list p on gd.root = p.tenant_global_id
left join hwm_dmarc_count d on gd.tenant_global_id = d.tenant_global_id
WHERE
approved = true
and billing_status in ('Active','Active-POC')
and ltp in ('EU-49000','EU-51541','US-11100')
GROUP BY
gd.date_recorded,
gd.root,
item,
sku,
dmarc_ironscales_plan,
DMARC_1,
DMARC_PRO_1 
having
(quantity > 0 and quantity is not null) 