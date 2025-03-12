with global_tenant_history_daily as (
    select * from {{ ref('global_tenant_history_daily_billing_tbl')}} 
),

ltp_pricing_list as (
    select * from {{ ref('ltp_pricing_tbl')}}
    where
    tenant_global_id not in ('US-11100','US-733','EU-25','EU-49000')
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

----------------------------------------------------------------------------------------
                                    -- Plans --
----------------------------------------------------------------------------------------
-- Non NFR Plans --
case 
WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 7500 then quantity * EP_7500
WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 3500 then quantity * EP_3500
WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 1000 then quantity * EP_1000
WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity < 1000 then quantity * EP_1

WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 7500 then quantity * CORE_7500
WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 3500 then quantity * CORE_3500
WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 1000 then quantity * CORE_1000
WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity < 1000 then quantity * CORE_1
-- end
WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and quantity >= 7500 then quantity * IP_7500
WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and quantity >= 3500 then quantity * IP_3500
WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and quantity >= 1000 then quantity * IP_1000
WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' and quantity < 1000 then quantity * IP_1

WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and quantity >= 7500 then quantity * CP_7500
WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and quantity >= 3500 then quantity * CP_3500
WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and quantity >= 1000 then quantity * CP_1000
WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and quantity < 1000 then quantity * CP_1

WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and quantity >= 7500 then quantity * PST_7500
WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and quantity >= 3500 then quantity * PST_3500
WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and quantity >= 1000 then quantity * PST_1000
WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and quantity < 1000 then quantity * PST_1

-- NFR Plans Only --

WHEN g.partner_pricing = True and plan_name = 'Email Protect' then quantity * EPNFR_1 
WHEN g.partner_pricing = True and plan_name = 'Core' then quantity * CORENFR_1
WHEN g.partner_pricing = True and plan_name = 'IRONSCALES Protect' then quantity * IPNFR_1
WHEN g.partner_pricing = True and plan_name = 'Complete Protect' then quantity * CPNFR_1
WHEN g.partner_pricing = True and plan_name = 'Phishing Simulation and Training' then quantity * PSTNFR_1   

END as amount

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and profile_type is not NULL
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
group by
    g.DATE_RECORDED,
    root,   
    plan_name,
    sku,
    profile_type,
    -- premium_name, --- need to think of a way to remove this ----
    g.partner_pricing,
    p.EP_7500,
    p.EP_3500,
    p.EP_1000,
    p.EP_1,
    p.Core_10000,
    p.CORE_7500,
    p.CORE_3500,
    p.CORE_1000,
    p.CORE_1,
    p.IP_7500,
    p.IP_3500,
    p.IP_1000,
    p.IP_1,
    p.CP_7500,
    p.CP_3500,
    p.CP_1000,
    p.CP_1,
    p.PST_7500,
    p.PST_3500,
    p.PST_1000,
    p.PST_1,
    p.EPNFR_1,
    p.CORENFR_1,
    IPNFR_1,
    CPNFR_1,
    PSTNFR_1

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
and billing_status = 'Active'
and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
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
quantity * IM_1 as amount,

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
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
---- themis co-pilot ----
-------------------------

UNION

select
g.DATE_RECORDED,
g.root as ltp,
'themis co-pilot' as item,
'IS-LTP-THEMIS' as sku,
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
quantity * THEMIS_1 as amount,

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
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

-------------------------
------ url scans --------
-------------------------

UNION

select
g.DATE_RECORDED,
g.root as ltp,
'url scans' as item,
'IS-LTP-URL' as sku,
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
quantity * URL_1 as amount,

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
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

--------------------------------
------ attachment scans --------
--------------------------------

UNION

select
g.DATE_RECORDED,
g.root as ltp,
'attachment scans' as item,
'IS-LTP-AS' as sku,
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
quantity * AS_1 as amount,

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
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
------ Security Awareness Training ------
-----------------------------------------

-- plan name is 'Phishing Simulation and Training' --
UNION

select
g.DATE_RECORDED,
g.root as ltp,
'Security Awareness Training' as item,
'IS-LTP-PSTSAT' as sku,
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
quantity * PSTSAT_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
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
quantity * SAT_1
 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
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
------ S&T Bundle ------
-----------------------------------------

-- plan name is 'Phishing Simulation and Training' --
UNION

select
g.DATE_RECORDED,
g.root as ltp,
'S&T Bundle' as item,
'IS-LTP-PSTSTB' as sku,
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
quantity * PSTSTB_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
    and simulation_and_training_bundle = true
    and simulation_and_training_bundle_plus = false
    and plan_name = 'Phishing Simulation and Training'
    and partner_pricing = false
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
quantity * STB_1 as amount
from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
    and simulation_and_training_bundle = true
    and simulation_and_training_bundle_plus = false
    and plan_name != 'Complete Protect'
    and plan_name != 'Phishing Simulation and Training'
    and partner_pricing = false
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    STB_1

--------------------------------
------ AI Empower Bundle -------
--------------------------------

UNION

select
g.DATE_RECORDED,
g.root as ltp,
'AI Empower Bundle' as item,
'IS-LTP-AIEB' as sku,
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
quantity * AIEB_1
 as amount,

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
    and AI_EMPOWER_BUNDLE = true
    and SIMULATION_AND_TRAINING_BUNDLE_PLUS = false
    and plan_name != 'Complete Protect'
    and partner_pricing = false
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    AIEB_1

--------------------------------
------- S&T Plus Bundle --------
--------------------------------

UNION

select
g.DATE_RECORDED,
g.root as ltp,
'S&T Plus Bundle' as item,
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
quantity * STBP_1 as amount,

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
    and SIMULATION_AND_TRAINING_BUNDLE_PLUS = true
    and plan_name != 'Complete Protect'
    and partner_pricing = false
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    STBP_1

--------------------------------
------- Account Takeover -------
--------------------------------

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
quantity * ATO_1 as amount,

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
    and ATO = true
    and plan_name != 'Complete Protect'
    and partner_pricing = false
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    ATO_1

--------------------------------
--------- Multi Tenant ---------
--------------------------------

UNION

select
g.DATE_RECORDED,
g.root as ltp,
'Multi Tenant' as item,
'IS-LTP-MT' as sku,
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
quantity * MT_1 as amount,

from global_tenant_history_daily g
left join ltp_pricing_list p on g.root = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and ltp not in ('US-11100','US-733','EU-25') -- exclude ofek & pax8
    and multi_tenancy = true
    and plan_name != 'Complete Protect'
    and partner_pricing = false
group by
    g.DATE_RECORDED,
    root,   
    item,
    sku,
    profile_type,
    -- g.partner_pricing,
    MT_1