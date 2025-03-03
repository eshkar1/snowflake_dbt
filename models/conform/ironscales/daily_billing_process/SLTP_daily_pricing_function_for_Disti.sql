with current_global_tenant_by_layer as (
    select * from {{ ref('current_global_tenant_by_layer')}} 
),

ltp_pricing_list as (
    select * from {{ ref('ltp_pricing_tbl')}}
    where
    tenant_global_id in ('EU-49000')
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
g.partner_pricing,   
-- p.profile_type,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,

-- Non NFR Plans --
CASE 

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
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and plan_name != 'Phishing Simulation and Training'
    and licensed_profiles is not NULL

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
null as partner_pricing, 
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
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
    and g.FIRST_LAYER_ID in ('EU-49000') 
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
null as partner_pricing,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,
billable_quantity * i.amount/i.quantity as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
left join ltp_daily_itemized_billing_tbl i on g.FIRST_LAYER_ID = i.ltp 
                                            and g.plan_name = i.item
                                            and g.partner_pricing = i.partner_pricing
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and incident_management = true



-------------------------
------ S&T Bundle -------
-------------------------

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
null as partner_pricing,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,
billable_quantity * i.amount/i.quantity as amount

from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
left join ltp_daily_itemized_billing_tbl i on g.FIRST_LAYER_ID = i.ltp 
                                            and g.plan_name = i.item
                                            and g.partner_pricing = i.partner_pricing
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and simulation_and_training_bundle = true
    and simulation_and_training_bundle_plus = false
    and plan_name = 'Phishing Simulation and Training'
    and g.partner_pricing = false


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
null as partner_pricing,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,

billable_quantity * SAT_1
 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and security_awareness_training = true
    and simulation_and_training_bundle = false
    and simulation_and_training_bundle_plus = false
    and plan_name != 'Complete Protect'
    and plan_name != 'Phishing Simulation and Training'


-----------------------------------------
------------ themis co-pilot ------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Themis Co-Pilot' as item,
null as partner_pricing,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,
billable_quantity * THEMIS_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and themis_co_pilot = true
    and AI_EMPOWER_BUNDLE = false
    and simulation_and_training_bundle_plus = false
    and plan_name != 'Complete Protect'


-----------------------------------------
--------------- url scans ---------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'URL Scans' as item,
null as partner_pricing,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,

billable_quantity * URL_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and link_scanning = true
    and plan_name != 'Complete Protect'
    and plan_name != 'Core'
    and plan_name != 'Email Protect'


    
-----------------------------------------
------------ attachment scans -----------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Attachment Scans' as item,
null as partner_pricing,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,

billable_quantity * AS_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and file_scanning = true
    and plan_name != 'Complete Protect'
    and plan_name != 'Core'
    and plan_name != 'Email Protect'


        
-----------------------------------------
---------- -AI Empower Bundle -----------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'AI Empower Bundle' as item,
null as partner_pricing,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,

billable_quantity * AIEB_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and AI_EMPOWER_BUNDLE = true
    and SIMULATION_AND_TRAINING_BUNDLE_PLUS = false
    and plan_name != 'Complete Protect'


-----------------------------------------
---------- S&T Plus Bundle --------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'S&T Plus Bundle' as item,
null as partner_pricing,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,

billable_quantity * STBP_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and SIMULATION_AND_TRAINING_BUNDLE_PLUS = true
    and plan_name != 'Complete Protect'


-----------------------------------------
---------- Account Takeover -------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Account Takeover' as item,
null as partner_pricing,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,

billable_quantity * ATO_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and ATO = true
    and plan_name != 'Complete Protect'


-----------------------------------------
---------- Multi Tenant -------------
-----------------------------------------

UNION 

select
g.DATE_RECORDED,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
'Multi Tenant' as item,
null as partner_pricing,
CASE p.profile_type
    when 'active' then sum(Active_profiles)
    when 'license' then sum(licensed_profiles)
    when 'shared' then 
                    case 
                        when sum(SHARED_PROFILES) is null then sum(Active_profiles)
                        else (sum(Active_profiles) - sum(SHARED_PROFILES))
                    end
end as billable_quantity,

billable_quantity * MT_1 as amount
from current_global_tenant_by_layer g
left join ltp_pricing_list p on g.FIRST_LAYER_ID = p.tenant_global_id
where
    approved = true
    and billing_status = 'Active'
    and g.FIRST_LAYER_ID in ('EU-49000') 
    and multi_tenancy = true
    and plan_name != 'Complete Protect'
