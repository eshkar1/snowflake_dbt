
with opp_table as (
    select * from {{ ref('stg_salesforce_opp_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
),

account_table as (
    select * from {{ ref('stg_salesforce_account_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
),

user_table as (
    select * from {{ ref('stg_salesforce_user_table')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
),

poc_table as (
    select * from {{ ref('stg_salesforce_poc')}}
    where
    date(_RIVERY_LAST_UPDATE) = current_date()
)


select
poc_date__c,
poc_start_date__c,
p.end_date__c,
o.name as opportunity_name,
u.name as opportunity_owner,
stagename,
p.name as poc_name,
p.start_date__c,
o.closedate as close_date,
annual_amount__c as annual_amount,
o.opportunity_arr_in_usd__c as arr_in_usd,
p.poc_w_competitors__c as poc_w_competitors,
p.silent_or_live_pov__c,
p.technical_win__c,
o.loss_reason__c,
o.lost_to_competitor__c,
p.reason_for_technical_loss__c,
o.closed_lost_details__c,
p.TECHNICAL_LOSS_REASONS__C,
case
when o.number_of_users__c < 1000 then '< 1000'
when o.number_of_users__c <= 2500 then '1000 - 2500'
when o.number_of_users__c > 2500 then '> 2500'
end as license_tier


from opp_table o
left join account_table a on o.account_master_id__c = a.account_master_id__c 
left join user_table u on o.ownerid = u.id
left join poc_table p on o.id = p.opportunity__c 
where
year( to_date(p.end_date__c)) = 2024
and u.name in (
                                        'Justin Pemberton',
                                        'Simon Carter',
                                        'David Murray',
                                        'Samuel Zejger',
                                        'Keith Berton',
                                        'Jeff Maffe',
                                        'Taylor Dee',
                                        'Carlos Casanova',
                                        'Jaime Munoz',
                                        'Diego Val',
                                        'Zachary Alexander'
                                        )
and stagename in ('Closed Lost', 'Closed Won')