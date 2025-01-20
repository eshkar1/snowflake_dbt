
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
)




select
churn_date__c,
u.name as csm_owner,
a.name as account_name,
o.name as opportunity_name,
o.type as type,
stagename,
o.closedate as close_date,
a.total_arr__c as total_arr,
CHANGE_IN_ARR_IN_USD__C as change_in_arr_in_usd,
previous_arr_in_usd__c as previous_arr_in_usd,
loss_reason__c as loss_reason,
a.churn_reason__c as primary_churn_reason,
closed_lost_details__c as closed_lost_details,
a.account_status__c,
a.CUSTOMER_JOURNEY_STAGE__C,
a.CSM_SENTIMENT__C,
a.sentiment_notes__c,
a.renewal_date__c as renewal_date,
o.id as opp_id,
license_start_date__c as license_start_date,
license_end_date__c as license_end_date,
a.secondary_churn_reason__c as secondary_churn_reason,
forcast__c as forecast,
o.account_master_id__c,
new_arr_in_usd__c as new_arr_in_usd, 
a.total_arr_in_usd__c as total_arr_in_usd,
at.Product_Issues__c as product_issues,
a.competitor_name__c as competitor_name,
o.number_of_users__c as number_of_users,
case
when o.number_of_users__c < 1000 then '< 1000'
when o.number_of_users__c <= 2500 then '1000 - 2500'
when o.number_of_users__c > 2500 then '> 2500'
end as license_tier
from opp_table o
left join account_table a on o.account_master_id__c = a.account_master_id__c 
left join user_table u on o.csm_owner__c = u.id
left join account_table at on a.id = at.id
where
YEAR (TO_DATE (churn_date__c)) >= 2024
and (o.type in ('Renewal', 'Renewal/Upsell')
or (o.type = 'Terminated' and o.stagename = 'Closed Won'))
and change_in_arr_in_usd__c < 0 