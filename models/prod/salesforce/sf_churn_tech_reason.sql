with sf_churn as (
    select * from {{ ref('sf_churn')}}
),

split_values AS (
                        SELECT 
                        *,
                        TRIM(f.value::STRING) as split_reason
                        FROM 
                            sf_churn,
                            TABLE(FLATTEN(SPLIT(product_issues, ';'))) f
                        WHERE 
                            PRODUCT_ISSUES IS NOT NULL
                        )

SELECT 
CHURN_DATE__C as churn_date,
csm_owner,
account_name,
opportunity_name,
type,
stagename,
close_date,
total_arr,
change_in_arr_in_usd,
previous_arr_in_usd,
loss_reason,
primary_churn_reason,
closed_lost_details,
account_status__c as account_status,
customer_journey_stage__c,
csm_sentiment__c as csm_sentiment,
sentiment_notes__c as sentiment_note,
renewal_date,
license_start_date,
license_end_date,
secondary_churn_reason,
forecast,
account_master_id__c as account_master_id,
new_arr_in_usd,
total_arr_in_usd,
split_reason,
competitor_name,
number_of_users,
license_tier
FROM 
    split_values
WHERE 
    split_reason IS NOT NULL
    AND split_reason != ''
union 

select
CHURN_DATE__C as churn_date,
csm_owner,
account_name,
opportunity_name,
type,
stagename,
close_date,
total_arr,
change_in_arr_in_usd,
previous_arr_in_usd,
loss_reason,
primary_churn_reason,
closed_lost_details,
account_status__c as account_status,
customer_journey_stage__c,
csm_sentiment__c as csm_sentiment,
sentiment_notes__c as sentiment_note,
renewal_date,
license_start_date,
license_end_date,
secondary_churn_reason,
forecast,
account_master_id__c as account_master_id,
new_arr_in_usd,
total_arr_in_usd,
PRODUCT_ISSUES,
competitor_name,
number_of_users,
license_tier
from sf_churn
where
    product_issues is null