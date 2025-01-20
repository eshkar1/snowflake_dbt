with sf_red_yellow_sentiment as (
    select * from {{ ref('sf_red_yellow_sentiment')}}
),

split_values AS (
                        SELECT 
                        *,
                        TRIM(f.value::STRING) as split_reason
                        FROM 
                            sf_red_yellow_sentiment,
                            TABLE(FLATTEN(SPLIT(PRODUCT_ISSUES, ';'))) f
                        WHERE 
                            PRODUCT_ISSUES IS NOT NULL
                        )

SELECT 
current_date as record_date,
last_modified_date,
industry,
account_name,
partner,
csm_owner,
tier,
renewal_date,
total_arr_in_usd,
csm_sentiment,
employees,
sub_region,
billing_country, 
account_sales_owner_name ,
last_updaterichtext ,
customer_journey_stage,
nps_score ,
reference_notes ,
onboarded ,
customer_journey_tier ,
-- product_issues,
split_reason

from split_values
where
split_reason is not null and split_reason != ''

union

select
current_date as record_date,
last_modified_date,
industry,
account_name,
partner,
csm_owner,
tier,
renewal_date,
total_arr_in_usd,
csm_sentiment,
employees,
sub_region,
billing_country, 
account_sales_owner_name ,
last_updaterichtext ,
customer_journey_stage,
nps_score ,
reference_notes ,
onboarded ,
customer_journey_tier ,
product_issues,
-- split_reason

from sf_red_yellow_sentiment
where
product_issues is null