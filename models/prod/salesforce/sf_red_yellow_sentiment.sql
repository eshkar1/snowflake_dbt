
with opp_table as (
    select * from {{ ref('stg_salesforce_opp_table')}}
    -- where
    -- date(_RIVERY_LAST_UPDATE) = current_date()
),

account_table as (
    select * from {{ ref('stg_salesforce_account_table')}}
    -- where
    -- date(_RIVERY_LAST_UPDATE) = current_date()
),

user_table as (
    select * from {{ ref('stg_salesforce_user_table')}}
    -- where
    -- date(_RIVERY_LAST_UPDATE) = current_date()
),

RankedRecords AS (
    SELECT 
        last_modified_date__c as last_modified_date,
        industry,
        a.name as account_name,
        a.partner__c as partner,
        -- u.partner
        u.name as csm_owner,
        tier__c as tier,
        renewal_date__c as renewal_date,
        total_arr_in_usd__c as total_arr_in_usd,
        csm_sentiment__c as csm_sentiment,
        employees__c as employees,
        a.sub_region__c as sub_region,
        billing_country__c as billing_country, 
        account_sales_owner_name__c as account_sales_owner_name ,
        last_updaterichtext__c as last_updaterichtext ,
        customer_journey_stage__c as customer_journey_stage,
        nps_score__c as nps_score ,
        a.reference_notes__c as reference_notes ,
        onboarded__c as onboarded ,
        customer_journey_tier__c as customer_journey_tier ,
        a.product_issues__c as product_issues,
        ROW_NUMBER() OVER (
            PARTITION BY a.name 
            ORDER BY LAST_MODIFIED_DATE__C DESC
        ) as rn
    FROM opp_table o
    LEFT JOIN account_table a ON o.account_master_id__c = a.account_master_id__c 
    LEFT JOIN user_table u ON o.csm_owner__c = u.id
    where
    a.type = 'Customer'
    and a.ACCOUNT_STATUS__C in ('Approved / Active', 'Active')
    and a.csm_sentiment__c in ('Red', 'Yellow')

)

SELECT 
r.* EXCLUDE (rn)
FROM RankedRecords r
WHERE rn = 1
order by account_name