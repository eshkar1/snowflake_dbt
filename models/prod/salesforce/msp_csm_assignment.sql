with global_tenant_history as (
    select * from 
    {{ ref('global_tenant_history')}} 
    -- PROD_MART.OPERATION.GLOBAL_TENANT_HISTORY -- need to adjust to ref
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
)

select
a.ironscales_account_tenant_id__c,
u.name

from global_tenant_history g
left join account_table a on g.tenant_global_id = a.ironscales_account_tenant_id__c
left join user_table u on a.csm2__c = u.id
-- where 
-- a.ironscales_account_tenant_id__c in (select tenant_global_id from prod_mart.upload_tables.ltp_pricing_list_today) 
where
g.record_date = current_date()
-- and l.tenant_global_id = 'US-200858'
qualify row_number() over (
                      partition by a.ironscales_account_tenant_id__c
                      order by a.lastmodifieddate desc, a.id desc
                        ) = 1