with global_tenant_history_daily_billing_tbl as (
    select * from {{ ref('global_tenant_history_daily_billing_tbl')}}
)

-- select
-- *
-- from PROD_CONFORM.DBT_PROD_DB.global_tenant_history_daily_agg_billing_tbl

-- union 

select
*
from global_tenant_history_daily_billing_tbl