with tenants_us_incremental as (
    select * from {{ ref('global_tenant_history_US_incremental')}}
),

tenants_eu_incremental as (
    select * from {{ ref('global_tenant_history_EU_incremental')}}
)

select
*
from tenants_us_incremental

union all 

select
*
from tenants_eu_incremental