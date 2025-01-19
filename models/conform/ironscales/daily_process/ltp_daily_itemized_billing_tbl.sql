
with pricing_function_pax8 as (
    select * from {{ ref('pricing_function_for_pax8')}} 
),

pricing_function_all_ltps as (
    select * from {{ ref('pricing_function_for_all_ltps')}}
)




SELECT
current_date as billing_date,
ltp,
item,
partner_pricing,
sum(quantity),
sum(amount)
from pricing_function_pax8 
group by
    billing_date,
    ltp,
    item,
    partner_pricing

union all

select
current_date as billing_date,
ltp,
item,
partner_pricing,
sum(quantity),
sum(amount)
from pricing_function_all_ltps
group by 
    billing_date,
    ltp,
    item,
    partner_pricing    
