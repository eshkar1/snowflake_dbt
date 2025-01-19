
with pricing_function_pax8 as (
    select * from {{ ref('monthly_pricing_function_for_pax8')}} 
),

pricing_function_all_ltps as (
    select * from {{ ref('monthly_pricing_function_for_all_ltps')}}
)


-- SELECT
-- *
-- from prod_conform.new_test.ltp_monthly_itemized_billing_tbl

-- union all 


SELECT
current_date as billing_date,
ltp,
item,
partner_pricing,
sum(quantity) as quantity,
sum(amount) as amount
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
sum(quantity) as quantity,
sum(amount) as amount
from pricing_function_all_ltps
group by 
    billing_date,
    ltp,
    item,
    partner_pricing    
