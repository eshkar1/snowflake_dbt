
with pricing_function_pax8 as (
    select * from {{ ref('LTP_daily_pricing_function_for_pax8')}} 
),

pricing_function_all_ltps as (
    select * from {{ ref('LTP_daily_pricing_tier_function_for_all_ltps')}} --this include tier or incremental--
),

pricing_function_disti as (
    select * from {{ ref('LTP_daily_pricing_function_for_Disti')}}
),

pricing_function_shareweb as (
    select * from {{ ref('LTP_daily_pricing_function_for_shareweb')}}
)


SELECT
*
from PROD_CONFORM.DBT_PROD_DB.LTP_DAILY_ITEMIZED_BILLING_TBL

union all 


SELECT
current_date as billing_date,
ltp,
item,
sku,
partner_pricing,
sum(quantity) as quantity,
sum(amount) as amount
from pricing_function_pax8 
group by
    billing_date,
    ltp,
    item,
    sku,
    partner_pricing

union all

select
current_date as billing_date,
ltp,
item,
sku,
partner_pricing,
sum(quantity) as quantity,
sum(amount) as amount
from pricing_function_all_ltps
group by 
    billing_date,
    ltp,
    item,
    sku,
    partner_pricing    

union all

select
current_date as billing_date,
ltp,
item,
sku,
partner_pricing,
sum(quantity) as quantity,
sum(amount) as amount
from pricing_function_disti
group by 
    billing_date,
    ltp,
    item,
    sku,
    partner_pricing    

-- union all

-- select
-- current_date as billing_date,
-- ltp,
-- item,
-- sku,
-- partner_pricing,
-- sum(quantity) as quantity,
-- sum(amount) as amount
-- from pricing_function_shareweb
-- group by 
--     billing_date,
--     ltp,
--     item,
--     sku,
--     partner_pricing    