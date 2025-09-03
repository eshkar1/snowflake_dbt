

with pricing_function_all_ltps as (
    select * from {{ ref('LTP_daily_pricing_tier_function_for_all_ltps_updated')}} --this include tier or incremental--
),

pricing_function_disti as (
    select * from {{ ref('LTP_daily_pricing_function_for_Disti_updated')}}
),

pricing_function_shareweb as (
    select * from {{ ref('LTP_daily_pricing_function_for_shareweb_updated')}}
),


pricing_function_pax8_updated as (
    select * from {{ ref('LTP_daily_pricing_function_for_pax8_updated')}} 
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
-- from pricing_function_pax8 
from pricing_function_pax8_updated --changed the old calc to the new calc
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

union all

select
current_date as billing_date,
ltp,
item,
sku,
partner_pricing,
sum(quantity) as quantity,
sum(amount) as amount
from pricing_function_shareweb
group by 
    billing_date,
    ltp,
    item,
    sku,
    partner_pricing    