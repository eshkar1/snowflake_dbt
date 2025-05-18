
with sltp_pricing_function_pax8 as (
    select * from {{ ref('SLTP_monthly_pricing_function_for_pax8')}} 
),

sltp_pricing_function_all_ltps as (
    select * from {{ ref('SLTP_monthly_pricing_function_for_all_ltps')}}  -- this include tier or incremental --
),

sltp_pricing_function_disti as (
    select * from {{ ref('SLTP_monthly_pricing_function_for_Disti')}}
),

sltp_pricing_function_shareweb as (
    select * from {{ ref('SLTP_monthly_pricing_function_for_shareweb')}}
)

-- SELECT
-- *
-- from PROD_CONFORM.DBT_PROD_DB.SLTP_MONTHLY_ITEMIZED_BILLING_TBL
 

-- union all 


SELECT
last_day(dateadd(month,-1,current_date)) as billing_date,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
item,
sku,
partner_pricing,
billable_quantity,
amount
from sltp_pricing_function_pax8 


union all

SELECT
last_day(dateadd(month,-1,current_date)) as billing_date,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
item,
sku,
partner_pricing,
billable_quantity,
amount
from sltp_pricing_function_all_ltps  

union all

SELECT
last_day(dateadd(month,-1,current_date)) as billing_date,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
item,
sku,
partner_pricing,
billable_quantity,
amount
from sltp_pricing_function_disti  