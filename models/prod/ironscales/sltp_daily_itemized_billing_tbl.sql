
with sltp_pricing_function_pax8 as (
    select * from {{ ref('SLTP_daily_pricing_function_for_pax8')}} 
),

sltp_pricing_function_all_ltps as (
    select * from {{ ref('SLTP_daily_pricing_function_for_all_ltps')}}
)


SELECT
*
from PROD_CONFORM.DBT_PROD_DB.SLTP_DAILY_ITEMIZED_BILLING_TBL
 

union all 


SELECT
current_date as billing_date,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
item,
partner_pricing,
billable_quantity,
amount
from sltp_pricing_function_pax8 


union all

SELECT
current_date as billing_date,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
item,
partner_pricing,
billable_quantity,
amount
from sltp_pricing_function_all_ltps  
