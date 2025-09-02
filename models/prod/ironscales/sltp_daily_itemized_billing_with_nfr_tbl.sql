
with sltp_pricing_function_pax8 as (
    select * from {{ ref('SLTP_daily_pricing_function_for_pax8')}} 
),

sltp_pricing_function_all_ltps as (
    select * from {{ ref('SLTP_daily_pricing_function_for_all_ltps')}}
),

sltp_pricing_function_disti as (
    select * from {{ ref('SLTP_daily_pricing_function_for_Disti')}}
),

sltp_pricing_function_shareweb as (
    select * from {{ ref('SLTP_daily_pricing_function_for_shareweb')}}
),

sltp_pricing_function_pax8_updated as (
    select * from {{ ref('SLTP_daily_pricing_function_for_pax8_updated')}} 
),

ltp_nfr_calc as (
    select * from {{ ref("LTP_NFR_calculation")}} 
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
sku,
partner_pricing,
billable_quantity,
amount
-- from sltp_pricing_function_pax8 
from sltp_pricing_function_pax8_updated

union all

SELECT
current_date as billing_date,
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
current_date as billing_date,
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

union all

SELECT
current_date as billing_date,
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
from sltp_pricing_function_shareweb  

union all

select
current_date as billing_date,
TENANT_GLOBAL_ID as FIRST_LAYER_ID,
null as SECOND_LAYER_ID,
null as THIRD_LAYER_ID,
null as FOURTH_LAYER_ID, 
null as FIFTH_LAYER_ID,
'NFR' as item,
'IS-LTP-NFR' as sku,
null as partner_pricing,
BILLABLE_QTY as billable_quantity,
INVOICE as amount
from ltp_nfr_calc