with pricing_function_all_ltps as (
    select * from {{ ref('sltp_daily_billing_non_hwm_calc')}} 
),

ltp_nfr_calc as (
    select * from {{ ref("ltp_nfr_calculation_new_billing")}} 
)



-- SELECT
-- *
-- -- from PROD_CONFORM.DBT_PROD_DB.LTP_DAILY_ITEMIZED_BILLING_WITH_NFR_TBL
-- from prod_conform.dbt_billing_prod_db.ltp_daily_itemized_new_billing_tbl

-- union all  


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
price_type,
sum(billable_quantity) as quantity,
sum(amount) as amount
-- from pricing_function_pax8 
from pricing_function_all_ltps --changed the old calc to the new calc
group by
billing_date,
FIRST_LAYER_ID,
SECOND_LAYER_ID,
THIRD_LAYER_ID,
FOURTH_LAYER_ID,
FIFTH_LAYER_ID,
item,
sku,
partner_pricing,
price_type


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
null as price_type,
BILLABLE_QTY as quantity,
INVOICE as amount
from ltp_nfr_calc


