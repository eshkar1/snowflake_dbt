
with ltp_daily_itemized_billing_tbl as (
    select * from {{ ref('ltp_daily_itemized_billing_tbl')}} 
),

ltp_nfr_calc as (
    select * from {{ ref("LTP_NFR_calculation")}} 
)



{# SELECT
*
from PROD_CONFORM.DBT_PROD_DB.LTP_DAILY_ITEMIZED_BILLING_TBL

union all  #}


SELECT
billing_date,
ltp,
item,
sku,
partner_pricing,
quantity,
amount
from ltp_daily_itemized_billing_tbl 

union all

select
current_date as billing_date,
TENANT_GLOBAL_ID as ltp,
'NFR' as item,
'IS-LTP-NFR' as sku,
null as partner_pricing,
BILLABLE_QTY as quantity,
INVOICE as amount
from ltp_nfr_calc


