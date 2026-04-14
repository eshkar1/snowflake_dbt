WITH pricing_function_all_ltps AS (
    SELECT * FROM {{ ref('ltp_daily_billing_non_hwm_calc') }}
),

ltp_nfr_calc AS (
    SELECT * FROM {{ ref('ltp_nfr_calculation_new_billing') }}
)

SELECT
    current_date                            AS billing_date,
    ltp,
    item,
    sku,
    partner_pricing,
    price_type,
    SUM(quantity)                           AS quantity,
    SUM(amount)                             AS amount
FROM pricing_function_all_ltps
GROUP BY
    billing_date,
    ltp,
    item,
    sku,
    partner_pricing,
    price_type

UNION ALL

SELECT
    current_date                            AS billing_date,
    TENANT_GLOBAL_ID                        AS ltp,
    'NFR'                                   AS item,
    'IS-LTP-NFR'                            AS sku,
    NULL                                    AS partner_pricing,
    NULL                                    AS price_type,
    BILLABLE_QTY                            AS quantity,
    INVOICE                                 AS amount
FROM ltp_nfr_calc