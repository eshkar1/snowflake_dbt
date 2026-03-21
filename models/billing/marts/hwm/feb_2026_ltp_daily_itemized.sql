WITH pricing_function_all_ltps AS (
    SELECT * FROM {{ ref('feb_2026_ltp_billing_calc') }}
),

ltp_nfr_calc AS (
    SELECT * FROM {{ ref('feb_2026_ltp_nfr_billing') }}
)

SELECT
    '2026-03-01'::date                  AS billing_date,
    ltp,
    item,
    sku,
    partner_pricing,
    price_type,
    SUM(quantity)                       AS quantity,
    SUM(amount)                         AS amount
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
    '2026-03-01'::date                  AS billing_date,
    tenant_global_id                    AS ltp,
    'NFR'                               AS item,
    'IS-LTP-NFR'                        AS sku,
    NULL                                AS partner_pricing,
    NULL                                AS price_type,
    billable_qty                        AS quantity,
    invoice                             AS amount
FROM ltp_nfr_calc