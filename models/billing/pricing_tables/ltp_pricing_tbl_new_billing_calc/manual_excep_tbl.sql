WITH source AS (
    SELECT * FROM {{ source('ltp_pricing_table_new_billing_calc', 'MANUAL_EXCEP_TBL') }}
),

renamed AS (
    SELECT
        TENANT_ID,
        TENANT_NAME,
        CURRENCY,
        PROFILE_TYPE,
        ITEM,
        SKU,
        PARTNER_PRICING,
        TIER_MIN,
        TIER_RATE,
        TIER_MAX,
        START_DATE::date    AS START_DATE,
        END_DATE::date      AS END_DATE,
        TENANT_MSP,
        PARENT_ID
    FROM source
)

SELECT * FROM renamed