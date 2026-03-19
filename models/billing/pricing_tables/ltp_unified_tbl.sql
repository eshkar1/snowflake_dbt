with source as (
    select * from {{ source('ltp_pricing_table_new_billing_calc', 'LTP_UNIFIED_TBL') }}
),

renamed as (
    select
        GLOBAL_TENANT_ID,
        TENANT_NAME,
        MASTER_TENANT_ID
    from source
)

select * from renamed