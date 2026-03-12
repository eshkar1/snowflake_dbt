with source as (
    select * from {{ source('ltp_pricing_table_new_billing_calc','LTP_ACCOUNT_META') }}
    where
    SNAPSHOT_DATE = current_date -- only takes the data for Today and no historical data is added
    and IS_TRACKED = TRUE
    
),

renamed as (
            select
            *
            from source

            )

select *  from renamed