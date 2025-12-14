with source as (
    select * from {{ source('ltp_pricing_table','ltp_pricing_list') }}
    -- where
    -- SNAPSHOT_DATE = current_date -- only takes the data for Today and no historical data is added
    
),

renamed as (
            select
            *
            from source

            )

select *  from renamed