with source as (
    select * from {{ source('ltp_pricing_table','ltp_pricing_list') }}
),

renamed as (
            select
            *
            from source

            )

select *  from renamed