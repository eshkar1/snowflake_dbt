with source as (
    select * from {{ source('cursor','historical_data') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed