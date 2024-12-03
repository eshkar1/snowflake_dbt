with source as (
    select * from {{ source('ironscales_us','campaigns_companylicense') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed