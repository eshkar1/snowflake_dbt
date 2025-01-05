with source as (
    select * from {{ source('ironscales_us','campaigns_company_table') }}
),

renamed as (
    select
    *
    from source

)

select *  from renamed