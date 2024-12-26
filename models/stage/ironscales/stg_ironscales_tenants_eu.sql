with source as (
    select * from {{ source('ironscales_eu','tenants_tbl') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed