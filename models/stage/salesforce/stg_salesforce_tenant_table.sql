with source as (
    select * from {{ source('salesforce','tenant_table') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed