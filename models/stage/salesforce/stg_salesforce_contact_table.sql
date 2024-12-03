with source as (
    select * from {{ source('salesforce','contact_table') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed