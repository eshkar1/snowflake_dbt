with source as (
    select * from {{ source('salesforce','account_table') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed