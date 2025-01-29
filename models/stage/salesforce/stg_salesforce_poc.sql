with source as (
    select * from {{ source('salesforce','poc_table') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed