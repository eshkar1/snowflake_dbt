with source as (
    select * from {{ source('salesforce','user_table') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed