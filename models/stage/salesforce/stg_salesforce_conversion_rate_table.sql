with source as (
    select * from {{ source('salesforce','conversion_rate_table') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed