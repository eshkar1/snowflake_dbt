with source as (
    select * from {{ source('salesforce','opportunity_line_item') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed