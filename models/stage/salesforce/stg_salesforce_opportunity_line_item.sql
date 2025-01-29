with source as (
    select * from {{ source('salesforce','OPPORTUNITY_LINE_ITEM_TABLE') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed