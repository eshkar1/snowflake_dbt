with source as (
    select * from {{ source('salesforce','opp_table') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed