with source as (
    select * from {{ source('cursor','sprint_date') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed