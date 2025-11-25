with source as (
    select * from {{ source('cursor','skill_set_compensation') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed