with source as (
    select * from {{ source('cursor','cursor_table') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed