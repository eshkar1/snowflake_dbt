with source as (
    select * from {{ source('ironscales_us','auth_user_table') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed