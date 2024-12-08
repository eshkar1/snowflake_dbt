with source as (
    select * from {{ source('ironscales_us','profiles_profile_table') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed