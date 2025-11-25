with source as (
    select * from {{ source('cursor','emp_emails') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed