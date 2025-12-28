with source as (
    select * from {{ source('ironscales_canada','profiles_tag_table') }}
),

renamed as (
            select
            *
            from source

            {% if is_incremental() %}

            where _rivery_last_update > (select max(_rivery_last_update) from {{this}})

            {% endif %}
            )

select *  from renamed