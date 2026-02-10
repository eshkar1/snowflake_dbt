with source as (
    select * from {{ source('zendesk','zendesk_ticket_metrics_enriched') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed