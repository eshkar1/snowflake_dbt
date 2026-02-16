with source as (
    select * from {{ source('jira','jira_issue') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed