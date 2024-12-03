with source as (
    select * from {{ source('salesforce','poc__c') }}
),

renamed as (
    select
    *
    from source
)

select *  from renamed