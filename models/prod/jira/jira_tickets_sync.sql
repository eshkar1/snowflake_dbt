with global_tenant_history as (
    select * from 
    {{ ref('global_tenant_history')}}
),

jira_tickets as (
    select * from 
    {{ ref('jira_tickets')}}
)


select
g.tenant_name as customer_name,
g.tenant_global_id as customer_id,
g.root as first_layer_name,
-- gth.tenant_name as first_layer_name,
g.parent_name as parent_name,
g.parent_global_id as partent_id,
j.*
-- g.tenant_name,
-- j.request_by,
-- z.organization_name
from global_tenant_history g
left join jira_tickets j on lower(g.tenant_name) = lower(j.request_by)
-- left join dev_raw.zendesk.zendesk_ticket_enriched z on lower(z.organization_name) = lower(g.tenant_name) 
where
record_date = current_date()
and billing_status in ('Active','Active-POC')
and approved = true
and request_by is not null and g.tenant_name != 'IronScales'
and j.FIELDS_ISSUETYPE_NAME in ('Business support','Accuracy Challenges ','Customer Request','Customer Bug')