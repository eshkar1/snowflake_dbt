with jira_data as (
    select * from 
    {{ ref('jira_data')}}
)

SELECT     
CASE 
    WHEN TYPEOF(PARSE_JSON(raw_customfields):customfield_10081) = 'ARRAY'
        THEN ARRAY_TO_STRING(PARSE_JSON(raw_customfields):customfield_10081, ',')
    ELSE NULL
END AS request_by,
key,
FIELDS_CREATED,
FIELDS_UPDATED,
FIELDS_TIMEORIGINALESTIMATE,
FIELDS_DUEDATE,
FIELDS_ISSUETYPE_NAME,
fields_priority_name,
fields_status_name,
fields_status_statuscategory_id,
-- fields_issuetype_name,
fields_summary,
FIELDS_RESOLUTION,
FIELDS_ASSIGNEE_DISPLAYNAME,
FIELDS_CREATOR_DISPLAYNAME,
FIELDS_STATUS_STATUSCATEGORY_COLORNAME
-- FROM dev_raw.salesforce_raw_db.jira_issue
from jira_data

