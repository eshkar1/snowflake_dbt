WITH parse_cursor_json AS (
    select * from {{ ref('parse_cursor_table')}}
),

emp_emails as (
    select * from {{ ref('stg_emp_emails')}}
),

sprint_date as (
    select * from {{ ref('stg_sprint_date')}}
),

historical_data as (
    select * from {{ ref('stg_historical_data')}}
)


select
date(s.datestamp) as date,
s.sprint,
e.email,
e.skill_set,
e.team,
e.team_drill_down,

h.chat_suggested_lines_added+h.chat_suggested_lines_deleted as total_chat_suggested,
h.chat_accepted_lines_added+h.chat_accepted_lines_deleted as total_chat_accepted,
h.agent_requests

-- from  dev_mart.dbt_prod_db.cursor_user_activity c
from historical_data h
left join emp_emails e on h.email = e.email 
left join sprint_date s on date(h.date) = date(s.datestamp)

union

select
date(s.datestamp) as date,
s.sprint,
e.email,
e.skill_set,
e.team,
e.team_drill_down,
c.TOTAL_LINES_ADDED + c.TOTAL_LINES_DELETED as total_chat_suggested,
c.ACCEPTED_LINES_ADDED+c.ACCEPTED_LINES_DELETED as total_chat_accepted,
c.agent_requests
-- *
from parse_cursor_json c
left join emp_emails e on c.email = e.email 
left join sprint_date s on date(c.day) = date(s.datestamp)