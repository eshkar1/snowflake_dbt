{{
    config(
        materialized = 'table',
    )
}}

with days as (
    {{
        dbt.date_spine(
            'day',
            "to_date('01/01/2020','MM/DD/YYYY')",
            "dateadd(year, 2, current_date)"
        )
    }}
)

select cast(date_day as date) as date_day
from days
