
with global_tenant_history as (
    select * from {{ ref('global_tenant_history')}} 
),

ltp_pricing_list as (
    select * from {{ ref('ltp_pricing_tbl')}}
)
-- ,


--  current_month_ltp_roundup_tbl AS (
    SELECT
        max_by(tenant_global_id, active_profiles) AS tenant_global_id,
        max_by(record_date, active_profiles) AS record_date
    FROM
        global_tenant_history
    WHERE
        record_date BETWEEN DATE_TRUNC('MONTH', current_date) AND current_date
        AND approved = true
        AND billing_status = 'Active'
        AND root IN (
            SELECT
                tenant_global_id
            FROM
                ltp_pricing_list
            WHERE
                is_highwatermark = true
        )
    GROUP BY
        tenant_global_id
    
    UNION
    
    SELECT
        tenant_global_id,
        MAX(record_date) AS record_date
    FROM
        global_tenant_history
    WHERE
        record_date BETWEEN DATE_TRUNC('MONTH', current_date) AND current_date
        AND approved = true
        AND billing_status = 'Active'
        AND root IN (
            SELECT
                tenant_global_id
            FROM
                ltp_pricing_list
            WHERE
                is_highwatermark = false
        )
    GROUP BY
        tenant_global_id