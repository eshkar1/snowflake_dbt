
with global_tenant_history as (
    select * from {{ ref('global_tenant_history')}} 
),

ltp_pricing_list as (
    select * from {{ ref('ltp_pricing_tbl')}}
),



profile_metrics AS (
    SELECT 
        g.root,
        g.tenant_global_id,
        g.record_date,
        p.profile_type,
        p.is_highwatermark,
        CASE p.profile_type
            WHEN 'active' THEN g.active_profiles
            WHEN 'license' THEN g.licensed_profiles
            WHEN 'shared' THEN g.active_profiles - g.shared_profiles
        END AS profile_count
    FROM global_tenant_history g
    LEFT JOIN ltp_pricing_list p 
        ON g.root = p.tenant_global_id
    WHERE 
       record_date BETWEEN  date_trunc('month', dateadd('month', -1, current_date))  -- First day of previous month
        AND dateadd('day', -1, date_trunc('month', current_date))
        AND approved = true
        AND billing_status = 'Active'
        AND root IN (
            SELECT tenant_global_id
            FROM ltp_pricing_list
        )
)

SELECT 
CASE is_highwatermark
    WHEN true then MAX_BY(tenant_global_id, COALESCE(profile_count, 0)) 
    WHEN false then tenant_global_id
end AS tenant_global_id,

CASE is_highwatermark
    WHEN true then MAX_BY(record_date, COALESCE(profile_count, 0))
    WHEN false then MAX(record_date)
end AS record_date
FROM profile_metrics
GROUP BY
    is_highwatermark,
    tenant_global_id