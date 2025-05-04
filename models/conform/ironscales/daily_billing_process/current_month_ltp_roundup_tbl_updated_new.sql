WITH global_tenant_history AS (
    SELECT * FROM 
    -- prod_mart.operation.global_tenant_history
    {{ ref('global_tenant_history')}} 
),
ltp_pricing_list AS (
    SELECT * FROM 
    -- prod_mart.upload_tables.ltp_pricing_list 
    {{ ref('ltp_pricing_tbl')}}
),
date_bounds AS (
    SELECT
        CASE 
            WHEN DATE_PART('day', CURRENT_DATE) = 1 THEN DATEADD(day, 1, DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE)))  -- 2nd of previous month
            WHEN DATE_PART('day', CURRENT_DATE) = 2 THEN CURRENT_DATE  -- 2nd of current month
            ELSE DATEADD(day, 1, DATE_TRUNC('month', CURRENT_DATE))  -- 2nd of current month
        END AS start_date,
        CURRENT_DATE -2 AS end_date
),
profile_metrics AS (
    SELECT 
        g.record_date,
        g.root,
        g.tenant_global_id,
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
    JOIN date_bounds d
        ON g.record_date BETWEEN d.start_date AND d.end_date
    WHERE 
        approved = TRUE
        AND billing_status = 'Active'
        AND root IN (
            SELECT tenant_global_id
            FROM ltp_pricing_list
        )
),
intermediate_results AS (
    SELECT 
        CASE is_highwatermark
            WHEN true THEN MAX_BY(tenant_global_id, COALESCE(profile_count, 0)) 
            WHEN false THEN tenant_global_id
        END AS tenant_global_id,
        CASE is_highwatermark
            WHEN true THEN MAX_BY(profile_metrics.record_date, COALESCE(profile_count, 0))
            WHEN false THEN MAX(profile_metrics.record_date)
        END AS record_date,
        is_highwatermark
    FROM profile_metrics 
    GROUP BY
        is_highwatermark,
        tenant_global_id
),
ranked_results AS (
    SELECT 
        tenant_global_id,
        record_date,
        is_highwatermark
    FROM intermediate_results
),
ranked_results_filter as (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY tenant_global_id 
            ORDER BY record_date DESC, is_highwatermark DESC
        ) AS rn
    FROM ranked_results
)
SELECT 
    tenant_global_id,
    record_date
FROM ranked_results_filter
WHERE 
    rn = 1
    OR
    (rn = 2 AND is_highwatermark = true)