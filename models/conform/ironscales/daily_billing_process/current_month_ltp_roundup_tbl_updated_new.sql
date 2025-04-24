with global_tenant_history as (
    select * from 
    -- prod_mart.operation.global_tenant_history
    {{ ref('global_tenant_history')}} 
),

ltp_pricing_list as (
    select * from 
    -- prod_mart.upload_tables.ltp_pricing_list 
    {{ ref('ltp_pricing_tbl')}}
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
    -- record_date BETWEEN DATE_TRUNC('MONTH', current_date) AND current_date
    record_date BETWEEN DATEADD('day', 1, DATE_TRUNC('MONTH', current_date)) AND current_date -- change from 2nd of month until current day
        AND approved = true
        AND billing_status = 'Active'
        AND root IN (
            SELECT tenant_global_id
            FROM ltp_pricing_list
            -- WHERE is_highwatermark = true
        )
)
,

intermediate_results AS (
    SELECT 
        CASE is_highwatermark
            WHEN true THEN MAX_BY(tenant_global_id, COALESCE(profile_count, 0)) 
            WHEN false THEN tenant_global_id
        END AS tenant_global_id,
        CASE is_highwatermark
            WHEN true THEN MAX_BY(record_date, COALESCE(profile_count, 0))
            WHEN false THEN MAX(record_date)
        END AS record_date,
        is_highwatermark
    FROM profile_metrics
    GROUP BY
        is_highwatermark,
        tenant_global_id
)
,

ranked_results AS (
    SELECT 
        tenant_global_id,
        record_date,
        is_highwatermark
    FROM intermediate_results
)
,
-- 
ranked_results_filter as (
    SELECT
    *,
        ROW_NUMBER() OVER (
            PARTITION BY tenant_global_id 
            ORDER BY record_date DESC, is_highwatermark DESC
        ) AS rn
    from ranked_results


    
)
SELECT 
    tenant_global_id,
    record_date
FROM ranked_results_filter
WHERE 
rn = 1
or
(rn = 2 and is_highwatermark = true)