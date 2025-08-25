
with global_tenant_history as (
    select * from 
    {{ ref('global_tenant_history')}} 
    -- PROD_MART.OPERATION.GLOBAL_TENANT_HISTORY -- need to adjust to ref
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
            WHEN 'shared' THEN g.active_profiles - ifnull(g.shared_profiles,0)
        END AS profile_count
    FROM global_tenant_history g
    LEFT JOIN ltp_pricing_list p 
        ON g.root = p.tenant_global_id

    WHERE 
    --    record_date BETWEEN  date_trunc('month', dateadd('month', -1, current_date))  -- First day of previous month
        -- AND dateadd('day', -1, date_trunc('month', current_date))
        (record_date BETWEEN dateadd('day', 1, date_trunc('month', dateadd('month', -1, current_date)))  -- 2nd day of previous month
        AND date_trunc('month', current_date) ) -- 1st day of current month 
        -- (record_date BETWEEN date_trunc('month', dateadd('month', -1, current_date))
            --  AND date_trunc('month', current_date))
        AND approved = true
        AND billing_status = 'Active'
        AND root IN (
            SELECT tenant_global_id
            FROM ltp_pricing_list)
),

intermediate_results AS (
    SELECT 
        CASE is_highwatermark
            WHEN true THEN MAX_BY(tenant_global_id, COALESCE(profile_count, 0)) 
            WHEN false THEN tenant_global_id
        END AS tenant_global_id,
        CASE is_highwatermark
            WHEN true THEN MAX_BY(record_date, COALESCE(profile_count, 0))
            WHEN false THEN MAX(record_date)
            -- WHEN false then date_trunc('month', current_date) --1st day of current month 
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
-- or
-- (rn = 2 and is_highwatermark = true)