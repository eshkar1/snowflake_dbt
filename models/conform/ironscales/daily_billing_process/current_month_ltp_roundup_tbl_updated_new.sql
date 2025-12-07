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
            WHEN DATE_PART('day', CURRENT_DATE) = 1 THEN DATEADD(day, 1, DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE)))
            WHEN DATE_PART('day', CURRENT_DATE) = 2 THEN CURRENT_DATE
            ELSE DATEADD(day, 1, DATE_TRUNC('month', CURRENT_DATE))
        END AS start_date,
        CURRENT_DATE AS end_date
),
profile_metrics AS (
    SELECT 
        g.record_date,
        g.root,
        g.tenant_global_id,
        p.profile_type,
        p.is_highwatermark,
        g.plan_name,
        CASE p.profile_type
            WHEN 'active' THEN ifnull(g.active_profiles,0)
            WHEN 'license' THEN ifnull(g.licensed_profiles,0)
            WHEN 'shared' THEN ifnull(g.active_profiles - IFNULL(g.shared_profiles, 0),0)
        END AS profile_count
    FROM global_tenant_history g
    JOIN date_bounds d ON g.record_date BETWEEN d.start_date AND d.end_date
    LEFT JOIN ltp_pricing_list p 
        ON g.root = p.tenant_global_id
    WHERE 
        g.approved = TRUE
        AND g.billing_status in ('Active','Active-POC')
        and g.root in (select 
                        tenant_global_id
                        from ltp_pricing_list)
),
-- Count distinct plans per tenant_global_id per is_highwatermark flag
plan_count AS (
    SELECT
        tenant_global_id,
        is_highwatermark,
        COUNT(DISTINCT plan_name) as num_plans
    FROM profile_metrics
    GROUP BY tenant_global_id, is_highwatermark
),
-- For multi-plan tenants, get the most recent plan info
most_recent_plan_info AS (
    SELECT
        pm.tenant_global_id,
        pm.is_highwatermark,
        pm.plan_name,
        pm.record_date
    FROM profile_metrics pm
    INNER JOIN plan_count pc 
        ON pm.tenant_global_id = pc.tenant_global_id 
        AND pm.is_highwatermark = pc.is_highwatermark
        AND pc.num_plans > 1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pm.tenant_global_id, pm.is_highwatermark
        ORDER BY pm.record_date DESC
    ) = 1
),
-- Get the start date of the most recent plan for multi-plan tenants
most_recent_plan_start AS (
    SELECT
        mrpi.tenant_global_id,
        mrpi.is_highwatermark,
        mrpi.plan_name,
        MIN(pm.record_date) as plan_start_date
    FROM most_recent_plan_info mrpi
    INNER JOIN profile_metrics pm
        ON mrpi.tenant_global_id = pm.tenant_global_id
        AND mrpi.is_highwatermark = pm.is_highwatermark
        AND mrpi.plan_name = pm.plan_name
    GROUP BY mrpi.tenant_global_id, mrpi.is_highwatermark, mrpi.plan_name
),
-- Filter: single-plan tenants get all records, multi-plan tenants get only most recent plan period
profile_metrics_filtered AS (
    -- Single plan tenants: all their records
    SELECT pm.*
    FROM profile_metrics pm
    INNER JOIN plan_count pc 
        ON pm.tenant_global_id = pc.tenant_global_id 
        AND pm.is_highwatermark = pc.is_highwatermark
    WHERE pc.num_plans = 1
    
    UNION ALL
    
    -- Multi-plan tenants: only most recent plan period
    SELECT pm.*
    FROM profile_metrics pm
    INNER JOIN most_recent_plan_start mrps
        ON pm.tenant_global_id = mrps.tenant_global_id
        AND pm.is_highwatermark = mrps.is_highwatermark
        AND pm.plan_name = mrps.plan_name
        AND pm.record_date >= mrps.plan_start_date
),
highwater_selected AS (
    SELECT
        TENANT_GLOBAL_ID,
        RECORD_DATE,
        IS_HIGHWATERMARK,
        plan_name
    FROM profile_metrics_filtered
    WHERE IS_HIGHWATERMARK = 1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TENANT_GLOBAL_ID
        ORDER BY PROFILE_COUNT DESC, RECORD_DATE ASC
    ) = 1
),
non_highwater_selected AS (
    SELECT
        TENANT_GLOBAL_ID,
        current_date as RECORD_DATE,
        IS_HIGHWATERMARK,
        plan_name
    FROM profile_metrics_filtered
    WHERE IS_HIGHWATERMARK = 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TENANT_GLOBAL_ID
        ORDER BY RECORD_DATE DESC
    ) = 1
),
combined AS (
    SELECT * FROM highwater_selected
    UNION ALL
    SELECT * FROM non_highwater_selected
)
SELECT
    TENANT_GLOBAL_ID,
    RECORD_DATE,
    IS_HIGHWATERMARK
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY TENANT_GLOBAL_ID
            ORDER BY RECORD_DATE DESC
        ) AS final_rank
    FROM combined
)
WHERE final_rank = 1
