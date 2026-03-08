WITH global_tenant_history AS (
    SELECT * FROM 
    -- prod_mart.operation.global_tenant_history
    {{ ref('global_tenant_history')}} 
),
ltp_pricing_list AS (
    SELECT * FROM 
    -- prod_mart.upload_tables.ltp_pricing_list_today
    -- {{ ref('ltp_pricing_tbl')}}
    {{ref('ltp_account_meta')}}
),

date_bounds AS (
    SELECT
        CASE
        WHEN DAY(CURRENT_DATE) = 1
            THEN DATEADD(day, 1, DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE)))
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
        AND g.billing_status IN ('Active','Active-POC')
        AND g.plan_name != 'No_Plan'
        AND g.root IN (
            SELECT tenant_global_id
            FROM ltp_pricing_list
        )
),

plan_count AS (
    SELECT
        tenant_global_id,
        is_highwatermark,
        COUNT(DISTINCT plan_name) AS num_plans
    FROM profile_metrics
    GROUP BY tenant_global_id, is_highwatermark
),

-- For multi-plan tenants, identify the most expensive plan
most_expensive_plan_info AS (
    SELECT
        pm.tenant_global_id,
        pm.is_highwatermark,
        pm.plan_name
    FROM profile_metrics pm
    INNER JOIN plan_count pc 
        ON pm.tenant_global_id = pc.tenant_global_id 
        AND pm.is_highwatermark = pc.is_highwatermark
        AND pc.num_plans > 1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pm.tenant_global_id, pm.is_highwatermark
        ORDER BY CASE pm.plan_name
            WHEN 'Complete Protect'                 THEN 1
            WHEN 'Email Protect'                    THEN 2
            WHEN 'Core'                             THEN 3
            WHEN 'IRONSCALES Protect'               THEN 4
            WHEN 'SAT Suite'                        THEN 5
            WHEN 'Phishing Simulation and Training' THEN 6
            WHEN 'Starter'                          THEN 7
            ELSE 99
        END ASC
    ) = 1
),

-- Get the date range during which the most expensive plan was active
most_expensive_plan_start AS (
    SELECT
        mepi.tenant_global_id,
        mepi.is_highwatermark,
        mepi.plan_name,
        MIN(pm.record_date) AS plan_start_date,
        MAX(pm.record_date) AS plan_end_date
    FROM most_expensive_plan_info mepi
    INNER JOIN profile_metrics pm
        ON mepi.tenant_global_id = pm.tenant_global_id
        AND mepi.is_highwatermark = pm.is_highwatermark
        AND mepi.plan_name = pm.plan_name
    GROUP BY mepi.tenant_global_id, mepi.is_highwatermark, mepi.plan_name
),

-- Single-plan tenants: all records; multi-plan tenants: only most expensive plan's window
profile_metrics_filtered AS (
    SELECT pm.*
    FROM profile_metrics pm
    INNER JOIN plan_count pc 
        ON pm.tenant_global_id = pc.tenant_global_id 
        AND pm.is_highwatermark = pc.is_highwatermark
    WHERE pc.num_plans = 1
    
    UNION ALL
    
    SELECT pm.*
    FROM profile_metrics pm
    INNER JOIN most_expensive_plan_start meps
        ON pm.tenant_global_id = meps.tenant_global_id
        AND pm.is_highwatermark = meps.is_highwatermark
        AND pm.plan_name = meps.plan_name
        AND pm.record_date BETWEEN meps.plan_start_date AND meps.plan_end_date
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
        RECORD_DATE,
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