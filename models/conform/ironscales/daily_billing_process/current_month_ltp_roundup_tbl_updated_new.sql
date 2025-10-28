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
        -- AND EXISTS (
        --     SELECT 1 
        --     FROM prod_mart.upload_tables.ltp_pricing_list l
        --     WHERE l.tenant_global_id = g.root
        -- )
),
highwater_selected AS (
    SELECT
        TENANT_GLOBAL_ID,
        RECORD_DATE,
        IS_HIGHWATERMARK
    FROM profile_metrics
    WHERE IS_HIGHWATERMARK = 1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TENANT_GLOBAL_ID
        ORDER BY PROFILE_COUNT DESC, RECORD_DATE ASC
    ) = 1
),
non_highwater_selected AS (
    SELECT
        TENANT_GLOBAL_ID,
        -- RECORD_DATE,
        current_date as RECORD_DATE, -- the last day for all non highwater should be always the current/last day of the month   
        IS_HIGHWATERMARK
    FROM profile_metrics
    WHERE IS_HIGHWATERMARK = 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TENANT_GLOBAL_ID
        ORDER BY RECORD_DATE DESC
    ) = 1
),
combined AS (
    SELECT * FROM highwater_selected
    UNION
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

-- WITH global_tenant_history AS (
--     SELECT *
--     FROM 
--     -- prod_mart.operation.global_tenant_history
--     {{ ref('global_tenant_history')}} 
-- ),
-- ltp_pricing_list AS (
--     SELECT *
--     FROM 
--     -- prod_mart.upload_tables.ltp_pricing_list 
--     {{ ref('ltp_pricing_tbl')}}
--     -- WHERE snapshot_date = CURRENT_DATE
-- ),
-- date_bounds AS (
--     SELECT
--         CASE 
--             WHEN DATE_PART('day', CURRENT_DATE) = 1 THEN DATEADD(day, 1, DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE)))
--             WHEN DATE_PART('day', CURRENT_DATE) = 2 THEN CURRENT_DATE
--             ELSE DATEADD(day, 1, DATE_TRUNC('month', CURRENT_DATE))
--         END AS start_date,
--         CURRENT_DATE AS end_date
-- ),
-- /* Build the base profile_metrics set for the current window */
-- profile_metrics AS (
--     SELECT 
--         g.record_date,
--         g.root,
--         g.tenant_global_id,
--         p.profile_type,
--         p.is_highwatermark,
--         CASE p.profile_type
--             WHEN 'active'  THEN IFNULL(g.active_profiles, 0)
--             WHEN 'license' THEN IFNULL(g.licensed_profiles, 0)
--             WHEN 'shared'  THEN IFNULL(g.active_profiles - IFNULL(g.shared_profiles, 0), 0)
--         END AS profile_count
--     FROM global_tenant_history g
--     JOIN date_bounds d
--       ON g.record_date BETWEEN d.start_date AND d.end_date
--     LEFT JOIN ltp_pricing_list p 
--       ON g.root = p.tenant_global_id
--     WHERE 
--         g.approved = TRUE
--         AND g.billing_status = 'Active'
--         AND g.root IN (
--             SELECT tenant_global_id
--             FROM ltp_pricing_list
--             -- WHERE snapshot_date = CURRENT_DATE
--         )
--         -- AND g.tenant_global_id = 'US-13535'
-- ),

-- /* Pick highwater row constrained to the latest root per tenant */
-- highwater_selected AS (
--     SELECT
--         p.tenant_global_id,
--         p.record_date,
--         p.is_highwatermark
--     FROM profile_metrics p
--     /* Latest root per tenant derived inline from history */
--     JOIN (
--         SELECT tenant_global_id, root
--         FROM (
--             SELECT
--                 tenant_global_id,
--                 root,
--                 ROW_NUMBER() OVER (
--                     PARTITION BY tenant_global_id
--                     ORDER BY max_record_date DESC, root DESC
--                 ) AS rn
--             FROM (
--                 SELECT
--                     tenant_global_id,
--                     root,
--                     MAX(record_date) AS max_record_date
--                 FROM global_tenant_history
--                 WHERE approved = TRUE AND billing_status = 'Active'
--                 GROUP BY 1,2
--             )
--         )
--         WHERE rn = 1
--     ) r
--       ON r.tenant_global_id = p.tenant_global_id
--      AND r.root = p.root
--     WHERE p.is_highwatermark = 1
--     QUALIFY ROW_NUMBER() OVER (
--         PARTITION BY p.tenant_global_id
--         ORDER BY p.profile_count DESC, p.record_date ASC
--     ) = 1
-- ),

-- /* Pick non-highwater row constrained to the latest root per tenant */
-- non_highwater_selected AS (
--     SELECT
--         p.tenant_global_id,
--         CURRENT_DATE AS record_date,   --- the last day for all non highwater should be always the current/last day of the month  
--         p.is_highwatermark
--     FROM profile_metrics p
--     /* Latest root per tenant derived inline from history */
--     JOIN (
--         SELECT tenant_global_id, root
--         FROM (
--             SELECT
--                 tenant_global_id,
--                 root,
--                 ROW_NUMBER() OVER (
--                     PARTITION BY tenant_global_id
--                     ORDER BY max_record_date DESC, root DESC
--                 ) AS rn
--             FROM (
--                 SELECT
--                     tenant_global_id,
--                     root,
--                     MAX(record_date) AS max_record_date
--                 FROM global_tenant_history
--                 WHERE approved = TRUE AND billing_status = 'Active'
--                 GROUP BY 1,2
--             )
--         )
--         WHERE rn = 1
--     ) r
--       ON r.tenant_global_id = p.tenant_global_id
--      AND r.root = p.root
--     WHERE p.is_highwatermark = 0
--     QUALIFY ROW_NUMBER() OVER (
--         PARTITION BY p.tenant_global_id
--         ORDER BY p.record_date DESC
--     ) = 1
-- ),

-- combined AS (
--     SELECT * FROM highwater_selected
--     UNION ALL
--     SELECT * FROM non_highwater_selected
-- )
-- SELECT
--     tenant_global_id,
--     record_date,
--     is_highwatermark
-- FROM (
--     SELECT
--         *,
--         ROW_NUMBER() OVER (
--             PARTITION BY tenant_global_id
--             ORDER BY record_date DESC
--         ) AS final_rank
--     FROM combined
-- )
-- WHERE final_rank = 1