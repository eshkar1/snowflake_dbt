WITH feb_bounds AS (
    SELECT
        '2026-02-02'::date AS start_date,
        '2026-03-01'::date AS end_date
),

ltp_account_meta AS (
    SELECT * FROM {{ ref('ltp_account_meta') }}
),

global_tenant_history AS (
    SELECT * FROM {{ ref('global_tenant_history') }}
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
            WHEN 'active'  THEN IFNULL(g.active_profiles, 0)
            WHEN 'license' THEN IFNULL(g.licensed_profiles, 0)
            WHEN 'shared'  THEN IFNULL(g.active_profiles - IFNULL(g.shared_profiles, 0), 0)
        END AS profile_count
    FROM global_tenant_history g
    JOIN feb_bounds d ON g.record_date BETWEEN d.start_date AND d.end_date
    LEFT JOIN ltp_account_meta p ON g.root = p.tenant_global_id
    WHERE
        g.approved = TRUE
        AND g.billing_status IN ('Active', 'Active-POC')
        AND g.plan_name != 'No_Plan'
        AND g.root IN (SELECT tenant_global_id FROM ltp_account_meta)
),

plan_count AS (
    SELECT
        tenant_global_id,
        is_highwatermark,
        COUNT(DISTINCT plan_name) AS num_plans
    FROM profile_metrics
    GROUP BY tenant_global_id, is_highwatermark
),

most_expensive_plan_info AS (
    SELECT
        pm.tenant_global_id,
        pm.is_highwatermark,
        pm.plan_name
    FROM profile_metrics pm
    INNER JOIN plan_count pc
        ON  pm.tenant_global_id = pc.tenant_global_id
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

most_expensive_plan_start AS (
    SELECT
        mepi.tenant_global_id,
        mepi.is_highwatermark,
        mepi.plan_name,
        MIN(pm.record_date) AS plan_start_date,
        MAX(pm.record_date) AS plan_end_date
    FROM most_expensive_plan_info mepi
    INNER JOIN profile_metrics pm
        ON  mepi.tenant_global_id = pm.tenant_global_id
        AND mepi.is_highwatermark = pm.is_highwatermark
        AND mepi.plan_name        = pm.plan_name
    GROUP BY mepi.tenant_global_id, mepi.is_highwatermark, mepi.plan_name
),

profile_metrics_filtered AS (
    SELECT pm.*
    FROM profile_metrics pm
    INNER JOIN plan_count pc
        ON  pm.tenant_global_id = pc.tenant_global_id
        AND pm.is_highwatermark = pc.is_highwatermark
    WHERE pc.num_plans = 1

    UNION ALL

    SELECT pm.*
    FROM profile_metrics pm
    INNER JOIN most_expensive_plan_start meps
        ON  pm.tenant_global_id = meps.tenant_global_id
        AND pm.is_highwatermark = meps.is_highwatermark
        AND pm.plan_name        = meps.plan_name
        AND pm.record_date BETWEEN meps.plan_start_date AND meps.plan_end_date
),

highwater_selected AS (
    SELECT
        tenant_global_id,
        record_date,
        is_highwatermark,
        plan_name
    FROM profile_metrics_filtered
    WHERE is_highwatermark = 1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tenant_global_id
        ORDER BY profile_count DESC, record_date ASC
    ) = 1
),

non_highwater_selected AS (
    SELECT
        tenant_global_id,
        record_date,
        is_highwatermark,
        plan_name
    FROM profile_metrics_filtered
    WHERE is_highwatermark = 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tenant_global_id
        ORDER BY record_date DESC
    ) = 1
),

combined AS (
    SELECT * FROM highwater_selected
    UNION ALL
    SELECT * FROM non_highwater_selected
),

hwm_selected AS (
    SELECT
        tenant_global_id,
        record_date,
        is_highwatermark
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY tenant_global_id
                ORDER BY record_date DESC
            ) AS final_rank
        FROM combined
    )
    WHERE final_rank = 1
)

SELECT
    '2026-03-01'::date                  AS date_recorded,
    g.root,
    g.parent_global_id,
    g.parent_name,
    g.tenant_global_id,
    g.tenant_name,
    g.registration_date,
    g.domain,
    g.partner_pricing,
    g.plan_id,
    g.plan_name,
    g.premium_id,
    g.premium_name,
    g.incident_management,
    g.security_awareness_training,
    g.simulation_and_training_bundle,
    g.simulation_and_training_bundle_plus,
    g.ai_empower_bundle,
    g.themis_co_pilot,
    g.ato,
    g.teams_protection,
    g.file_scanning,
    g.link_scanning,
    g.multi_tenancy,
    g.licensed_profiles,
    g.active_profiles,
    g.shared_profiles,
    g.trial_plan_id,
    g.trial_plan_name,
    g.trial_premium_id,
    g.trial_premium_name,
    g.trial_plan_expiry_date,
    g.depth,
    g.approved,
    g.tree_key,
    g.record_date                       AS hwm_date,
    g.billing_status,
    g.dmarc_management,
    g.dmarc_domains_number,
    g.dmarc_ironscales_plan,
    g.not_nfr_partner
FROM hwm_selected r
JOIN global_tenant_history g
    ON  g.tenant_global_id = r.tenant_global_id
    AND g.record_date      = r.record_date