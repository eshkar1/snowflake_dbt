with global_tenant_history as (
    select * from {{ ref('global_tenant_history_monthlu_billing_tbl')}}
),

main_data as (
    with extracted_data AS (
                WITH global_tenant_history_main as (
                        select * from {{ ref('global_tenant_history_monthlu_billing_tbl')}}
                                                    ),
                ltp_pricing_list as (
                        select * from {{ ref('ltp_pricing_tbl')}}
                    )

                SELECT
                    CASE
                    when l.tenant_global_id is null then 'not_ltp'
                    else 'ltp'
                    end as is_ltp,
                    LEFT(ROOT, 2) AS prefix,
                    REGEXP_SUBSTR(TREE_KEY, '[0-9]{1,}', 1, 1) AS first_number,
                    REGEXP_SUBSTR(TREE_KEY, '[0-9]{1,}', 1, 2) AS second_number,
                    REGEXP_SUBSTR(TREE_KEY, '[0-9]{1,}', 1, 3) AS third_number,
                    REGEXP_SUBSTR(TREE_KEY, '[0-9]{1,}', 1, 4) AS fourth_number,
                    REGEXP_SUBSTR(TREE_KEY, '[0-9]{1,}', 1, 5) AS fifth_number,
                    *
                FROM global_tenant_history_main g
                left join ltp_pricing_list l on g.root = l.tenant_global_id
                WHERE
                    date_recorded = current_date
                    AND billing_status = 'Active'
                    AND approved = true
            )
        SELECT
            d.is_ltp,
            COALESCE(prefix || '-' || first_number, '') AS first_layer,
            COALESCE(prefix || '-' || second_number, '') AS second_layer,
            COALESCE(prefix || '-' || third_number, '') AS third_layer,
            COALESCE(prefix || '-' || fourth_number, '') AS fourth_layer,
            COALESCE(prefix || '-' || fifth_number, '') AS fifth_layer,
            * EXCLUDE (is_ltp, first_number, second_number, third_number, fourth_number, fifth_number)
        FROM extracted_data d
)

SELECT
    a.is_ltp,
    a.first_layer AS FIRST_LAYER_ID,
    COALESCE(b.tenant_name, '') AS FIRST_LAYER_NAME,
    a.second_layer AS SECOND_LAYER_ID,
    COALESCE(c.tenant_name, '') AS SECOND_LAYER_NAME,
    a.third_layer AS THIRD_LAYER_ID,
    COALESCE(d.tenant_name, '') AS THIRD_LAYER_NAME,
    a.fourth_layer AS FOURTH_LAYER_ID,
    COALESCE(e.tenant_name, '') AS FOURTH_LAYER_NAME,
    a.fifth_layer AS FIFTH_LAYER_ID,
    COALESCE(f.tenant_name, '') AS FIFTH_LAYER_NAME,
    a.approved,
    a.billing_status,
    a.DOMAIN,
    a.PARTNER_PRICING,
    a.PLAN_ID,
    a.PLAN_NAME,
    a.PREMIUM_ID,
    a.PREMIUM_NAME,
    -- a.PLAN_EXPIRY_date,
    a.TRIAL_PLAN_ID,
    a.TRIAL_PLAN_NAME,
    a.TRIAL_PREMIUM_ID,
    a.TRIAL_PREMIUM_NAME,
    a.TRIAL_PLAN_EXPIRY_date,
    a.LICENSED_PROFILES,
    a.ACTIVE_PROFILES,
    a.record_date,
    a.TREE_KEY,
    a.INCIDENT_MANAGEMENT,
    a.SECURITY_AWARENESS_TRAINING,
    a.ATO,
    a.MULTI_TENANCY,
    a.PARENT_NAME,
    a.SIMULATION_AND_TRAINING_BUNDLE,
    a.SIMULATION_AND_TRAINING_BUNDLE_PLUS,
    a.AI_EMPOWER_BUNDLE,
    a.THEMIS_CO_PILOT,
    a.TEAMS_PROTECTION,
    a.FILE_SCANNING,
    a.LINK_SCANNING,
    a.SHARED_PROFILES,
    a.date_recorded
FROM main_data a
LEFT JOIN global_tenant_history b ON a.record_date = b.record_date AND a.first_layer = b.tenant_global_id
LEFT JOIN global_tenant_history c ON a.record_date = c.record_date AND a.second_layer = c.tenant_global_id
LEFT JOIN global_tenant_history d ON a.record_date = d.record_date AND a.third_layer = d.tenant_global_id
LEFT JOIN global_tenant_history e ON a.record_date = e.record_date AND a.fourth_layer = e.tenant_global_id
LEFT JOIN global_tenant_history f ON a.record_date = f.record_date AND a.fifth_layer = f.tenant_global_id