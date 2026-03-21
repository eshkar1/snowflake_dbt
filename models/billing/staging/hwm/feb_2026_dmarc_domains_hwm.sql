WITH global_tenant_history AS (
    SELECT * FROM {{ ref('global_tenant_history') }}
),

ltp_pricing_list AS (
    SELECT * FROM {{ ref('ltp_pricing_tbl') }}
),

feb_bounds AS (
    SELECT
        '2026-02-02'::date AS start_date,
        '2026-03-01'::date AS end_date
),

profile_metrics AS (
    SELECT
        g.record_date,
        g.root,
        g.tenant_global_id,
        g.dmarc_domains_number
    FROM global_tenant_history g
    JOIN feb_bounds d ON g.record_date BETWEEN d.start_date AND d.end_date
    LEFT JOIN ltp_pricing_list p ON g.root = p.tenant_global_id
    WHERE
        g.approved = TRUE
        AND g.billing_status IN ('Active', 'Active-POC')
        AND g.root IN (SELECT tenant_global_id FROM ltp_pricing_list)
        AND g.dmarc_management = TRUE
),

highwater_selected AS (
    SELECT
        tenant_global_id,
        record_date,
        dmarc_domains_number
    FROM profile_metrics
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tenant_global_id
        ORDER BY dmarc_domains_number DESC, record_date ASC
    ) = 1
)

SELECT
    tenant_global_id,
    record_date,
    dmarc_domains_number
FROM highwater_selected