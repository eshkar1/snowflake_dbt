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
            WHEN DATE_PART('day', CURRENT_DATE) = 1 
                THEN DATEADD(day, 1, DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE)))
            WHEN DATE_PART('day', CURRENT_DATE) = 2 
                THEN CURRENT_DATE
            ELSE DATEADD(day, 1, DATE_TRUNC('month', CURRENT_DATE))
        END AS start_date,
        CURRENT_DATE AS end_date
),

profile_metrics AS (
    SELECT 
        g.record_date,
        g.root,
        g.tenant_global_id,
        g.dmarc_domains_number
    FROM global_tenant_history g
    JOIN date_bounds d 
        ON g.record_date BETWEEN d.start_date AND d.end_date
    LEFT JOIN ltp_pricing_list p 
        ON g.root = p.tenant_global_id
    WHERE 
        g.approved = TRUE
        AND g.billing_status = 'Active'
        AND g.root IN (
            SELECT tenant_global_id
            FROM ltp_pricing_list
        )
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
