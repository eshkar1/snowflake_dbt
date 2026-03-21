WITH global_tenant_history_daily AS (
    SELECT * FROM {{ ref('feb_2026_ltp_hwm') }}
),

ltp_pricing_list AS (
    SELECT * FROM {{ ref('ltp_pricing_tbl') }}
    WHERE IS_TRACKED = TRUE
),

ltp_daily_itemized AS (
    SELECT * FROM {{ ref('feb_2026_ltp_billing_calc') }}
),

conversion_tbl_sf AS (
    SELECT * FROM {{ ref('stg_salesforce_conversion_rate_table') }}
),

tenant_details AS (
    SELECT
        root                                                        AS ltp,
        COUNT(DISTINCT g.tenant_global_id)                         AS total_customers,
        CASE p.profile_type
            WHEN 'active'  THEN SUM(active_profiles)
            WHEN 'license' THEN SUM(licensed_profiles)
            WHEN 'shared'  THEN
                CASE
                    WHEN SUM(shared_profiles) IS NULL THEN SUM(active_profiles)
                    ELSE (SUM(active_profiles) - SUM(shared_profiles))
                END
        END                                                         AS total_emails
    FROM global_tenant_history_daily g
    LEFT JOIN ltp_pricing_list p
        ON  g.root          = p.tenant_global_id
        AND p.snapshot_date = current_date()
    WHERE
        billing_status IN ('Active', 'Active-POC')
        AND approved = TRUE
        AND partner_pricing = FALSE
        AND g.root IN (
            SELECT tenant_global_id
            FROM {{ ref('ltp_pricing_tbl') }}
            WHERE snapshot_date = current_date()
        )
    GROUP BY
        root,
        p.profile_type
),

conversion_tbl AS (
    SELECT
        isocode,
        conversionrate
    FROM (
        SELECT
            isocode,
            conversionrate,
            ROW_NUMBER() OVER (
                PARTITION BY isocode
                ORDER BY ABS(DATEDIFF('day', DATE(startdate), '2026-03-01'::date)) ASC
            ) AS rn
        FROM conversion_tbl_sf
    )
    WHERE rn = 1
)

SELECT
    p.tenant_global_id,
    p.tenant_name,
    p.account_master_id,
    l.item,
    l.quantity,
    l.amount,
    td.total_customers,
    td.total_emails,
    CASE
        WHEN td.total_customers > 1
             OR td.total_emails > 100
             OR l.amount > 0
        THEN CASE
                 WHEN l.quantity > 100 THEN l.quantity - 100
                 ELSE 0
             END
        ELSE l.quantity
    END                                                             AS billable_qty,
    p.currency,
    CASE
        WHEN l.item = 'Complete Protect' THEN IFNULL(conversionrate * 2, 2)
        WHEN l.item = 'Email Protect'    THEN IFNULL(conversionrate * 1, 1)
    END                                                             AS nfr_rate,
    billable_qty * nfr_rate                                         AS invoice
FROM ltp_pricing_list p
LEFT JOIN ltp_daily_itemized l
    ON  p.tenant_global_id = l.ltp
LEFT JOIN tenant_details td
    ON  p.tenant_global_id = td.ltp
LEFT JOIN conversion_tbl c
    ON  p.currency         = c.isocode
WHERE
    p.snapshot_date        = current_date()
    AND ltp_type           = 'msp'
    AND registration_date <= DATEADD(day, -90, '2026-03-01'::date)
    AND l.partner_pricing  = TRUE
    AND l.item             IN ('Complete Protect', 'Email Protect')
    AND billable_qty       > 0
    AND l.quantity         > 1