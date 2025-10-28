with global_tenant_history_daily as (
    select * from 
    {{ ref('global_tenant_history_daily_billing_no_hwm_tbl')}}
    -- prod_conform.dbt_stage_db.global_tenant_history_daily_billing_tbl
    -- {{ ref('global_tenant_history_daily_billing_tbl')}} 
),

ltp_pricing_list as (
    select * from 
    -- prod_conform.dbt_raw_db.ltp_pricing_tbl
    {{ ref('ltp_pricing_tbl')}}
    where
    tenant_global_id in ('US-733','EU-25')
    and IS_TRACKED = true
)
,

-- hwm_dmarc_count as (
--     select * from 
--     -- prod_conform.dbt_stage_db.current_month_hwm_dmarc_domains_number
--     {{ ref('current_month_hwm_dmarc_domains_number')}}
-- ),

-- =================== ALL BRANCHES RETURN 8 COLS (incl. combined_quantity) ===================
unioned AS (

  /* --------------------------- PLANS (has combined_quantity) --------------------------- */
  SELECT
    s.date_recorded,
    s.ltp,
    s.item,
    s.sku,
    s.quantity,
    s.combined_quantity,                -- only this branch sets it
    s.partner_pricing,
    /* amount uses quantity/combined_quantity already computed in inner select */
    CASE
      /* EP (non-NFR) */
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Email Protect' AND s.combined_quantity >= 160000 AND s.ltp = 'US-733'
        THEN (150000 * s.EP_1) + ((160000-150000) * s.EP_1000) + (s.quantity - 160000) * s.EP_3500
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Email Protect' AND s.combined_quantity >= 160000 AND s.ltp = 'EU-25'
        THEN s.quantity * s.EP_3500
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Email Protect' AND s.combined_quantity >= 150000 AND s.ltp = 'US-733'
        THEN (150000 * s.EP_1) + (s.quantity-150000) * s.EP_1000
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Email Protect' AND s.combined_quantity >= 150000 AND s.ltp = 'EU-25'
        THEN s.quantity * s.EP_1000
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Email Protect' AND s.combined_quantity < 150000
        THEN s.quantity * s.EP_1

      /* CORE (non-NFR) */
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity >= 120000 AND s.ltp = 'US-733'
        THEN (5000 * s.CORE_1) + ((10000-5000) * s.CORE_1000) + ((25000-10000) * s.CORE_3500)
           + ((50000-25000) * s.CORE_7500) + ((120000-50000) * s.CORE_10000) + (s.quantity-120000) * s.CORE_120000
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity >= 120000 AND s.ltp = 'EU-25'
        THEN s.quantity * s.CORE_120000
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity >= 50000 AND s.ltp = 'US-733'
        THEN (5000 * s.CORE_1) + ((10000-5000) * s.CORE_1000) + ((25000-10000) * s.CORE_3500)
           + ((50000-25000) * s.CORE_7500) + (s.quantity-50000) * s.CORE_10000
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity >= 50000 AND s.ltp = 'EU-25'
        THEN s.quantity * s.CORE_10000
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity >= 25000 AND s.ltp = 'US-733'
        THEN (5000 * s.CORE_1) + ((10000-5000) * s.CORE_1000) + ((25000-10000) * s.CORE_3500) + (s.quantity-25000) * s.CORE_7500
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity >= 25000 AND s.ltp = 'EU-25'
        THEN s.quantity * s.CORE_7500
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity >= 10000 AND s.ltp = 'US-733'
        THEN (5000 * s.CORE_1) + ((10000-5000) * s.CORE_1000) + (s.quantity-10000) * s.CORE_3500
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity >= 10000 AND s.ltp = 'EU-25'
        THEN s.quantity * s.CORE_3500
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity >= 5000 AND s.ltp = 'US-733'
        THEN (5000 * s.CORE_1) + (s.quantity-5000) * s.CORE_1000
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity >= 5000 AND s.ltp = 'EU-25'
        THEN s.quantity * s.CORE_1000
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Core' AND s.combined_quantity < 5000
        THEN s.quantity * s.CORE_1

      /* IP (non-NFR) */
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'IRONSCALES Protect'
        THEN s.quantity * s.IP_1

      /* CP (non-NFR) */
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Complete Protect' AND s.combined_quantity >= 40000 AND s.ltp = 'US-733'
        THEN (40000 * s.CP_1) + (s.quantity-40000) * s.CP_1000
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Complete Protect' AND s.combined_quantity >= 40000 AND s.ltp = 'EU-25'
        THEN s.quantity * s.CP_1000
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Complete Protect' AND s.combined_quantity < 40000
        THEN s.quantity * s.CP_1

      /* Starter / SAT (non-NFR) */
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'Starter'
        THEN s.combined_quantity * s.STARTER_1
      WHEN s.partner_pricing = FALSE AND s.plan_name = 'SAT Suite'
        THEN s.combined_quantity * s.SAT_SUITE_1

      /* NFR plans */
      WHEN s.partner_pricing = TRUE AND s.plan_name = 'Email Protect'          THEN s.quantity * s.EPNFR_1
      WHEN s.partner_pricing = TRUE AND s.plan_name = 'Core'                   THEN s.quantity * s.CORENFR_1
      WHEN s.partner_pricing = TRUE AND s.plan_name = 'IRONSCALES Protect'     THEN s.quantity * s.IPNFR_1
      WHEN s.partner_pricing = TRUE AND s.plan_name = 'Complete Protect'       THEN s.quantity * s.CPNFR_1
      WHEN s.partner_pricing = TRUE AND s.plan_name = 'Starter'                THEN s.quantity * s.STARTERNFR_1
      WHEN s.partner_pricing = TRUE AND s.plan_name = 'SAT Suite'              THEN s.quantity * s.SAT_SUITENFR_1
    END AS amount
  FROM (
    SELECT
      g.date_recorded,
      g.root AS ltp,
      g.plan_name AS item,
      CASE g.partner_pricing
        WHEN FALSE THEN CASE g.plan_name
          WHEN 'Starter'            THEN 'IS-LTP-STARTER'
          WHEN 'Email Protect'      THEN 'IS-LTP-EP'
          WHEN 'Complete Protect'   THEN 'IS-LTP-CP'
          WHEN 'Core'               THEN 'IS-LTP-CORE'
          WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IP'
          WHEN 'SAT Suite'          THEN 'IS-SAT_SUITE_1'
        END
        WHEN TRUE THEN CASE g.plan_name
          WHEN 'Starter'            THEN 'IS-LTP-STARTERNFR'
          WHEN 'Email Protect'      THEN 'IS-LTP-EPNFR'
          WHEN 'Complete Protect'   THEN 'IS-LTP-CPNFR'
          WHEN 'Core'               THEN 'IS-LTP-CORENFR'
          WHEN 'IRONSCALES Protect' THEN 'IS-LTP-IPNFR'
          WHEN 'SAT Suite'          THEN 'IS-SAT_SUITENFR_1'
        END
      END AS sku,
      g.plan_name,
      g.partner_pricing,
      /* quantity */
      CASE p.profile_type
        WHEN 'active'  THEN SUM(active_profiles)
        WHEN 'license' THEN SUM(licensed_profiles)
        WHEN 'shared'  THEN CASE WHEN SUM(shared_profiles) IS NULL
                                 THEN SUM(active_profiles)
                                 ELSE (SUM(active_profiles) - SUM(shared_profiles))
                            END
      END AS quantity,
      /* combined_quantity (window) */
      SUM(
        CASE p.profile_type
          WHEN 'active'  THEN SUM(active_profiles)
          WHEN 'license' THEN SUM(licensed_profiles)
          WHEN 'shared'  THEN CASE WHEN SUM(shared_profiles) IS NULL
                                   THEN SUM(active_profiles)
                                   ELSE (SUM(active_profiles) - SUM(shared_profiles))
                              END
        END
      ) OVER (PARTITION BY g.date_recorded, g.plan_name, p.profile_type, g.partner_pricing) AS combined_quantity,
      /* price columns */
      p.EP_3500, p.EP_1000, p.EP_1,
      p.CORE_120000, p.CORE_10000, p.CORE_7500, p.CORE_3500, p.CORE_1000, p.CORE_1,
      p.IP_1,
      p.CP_1000, p.CP_1,
      p.STARTER_1, p.EPNFR_1, p.CORENFR_1, p.IPNFR_1, p.CPNFR_1, p.STARTERNFR_1,
      p.SAT_SUITE_1, p.SAT_SUITENFR_1
    FROM global_tenant_history_daily g
    LEFT JOIN ltp_pricing_list p ON g.root = p.tenant_global_id
    WHERE
      g.approved = TRUE
      AND g.billing_status in ('Active','Active-POC')
      AND p.profile_type IS NOT NULL
      AND g.root IN ('US-733','EU-25')
      AND g.licensed_profiles IS NOT NULL
    GROUP BY
      g.date_recorded, g.root, g.plan_name, g.partner_pricing, p.profile_type,
      p.EP_3500, p.EP_1000, p.EP_1,
      p.CORE_120000, p.CORE_10000, p.CORE_7500, p.CORE_3500, p.CORE_1000, p.CORE_1,
      p.IP_1,
      p.CP_1000, p.CP_1,
      p.STARTER_1, p.EPNFR_1, p.CORENFR_1, p.IPNFR_1, p.CPNFR_1, p.STARTERNFR_1,
      p.SAT_SUITE_1, p.SAT_SUITENFR_1
  ) s

  UNION ALL

  /* --------------------------- PREMIUM (set combined_quantity = NULL) --------------------------- */
  SELECT
    t.date_recorded,
    t.ltp,
    t.item,
    t.sku,
    t.quantity,
    NULL AS combined_quantity,
    NULL AS partner_pricing,
    CASE t.premium_name
      WHEN 'NINJIO'               THEN t.quantity * t.PSCP_1
      WHEN 'Cybermaniacs Videos'  THEN t.quantity * t.PSCP_1
      WHEN 'Habitu8'              THEN t.quantity * t.PSCP_1
    END AS amount
  FROM (
    SELECT
      g.date_recorded,
      g.root AS ltp,
      g.premium_name,
      CASE g.premium_name
        WHEN 'NINJIO'              THEN 'IS-LTP-PSCP'
        WHEN 'Cybermaniacs Videos' THEN 'IS-LTP-PSCP'
        WHEN 'Habitu8'             THEN 'IS-LTP-PSCP'
      END AS sku,
      CASE p.profile_type
        WHEN 'active'  THEN SUM(active_profiles)
        WHEN 'license' THEN SUM(licensed_profiles)
        WHEN 'shared'  THEN CASE WHEN SUM(shared_profiles) IS NULL
                                 THEN SUM(active_profiles)
                                 ELSE (SUM(active_profiles) - SUM(shared_profiles))
                            END
      END AS quantity,
      'Premium' AS item,
      p.PSCP_1
    FROM global_tenant_history_daily g
    LEFT JOIN ltp_pricing_list p ON g.root = p.tenant_global_id
    WHERE g.approved = TRUE
      AND g.billing_status in ('Active','Active-POC')
      AND g.root IN ('US-733','EU-25')
      AND g.premium_name <> 'No Premium'
    GROUP BY g.date_recorded, g.root, g.premium_name, p.profile_type, p.PSCP_1
  ) t

  UNION ALL

  /* --------------------------- INCIDENT MANAGEMENT --------------------------- */
  SELECT
    u.date_recorded,
    u.ltp,
    'Incident Management' AS item,
    'IS-LTP-IM'           AS sku,
    u.quantity,
    NULL AS combined_quantity,
    NULL AS partner_pricing,
    u.quantity * u.IM_1   AS amount
  FROM (
    SELECT
      g.date_recorded,
      g.root AS ltp,
      CASE p.profile_type
        WHEN 'active'  THEN SUM(active_profiles)
        WHEN 'license' THEN SUM(licensed_profiles)
        WHEN 'shared'  THEN CASE WHEN SUM(shared_profiles) IS NULL
                                 THEN SUM(active_profiles)
                                 ELSE (SUM(active_profiles) - SUM(shared_profiles))
                            END
      END AS quantity,
      p.IM_1
    FROM global_tenant_history_daily g
    LEFT JOIN ltp_pricing_list p ON g.root = p.tenant_global_id
    WHERE g.approved = TRUE
      AND g.billing_status in ('Active','Active-POC')
      AND g.root IN ('US-733','EU-25')
      AND g.incident_management = TRUE
    GROUP BY g.date_recorded, g.root, p.profile_type, p.IM_1
  ) u

  UNION ALL

  /* --------------------------- S&T PLUS BUNDLE --------------------------- */
  SELECT
    u.date_recorded,
    u.ltp,
    'S&T Plus Bundle' AS item,
    'IS-LTP-STBP'     AS sku,
    u.quantity,
    NULL AS combined_quantity,
    NULL AS partner_pricing,
    u.quantity * u.STBP_1 AS amount
  FROM (
    SELECT
      g.date_recorded,
      g.root AS ltp,
      CASE p.profile_type
        WHEN 'active'  THEN SUM(active_profiles)
        WHEN 'license' THEN SUM(licensed_profiles)
        WHEN 'shared'  THEN CASE WHEN SUM(shared_profiles) IS NULL
                                 THEN SUM(active_profiles)
                                 ELSE (SUM(active_profiles) - SUM(shared_profiles))
                            END
      END AS quantity,
      p.STBP_1
    FROM global_tenant_history_daily g
    LEFT JOIN ltp_pricing_list p ON g.root = p.tenant_global_id
    WHERE g.approved = TRUE
      AND g.billing_status in ('Active','Active-POC')
      AND g.root IN ('US-733','EU-25')
      AND g.simulation_and_training_bundle_plus = TRUE
      AND g.plan_name NOT IN ('Complete Protect','SAT Suite')
    GROUP BY g.date_recorded, g.root, p.profile_type, p.STBP_1
  ) u

  UNION ALL

  /* --------------------------- ACCOUNT TAKEOVER --------------------------- */
  SELECT
    u.date_recorded,
    u.ltp,
    'Account Takeover' AS item,
    'IS-LTP-ATO'       AS sku,
    u.quantity,
    NULL AS combined_quantity,
    NULL AS partner_pricing,
    u.quantity * u.ATO_1 AS amount
  FROM (
    SELECT
      g.date_recorded,
      g.root AS ltp,
      CASE p.profile_type
        WHEN 'active'  THEN SUM(active_profiles)
        WHEN 'license' THEN SUM(licensed_profiles)
        WHEN 'shared'  THEN CASE WHEN SUM(shared_profiles) IS NULL
                                 THEN SUM(active_profiles)
                                 ELSE (SUM(active_profiles) - SUM(shared_profiles))
                            END
      END AS quantity,
      p.ATO_1
    FROM global_tenant_history_daily g
    LEFT JOIN ltp_pricing_list p ON g.root = p.tenant_global_id
    WHERE g.approved = TRUE
      AND g.billing_status in ('Active','Active-POC')
      AND g.root IN ('US-733','EU-25')
      AND g.ATO = TRUE
      AND g.plan_name <> 'Complete Protect'
    GROUP BY g.date_recorded, g.root, p.profile_type, p.ATO_1
  ) u

  UNION ALL

  /* --------------------------- DMARC --------------------------- */
  SELECT
    d.date_recorded,
    d.ltp,
    'DMARC'            AS item,
    'IS-LTP-DMARC'     AS sku,
    d.dmarc_quantity as quantity,
    NULL AS combined_quantity,
    NULL AS partner_pricing,
    d.dmarc_quantity * d.DMARC_1 AS amount
  FROM (
    SELECT
      g.date_recorded,
      g.root AS ltp,
      SUM(g.dmarc_domains_number) AS dmarc_quantity,
      p.DMARC_1
    FROM global_tenant_history_daily g
    LEFT JOIN ltp_pricing_list p ON g.root = p.tenant_global_id
    -- LEFT JOIN hwm_dmarc_count h   ON g.tenant_global_id = h.tenant_global_id
    WHERE g.approved = TRUE
      AND g.billing_status in ('Active','Active-POC')
      AND g.root IN ('US-733','EU-25')
    GROUP BY g.date_recorded, g.root, p.DMARC_1
    HAVING sum(g.dmarc_domains_number) > 0
  ) d
)

-- =================== HIDE combined_quantity FROM FINAL OUTPUT ===================
SELECT
  date_recorded,
  ltp,
  item,
  sku,
  quantity,
  partner_pricing,
  amount
FROM unioned










-- with global_tenant_history_daily as (
--     select * from 
--     -- prod_conform.dbt_stage_db.global_tenant_history_daily_billing_tbl
--     {{ ref('global_tenant_history_daily_billing_tbl')}} 
-- ),

-- ltp_pricing_list as (
--     select * from 
--     -- prod_conform.dbt_raw_db.ltp_pricing_tbl
--     {{ ref('ltp_pricing_tbl')}}
--     where
--     tenant_global_id in ('US-733','EU-25')
--     and IS_TRACKED = true
-- ),

-- hwm_dmarc_count as (
--     select * from 
--     -- prod_conform.dbt_stage_db.current_month_hwm_dmarc_domains_number
--     {{ ref('current_month_hwm_dmarc_domains_number')}}
-- )


-- ----------------------------------------------------------------------------------------
--                                     -- Plans --
-- ----------------------------------------------------------------------------------------

-- select
-- g.DATE_RECORDED,
-- g.root as ltp,
-- g.plan_name as item,  
-- CASE partner_pricing
--     WHEN FALSE then 
--         CASE plan_name
--             WHEN 'Starter'                          THEN 'IS-LTP-STARTER'
--             WHEN 'Email Protect'                    THEN 'IS-LTP-EP'
--             WHEN 'Complete Protect'                 THEN 'IS-LTP-CP'
--             WHEN 'Core'                             THEN 'IS-LTP-CORE'
--             WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IP'
--             WHEN 'SAT Suite'                        THEN 'IS-SAT_SUITE_1' ---????????----
--         end
--     WHEN TRUE THEN
--         CASE plan_name
--             WHEN 'Starter'                          THEN 'IS-LTP-STARTERNFR'
--             WHEN 'Email Protect'                    THEN 'IS-LTP-EPNFR'
--             WHEN 'Complete Protect'                 THEN 'IS-LTP-CPNFR'
--             WHEN 'Core'                             THEN 'IS-LTP-CORENFR'
--             WHEN 'IRONSCALES Protect'               THEN 'IS-LTP-IPNFR'
--             WHEN 'SAT Suite'                        THEN 'IS-SAT_SUITENFR_1' ---????????----
--         end    
-- else null
-- end as sku,
-- CASE p.profile_type
--     when 'active' then sum(Active_profiles)
--     when 'license' then sum(licensed_profiles)
--     when 'shared' then 
--                     case 
--                         when sum(SHARED_PROFILES) is null then sum(Active_profiles)
--                         else (sum(Active_profiles) - sum(SHARED_PROFILES))
--                     end
-- end as quantity,
-- g.partner_pricing,


-- -- Non NFR Plans --
-- CASE 

--     WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 160000 then (150000 * EP_1) + ((160000-150000) * EP_1000) + (quantity - 160000) * EP_3500
--     WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity >= 150000 then (150000 * EP_1) + (quantity-150000) * EP_1000
--     WHEN g.partner_pricing = FALSE and plan_name = 'Email Protect' and quantity < 150000 then quantity * EP_1
    
    
--     WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 120000 then (5000 * CORE_1) + ((10000-5000) * CORE_1000) + ((25000-10000) * CORE_3500) + ((50000-25000) * CORE_7500) + ((120000-50000) * CORE_10000) + (quantity-120000) * CORE_120000
--     WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 50000 then (5000 * CORE_1) + ((10000-5000) * CORE_1000) + ((25000-10000) * CORE_3500) + ((50000-25000) * CORE_7500) + (quantity-50000) * CORE_10000
--     WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 25000 then (5000 * CORE_1) + ((10000-5000) * CORE_1000) + ((25000-10000) * CORE_3500) + (quantity-25000) * CORE_7500
--     WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 10000 then (5000 * CORE_1) + ((10000-5000) * CORE_1000) + (quantity-10000) * CORE_3500
--     WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity >= 5000 then (5000 * CORE_1) + (quantity-5000) * CORE_1000
--     WHEN g.partner_pricing = FALSE and plan_name = 'Core' and quantity < 5000 then quantity * CORE_1
--     -- end
--     WHEN g.partner_pricing = FALSE and plan_name = 'IRONSCALES Protect' then quantity * IP_1

--     WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and quantity >= 40000 then (40000 * CP_1) + (quantity-40000) * cp_1000
--     WHEN g.partner_pricing = FALSE and plan_name = 'Complete Protect' and quantity < 40000 then quantity * CP_1

--     WHEN g.partner_pricing = FALSE and plan_name = 'Starter' then quantity * STARTER_1

--     WHEN g.partner_pricing = FALSE and plan_name = 'SAT Suite' then quantity * SAT_SUITE_1


--     -- WHEN g.partner_pricing = FALSE and plan_name = 'Phishing Simulation and Training' and premium_name = 'No Premium' then quantity * PST_1

--     -- NFR Plans Only --

--     WHEN g.partner_pricing = True and plan_name = 'Email Protect' then quantity * EPNFR_1
--     WHEN g.partner_pricing = True and plan_name = 'Core' then quantity * CORENFR_1
--     WHEN g.partner_pricing = True and plan_name = 'IRONSCALES Protect' then quantity * IPNFR_1
--     WHEN g.partner_pricing = True and plan_name = 'Complete Protect' then quantity * CPNFR_1
--     WHEN g.partner_pricing = True and plan_name = 'Starter' then quantity * STARTERNFR_1
--     WHEN g.partner_pricing = True and plan_name = 'SAT Suite' then quantity * SAT_SUITENFR_1

--     -- WHEN g.partner_pricing = True and plan_name = 'Phishing Simulation and Training' and premium_name = 'No Premium' then quantity * PSTNFR_1    
                     
-- end as amount        
-- -- my_record_date as record_date
-- from global_tenant_history_daily g
-- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- where
--     approved = true
--     and billing_status = 'Active'
--     and profile_type is not NULL
--     and ltp in ('US-733','EU-25') 
--     -- and plan_name != 'SAT Suite'
--     and licensed_profiles is not NULL

-- group by
-- g.DATE_RECORDED,
-- root,   
-- plan_name,
-- sku,
-- profile_type,
-- -- premium_name,
-- g.partner_pricing,
-- p.EP_3500,
-- p.EP_1000,
-- p.EP_1,
-- p.CORE_120000,
-- p.Core_10000,
-- p.CORE_7500,
-- p.CORE_3500,
-- p.CORE_1000,
-- p.CORE_1,
-- p.IP_1,
-- p.CP_1000,
-- p.CP_1,
-- STARTER_1,
-- p.EPNFR_1,
-- p.CORENFR_1,
-- IPNFR_1,
-- CPNFR_1,
-- STARTERNFR_1,
-- SAT_SUITE_1,
-- SAT_SUITENFR_1


-- -- -------------------------------------------
-- -- ---- Phishing Simulation and Training -----
-- -- -------------------------------------------
-- -- UNION

-- -- select
-- -- g.DATE_RECORDED,
-- -- g.root as ltp,
-- -- g.plan_name as item,    
-- -- CASE
-- --     WHEN g.partner_pricing = True then  'IS-LTP-PSTNFR'
-- --     WHEN g.partner_pricing = False then  'IS-LTP-PST'
-- -- end as sku,
-- -- sum(licensed_profiles) as quantity,
-- -- g.partner_pricing,
-- -- CASE
-- --     WHEN g.partner_pricing = True then  quantity * PSTNFR_1
-- --     WHEN g.partner_pricing = False then  quantity * PST_1
-- -- end as amount,
-- -- from global_tenant_history_daily g
-- -- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- -- where
-- --     approved = true
-- --     and billing_status = 'Active'
-- --     and profile_type is not NULL
-- --     and ltp in ('US-733','EU-25')  
-- --     and plan_name = 'Phishing Simulation and Training'
-- --     and premium_name = 'No Premium'
-- -- group by 
-- -- g.DATE_RECORDED,
-- -- root,   
-- -- plan_name,
-- -- sku,
-- -- g.partner_pricing,
-- -- PSTNFR_1,
-- -- PST_1

-- ----------------------------------------------------------------------------------------
--                                     -- Add Ons --
-- ----------------------------------------------------------------------------------------

-- -------------
-- -- premium --
-- -------------

-- UNION

-- select
-- g.DATE_RECORDED,
-- g.root as ltp,
-- premium_name as item,
-- case
--     premium_name
--     when 'NINJIO'              then 'IS-LTP-PSCP'
--     when 'Cybermaniacs Videos' then 'IS-LTP-PSCP'
--     when 'Habitu8'             then 'IS-LTP-PSCP'
-- end as sku, 
-- CASE p.profile_type
--     when 'active' then sum(Active_profiles)
--     when 'license' then sum(licensed_profiles)
--     when 'shared' then 
--                     case 
--                         when sum(SHARED_PROFILES) is null then sum(Active_profiles)
--                         else (sum(Active_profiles) - sum(SHARED_PROFILES))
--                     end
-- end as quantity,
-- null as partner_pricing,
-- case
--     premium_name
--     when 'NINJIO' then quantity * PSCP_1
--     when 'Cybermaniacs Videos' then quantity * PSCP_1
--     when 'Habitu8' then quantity * PSCP_1
-- end as amount,

-- from global_tenant_history_daily g
-- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- where
--     approved = true
--     and billing_status = 'Active'
--     and ltp in ('US-733','EU-25') 
--     and premium_name != 'No Premium'
-- group by                              
-- g.DATE_RECORDED,
-- root,   
-- item,
-- sku,
-- profile_type,
-- -- g.partner_pricing,
-- premium_name,
-- PSCP_1

-- -------------------------
-- -- incident management --
-- -------------------------

-- UNION

-- select
-- g.DATE_RECORDED,
-- g.root as ltp,
-- 'Incident Management' as item,
-- 'IS-LTP-IM' as sku,
-- CASE p.profile_type
--     when 'active' then sum(Active_profiles)
--     when 'license' then sum(licensed_profiles)
--     when 'shared' then 
--                     case 
--                         when sum(SHARED_PROFILES) is null then sum(Active_profiles)
--                         else (sum(Active_profiles) - sum(SHARED_PROFILES))
--                     end
-- end as quantity,
-- null as partner_pricing,
-- quantity * IM_1 as amount

-- from global_tenant_history_daily g
-- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- where
--     approved = true
--     and billing_status = 'Active'
--     and ltp in ('US-733','EU-25') 
--     and incident_management = true
-- group by
--     g.DATE_RECORDED,
--     root,   
--     item,
--     sku,
--     profile_type,
--     -- g.partner_pricing,
--     IM_1


-- -- -------------------------
-- -- ------ S&T Bundle -------
-- -- -------------------------

-- -- -- plan name is 'Phishing Simulation and Training' --
-- -- UNION

-- -- select
-- -- g.DATE_RECORDED,
-- -- g.root as ltp,
-- -- 'S&T Bundle' as item,
-- -- 'IS-LTP-PSTSTB' as sku,
-- -- sum(licensed_profiles) as quantity,
-- -- null as partner_pricing,
-- -- quantity * PSTSTB_1 as amount
-- -- from global_tenant_history_daily g
-- -- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- -- where
-- --     approved = true
-- --     and billing_status = 'Active'
-- --     and ltp in ('US-733','EU-25') 
-- --     and simulation_and_training_bundle = true
-- --     and simulation_and_training_bundle_plus = false
-- --     and plan_name = 'Phishing Simulation and Training'
-- -- group by
-- --     g.DATE_RECORDED,
-- --     root,   
-- --     item,
-- --     sku,
-- --     profile_type,
-- --     -- g.partner_pricing,
-- --     PSTSTB_1

-- -- -- plan name is not 'Phishing Simulation and Training' --
-- -- UNION

-- -- select
-- -- g.DATE_RECORDED,
-- -- g.root as ltp,
-- -- 'S&T Bundle' as item,
-- -- 'IS-LTP-STB' as sku,
-- -- sum(licensed_profiles) as quantity,
-- -- null as partner_pricing,
-- -- quantity * STB_1 as amount
-- -- from global_tenant_history_daily g
-- -- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- -- where
-- --     approved = true
-- --     and billing_status = 'Active'
-- --     and ltp in ('US-733','EU-25') 
-- --     and simulation_and_training_bundle = true
-- --     and simulation_and_training_bundle_plus = false
-- --     and plan_name != 'Complete Protect'
-- --     and plan_name != 'Phishing Simulation and Training'
-- -- group by
-- --     g.DATE_RECORDED,
-- --     root,   
-- --     item,
-- --     sku,
-- --     profile_type,
-- --     -- g.partner_pricing,
-- --     STB_1

-- -- -----------------------------------------
-- -- ------ Security Awareness Training ------
-- -- -----------------------------------------

-- -- -- plan name is 'Phishing Simulation and Training' --
-- -- UNION

-- -- select
-- -- g.DATE_RECORDED,
-- -- g.root as ltp,
-- -- 'Security Awareness Training' as item,
-- -- 'IS-LTP-PSTSAT' as sku,
-- -- sum(licensed_profiles) as quantity,
-- -- null as partner_pricing,
-- -- quantity * PSTSAT_1

-- --  as amount
-- -- from global_tenant_history_daily g
-- -- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- -- where
-- --     approved = true
-- --     and billing_status = 'Active'
-- --     and ltp in ('US-733','EU-25') 
-- --     and security_awareness_training = true
-- --     and simulation_and_training_bundle = false
-- --     and simulation_and_training_bundle_plus = false
-- --     and plan_name = 'Phishing Simulation and Training'
-- -- group by
-- --     g.DATE_RECORDED,
-- --     root,   
-- --     item,
-- --     sku,
-- --     profile_type,
-- --     -- g.partner_pricing,
-- --     PSTSAT_1
    
-- -- -- plan name is not 'Phishing Simulation and Training' --    
-- -- UNION

-- -- select
-- -- g.DATE_RECORDED,
-- -- g.root as ltp,
-- -- 'Security Awareness Training' as item,
-- -- 'IS-LTP-SAT' as sku,
-- -- sum(licensed_profiles) as quantity,
-- -- null as partner_pricing,
-- -- quantity * SAT_1
-- --  as amount
-- -- from global_tenant_history_daily g
-- -- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- -- where
-- --     approved = true
-- --     and billing_status = 'Active'
-- --     and ltp in ('US-733','EU-25') 
-- --     and security_awareness_training = true
-- --     and simulation_and_training_bundle = false
-- --     and simulation_and_training_bundle_plus = false
-- --     and plan_name != 'Complete Protect'
-- --     and plan_name != 'Phishing Simulation and Training'
-- -- group by
-- --     g.DATE_RECORDED,
-- --     root,   
-- --     item,
-- --     sku,
-- --     profile_type,
-- --     -- g.partner_pricing,
-- --     SAT_1

-- -- -----------------------------------------
-- -- ------------ themis co-pilot ------------
-- -- -----------------------------------------

-- -- UNION 

-- -- select
-- -- g.DATE_RECORDED,
-- -- g.root as ltp,
-- -- 'Themis Co-Pilot' as item,
-- -- 'IS-LTP-THEMIS' as sku,
-- -- sum(licensed_profiles) as quantity,
-- -- null as partner_pricing,
-- -- quantity * THEMIS_1 as amount
-- -- from global_tenant_history_daily g
-- -- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- -- where
-- --     approved = true
-- --     and billing_status = 'Active'
-- --     and ltp in ('US-733','EU-25') 
-- --     and themis_co_pilot = true
-- --     and AI_EMPOWER_BUNDLE = false
-- --     and simulation_and_training_bundle_plus = false
-- --     and plan_name != 'Complete Protect'
-- -- group by
-- --     g.DATE_RECORDED,
-- --     root,   
-- --     item,
-- --     sku,
-- --     profile_type,
-- --     -- g.partner_pricing,
-- --     THEMIS_1

-- -- -----------------------------------------
-- -- --------------- url scans ---------------
-- -- -----------------------------------------

-- -- UNION 

-- -- select
-- -- g.DATE_RECORDED,
-- -- g.root as ltp,
-- -- 'URL Scans' as item,
-- -- 'IS-LTP-URL' as sku,
-- -- sum(licensed_profiles) as quantity,
-- -- null as partner_pricing,
-- -- quantity * URL_1 as amount
-- -- from global_tenant_history_daily g
-- -- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- -- where
-- --     approved = true
-- --     and billing_status = 'Active'
-- --     and ltp in ('US-733','EU-25') 
-- --     and link_scanning = true
-- --     and plan_name != 'Complete Protect'
-- --     and plan_name != 'Core'
-- --     and plan_name != 'Email Protect'
-- -- group by
-- --     g.DATE_RECORDED,
-- --     root,   
-- --     item,
-- --     sku,
-- --     profile_type,
-- --     -- g.partner_pricing,
-- --     URL_1

    
-- -- -----------------------------------------
-- -- ------------ attachment scans -----------
-- -- -----------------------------------------

-- -- UNION 

-- -- select
-- -- g.DATE_RECORDED,
-- -- g.root as ltp,
-- -- 'Attachment Scans' as item,
-- -- 'IS-LTP-AS' as sku,
-- -- sum(licensed_profiles) as quantity,
-- -- null as partner_pricing,
-- -- quantity * AS_1 as amount
-- -- from global_tenant_history_daily g
-- -- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- -- where
-- --     approved = true
-- --     and billing_status = 'Active'
-- --     and ltp in ('US-733','EU-25') 
-- --     and file_scanning = true
-- --     and plan_name != 'Complete Protect'
-- --     and plan_name != 'Core'
-- --     and plan_name != 'Email Protect'
-- -- group by
-- --     g.DATE_RECORDED,
-- --     root,   
-- --     item,
-- --     sku,
-- --     profile_type,
-- --     -- g.partner_pricing,
-- --     AS_1

        
-- -- -----------------------------------------
-- -- ---------- -AI Empower Bundle -----------
-- -- -----------------------------------------

-- -- UNION 

-- -- select
-- -- g.DATE_RECORDED,
-- -- g.root as ltp,
-- -- 'AI Empower Bundle' as item,
-- -- 'IS-LTP-AIEB' as sku,
-- -- sum(licensed_profiles) as quantity,
-- -- null as partner_pricing,
-- -- quantity * AIEB_1 as amount
-- -- from global_tenant_history_daily g
-- -- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- -- where
-- --     approved = true
-- --     and billing_status = 'Active'
-- --     and ltp in ('US-733','EU-25') 
-- --     and AI_EMPOWER_BUNDLE = true
-- --     and SIMULATION_AND_TRAINING_BUNDLE_PLUS = false
-- --     and plan_name != 'Complete Protect'
-- -- group by
-- --     g.DATE_RECORDED,
-- --     root,   
-- --     item,
-- --     sku,
-- --     profile_type,
-- --     -- g.partner_pricing,
-- --     AIEB_1

-- -----------------------------------------
-- ---------- S&T Plus Bundle --------------
-- -----------------------------------------

-- UNION 

-- select
-- g.DATE_RECORDED,
-- g.root as ltp,
-- 'S&T Plus Bundle' as item,
-- 'IS-LTP-STBP' as sku,
-- CASE p.profile_type
--     when 'active' then sum(Active_profiles)
--     when 'license' then sum(licensed_profiles)
--     when 'shared' then 
--                     case 
--                         when sum(SHARED_PROFILES) is null then sum(Active_profiles)
--                         else (sum(Active_profiles) - sum(SHARED_PROFILES))
--                     end
-- end as quantity,
-- null as partner_pricing,
-- quantity * STBP_1 as amount
-- from global_tenant_history_daily g
-- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- where
--     approved = true
--     and billing_status = 'Active'
--     and ltp in ('US-733','EU-25') 
--     and SIMULATION_AND_TRAINING_BUNDLE_PLUS = true
--     and plan_name not in  ('Complete Protect','SAT Suite')
-- group by
--     g.DATE_RECORDED,
--     root,   
--     item,
--     sku,
--     profile_type,
--     -- g.partner_pricing,
--     STBP_1

-- -----------------------------------------
-- ---------- Account Takeover -------------
-- -----------------------------------------

-- UNION 

-- select
-- g.DATE_RECORDED,
-- g.root as ltp,
-- 'Account Takeover' as item,
-- 'IS-LTP-ATO' as sku,
-- CASE p.profile_type
--     when 'active' then sum(Active_profiles)
--     when 'license' then sum(licensed_profiles)
--     when 'shared' then 
--                     case 
--                         when sum(SHARED_PROFILES) is null then sum(Active_profiles)
--                         else (sum(Active_profiles) - sum(SHARED_PROFILES))
--                     end
-- end as quantity,
-- null as partner_pricing,
-- quantity * ATO_1 as amount
-- from global_tenant_history_daily g
-- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- where
--     approved = true
--     and billing_status = 'Active'
--     and ltp in ('US-733','EU-25') 
--     and ATO = true
--     and plan_name != 'Complete Protect'
-- group by
--     g.DATE_RECORDED,
--     root,   
--     item,
--     sku,
--     profile_type,
--     -- g.partner_pricing,
--     ATO_1

-- -- -----------------------------------------
-- -- ---------- Multi Tenant -------------
-- -- -----------------------------------------

-- -- UNION 

-- -- select
-- -- g.DATE_RECORDED,
-- -- g.root as ltp,
-- -- 'Multi Tenant' as item,
-- -- 'IS-LTP-MT' as sku,
-- -- sum(licensed_profiles) as quantity,
-- -- null as partner_pricing,
-- -- quantity * MT_1 as amount
-- -- from global_tenant_history_daily g
-- -- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- -- where
-- --     approved = true
-- --     and billing_status = 'Active'
-- --     and ltp in ('US-733','EU-25') 
-- --     and multi_tenancy = true
-- --     and plan_name != 'Complete Protect'
-- -- group by
-- --     g.DATE_RECORDED,
-- --     root,   
-- --     item,
-- --     sku,
-- --     profile_type,
-- --     -- g.partner_pricing,
-- --     MT_1


-- -----------------------------------------
-- ----------------- DMARC -----------------
-- -----------------------------------------

-- union

-- select
-- g.DATE_RECORDED,
-- g.root as ltp,
-- 'DMARC' as item,
-- 'IS-LTP-DMARC' as sku,
-- sum(d.dmarc_domains_number) as quantity,
-- null as partner_pricing,
-- quantity * DMARC_1 as amount
-- from global_tenant_history_daily g
-- left join ltp_pricing_list p on g.root = p.tenant_global_id
-- left join hwm_dmarc_count d on g.tenant_global_id = d.tenant_global_id
-- where
--     approved = true
--     and billing_status = 'Active'
--     and ltp in ('US-733','EU-25') 
--     -- and DMARC_MANAGEMENT = true

-- group by
--     g.DATE_RECORDED,
--     root,   
--     item,
--     sku,
--     profile_type,
--     -- g.partner_pricing,
--     DMARC_1
-- having
--     quantity is not null