/*
KPI Comparison – Last 30 Days vs Prior 30 Days
----------------------------------------------

Purpose:
    This query calculates key marketing performance metrics 
    (Spend, Conversions, Revenue, CAC, ROAS) for the last 30 days 
    compared to the prior 30-day period. Results include absolute 
    values and percent change.

Logic Overview:
    1. bounds   → Get anchor_date = min(MAX(date), CURRENT_DATE)
    2. ranges   → Define current (last_30) and prior (prior_30) windows
    3. agg      → Aggregate spend, conversions, revenue by window
    4. metrics  → Derive CAC (spend/conversions) and ROAS (revenue/spend)
    5. pvt      → Pivot to align last_30 vs prior_30 values side-by-side
    6. final    → Output tidy rows (metric, current_value, previous_value, pct_change)

Outputs:
    - metric          → spend | conversions | revenue | CAC | ROAS
    - current_value   → value in last 30 days
    - previous_value  → value in prior 30 days
    - pct_change      → % change from prior to last 30 days

Ordering:
    Results are ordered for readability:
        1) spend
        2) conversions
        3) revenue
        4) CAC
        5) ROAS

*/

WITH bounds AS (
  SELECT LEAST(MAX(date), CURRENT_DATE) AS anchor_date
  FROM ads_spend
),
ranges AS (
  SELECT
    anchor_date,
    anchor_date - INTERVAL '29 days' AS cur_start,
    anchor_date - INTERVAL '59 days' AS prev_start,
    anchor_date - INTERVAL '30 days' AS prev_end
  FROM bounds
),
agg AS (
  SELECT
    CASE
      WHEN a."date" BETWEEN r.cur_start  AND r.anchor_date THEN 'last_30'
      WHEN a."date" BETWEEN r.prev_start AND r.prev_end    THEN 'prior_30'
    END AS win,
    SUM(a.spend)               AS spend,
    SUM(a.conversions)         AS conversions,
    SUM(a.conversions) * 100.0 AS revenue
  FROM ads_spend a
  CROSS JOIN ranges r
  WHERE a."date" BETWEEN r.prev_start AND r.anchor_date
  GROUP BY 1
),
metrics AS (
  SELECT
    win,
    spend,
    conversions,
    revenue,
    CASE WHEN conversions > 0 THEN spend / conversions END AS cac,
    CASE WHEN spend > 0 THEN revenue / spend END            AS roas
  FROM agg
),
pvt AS (  
  SELECT
    MAX(CASE WHEN win='last_30'  THEN spend       END) AS spend_last_30,
    MAX(CASE WHEN win='prior_30' THEN spend       END) AS spend_prior_30,
    MAX(CASE WHEN win='last_30'  THEN conversions END) AS conv_last_30,
    MAX(CASE WHEN win='prior_30' THEN conversions END) AS conv_prior_30,
    MAX(CASE WHEN win='last_30'  THEN revenue     END) AS revenue_last_30,
    MAX(CASE WHEN win='prior_30' THEN revenue     END) AS revenue_prior_30,
    MAX(CASE WHEN win='last_30'  THEN cac         END) AS cac_last_30,
    MAX(CASE WHEN win='prior_30' THEN cac         END) AS cac_prior_30,
    MAX(CASE WHEN win='last_30'  THEN roas        END) AS roas_last_30,
    MAX(CASE WHEN win='prior_30' THEN roas        END) AS roas_prior_30
  FROM metrics
),
final AS (
  SELECT 'spend' AS metric, spend_last_30 AS current_value, spend_prior_30 AS previous_value,
         CASE WHEN spend_prior_30 IS NULL OR spend_prior_30 = 0 THEN NULL
              ELSE (spend_last_30 - spend_prior_30) / spend_prior_30 END AS pct_change
  FROM pvt
  UNION ALL
  SELECT 'conversions', conv_last_30, conv_prior_30,
         CASE WHEN conv_prior_30 IS NULL OR conv_prior_30 = 0 THEN NULL
              ELSE (conv_last_30 - conv_prior_30) / conv_prior_30 END
  FROM pvt
  UNION ALL
  SELECT 'revenue', revenue_last_30, revenue_prior_30,
         CASE WHEN revenue_prior_30 IS NULL OR revenue_prior_30 = 0 THEN NULL
              ELSE (revenue_last_30 - revenue_prior_30) / revenue_prior_30 END
  FROM pvt
  UNION ALL
  SELECT 'CAC', cac_last_30, cac_prior_30,
         CASE WHEN cac_prior_30 IS NULL OR cac_prior_30 = 0 THEN NULL
              ELSE (cac_last_30 - cac_prior_30) / cac_prior_30 END
  FROM pvt
  UNION ALL
  SELECT 'ROAS', roas_last_30, roas_prior_30,
         CASE WHEN roas_prior_30 IS NULL OR roas_prior_30 = 0 THEN NULL
              ELSE (roas_last_30 - roas_prior_30) / roas_prior_30 END
  FROM pvt
)
SELECT * FROM final
ORDER BY
  CASE metric
    WHEN 'spend' THEN 1
    WHEN 'conversions' THEN 2
    WHEN 'revenue' THEN 3
    WHEN 'CAC' THEN 4
    WHEN 'ROAS' THEN 5
    ELSE 6
  END;

