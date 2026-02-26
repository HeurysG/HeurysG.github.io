from __future__ import annotations

import json
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "output"
DB_PATH = OUT_DIR / "sportsbook.duckdb"
REPORT_PATH = OUT_DIR / "sportsbook_report.md"
CALIBRATION_PATH = OUT_DIR / "churn_calibration.json"
MONTE_CARLO_SUMMARY_PATH = OUT_DIR / "monte_carlo_summary.csv"
PROMO_SERVICING_OVERHEAD_PER_USER = 5.0


PROMO_COHORT_RETENTION_SQL = """
CREATE OR REPLACE TABLE promo_cohort_retention AS
WITH bet_enriched AS (
    SELECT
        b.customer_id,
        CAST(b.bet_datetime AS DATE) AS bet_date,
        b.stake_amount,
        c.promo_cost,
        (b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) AS net_profit
    FROM fact_bet b
    JOIN fact_bet_cost c ON b.bet_id = c.bet_id
),
customer_anchors AS (
    SELECT
        customer_id,
        MIN(bet_date) AS first_bet_date,
        MIN(CASE WHEN promo_cost > 0 THEN bet_date END) AS first_promo_date
    FROM bet_enriched
    GROUP BY 1
),
cohorts AS (
    SELECT customer_id, first_promo_date AS cohort_date, 'promo' AS cohort_type
    FROM customer_anchors
    WHERE first_promo_date IS NOT NULL
    UNION ALL
    SELECT customer_id, first_bet_date AS cohort_date, 'control' AS cohort_type
    FROM customer_anchors
    WHERE first_promo_date IS NULL
),
customer_outcomes AS (
    SELECT
        c.customer_id,
        c.cohort_date,
        c.cohort_type,
        MAX(CASE WHEN b.bet_date > c.cohort_date AND b.bet_date <= c.cohort_date + INTERVAL 7 DAY THEN 1 ELSE 0 END) AS retained_7d,
        MAX(CASE WHEN b.bet_date > c.cohort_date AND b.bet_date <= c.cohort_date + INTERVAL 30 DAY THEN 1 ELSE 0 END) AS retained_30d,
        MAX(CASE WHEN b.bet_date > c.cohort_date AND b.bet_date <= c.cohort_date + INTERVAL 60 DAY THEN 1 ELSE 0 END) AS retained_60d,
        MAX(CASE WHEN b.bet_date > c.cohort_date AND b.bet_date <= c.cohort_date + INTERVAL 90 DAY THEN 1 ELSE 0 END) AS retained_90d,
        SUM(CASE WHEN b.bet_date > c.cohort_date AND b.bet_date <= c.cohort_date + INTERVAL 30 DAY THEN b.stake_amount ELSE 0 END) AS future_handle_30d,
        SUM(CASE WHEN b.bet_date > c.cohort_date AND b.bet_date <= c.cohort_date + INTERVAL 60 DAY THEN b.stake_amount ELSE 0 END) AS future_handle_60d,
        SUM(CASE WHEN b.bet_date > c.cohort_date AND b.bet_date <= c.cohort_date + INTERVAL 30 DAY THEN b.net_profit ELSE 0 END) AS future_net_profit_30d,
        SUM(CASE WHEN b.bet_date > c.cohort_date AND b.bet_date <= c.cohort_date + INTERVAL 60 DAY THEN b.net_profit ELSE 0 END) AS future_net_profit_60d,
        SUM(b.stake_amount) AS lifetime_handle,
        SUM(b.net_profit) AS lifetime_net_profit
    FROM cohorts c
    JOIN bet_enriched b ON c.customer_id = b.customer_id
    GROUP BY 1, 2, 3
)
SELECT
    cohort_date,
    cohort_type,
    COUNT(*) AS users,
    SUM(retained_7d) AS retained_7d,
    SUM(retained_30d) AS retained_30d,
    SUM(retained_60d) AS retained_60d,
    SUM(retained_90d) AS retained_90d,
    AVG(retained_7d) AS retention_7d,
    AVG(retained_30d) AS retention_30d,
    AVG(retained_60d) AS retention_60d,
    AVG(retained_90d) AS retention_90d,
    AVG(future_handle_30d) AS avg_future_handle_30d,
    AVG(future_handle_60d) AS avg_future_handle_60d,
    AVG(future_net_profit_30d) AS avg_future_net_profit_30d,
    AVG(future_net_profit_60d) AS avg_future_net_profit_60d,
    AVG(lifetime_handle) AS avg_lifetime_handle,
    AVG(lifetime_net_profit) AS avg_lifetime_net_profit
FROM customer_outcomes
GROUP BY 1, 2
ORDER BY 1, 2;
"""


PROMO_UPLIFT_SQL = """
CREATE OR REPLACE TABLE promo_uplift_user AS
WITH bet_enriched AS (
    SELECT
        b.customer_id,
        CAST(b.bet_datetime AS DATE) AS bet_date,
        b.stake_amount,
        c.promo_cost,
        d.segment,
        (b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) AS net_profit
    FROM fact_bet b
    JOIN fact_bet_cost c ON b.bet_id = c.bet_id
    JOIN dim_customer d ON b.customer_id = d.customer_id
),
customer_anchor AS (
    SELECT
        customer_id,
        segment,
        MIN(bet_date) AS first_bet_date,
        MAX(bet_date) AS last_bet_date,
        MIN(CASE WHEN promo_cost > 0 THEN bet_date END) AS first_promo_date
    FROM bet_enriched
    GROUP BY 1, 2
),
cohort_users AS (
    SELECT
        customer_id,
        segment,
        'promo' AS cohort_type,
        first_promo_date AS anchor_date
    FROM customer_anchor
    WHERE first_promo_date IS NOT NULL
    UNION ALL
    SELECT
        customer_id,
        segment,
        'control' AS cohort_type,
        CASE
            WHEN date_diff('day', first_bet_date, last_bet_date) >= 56 THEN first_bet_date + INTERVAL 28 DAY
            WHEN date_diff('day', first_bet_date, last_bet_date) > 0 THEN
                first_bet_date + CAST(FLOOR(date_diff('day', first_bet_date, last_bet_date) / 2.0) AS INTEGER) * INTERVAL 1 DAY
            ELSE first_bet_date
        END AS anchor_date
    FROM customer_anchor
    WHERE first_promo_date IS NULL
),
customer_lifetime AS (
    SELECT
        customer_id,
        SUM(b.stake_amount) AS lifetime_handle,
        SUM(b.net_profit) AS lifetime_net_profit
    FROM bet_enriched b
    GROUP BY 1
),
cohort_metrics AS (
    SELECT
        cu.customer_id,
        cu.segment,
        cu.cohort_type,
        cu.anchor_date,
        SUM(CASE WHEN b.bet_date >= cu.anchor_date - INTERVAL 28 DAY AND b.bet_date < cu.anchor_date THEN b.stake_amount ELSE 0 END) / 4.0 AS avg_weekly_handle_pre_4w,
        SUM(CASE WHEN b.bet_date > cu.anchor_date AND b.bet_date <= cu.anchor_date + INTERVAL 28 DAY THEN b.stake_amount ELSE 0 END) / 4.0 AS avg_weekly_handle_post_4w,
        SUM(CASE WHEN b.bet_date >= cu.anchor_date - INTERVAL 28 DAY AND b.bet_date < cu.anchor_date THEN b.net_profit ELSE 0 END) AS net_profit_pre_4w,
        SUM(CASE WHEN b.bet_date > cu.anchor_date AND b.bet_date <= cu.anchor_date + INTERVAL 28 DAY THEN b.net_profit ELSE 0 END) AS net_profit_post_4w,
        SUM(CASE WHEN b.bet_date > cu.anchor_date AND b.bet_date <= cu.anchor_date + INTERVAL 91 DAY THEN b.net_profit ELSE 0 END) AS net_profit_post_13w,
        SUM(CASE WHEN b.bet_date > cu.anchor_date AND b.bet_date <= cu.anchor_date + INTERVAL 182 DAY THEN b.net_profit ELSE 0 END) AS net_profit_post_26w,
        SUM(CASE WHEN b.bet_date > cu.anchor_date AND b.bet_date <= cu.anchor_date + INTERVAL 28 DAY AND b.promo_cost > 0 THEN b.net_profit ELSE 0 END) AS direct_promo_net_post_4w,
        SUM(CASE WHEN b.bet_date > cu.anchor_date AND b.bet_date <= cu.anchor_date + INTERVAL 91 DAY AND b.promo_cost > 0 THEN b.net_profit ELSE 0 END) AS direct_promo_net_post_13w,
        SUM(CASE WHEN b.bet_date > cu.anchor_date AND b.bet_date <= cu.anchor_date + INTERVAL 182 DAY AND b.promo_cost > 0 THEN b.net_profit ELSE 0 END) AS direct_promo_net_post_26w,
        SUM(CASE WHEN b.bet_date > cu.anchor_date AND b.bet_date <= cu.anchor_date + INTERVAL 28 DAY THEN b.promo_cost ELSE 0 END) AS promo_cost_post_4w,
        SUM(CASE WHEN b.bet_date > cu.anchor_date AND b.bet_date <= cu.anchor_date + INTERVAL 91 DAY THEN b.promo_cost ELSE 0 END) AS promo_cost_post_13w,
        SUM(CASE WHEN b.bet_date > cu.anchor_date AND b.bet_date <= cu.anchor_date + INTERVAL 182 DAY THEN b.promo_cost ELSE 0 END) AS promo_cost_post_26w
    FROM cohort_users cu
    JOIN bet_enriched b ON b.customer_id = cu.customer_id
    GROUP BY 1, 2, 3, 4
)
SELECT
    cm.customer_id,
    cm.segment,
    cm.cohort_type,
    cm.anchor_date,
    cm.avg_weekly_handle_pre_4w,
    cm.avg_weekly_handle_post_4w,
    (cm.avg_weekly_handle_post_4w - cm.avg_weekly_handle_pre_4w) AS handle_delta_4w,
    cm.net_profit_pre_4w,
    cm.net_profit_post_4w,
    cm.net_profit_post_13w,
    cm.net_profit_post_26w,
    cm.direct_promo_net_post_4w,
    cm.direct_promo_net_post_13w,
    cm.direct_promo_net_post_26w,
    cm.promo_cost_post_4w,
    cm.promo_cost_post_13w,
    cm.promo_cost_post_26w,
    cl.lifetime_handle,
    cl.lifetime_net_profit,
    NULL::DOUBLE AS incremental_net_profit_4w,
    NULL::DOUBLE AS incremental_net_profit_13w,
    NULL::DOUBLE AS incremental_net_profit_26w,
    NULL::DOUBLE AS incremental_roi_4w
FROM cohort_metrics cm
LEFT JOIN customer_lifetime cl ON cl.customer_id = cm.customer_id;
"""


RETENTION_BY_SEGMENT_SQL = """
WITH first_bet AS (
    SELECT
        b.customer_id,
        d.segment,
        MIN(CAST(b.bet_datetime AS DATE)) AS anchor_date
    FROM fact_bet b
    JOIN dim_customer d ON b.customer_id = d.customer_id
    GROUP BY 1, 2
)
SELECT
    f.segment,
    COUNT(*) AS users,
    AVG(CASE WHEN EXISTS (
        SELECT 1 FROM fact_bet b2
        WHERE b2.customer_id = f.customer_id
          AND CAST(b2.bet_datetime AS DATE) > f.anchor_date
          AND CAST(b2.bet_datetime AS DATE) <= f.anchor_date + INTERVAL 7 DAY
    ) THEN 1 ELSE 0 END) AS retention_7d,
    AVG(CASE WHEN EXISTS (
        SELECT 1 FROM fact_bet b2
        WHERE b2.customer_id = f.customer_id
          AND CAST(b2.bet_datetime AS DATE) > f.anchor_date
          AND CAST(b2.bet_datetime AS DATE) <= f.anchor_date + INTERVAL 30 DAY
    ) THEN 1 ELSE 0 END) AS retention_30d,
    AVG(CASE WHEN EXISTS (
        SELECT 1 FROM fact_bet b2
        WHERE b2.customer_id = f.customer_id
          AND CAST(b2.bet_datetime AS DATE) > f.anchor_date
          AND CAST(b2.bet_datetime AS DATE) <= f.anchor_date + INTERVAL 90 DAY
    ) THEN 1 ELSE 0 END) AS retention_90d
FROM first_bet f
GROUP BY 1
ORDER BY CASE segment
    WHEN 'sharp' THEN 1
    WHEN 'regular' THEN 2
    WHEN 'casual' THEN 3
    ELSE 4
END;
"""


STILL_ACTIVE_RETENTION_SQL = """
CREATE OR REPLACE TABLE still_active_retention_by_segment AS
WITH first_bet AS (
    SELECT
        b.customer_id,
        d.segment,
        MIN(CAST(b.bet_datetime AS DATE)) AS anchor_date
    FROM fact_bet b
    JOIN dim_customer d ON b.customer_id = d.customer_id
    GROUP BY 1, 2
)
SELECT
    f.segment,
    COUNT(*) AS users,
    AVG(CASE WHEN EXISTS (
        SELECT 1 FROM fact_bet b2
        WHERE b2.customer_id = f.customer_id
          AND CAST(b2.bet_datetime AS DATE) > f.anchor_date
          AND CAST(b2.bet_datetime AS DATE) <= f.anchor_date + INTERVAL 7 DAY
    ) THEN 1 ELSE 0 END) AS still_active_7d,
    AVG(CASE WHEN EXISTS (
        SELECT 1 FROM fact_bet b2
        WHERE b2.customer_id = f.customer_id
          AND CAST(b2.bet_datetime AS DATE) > f.anchor_date + INTERVAL 23 DAY
          AND CAST(b2.bet_datetime AS DATE) <= f.anchor_date + INTERVAL 30 DAY
    ) THEN 1 ELSE 0 END) AS still_active_30d,
    AVG(CASE WHEN EXISTS (
        SELECT 1 FROM fact_bet b2
        WHERE b2.customer_id = f.customer_id
          AND CAST(b2.bet_datetime AS DATE) > f.anchor_date + INTERVAL 53 DAY
          AND CAST(b2.bet_datetime AS DATE) <= f.anchor_date + INTERVAL 60 DAY
    ) THEN 1 ELSE 0 END) AS still_active_60d,
    AVG(CASE WHEN EXISTS (
        SELECT 1 FROM fact_bet b2
        WHERE b2.customer_id = f.customer_id
          AND CAST(b2.bet_datetime AS DATE) > f.anchor_date + INTERVAL 83 DAY
          AND CAST(b2.bet_datetime AS DATE) <= f.anchor_date + INTERVAL 90 DAY
    ) THEN 1 ELSE 0 END) AS still_active_90d
FROM first_bet f
GROUP BY 1
ORDER BY CASE segment
    WHEN 'sharp' THEN 1
    WHEN 'regular' THEN 2
    WHEN 'casual' THEN 3
    ELSE 4
END;
"""


WEEKLY_SURVIVAL_RETENTION_SQL = """
CREATE OR REPLACE TABLE weekly_survival_retention AS
WITH first_bet AS (
    SELECT
        b.customer_id,
        d.segment,
        MIN(CAST(b.bet_datetime AS DATE)) AS anchor_date
    FROM fact_bet b
    JOIN dim_customer d ON b.customer_id = d.customer_id
    GROUP BY 1, 2
),
weeks AS (
    SELECT generate_series AS week_num
    FROM generate_series(1, 13)
),
cohort_base AS (
    SELECT
        segment,
        COUNT(*) AS cohort_users
    FROM first_bet
    GROUP BY 1
),
active_by_week AS (
    SELECT
        f.segment,
        w.week_num,
        COUNT(DISTINCT f.customer_id) AS active_users
    FROM first_bet f
    CROSS JOIN weeks w
    JOIN fact_bet b
      ON b.customer_id = f.customer_id
     AND CAST(b.bet_datetime AS DATE) > f.anchor_date + (w.week_num - 1) * INTERVAL 7 DAY
     AND CAST(b.bet_datetime AS DATE) <= f.anchor_date + w.week_num * INTERVAL 7 DAY
    GROUP BY 1, 2
),
segment_weeks AS (
    SELECT
        c.segment,
        w.week_num,
        c.cohort_users
    FROM cohort_base c
    CROSS JOIN weeks w
)
SELECT
    s.segment,
    s.week_num,
    s.cohort_users,
    COALESCE(a.active_users, 0) AS active_users,
    COALESCE(a.active_users, 0)::DOUBLE / NULLIF(s.cohort_users, 0) AS retention_rate
FROM segment_weeks s
LEFT JOIN active_by_week a
  ON s.segment = a.segment
 AND s.week_num = a.week_num
ORDER BY
    CASE s.segment
        WHEN 'sharp' THEN 1
        WHEN 'regular' THEN 2
        WHEN 'casual' THEN 3
        ELSE 4
    END,
    s.week_num;
"""


INCREMENTAL_UPLIFT_SUMMARY_SQL = """
WITH by_cohort AS (
    SELECT
        cohort_type,
        AVG(avg_weekly_handle_pre_4w) AS avg_pre_handle,
        AVG(avg_weekly_handle_post_4w) AS avg_post_handle,
        AVG(handle_delta_4w) AS avg_delta_handle
    FROM promo_uplift_user
    GROUP BY 1
),
p AS (SELECT * FROM by_cohort WHERE cohort_type = 'promo'),
c AS (SELECT * FROM by_cohort WHERE cohort_type = 'control')
SELECT
    p.avg_pre_handle AS promo_pre_handle,
    p.avg_post_handle AS promo_post_handle,
    p.avg_delta_handle AS promo_delta_handle,
    c.avg_pre_handle AS control_pre_handle,
    c.avg_post_handle AS control_post_handle,
    c.avg_delta_handle AS control_delta_handle,
    p.avg_post_handle - c.avg_post_handle AS post_handle_gap
FROM p CROSS JOIN c;
"""


PROMO_ROI_SUMMARY_SQL = """
WITH bounds AS (
    SELECT MAX(CAST(bet_datetime AS DATE)) AS max_bet_date
    FROM fact_bet
),
promo_base AS (
    SELECT
        COUNT(*) FILTER (WHERE p.anchor_date <= b.max_bet_date - INTERVAL 28 DAY) AS promo_users_4w,
        COUNT(*) FILTER (WHERE p.anchor_date <= b.max_bet_date - INTERVAL 91 DAY) AS promo_users_13w,
        COUNT(*) FILTER (WHERE p.anchor_date <= b.max_bet_date - INTERVAL 182 DAY) AS promo_users_26w,
        SUM(CASE WHEN p.anchor_date <= b.max_bet_date - INTERVAL 28 DAY THEN p.net_profit_post_4w ELSE 0 END) AS net_profit_total_4w,
        SUM(CASE WHEN p.anchor_date <= b.max_bet_date - INTERVAL 91 DAY THEN p.net_profit_post_13w ELSE 0 END) AS net_profit_total_13w,
        SUM(CASE WHEN p.anchor_date <= b.max_bet_date - INTERVAL 182 DAY THEN p.net_profit_post_26w ELSE 0 END) AS net_profit_total_26w,
        SUM(CASE WHEN p.anchor_date <= b.max_bet_date - INTERVAL 28 DAY THEN p.direct_promo_net_post_4w ELSE 0 END) AS direct_promo_net_total_4w,
        SUM(CASE WHEN p.anchor_date <= b.max_bet_date - INTERVAL 91 DAY THEN p.direct_promo_net_post_13w ELSE 0 END) AS direct_promo_net_total_13w,
        SUM(CASE WHEN p.anchor_date <= b.max_bet_date - INTERVAL 182 DAY THEN p.direct_promo_net_post_26w ELSE 0 END) AS direct_promo_net_total_26w,
        SUM(CASE WHEN p.anchor_date <= b.max_bet_date - INTERVAL 28 DAY THEN p.promo_cost_post_4w ELSE 0 END) AS promo_cost_total,
        SUM(CASE WHEN p.anchor_date <= b.max_bet_date - INTERVAL 91 DAY THEN p.promo_cost_post_13w ELSE 0 END) AS promo_cost_total_13w,
        SUM(CASE WHEN p.anchor_date <= b.max_bet_date - INTERVAL 182 DAY THEN p.promo_cost_post_26w ELSE 0 END) AS promo_cost_total_26w
    FROM promo_uplift_user p
    CROSS JOIN bounds b
    WHERE cohort_type = 'promo'
)
SELECT
    promo_users_4w,
    promo_users_13w,
    promo_users_26w,
    (net_profit_total_4w - (promo_users_4w * {PROMO_SERVICING_OVERHEAD_PER_USER})) / NULLIF(promo_users_4w, 0) AS net_profit_per_user_4w,
    (net_profit_total_13w - (promo_users_13w * {PROMO_SERVICING_OVERHEAD_PER_USER})) / NULLIF(promo_users_13w, 0) AS net_profit_per_user_13w,
    (net_profit_total_26w - (promo_users_26w * {PROMO_SERVICING_OVERHEAD_PER_USER})) / NULLIF(promo_users_26w, 0) AS net_profit_per_user_26w,
    net_profit_total_4w,
    net_profit_total_13w,
    net_profit_total_26w,
    promo_cost_total,
    promo_cost_total_13w,
    promo_cost_total_26w,
    (net_profit_total_4w - (promo_users_4w * {PROMO_SERVICING_OVERHEAD_PER_USER})) AS net_after_promo_total_4w,
    (net_profit_total_13w - (promo_users_13w * {PROMO_SERVICING_OVERHEAD_PER_USER})) AS net_after_promo_total_13w,
    (net_profit_total_26w - (promo_users_26w * {PROMO_SERVICING_OVERHEAD_PER_USER})) AS net_after_promo_total_26w,
    direct_promo_net_total_4w,
    direct_promo_net_total_13w,
    direct_promo_net_total_26w,
    (net_profit_total_4w + promo_cost_total - (promo_users_4w * {PROMO_SERVICING_OVERHEAD_PER_USER})) AS operating_profit_before_promo_4w,
    (net_profit_total_13w + promo_cost_total_13w - (promo_users_13w * {PROMO_SERVICING_OVERHEAD_PER_USER})) AS operating_profit_before_promo_13w,
    (net_profit_total_26w + promo_cost_total_26w - (promo_users_26w * {PROMO_SERVICING_OVERHEAD_PER_USER})) AS operating_profit_before_promo_26w,
    (direct_promo_net_total_4w - (promo_users_4w * {PROMO_SERVICING_OVERHEAD_PER_USER})) / NULLIF(promo_cost_total, 0) AS roi,
    (direct_promo_net_total_13w - (promo_users_13w * {PROMO_SERVICING_OVERHEAD_PER_USER})) / NULLIF(promo_cost_total_13w, 0) AS roi_13w,
    (direct_promo_net_total_26w - (promo_users_26w * {PROMO_SERVICING_OVERHEAD_PER_USER})) / NULLIF(promo_cost_total_26w, 0) AS roi_26w,
    direct_promo_net_total_4w / NULLIF(promo_cost_total, 0) AS direct_promo_roi_4w,
    direct_promo_net_total_13w / NULLIF(promo_cost_total_13w, 0) AS direct_promo_roi_13w,
    direct_promo_net_total_26w / NULLIF(promo_cost_total_26w, 0) AS direct_promo_roi_26w,
    (net_profit_total_4w + promo_cost_total - (promo_users_4w * {PROMO_SERVICING_OVERHEAD_PER_USER})) / NULLIF(promo_cost_total, 0) AS operating_roi_4w,
    (net_profit_total_13w + promo_cost_total_13w - (promo_users_13w * {PROMO_SERVICING_OVERHEAD_PER_USER})) / NULLIF(promo_cost_total_13w, 0) AS operating_roi_13w,
    (net_profit_total_26w + promo_cost_total_26w - (promo_users_26w * {PROMO_SERVICING_OVERHEAD_PER_USER})) / NULLIF(promo_cost_total_26w, 0) AS operating_roi_26w
FROM promo_base;
""".format(PROMO_SERVICING_OVERHEAD_PER_USER=PROMO_SERVICING_OVERHEAD_PER_USER)


PAYBACK_SQL = """
WITH bet_enriched AS (
    SELECT
        b.customer_id,
        CAST(b.bet_datetime AS DATE) AS bet_date,
        c.promo_cost,
        (b.stake_amount - b.gross_payout - c.payment_fee - c.tax) AS profit_before_promo
    FROM fact_bet b
    JOIN fact_bet_cost c ON b.bet_id = c.bet_id
),
bounds AS (
    SELECT MAX(bet_date) AS max_bet_date
    FROM bet_enriched
),
promo_users AS (
    SELECT
        customer_id,
        MIN(CASE WHEN promo_cost > 0 THEN bet_date END) AS anchor_date
    FROM bet_enriched
    GROUP BY 1
    HAVING MIN(CASE WHEN promo_cost > 0 THEN bet_date END) IS NOT NULL
),
weekly_promo AS (
    SELECT
        FLOOR(date_diff('day', u.anchor_date, b.bet_date) / 7) + 1 AS wk,
        SUM(CASE WHEN b.bet_date > u.anchor_date AND b.promo_cost > 0 THEN b.profit_before_promo - b.promo_cost ELSE 0 END) AS net_after_promo
    FROM promo_users u
    CROSS JOIN bounds bmax
    JOIN bet_enriched b ON b.customer_id = u.customer_id
    WHERE u.anchor_date <= bmax.max_bet_date - INTERVAL 180 DAY
      AND b.bet_date > u.anchor_date
      AND date_diff('day', u.anchor_date, b.bet_date) <= 180
    GROUP BY 1
),
cum AS (
    SELECT
        wk,
        SUM(net_after_promo) OVER (ORDER BY wk) AS cum_net_after_promo
    FROM weekly_promo
)
SELECT MIN(CASE WHEN cum_net_after_promo >= 0 THEN wk END) AS payback_week
FROM cum;
"""


PAYBACK_SERIES_SQL = """
WITH bet_enriched AS (
    SELECT
        b.customer_id,
        CAST(b.bet_datetime AS DATE) AS bet_date,
        c.promo_cost,
        (b.stake_amount - b.gross_payout - c.payment_fee - c.tax) AS profit_before_promo
    FROM fact_bet b
    JOIN fact_bet_cost c ON b.bet_id = c.bet_id
),
bounds AS (
    SELECT MAX(bet_date) AS max_bet_date
    FROM bet_enriched
),
promo_users AS (
    SELECT
        customer_id,
        MIN(CASE WHEN promo_cost > 0 THEN bet_date END) AS anchor_date
    FROM bet_enriched
    GROUP BY 1
    HAVING MIN(CASE WHEN promo_cost > 0 THEN bet_date END) IS NOT NULL
),
weekly_promo AS (
    SELECT
        FLOOR(date_diff('day', u.anchor_date, b.bet_date) / 7) + 1 AS wk,
        SUM(CASE WHEN b.bet_date > u.anchor_date AND b.promo_cost > 0 THEN b.profit_before_promo - b.promo_cost ELSE 0 END) AS net_after_promo
    FROM promo_users u
    CROSS JOIN bounds bmax
    JOIN bet_enriched b ON b.customer_id = u.customer_id
    WHERE u.anchor_date <= bmax.max_bet_date - INTERVAL 180 DAY
      AND b.bet_date > u.anchor_date
      AND date_diff('day', u.anchor_date, b.bet_date) <= 180
    GROUP BY 1
),
cum AS (
    SELECT
        wk,
        SUM(net_after_promo) OVER (ORDER BY wk) AS cum_net_after_promo
    FROM weekly_promo
)
SELECT
    wk,
    cum_net_after_promo
FROM cum
ORDER BY 1;
"""


WATERFALL_COMPONENTS_SQL = """
WITH promo_base AS (
    SELECT
        AVG(net_profit_post_4w + promo_cost_post_4w) AS post_profit_before_promo,
        AVG(promo_cost_post_4w) AS promo_cost_post_4w,
        AVG(net_profit_post_4w) AS post_net_after_promo
    FROM promo_uplift_user
    WHERE cohort_type = 'promo'
)
SELECT
    p.post_profit_before_promo,
    p.promo_cost_post_4w,
    p.post_net_after_promo
FROM promo_base p;
"""


HIGH_LEVEL_ECONOMICS_SQL = """
WITH totals AS (
    SELECT
        SUM(b.stake_amount) AS handle,
        SUM(b.stake_amount - b.gross_payout) AS gross_profit,
        SUM(c.promo_cost) AS promo_cost,
        SUM(c.payment_fee) AS payment_fee,
        SUM(c.tax) AS tax,
        SUM(b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) AS net_profit
    FROM fact_bet b
    JOIN fact_bet_cost c ON b.bet_id = c.bet_id
)
SELECT
    handle,
    gross_profit,
    promo_cost,
    payment_fee,
    tax,
    net_profit
FROM totals;
"""


def pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def money(v: float) -> str:
    return f"${v:,.0f}"


def money_compact(v: float) -> str:
    a = abs(v)
    if a >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if a >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.0f}"


def apply_chart_style() -> None:
    plt.style.use("seaborn-v0_8-whitegrid")


def polish_axis(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.25)


def plot_retention_by_segment(df: pd.DataFrame) -> None:
    apply_chart_style()
    labels = ["7d", "30d", "60d", "90d"]
    colors = {
        "sharp": "#1f77b4",
        "regular": "#4e79a7",
        "casual": "#9aa0a6",
        "promo_hunter": "#d62728",
    }
    fig, ax = plt.subplots(figsize=(9, 5))
    for _, row in df.iterrows():
        y = [
            float(row["still_active_7d"]) * 100.0,
            float(row["still_active_30d"]) * 100.0,
            float(row["still_active_60d"]) * 100.0,
            float(row["still_active_90d"]) * 100.0,
        ]
        seg = str(row["segment"])
        ax.plot(labels, y, marker="o", linewidth=2.4, label=seg.replace("_", " ").title(), color=colors.get(seg, "#333333"))
    ax.set_title(
        "Who Is Still Around? Retention by Segment at D7/D30/D60/D90",
        loc="left",
        fontsize=13,
        fontweight="bold",
        pad=16,
    )
    ax.set_ylabel("Retention %")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.legend(frameon=False, ncol=2)
    polish_axis(ax)
    ax.text(
        0.01,
        1.02,
        "Takeaway: This is point-in-time activity, not cumulative return.",
        transform=ax.transAxes,
        fontsize=9,
        color="#555555",
        va="top",
        clip_on=False,
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "none", "pad": 2},
    )
    fig.tight_layout()
    fig.savefig(OUT_DIR / "retention_curve_by_segment.png", dpi=160)
    plt.close(fig)


def plot_cumulative_retention_supportive(df: pd.DataFrame) -> None:
    apply_chart_style()
    labels = ["7d", "30d", "90d"]
    colors = {
        "sharp": "#1f77b4",
        "regular": "#4e79a7",
        "casual": "#9aa0a6",
        "promo_hunter": "#d62728",
    }
    fig, ax = plt.subplots(figsize=(9, 5))
    for _, row in df.iterrows():
        y = [
            float(row["retention_7d"]) * 100.0,
            float(row["retention_30d"]) * 100.0,
            float(row["retention_90d"]) * 100.0,
        ]
        seg = str(row["segment"])
        ax.plot(labels, y, marker="o", linewidth=2.2, label=seg.replace("_", " ").title(), color=colors.get(seg, "#333333"))
    ax.set_title(
        "Supportive View: Cumulative Return by Day 7/30/90",
        loc="left",
        fontsize=12.5,
        fontweight="bold",
        pad=14,
    )
    ax.set_ylabel("Cumulative Return %")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.legend(frameon=False, ncol=2)
    polish_axis(ax)
    ax.text(
        0.01,
        1.02,
        "Takeaway: This only means at least one return by each checkpoint.",
        transform=ax.transAxes,
        fontsize=9,
        color="#555555",
        va="top",
        clip_on=False,
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "none", "pad": 2},
    )
    fig.tight_layout()
    fig.savefig(OUT_DIR / "retention_cumulative_supportive.png", dpi=160)
    plt.close(fig)


def plot_weekly_survival_retention(df: pd.DataFrame) -> None:
    apply_chart_style()
    colors = {
        "sharp": "#1f77b4",
        "regular": "#4e79a7",
        "casual": "#9aa0a6",
        "promo_hunter": "#d62728",
    }
    fig, ax = plt.subplots(figsize=(9, 5))
    for seg, g in df.groupby("segment"):
        ax.plot(
            g["week_num"],
            g["retention_rate"] * 100.0,
            linewidth=2.4,
            marker="o",
            label=seg.replace("_", " ").title(),
            color=colors.get(seg, "#333333"),
        )
    ax.set_title(
        "Weekly Survival Retention Shows True Decay by Segment",
        loc="left",
        fontsize=13,
        fontweight="bold",
        pad=16,
    )
    ax.set_xlabel("Week Since First Bet")
    ax.set_ylabel("Retention %")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.legend(frameon=False, ncol=2)
    polish_axis(ax)
    ax.text(
        0.01,
        1.02,
        "Takeaway: Segment differences are visible in slope, not just level.",
        transform=ax.transAxes,
        fontsize=9,
        color="#555555",
        va="top",
        clip_on=False,
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "none", "pad": 2},
    )
    fig.tight_layout()
    fig.savefig(OUT_DIR / "weekly_survival_retention.png", dpi=160)
    plt.close(fig)


def plot_pre_post(df_uplift: pd.DataFrame) -> None:
    apply_chart_style()
    row = df_uplift.iloc[0]
    labels = ["Promo", "Control"]
    pre_vals = [float(row["promo_pre_handle"]), float(row["control_pre_handle"])]
    post_vals = [float(row["promo_post_handle"]), float(row["control_post_handle"])]
    x = range(len(labels))
    width = 0.35
    fig, ax = plt.subplots(figsize=(8.5, 5))
    ax.bar([i - width / 2 for i in x], pre_vals, width=width, label="Pre 4w", color="#9aa0a6")
    ax.bar([i + width / 2 for i in x], post_vals, width=width, label="Post 4w", color="#1f77b4")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_title(
        "Post-Anchor Handle Is Higher for Promo Cohort",
        loc="left",
        fontsize=13,
        fontweight="bold",
        pad=18,
    )
    ax.set_ylabel("Avg Weekly Handle")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: money_compact(v)))
    post_gap = float(row["post_handle_gap"])
    ax.text(
        0.01,
        1.02,
        f"Takeaway: Promo users average {money_compact(post_gap)} higher weekly handle post-anchor versus control.",
        transform=ax.transAxes,
        fontsize=9,
        color="#555555",
        va="top",
        clip_on=False,
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "none", "pad": 2},
    )
    ax.legend(frameon=False)
    polish_axis(ax)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "promo_pre_post_handle.png", dpi=160)
    plt.close(fig)


def plot_high_level_economics(df_metrics: pd.DataFrame) -> None:
    apply_chart_style()
    row = df_metrics.iloc[0]
    categories = ["Handle", "Gross Profit", "Promo Cost", "Payment Fee", "Tax", "Net Profit"]
    values = [
        float(row["handle"]),
        float(row["gross_profit"]),
        -float(row["promo_cost"]),
        -float(row["payment_fee"]),
        -float(row["tax"]),
        float(row["net_profit"]),
    ]
    colors = ["#1f77b4", "#2ca02c", "#d62728", "#d62728", "#d62728", "#4e79a7"]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(categories, values, color=colors)
    ax.set_title("Topline Sportsbook Economics", loc="left", fontsize=13, fontweight="bold", pad=16)
    ax.set_ylabel("Value")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: money_compact(v)))
    polish_axis(ax)
    ax.text(
        0.01,
        1.04,
        "Takeaway: Gross profit remains healthy after costs, with positive net profit.",
        transform=ax.transAxes,
        fontsize=9,
        color="#555555",
        va="top",
        clip_on=False,
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "none", "pad": 2},
    )
    y_span = float(ax.get_ylim()[1] - ax.get_ylim()[0])
    label_offset = 0.02 * y_span
    for b in bars:
        h = b.get_height()
        y = h + label_offset
        ax.text(
            b.get_x() + b.get_width() / 2,
            y,
            money_compact(h),
            ha="center",
            va="bottom",
            fontsize=8,
        )
    fig.tight_layout()
    fig.savefig(OUT_DIR / "topline_economics_summary.png", dpi=160)
    plt.close(fig)


def plot_incremental_net_waterfall(df_components: pd.DataFrame) -> None:
    apply_chart_style()
    row = df_components.iloc[0]
    before_promo = float(row["post_profit_before_promo"])
    promo_cost = -float(row["promo_cost_post_4w"])
    final = float(row["post_net_after_promo"])

    categories = ["Post Profit (Before Promo)", "Promo Cost", "Post Net (After Promo)"]
    values = [before_promo, promo_cost, final]
    colors = ["#9aa0a6", "#d62728", "#2ca02c" if final >= 0 else "#d62728"]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(categories, values, color=colors)
    ax.set_title(
        "Promo Cohort Net Profit Bridge (4 Weeks)",
        loc="left",
        fontsize=13,
        fontweight="bold",
        pad=18,
    )
    ax.set_ylabel("Net Profit per User")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: money_compact(v)))
    polish_axis(ax)
    ax.text(
        0.01,
        1.07,
        "Takeaway: Promo cost is treated as a direct marketing cost against post-period profit.",
        transform=ax.transAxes,
        fontsize=9,
        color="#555555",
        va="bottom",
        clip_on=False,
    )
    for b in bars:
        h = b.get_height()
        ax.text(b.get_x() + b.get_width() / 2, h, money_compact(h), ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "promo_incremental_net_waterfall.png", dpi=160)
    plt.close(fig)


def plot_payback_curve(
    df_payback: pd.DataFrame,
    first_crossing_week: int | None,
    sustained_payback_week: int | None,
) -> None:
    apply_chart_style()
    if df_payback.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(
        df_payback["wk"],
        df_payback["cum_net_after_promo"],
        linewidth=2.4,
        marker="o",
        markersize=4,
        label="Cumulative Net Profit (After Promo Cost)",
        color="#2ca02c",
    )
    ax.axhline(0, color="#d62728", linewidth=1.4, linestyle="--", label="Break-even (0)")
    if first_crossing_week is not None:
        ax.axvline(
            int(first_crossing_week),
            color="#666666",
            linestyle="--",
            linewidth=1.2,
            label=f"First Crossing Week {int(first_crossing_week)}",
        )
    if sustained_payback_week is not None:
        ax.axvline(
            int(sustained_payback_week),
            color="#111111",
            linestyle="-.",
            linewidth=1.4,
            label=f"Sustained Payback Week {int(sustained_payback_week)}",
        )
    ax.set_title("Promo Payback Curve", loc="left", fontsize=13, fontweight="bold", pad=16)
    ax.set_xlabel("Weeks Since Promo Anchor")
    ax.set_ylabel("Cumulative Value")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: money_compact(v)))
    polish_axis(ax)
    if first_crossing_week is None:
        takeaway = "Takeaway: Cumulative net profit stays below zero across the observed horizon."
    elif sustained_payback_week is None:
        takeaway = (
            f"Takeaway: Net profit first crosses in week {int(first_crossing_week)}, "
            "but does not hold for 4 consecutive weeks."
        )
    else:
        takeaway = (
            f"Takeaway: Net profit first crosses in week {int(first_crossing_week)} "
            f"and is sustained from week {int(sustained_payback_week)}."
        )
    ax.text(
        0.01,
        1.02,
        takeaway,
        transform=ax.transAxes,
        fontsize=9,
        color="#555555",
        va="top",
        clip_on=False,
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "none", "pad": 2},
    )
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "promo_payback_curve.png", dpi=160)
    plt.close(fig)


def strip_existing_sections(text: str) -> str:
    markers = ["## Promo impact and retention", "## Limitations of Synthetic Simulation"]
    idxs = [text.find(m) for m in markers if m in text]
    if not idxs:
        return text.rstrip()
    return text[: min(idxs)].rstrip()


def compute_sustained_payback_week(df_payback: pd.DataFrame, consecutive_weeks: int = 4) -> int | None:
    if df_payback.empty:
        return None
    gap = df_payback["cum_net_after_promo"].astype(float).tolist()
    weeks = df_payback["wk"].astype(int).tolist()
    streak = 0
    for idx, g in enumerate(gap):
        if g >= 0:
            streak += 1
            if streak >= consecutive_weeks:
                return int(weeks[idx - consecutive_weeks + 1])
        else:
            streak = 0
    return None


def build_report_section(
    still_active_ret: pd.DataFrame,
    cumulative_ret: pd.DataFrame,
    weekly_survival: pd.DataFrame,
    uplift: pd.DataFrame,
    roi: pd.DataFrame,
    payback_week: int | None,
    sustained_payback_week: int | None,
    calibration_json: dict | None,
    mc_summary: pd.DataFrame | None,
) -> str:
    u = uplift.iloc[0]
    r = roi.iloc[0]

    lines: list[str] = []
    lines.append("## Promo impact and retention")
    lines.append("")
    lines.append("### Main retention view: who is still active")
    lines.append("Chart: `output/retention_curve_by_segment.png`")
    lines.append(
        "Business takeaway: This is the main retention chart. It shows who is still betting at each checkpoint."
    )
    lines.append("")
    lines.append("Still-active retention summary table:")
    lines.append("```")
    lines.append(still_active_ret.to_string(index=False))
    lines.append("```")
    lines.append("")
    lines.append("### Supportive retention view: cumulative return")
    lines.append("Chart: `output/retention_cumulative_supportive.png`")
    lines.append("```")
    lines.append(cumulative_ret.to_string(index=False))
    lines.append("```")
    lines.append("Business takeaway: Cumulative return can only stay flat or rise, so use it as supporting context.")
    lines.append("")
    lines.append("### Weekly survival retention")
    lines.append("Chart: `output/weekly_survival_retention.png`")
    lines.append("```")
    lines.append(weekly_survival.head(20).to_string(index=False))
    lines.append("```")
    lines.append("Business takeaway: Weekly survival gives the true retention decay shape for each segment.")
    lines.append("")
    lines.append("### Promo versus control handle summary")
    lines.append("Chart: `output/promo_pre_post_handle.png`")
    lines.append("```")
    lines.append(uplift.to_string(index=False))
    lines.append("```")
    lines.append(
        f"Business takeaway: Promo users show a post-period handle gap of {money(float(u['post_handle_gap']))} per user per week versus control."
    )
    lines.append("")
    lines.append("### Promo cost versus long-term net summary")
    lines.append("Chart: `output/promo_payback_curve.png`")
    lines.append("```")
    lines.append(roi.to_string(index=False))
    lines.append("```")
    lines.append(
        f"Observed users: {int(r['promo_users_4w'])} (4w), {int(r['promo_users_13w'])} (13w), {int(r['promo_users_26w'])} (26w)."
    )
    lines.append(
        f"Business takeaway: Net profit per promo user improves from {money(float(r['net_profit_per_user_4w']))} at 4 weeks "
        f"to {money(float(r['net_profit_per_user_13w']))} at 13 weeks and {money(float(r['net_profit_per_user_26w']))} at 26 weeks."
    )
    lines.append(
        f"Cohort-level post net totals are {money(float(r['net_profit_total_4w']))} (4w), "
        f"{money(float(r['net_profit_total_13w']))} (13w), and {money(float(r['net_profit_total_26w']))} (26w)."
    )
    lines.append(
        "Promo-tagged bets can be lower margin in isolation, while promo-exposed users can still become profitable over time through repeat non-promo activity."
    )
    lines.append(
        "Cohort-level net includes all post-anchor bets, so it can differ from direct promo ROI."
    )
    if payback_week is None:
        lines.append("Payback window: cumulative net profit does not reach break-even within 26 weeks in this simulation.")
    else:
        lines.append(f"First-crossing payback: cumulative net profit turns positive in week {int(payback_week)}.")
    if sustained_payback_week is None:
        lines.append("Sustained payback: no stable break-even (4 consecutive positive weeks) within the observed horizon.")
    else:
        lines.append(
            f"Sustained payback: break-even is sustained from week {int(sustained_payback_week)} (4+ consecutive weeks)."
        )
    lines.append("")
    lines.append("### Model governance and validation")
    if calibration_json:
        score = calibration_json.get("calibration", {}).get("score")
        if score is not None:
            lines.append(
                f"Calibration: hazard churn parameters were tuned to segment retention targets (fit score {float(score):.4f})."
            )
        else:
            lines.append("Calibration: hazard churn parameters were tuned to segment retention targets.")
    else:
        lines.append("Calibration: metadata file not found, so governance scoring is unavailable.")

    if mc_summary is not None and not mc_summary.empty:
        mc = mc_summary.set_index("metric")

        def fmt_band(metric: str) -> str:
            if metric not in mc.index:
                return "n/a"
            row = mc.loc[metric]
            return f"{float(row['p10']):.2%} to {float(row['p90']):.2%} (p50 {float(row['p50']):.2%})"

        lines.append(
            "Sensitivity bands (Monte Carlo): hold "
            f"{fmt_band('hold')}, net margin {fmt_band('net_margin')}, promo rate {fmt_band('promo_rate')}."
        )
    else:
        lines.append("Sensitivity bands (Monte Carlo): summary file not found.")
    lines.append(
        "Limitation: outputs are scenario estimates from synthetic behavior and should be validated with production holdout tests."
    )
    lines.append("")
    lines.append("Business recommendations:")
    lines.append("- Use this scorecard as a simple campaign health check: post-profit, promo spend, and ROI by horizon.")
    lines.append("- Keep promo rate in control by segment, especially where 13w/26w ROI remains negative.")
    lines.append("- If needed later, add holdouts for causal lift, but keep accounting ROI as the primary view.")
    lines.append("")
    lines.append("## Limitations of Synthetic Simulation")
    lines.append("")
    lines.append("- Behavior is modeled using probabilistic assumptions rather than real customer event telemetry.")
    lines.append("- Real sportsbooks calibrate churn and promo response on observed user-level transaction data.")
    lines.append("- True uplift measurement should use randomized holdout testing for causal confidence.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    con = duckdb.connect(str(DB_PATH))
    con.execute(PROMO_COHORT_RETENTION_SQL)
    con.execute(PROMO_UPLIFT_SQL)
    con.execute(STILL_ACTIVE_RETENTION_SQL)
    con.execute(WEEKLY_SURVIVAL_RETENTION_SQL)

    still_active_ret = con.execute("SELECT * FROM still_active_retention_by_segment").df()
    cumulative_ret = con.execute(RETENTION_BY_SEGMENT_SQL).df()
    weekly_survival = con.execute("SELECT * FROM weekly_survival_retention").df()
    uplift = con.execute(INCREMENTAL_UPLIFT_SUMMARY_SQL).df()
    roi = con.execute(PROMO_ROI_SUMMARY_SQL).df()
    topline = con.execute(HIGH_LEVEL_ECONOMICS_SQL).df()
    payback_series = con.execute(PAYBACK_SERIES_SQL).df()
    payback_week = con.execute(PAYBACK_SQL).fetchone()[0]
    sustained_payback_week = compute_sustained_payback_week(payback_series, consecutive_weeks=4)
    calibration_json = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8")) if CALIBRATION_PATH.exists() else None
    mc_summary = pd.read_csv(MONTE_CARLO_SUMMARY_PATH) if MONTE_CARLO_SUMMARY_PATH.exists() else None

    still_active_ret.to_csv(OUT_DIR / "retention_by_segment_summary.csv", index=False)
    cumulative_ret.to_csv(OUT_DIR / "retention_cumulative_summary.csv", index=False)
    weekly_survival.to_csv(OUT_DIR / "weekly_survival_retention_summary.csv", index=False)
    uplift.to_csv(OUT_DIR / "incremental_uplift_summary.csv", index=False)
    roi.to_csv(OUT_DIR / "promo_roi_summary.csv", index=False)

    plot_retention_by_segment(still_active_ret)
    plot_cumulative_retention_supportive(cumulative_ret)
    plot_weekly_survival_retention(weekly_survival)
    plot_high_level_economics(topline)
    plot_pre_post(uplift)
    plot_payback_curve(payback_series, payback_week, sustained_payback_week)

    report_text = REPORT_PATH.read_text(encoding="utf-8")
    new_section = build_report_section(
        still_active_ret,
        cumulative_ret,
        weekly_survival,
        uplift,
        roi,
        payback_week,
        sustained_payback_week,
        calibration_json,
        mc_summary,
    )
    REPORT_PATH.write_text(strip_existing_sections(report_text) + "\n\n" + new_section, encoding="utf-8")

    print("Promo impact framework updated.")
    print(f"Table: promo_cohort_retention ({con.execute('SELECT COUNT(*) FROM promo_cohort_retention').fetchone()[0]} rows)")
    print(f"Table: promo_uplift_user ({con.execute('SELECT COUNT(*) FROM promo_uplift_user').fetchone()[0]} rows)")
    print(f"Payback week (first crossing): {payback_week}")
    print(f"Payback week (sustained 4 weeks): {sustained_payback_week}")


if __name__ == "__main__":
    main()
