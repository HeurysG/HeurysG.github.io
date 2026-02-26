# NFL Sportsbook Analytics Report

## Step 1 - Data Inspection

### raw_scores
- Row count: 14371
- Columns and data types:
  - `schedule_date` (DATE)
  - `schedule_season` (BIGINT)
  - `schedule_week` (VARCHAR)
  - `schedule_playoff` (BOOLEAN)
  - `team_home` (VARCHAR)
  - `score_home` (BIGINT)
  - `score_away` (BIGINT)
  - `team_away` (VARCHAR)
  - `team_favorite_id` (VARCHAR)
  - `spread_favorite` (DOUBLE)
  - `over_under_line` (VARCHAR)
  - `stadium` (VARCHAR)
  - `stadium_neutral` (BOOLEAN)
  - `weather_temperature` (BIGINT)
  - `weather_wind_mph` (BIGINT)
  - `weather_humidity` (BIGINT)
  - `weather_detail` (VARCHAR)
- Min/Max summary: `{"min_season": 1966, "max_season": 2025, "min_game_date": "1966-09-02", "max_game_date": "2026-02-08"}`
- Potential PKs: single=[], composite=[['schedule_date', 'team_home', 'team_away'], ['schedule_season', 'schedule_week', 'team_home', 'team_away']]
- Null percentage by column:
  - `schedule_date`: 0.00%
  - `schedule_season`: 0.00%
  - `schedule_week`: 0.00%
  - `schedule_playoff`: 0.00%
  - `team_home`: 0.00%
  - `score_home`: 0.00%
  - `score_away`: 0.00%
  - `team_away`: 0.00%
  - `team_favorite_id`: 0.03%
  - `spread_favorite`: 0.03%
  - `over_under_line`: 17.15%
  - `stadium`: 0.00%
  - `stadium_neutral`: 0.00%
  - `weather_temperature`: 10.51%
  - `weather_wind_mph`: 10.63%
  - `weather_humidity`: 39.34%
  - `weather_detail`: 76.93%

### raw_teams
- Row count: 44
- Columns and data types:
  - `team_name` (VARCHAR)
  - `team_name_short` (VARCHAR)
  - `team_id` (VARCHAR)
  - `team_id_pfr` (VARCHAR)
  - `team_conference` (VARCHAR)
  - `team_division` (VARCHAR)
  - `team_conference_pre2002` (VARCHAR)
  - `team_division_pre2002` (VARCHAR)
- Min/Max summary: `{"min_team_name": "Arizona Cardinals", "max_team_name": "Washington Redskins"}`
- Potential PKs: single=['team_name'], composite=[['team_name', 'team_id'], ['team_name', 'team_id_pfr']]
- Null percentage by column:
  - `team_name`: 0.00%
  - `team_name_short`: 0.00%
  - `team_id`: 0.00%
  - `team_id_pfr`: 0.00%
  - `team_conference`: 0.00%
  - `team_division`: 20.45%
  - `team_conference_pre2002`: 0.00%
  - `team_division_pre2002`: 4.55%

## Step 2 - Analytic Tables Built

Created and populated tables:
- `dim_team` from `nfl_teams.csv`
- `dim_event` from `spreadspoke_scores.csv` with cleaned team mapping and playoff/week normalization
- `dim_customer` (synthetic)
- `fact_bet`, `fact_bet_leg`, `fact_bet_cost`

## Step 3 - Synthetic Bet Ledger

- Customer heterogeneity modeled with segment-specific frequency, stake size, market preference, and promo sensitivity
- Bets include singles and parlays, realistic stake distribution, mild favorite/over bias, and price drift from closing numbers
- Settlement logic uses real game results from `dim_event`
- Costs attached per bet: promo, payment fee, tax

## Step 4 - Core KPI Definitions

- `handle = SUM(stake_amount)`
- `payout = SUM(gross_payout)`
- `gross_profit = handle - payout`
- `net_profit = gross_profit - promo_cost - payment_fee - tax`
- `hold_pct = gross_profit / handle`
- `margin_pct = net_profit / handle`

Portfolio totals: handle=$3,828,225, gross_profit=$185,473, net_profit=$77,394, hold=4.84%, margin=2.02%

## Step 5 and 6 - Business Questions, SQL, Charts, Explanations, Recommendations

### 1) Revenue and profit over time

SQL:
```sql
SELECT
            DATE_TRUNC('month', b.bet_datetime) AS month,
            SUM(b.stake_amount) AS handle,
            SUM(b.gross_payout) AS payout,
            SUM(b.stake_amount - b.gross_payout) AS gross_profit,
            SUM(b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) AS net_profit,
            SUM(b.stake_amount - b.gross_payout) / NULLIF(SUM(b.stake_amount), 0) AS hold_pct,
            SUM(b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) / NULLIF(SUM(b.stake_amount), 0) AS margin_pct
        FROM fact_bet b
        JOIN fact_bet_cost c ON b.bet_id = c.bet_id
        GROUP BY 1
        ORDER BY 1;
```

Chart: `output/q1_revenue_profit_over_time.png`

Business explanation: Track monthly trend in handle, gross profit, and net profit.

Recommendations:
- Set monthly net-profit guardrails by segment and channel.
- Flag months where margin compression is driven by promo over-spend.
- Use seasonality to pre-allocate risk and promo budgets.

Sample output (top rows):

```
     month       handle       payout  gross_profit   net_profit  hold_pct  margin_pct
2010-09-01   973.984206  1167.455288   -193.471082  -221.216426 -0.198639   -0.227125
2010-10-01 11149.055207  9451.245388   1697.809818  1338.066454  0.152283    0.120016
2010-11-01  9168.166055 10862.585423  -1694.419368 -1913.742613 -0.184816   -0.208738
2010-12-01 16411.963358 11960.690747   4451.272610  3943.030177  0.271221    0.240253
2011-01-01  7305.266900  5951.704385   1353.562515  1123.499137  0.185286    0.153793
2011-02-01  3374.432648  3040.395105    334.037543   242.647321  0.098991    0.071908
2011-09-01 16214.226250 18509.760756  -2295.534506 -2708.629651 -0.141575   -0.167053
2011-10-01 31892.632632 30489.598130   1403.034502   499.619160  0.043992    0.015666
2011-11-01 30917.723959 29440.267363   1477.456596   559.445447  0.047787    0.018095
2011-12-01 38325.284910 45270.639170  -6945.354260 -8032.516319 -0.181221   -0.209588
```

### 2) Bet type contribution to profit

SQL:
```sql
SELECT
            b.bet_type,
            COUNT(*) AS bets,
            SUM(b.stake_amount) AS handle,
            SUM(b.stake_amount - b.gross_payout) AS gross_profit,
            SUM(b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) AS net_profit,
            SUM(b.stake_amount - b.gross_payout) / NULLIF(SUM(b.stake_amount), 0) AS hold_pct
        FROM fact_bet b
        JOIN fact_bet_cost c ON b.bet_id = c.bet_id
        GROUP BY 1
        ORDER BY net_profit DESC;
```

Chart: `output/q2_profit_by_bet_type.png`

Business explanation: Compare bet types on net profit and hold efficiency.

Recommendations:
- Scale acquisition into bet types with strong net-profit density.
- Adjust pricing/promos for low-hold bet types before scaling volume.
- Use parlay risk limits when payout volatility spikes.

Sample output (top rows):

```
 bet_type  bets       handle  gross_profit    net_profit  hold_pct
    total  5359 9.455220e+05  76152.162408  50023.010691  0.080540
   spread  9647 1.702784e+06  88765.647747  42215.990356  0.052130
   parlay  2222 3.865524e+05  10327.591936  -3862.993659  0.026717
moneyline  4706 7.933668e+05  10227.828924 -10982.373443  0.012892
```

### 3) Profit variation by week and playoffs

SQL:
```sql
SELECT
            e.week,
            e.is_playoff,
            SUM(b.stake_amount) AS handle,
            SUM(b.stake_amount - b.gross_payout) AS gross_profit,
            SUM(b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) AS net_profit,
            SUM(b.stake_amount - b.gross_payout) / NULLIF(SUM(b.stake_amount), 0) AS hold_pct
        FROM fact_bet b
        JOIN fact_bet_cost c ON b.bet_id = c.bet_id
        JOIN fact_bet_leg l ON b.bet_id = l.bet_id
        JOIN dim_event e ON l.event_id = e.event_id
        WHERE e.week IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1, 2;
```

Chart: `output/q3_profit_by_week_playoff.png`

Business explanation: Understand where weekly and playoff timing creates upside or risk.

Recommendations:
- Tighten limits in historically volatile playoff windows.
- Shift promos to stronger in-season weeks with stable hold.
- Use week-level dashboards for rapid pricing response.

Sample output (top rows):

```
 week  is_playoff        handle  gross_profit    net_profit  hold_pct
    1       False 234423.862894   9672.561849   2583.782871  0.041261
    2       False 230610.316768  12406.284233   5569.454488  0.053798
    3       False 227222.906373   3780.925555  -2910.002378  0.016640
    4       False 240058.915313 -24176.455708 -31312.317206 -0.100711
    5       False 242902.691501  21964.466464  14663.761552  0.090425
    6       False 218518.059229  10524.163895   3923.151502  0.048162
    7       False 204811.469823  25415.493122  19402.421237  0.124092
    8       False 221169.894762  19731.974008  13026.964729  0.089216
    9       False 219448.957335  32486.323422  25776.387293  0.148036
   10       False 226754.285473  44943.573209  37981.650653  0.198204
```

### 4) Team-level profit association

SQL:
```sql
SELECT
            t.team_name,
            SUM(b.stake_amount) AS handle,
            SUM(b.stake_amount - b.gross_payout) AS gross_profit,
            SUM(b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) AS net_profit,
            SUM(b.stake_amount - b.gross_payout) / NULLIF(SUM(b.stake_amount), 0) AS hold_pct
        FROM fact_bet b
        JOIN fact_bet_cost c ON b.bet_id = c.bet_id
        JOIN fact_bet_leg l ON b.bet_id = l.bet_id
        JOIN dim_event e ON l.event_id = e.event_id
        JOIN dim_team t ON (
            l.selection = 'home' AND e.home_team_id = t.team_id
        ) OR (
            l.selection = 'away' AND e.away_team_id = t.team_id
        )
        GROUP BY 1
        HAVING SUM(b.stake_amount) > 50000
        ORDER BY net_profit DESC;
```

Chart: `output/q4_profit_by_team.png`

Business explanation: Identify team exposure where customer behavior impacts sportsbook profit.

Recommendations:
- Monitor high-handle teams with weak margin for exposure control.
- Tailor campaigns to teams with durable net-profit profile.
- Set dynamic limits for heavily biased one-sided markets.

Sample output (top rows):

```
           team_name        handle  gross_profit   net_profit  hold_pct
   Carolina Panthers  86163.139615  27777.534403 24961.414508  0.322383
       New York Jets  79059.807468  20371.150022 17760.843494  0.257668
Jacksonville Jaguars  86210.185801  18825.750512 16105.196625  0.218370
     Oakland Raiders 127877.055792  18280.949212 14219.770348  0.142957
    Cleveland Browns  77088.513814  16660.166528 14130.838657  0.216117
      Miami Dolphins  85313.465615  16307.526791 13725.272688  0.191148
Tampa Bay Buccaneers 100495.033371  15581.760200 12520.780873  0.155050
 San Francisco 49ers 106991.900668  13910.796650 10758.481190  0.130017
   Arizona Cardinals  86577.933680  11112.778894  8437.707162  0.128356
Los Angeles Chargers 125350.405468  12092.718231  8243.713217  0.096471
```

### 5) Promotion profitability

SQL:
```sql
SELECT
            CASE WHEN c.promo_cost > 0 THEN 'promo_applied' ELSE 'no_promo' END AS promo_flag,
            COUNT(*) AS bets,
            SUM(b.stake_amount) AS handle,
            SUM(c.promo_cost) AS promo_spend,
            SUM(b.stake_amount - b.gross_payout) AS gross_profit,
            SUM(b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) AS net_profit,
            SUM(b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) / NULLIF(SUM(b.stake_amount), 0) AS margin_pct
        FROM fact_bet b
        JOIN fact_bet_cost c ON b.bet_id = c.bet_id
        GROUP BY 1
        ORDER BY 1;
```

Chart: `output/q5_promo_profitability.png`

Business explanation: Compare promo-applied bets vs non-promo bets on margin.

Recommendations:
- Cap promo depth where incremental handle does not convert to net profit.
- Target promos to cohorts with positive post-cost margin.
- Measure promo ROI by campaign, not only by handle lift.

Sample output (top rows):

```
   promo_flag  bets       handle  promo_spend  gross_profit   net_profit  margin_pct
     no_promo 21443 3.763730e+06     0.000000 184685.423448 79679.279749    0.021170
promo_applied   491 6.449571e+04  1334.401623    787.807567 -2285.645804   -0.035439
```

### 6) Customer group differences

SQL:
```sql
SELECT
            d.segment,
            COUNT(*) AS bets,
            COUNT(DISTINCT b.customer_id) AS customers,
            SUM(b.stake_amount) AS handle,
            AVG(b.stake_amount) AS avg_stake,
            SUM(b.stake_amount - b.gross_payout) AS gross_profit,
            SUM(b.stake_amount - b.gross_payout - c.promo_cost - c.payment_fee - c.tax) AS net_profit,
            SUM(b.stake_amount - b.gross_payout) / NULLIF(SUM(b.stake_amount), 0) AS hold_pct
        FROM fact_bet b
        JOIN dim_customer d ON b.customer_id = d.customer_id
        JOIN fact_bet_cost c ON b.bet_id = c.bet_id
        GROUP BY 1
        ORDER BY net_profit DESC;
```

Chart: `output/q6_customer_group_differences.png`

Business explanation: Segment economics by customer type to improve lifetime value and risk.

Recommendations:
- Increase retention spend on segments with sustained positive net margin.
- Reduce broad promos for promo-sensitive cohorts with weak ROI.
- Use segment-level risk and pricing policies instead of one-size-fits-all.

Sample output (top rows):

```
     segment  bets  customers       handle  avg_stake  gross_profit   net_profit  hold_pct
       sharp 18095        212 3.593824e+06 198.608688 162517.405491 61814.829596  0.045221
     regular  2358        495 1.791915e+05  75.992984  16910.563339 11286.566203  0.094371
      casual  1250        729 4.266864e+04  34.134913   4389.720806  3038.764967  0.102879
promo_hunter   231        164 1.254095e+04  54.289819   1655.541380  1253.473180  0.132011
```

## Promo impact and retention

### Main retention view: who is still active
Chart: `output/retention_curve_by_segment.png`
Business takeaway: This is the main retention chart. It shows who is still betting at each checkpoint.

Still-active retention summary table:
```
     segment  users  still_active_7d  still_active_30d  still_active_60d  still_active_90d
       sharp    212         0.792453          0.650943          0.443396          0.226415
     regular    495         0.359596          0.197980          0.046465          0.014141
      casual    729         0.165981          0.037037          0.001372          0.000000
promo_hunter    164         0.128049          0.018293          0.000000          0.000000
```

### Supportive retention view: cumulative return
Chart: `output/retention_cumulative_supportive.png`
```
     segment  users  retention_7d  retention_30d  retention_90d
       sharp    212      0.792453       0.948113       0.948113
     regular    495      0.359596       0.694949       0.711111
      casual    729      0.165981       0.337449       0.344307
promo_hunter    164      0.128049       0.256098       0.256098
```
Business takeaway: Cumulative return can only stay flat or rise, so use it as supporting context.

### Weekly survival retention
Chart: `output/weekly_survival_retention.png`
```
segment  week_num  cohort_users  active_users  retention_rate
  sharp         1           212           168        0.792453
  sharp         2           212           148        0.698113
  sharp         3           212           141        0.665094
  sharp         4           212           148        0.698113
  sharp         5           212           125        0.589623
  sharp         6           212           113        0.533019
  sharp         7           212           112        0.528302
  sharp         8           212            97        0.457547
  sharp         9           212            92        0.433962
  sharp        10           212            81        0.382075
  sharp        11           212            73        0.344340
  sharp        12           212            61        0.287736
  sharp        13           212            51        0.240566
regular         1           495           178        0.359596
regular         2           495           177        0.357576
regular         3           495           148        0.298990
regular         4           495            97        0.195960
regular         5           495            89        0.179798
regular         6           495            63        0.127273
regular         7           495            47        0.094949
```
Business takeaway: Weekly survival gives the true retention decay shape for each segment.

### Promo versus control handle summary
Chart: `output/promo_pre_post_handle.png`
```
 promo_pre_handle  promo_post_handle  promo_delta_handle  control_pre_handle  control_post_handle  control_delta_handle  post_handle_gap
        80.863236         109.299037           28.435801           42.684894            32.366374             -10.31852        76.932663
```
Business takeaway: Promo users show a post-period handle gap of $77 per user per week versus control.

### Promo cost versus long-term net summary
Chart: `output/promo_payback_curve.png`
```
 promo_users_4w  promo_users_13w  promo_users_26w  net_profit_per_user_4w  net_profit_per_user_13w  net_profit_per_user_26w  net_profit_total_4w  net_profit_total_13w  net_profit_total_26w  promo_cost_total  promo_cost_total_13w  promo_cost_total_26w  net_after_promo_total_4w  net_after_promo_total_13w  net_after_promo_total_26w  direct_promo_net_total_4w  direct_promo_net_total_13w  direct_promo_net_total_26w  operating_profit_before_promo_4w  operating_profit_before_promo_13w  operating_profit_before_promo_26w        roi   roi_13w   roi_26w  direct_promo_roi_4w  direct_promo_roi_13w  direct_promo_roi_26w  operating_roi_4w  operating_roi_13w  operating_roi_26w
            291              290              288                -1.90083                18.622742                27.598344           901.858347           6850.595072           9388.322928         80.787621            153.534491            151.333807               -553.141653                5400.595072                7948.322928                -296.499908                  -79.747413                  366.911184                       -472.354031                        5554.129563                        8099.656736 -21.680301 -9.963542 -7.090873            -3.670116              -0.51941              2.424516         -5.846861          36.175126          53.521793
```
Observed users: 291 (4w), 290 (13w), 288 (26w).
Business takeaway: Net profit per promo user improves from $-2 at 4 weeks to $19 at 13 weeks and $28 at 26 weeks.
Cohort-level post net totals are $902 (4w), $6,851 (13w), and $9,388 (26w).
Promo-tagged bets can be lower margin in isolation, while promo-exposed users can still become profitable over time through repeat non-promo activity.
Cohort-level net includes all post-anchor bets, so it can differ from direct promo ROI.
First-crossing payback: cumulative net profit turns positive in week 2.
Sustained payback: break-even is sustained from week 8 (4+ consecutive weeks).

### Model governance and validation
Calibration: hazard churn parameters were tuned to segment retention targets (fit score 0.0062).
Sensitivity bands (Monte Carlo): hold 5.15% to 7.30% (p50 6.52%), net margin 2.29% to 4.40% (p50 3.62%), promo rate 0.08% to 0.10% (p50 0.09%).
Limitation: outputs are scenario estimates from synthetic behavior and should be validated with production holdout tests.

Business recommendations:
- Use this scorecard as a simple campaign health check: post-profit, promo spend, and ROI by horizon.
- Keep promo rate in control by segment, especially where 13w/26w ROI remains negative.
- If needed later, add holdouts for causal lift, but keep accounting ROI as the primary view.

## Limitations of Synthetic Simulation

- Behavior is modeled using probabilistic assumptions rather than real customer event telemetry.
- Real sportsbooks calibrate churn and promo response on observed user-level transaction data.
- True uplift measurement should use randomized holdout testing for causal confidence.
