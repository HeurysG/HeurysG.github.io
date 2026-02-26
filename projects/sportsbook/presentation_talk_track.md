# Sportsbook Project Talk Track

## 30-second version
I built a SQL-first sportsbook analytics project using real NFL outcomes and a synthetic betting ledger to analyze revenue, profitability, retention, and promo economics.
The model includes customer segments, bet-level costs, and a promo scorecard at 4/13/26 weeks.
Final output is commercially realistic: hold around 4.8%, margin around 2.0%, and promo user net improving from -$2 at 4 weeks to +$28 at 26 weeks.

## 90-second version
This project starts with a warehouse-style model:
- dimensions: teams, events, customers
- facts: bets, bet legs, and costs

I used SQL for KPI analysis and Python for simulation/calibration.
Every simulated bet settles against real NFL game outcomes.
I modeled customer heterogeneity (sharp, regular, casual, promo hunter), segment-level churn, stake behavior, and market preferences.

Then I built a promo scorecard:
- net profit per promo user at 4/13/26 weeks
- promo cost efficiency
- payback timing

Main business takeaway:
- baseline sportsbook economics are healthy
- promo bets can be weak in isolation
- promo cohorts can still improve over time if exposure and cost are controlled

## Key numbers to cite
- Handle: ~$3.83M
- Gross Profit: ~$185K
- Net Profit: ~$77K
- Hold: 4.84%
- Margin: 2.02%
- Promo net/user: -$2 -> $19 -> $28 (4w/13w/26w)
- Payback: first crossing week 2, sustained week 8

## What this demonstrates
- SQL analytics + business framing
- Metric design and calibration discipline
- Turning noisy behavioral data into decisions

## Caveat to mention
Customer behavior is simulated; the methodology is the point.
In production, I would validate promo lift with randomized holdouts.
