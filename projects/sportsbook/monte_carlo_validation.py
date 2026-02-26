from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd

from run_sportsbook_analysis import (
    CALIBRATION_PATH,
    DEFAULT_CHURN_CONFIG,
    build_core_tables,
    compute_still_active_retention,
    generate_customers,
    simulate_ledger,
)


BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "output"
DB_PATH = OUT_DIR / "sportsbook.duckdb"
RUNS_PATH = OUT_DIR / "monte_carlo_runs.csv"
SUMMARY_PATH = OUT_DIR / "monte_carlo_summary.csv"


def summarize_quantiles(df: pd.DataFrame, metric_cols: list[str]) -> pd.DataFrame:
    rows = []
    for m in metric_cols:
        s = df[m].dropna()
        rows.append(
            {
                "metric": m,
                "mean": float(s.mean()) if not s.empty else None,
                "p10": float(s.quantile(0.10)) if not s.empty else None,
                "p50": float(s.quantile(0.50)) if not s.empty else None,
                "p90": float(s.quantile(0.90)) if not s.empty else None,
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Monte Carlo validation for sportsbook simulation")
    parser.add_argument("--runs", type=int, default=30, help="Number of simulation runs")
    parser.add_argument("--n-customers", type=int, default=1200, help="Customers per run")
    parser.add_argument("--customer-seed", type=int, default=100, help="Base customer seed")
    parser.add_argument("--ledger-seed", type=int, default=500, help="Base ledger seed")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    build_core_tables(con)

    if CALIBRATION_PATH.exists():
        calibration = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
        churn_cfg = calibration.get("churn_config", DEFAULT_CHURN_CONFIG)
    else:
        churn_cfg = DEFAULT_CHURN_CONFIG

    run_rows = []
    for i in range(args.runs):
        customer_seed = args.customer_seed + i
        ledger_seed = args.ledger_seed + i
        customers = generate_customers(n_customers=args.n_customers, seed=customer_seed)
        bets, _, costs = simulate_ledger(con, customers, seed=ledger_seed, churn_cfg=churn_cfg)
        if bets.empty or costs.empty:
            continue

        handle = float(bets["stake_amount"].sum())
        ggr = float((bets["stake_amount"] - bets["gross_payout"]).sum())
        merged = bets[["bet_id", "stake_amount", "gross_payout"]].merge(
            costs[["bet_id", "promo_cost", "payment_fee", "tax"]],
            on="bet_id",
            how="left",
        )
        net = float(
            (
                merged["stake_amount"]
                - merged["gross_payout"]
                - merged["promo_cost"]
                - merged["payment_fee"]
                - merged["tax"]
            ).sum()
        )
        promo_rate = float(merged["promo_cost"].sum() / handle) if handle else 0.0
        hold = float(ggr / handle) if handle else 0.0
        margin = float(net / handle) if handle else 0.0
        ret = compute_still_active_retention(bets, customers)

        row = {
            "run_id": i + 1,
            "customer_seed": customer_seed,
            "ledger_seed": ledger_seed,
            "handle": handle,
            "hold": hold,
            "net_margin": margin,
            "promo_rate": promo_rate,
        }
        for seg in ["sharp", "regular", "casual", "promo_hunter"]:
            for k in ["still_active_7d", "still_active_30d", "still_active_60d", "still_active_90d"]:
                row[f"{seg}_{k}"] = ret.get(seg, {}).get(k, 0.0)
        run_rows.append(row)

    runs_df = pd.DataFrame(run_rows)
    runs_df.to_csv(RUNS_PATH, index=False)

    metric_cols = [c for c in runs_df.columns if c not in ["run_id", "customer_seed", "ledger_seed"]]
    summary_df = summarize_quantiles(runs_df, metric_cols)
    summary_df.to_csv(SUMMARY_PATH, index=False)

    print(f"Monte Carlo runs saved: {RUNS_PATH}")
    print(f"Monte Carlo summary saved: {SUMMARY_PATH}")
    print(f"Runs completed: {len(runs_df)}")


if __name__ == "__main__":
    main()

