from __future__ import annotations

import argparse
import itertools
import json
import math
import random
from dataclasses import dataclass
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "spdata"
OUT_DIR = BASE_DIR / "output"
DB_PATH = OUT_DIR / "sportsbook.duckdb"
REPORT_PATH = OUT_DIR / "sportsbook_report.md"
PROFILE_PATH = OUT_DIR / "data_profile.json"
CALIBRATION_PATH = OUT_DIR / "churn_calibration.json"


DEFAULT_CHURN_CONFIG = {
    "base_weekly_churn": {
        "sharp": 0.020,
        "regular": 0.050,
        "casual": 0.080,
        "promo_hunter": 0.120,
    },
    "alpha": 0.05,
    "churn_plateau_mult": 0.68,
    "early_churn_mult": {
        "sharp": 1.15,
        "regular": 1.25,
        "casual": 1.40,
        "promo_hunter": 1.60,
    },
    "mid_churn_mult": {
        "regular": 2.00,
        "casual": 2.40,
        "promo_hunter": 2.90,
    },
    "tail_churn_mult": {
        "regular": 1.35,
        "casual": 1.70,
        "promo_hunter": 2.00,
    },
    "inactivity_mult": 2.5,
    "negative_net_mult": 1.45,
    "promo_hunter_post_promo_mult": 1.20,
    "promo_hunter_post_promo_weeks": [1, 2, 3, 4],
    "min_churn": 0.006,
    "max_churn": 0.65,
}


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def fmt_pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def fmt_money(v: float) -> str:
    return f"${v:,.0f}"


def profile_table(con: duckdb.DuckDBPyConnection, table_name: str) -> dict:
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    col_df = con.execute(f"DESCRIBE {table_name}").df()

    stats = {
        "table": table_name,
        "row_count": int(row_count),
        "columns": [],
        "min_max": {},
        "primary_key_candidates": {"single_column": [], "composite": []},
        "null_pct": {},
    }

    for _, r in col_df.iterrows():
        col_name = r["column_name"]
        col_type = r["column_type"]
        null_count = con.execute(
            f"SELECT COUNT(*) FILTER (WHERE {col_name} IS NULL) FROM {table_name}"
        ).fetchone()[0]
        null_pct = (null_count / row_count) if row_count else 0
        distinct_count = con.execute(
            f"SELECT COUNT(DISTINCT {col_name}) FROM {table_name}"
        ).fetchone()[0]
        is_unique_non_null = distinct_count == row_count and null_count == 0

        stats["columns"].append({"name": col_name, "type": col_type})
        stats["null_pct"][col_name] = null_pct
        if is_unique_non_null:
            stats["primary_key_candidates"]["single_column"].append(col_name)

    if table_name == "raw_scores":
        min_max = con.execute(
            """
            SELECT
                MIN(TRY_CAST(schedule_season AS INTEGER)) AS min_season,
                MAX(TRY_CAST(schedule_season AS INTEGER)) AS max_season,
                MIN(
                    COALESCE(
                        TRY_CAST(schedule_date AS DATE),
                        TRY_STRPTIME(CAST(schedule_date AS VARCHAR), '%m/%d/%Y')::DATE
                    )
                ) AS min_game_date,
                MAX(
                    COALESCE(
                        TRY_CAST(schedule_date AS DATE),
                        TRY_STRPTIME(CAST(schedule_date AS VARCHAR), '%m/%d/%Y')::DATE
                    )
                ) AS max_game_date
            FROM raw_scores
            """
        ).fetchdf().iloc[0].to_dict()
        stats["min_max"] = {
            "min_season": int(min_max["min_season"]),
            "max_season": int(min_max["max_season"]),
            "min_game_date": str(min_max["min_game_date"].date()),
            "max_game_date": str(min_max["max_game_date"].date()),
        }

        composite_candidates = [
            ["schedule_date", "team_home", "team_away"],
            ["schedule_season", "schedule_week", "team_home", "team_away"],
        ]
        for cols in composite_candidates:
            col_list = ", ".join(cols)
            q = f"""
                SELECT
                    COUNT(*) AS c_all,
                    COUNT(DISTINCT ({col_list})) AS c_distinct
                FROM raw_scores
                WHERE {" AND ".join([f"{c} IS NOT NULL" for c in cols])}
            """
            c_all, c_distinct = con.execute(q).fetchone()
            if c_all == c_distinct:
                stats["primary_key_candidates"]["composite"].append(cols)

    if table_name == "raw_teams":
        min_max = con.execute(
            """
            SELECT
                MIN(team_name) AS min_team_name,
                MAX(team_name) AS max_team_name
            FROM raw_teams
            """
        ).fetchdf().iloc[0].to_dict()
        stats["min_max"] = min_max

        composite_candidates = [
            ["team_name", "team_id"],
            ["team_name", "team_id_pfr"],
        ]
        for cols in composite_candidates:
            col_list = ", ".join(cols)
            q = f"""
                SELECT
                    COUNT(*) AS c_all,
                    COUNT(DISTINCT ({col_list})) AS c_distinct
                FROM raw_teams
                WHERE {" AND ".join([f"{c} IS NOT NULL" for c in cols])}
            """
            c_all, c_distinct = con.execute(q).fetchone()
            if c_all == c_distinct:
                stats["primary_key_candidates"]["composite"].append(cols)

    return stats


def build_core_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TABLE raw_scores AS
        SELECT *
        FROM read_csv_auto('{(DATA_DIR / "spreadspoke_scores.csv").as_posix()}', header=true);
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE raw_teams AS
        SELECT *
        FROM read_csv_auto('{(DATA_DIR / "nfl_teams.csv").as_posix()}', header=true);
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE dim_team AS
        WITH ranked AS (
            SELECT
                team_id,
                team_name,
                team_conference AS conference,
                NULLIF(team_division, '') AS division,
                ROW_NUMBER() OVER (
                    PARTITION BY team_id
                    ORDER BY
                        (team_division IS NOT NULL AND team_division <> '') DESC,
                        LENGTH(team_name) DESC
                ) AS rn
            FROM raw_teams
            WHERE team_id IS NOT NULL
        )
        SELECT
            team_id,
            team_name,
            conference,
            division
        FROM ranked
        WHERE rn = 1;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE team_name_map AS
        SELECT DISTINCT team_name, team_id
        FROM raw_teams
        WHERE team_name IS NOT NULL
          AND team_id IS NOT NULL;
        """
    )

    con.execute(
        """
        INSERT INTO team_name_map VALUES
            ('Washington Football Team', 'WAS'),
            ('Washington Redskins', 'WAS'),
            ('Washington Commanders', 'WAS'),
            ('St. Louis Rams', 'LAR'),
            ('San Diego Chargers', 'LAC'),
            ('Oakland Raiders', 'LVR'),
            ('Los Angeles Raiders', 'LVR'),
            ('Phoenix Cardinals', 'ARI'),
            ('St. Louis Cardinals', 'ARI'),
            ('Houston Oilers', 'TEN'),
            ('Baltimore Colts', 'IND');
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE dim_event AS
        WITH base AS (
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY
                        COALESCE(
                            TRY_CAST(schedule_date AS DATE),
                            TRY_STRPTIME(CAST(schedule_date AS VARCHAR), '%m/%d/%Y')::DATE
                        ),
                        TRY_CAST(schedule_season AS INTEGER),
                        schedule_week,
                        team_home,
                        team_away
                ) AS event_id,
                TRY_CAST(schedule_season AS INTEGER) AS season,
                CASE
                    WHEN REGEXP_MATCHES(LOWER(schedule_week), '^[0-9]+$') THEN TRY_CAST(schedule_week AS INTEGER)
                    WHEN LOWER(schedule_week) LIKE '%wild%' THEN 19
                    WHEN LOWER(schedule_week) LIKE '%division%' THEN 20
                    WHEN LOWER(schedule_week) LIKE '%champ%' THEN 21
                    WHEN LOWER(schedule_week) LIKE '%super%' THEN 22
                    ELSE NULL
                END AS week,
                COALESCE(
                    TRY_CAST(schedule_date AS DATE),
                    TRY_STRPTIME(CAST(schedule_date AS VARCHAR), '%m/%d/%Y')::DATE
                ) AS game_date,
                th.team_id AS home_team_id,
                ta.team_id AS away_team_id,
                TRY_CAST(score_home AS INTEGER) AS home_score,
                TRY_CAST(score_away AS INTEGER) AS away_score,
                TRY_CAST(spread_favorite AS DOUBLE) AS spread_favorite_raw,
                TRY_CAST(over_under_line AS DOUBLE) AS total_line,
                CASE WHEN schedule_playoff = TRUE THEN TRUE ELSE FALSE END AS is_playoff,
                team_favorite_id
            FROM raw_scores s
            LEFT JOIN team_name_map th ON s.team_home = th.team_name
            LEFT JOIN team_name_map ta ON s.team_away = ta.team_name
        )
        SELECT
            event_id,
            season,
            week,
            game_date,
            home_team_id,
            away_team_id,
            home_score,
            away_score,
            CASE
                WHEN team_favorite_id = home_team_id THEN spread_favorite_raw
                WHEN team_favorite_id = away_team_id THEN -spread_favorite_raw
                ELSE NULL
            END AS spread_line,
            total_line,
            is_playoff
        FROM base
        WHERE home_team_id IS NOT NULL
          AND away_team_id IS NOT NULL
          AND season IS NOT NULL
          AND game_date IS NOT NULL
          AND home_score IS NOT NULL
          AND away_score IS NOT NULL;
        """
    )


@dataclass
class CustomerProfile:
    customer_id: int
    segment: str
    freq: float
    avg_stake: float
    pref_moneyline: float
    pref_spread: float
    pref_total: float
    pref_parlay: float
    promo_sensitivity: float


def generate_customers(n_customers: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    segments = ["casual", "regular", "sharp", "promo_hunter"]
    seg_prob = [0.45, 0.30, 0.15, 0.10]
    seg_freq = {
        "casual": (0.12, 0.50),
        "regular": (0.40, 1.00),
        "sharp": (1.30, 2.80),
        "promo_hunter": (0.10, 0.38),
    }
    seg_stake = {
        "casual": (18, 40),
        "regular": (35, 95),
        "sharp": (80, 260),
        "promo_hunter": (25, 70),
    }

    rows = []
    for cid in range(1, n_customers + 1):
        seg = rng.choice(segments, p=seg_prob)
        freq = float(rng.uniform(*seg_freq[seg]))
        avg_stake = float(rng.uniform(*seg_stake[seg]))

        if seg == "sharp":
            prefs = np.array([0.20, 0.45, 0.25, 0.10])
            promo_sens = float(rng.uniform(0.05, 0.35))
        elif seg == "promo_hunter":
            prefs = np.array([0.25, 0.35, 0.20, 0.20])
            promo_sens = float(rng.uniform(0.65, 0.98))
        elif seg == "regular":
            prefs = np.array([0.25, 0.40, 0.25, 0.10])
            promo_sens = float(rng.uniform(0.25, 0.60))
        else:
            prefs = np.array([0.30, 0.35, 0.25, 0.10])
            promo_sens = float(rng.uniform(0.30, 0.70))

        noise = rng.normal(0, 0.03, 4)
        prefs = np.clip(prefs + noise, 0.05, None)
        prefs = prefs / prefs.sum()

        rows.append(
            CustomerProfile(
                customer_id=cid,
                segment=seg,
                freq=freq,
                avg_stake=avg_stake,
                pref_moneyline=float(prefs[0]),
                pref_spread=float(prefs[1]),
                pref_total=float(prefs[2]),
                pref_parlay=float(prefs[3]),
                promo_sensitivity=promo_sens,
            ).__dict__
        )
    return pd.DataFrame(rows)


def american_to_decimal(odds: int) -> float:
    if odds > 0:
        return 1.0 + odds / 100.0
    return 1.0 + 100.0 / abs(odds)


def simulate_ledger(
    con: duckdb.DuckDBPyConnection,
    customers: pd.DataFrame,
    seed: int = 7,
    churn_cfg: dict | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    random.seed(seed)
    churn_cfg = churn_cfg or DEFAULT_CHURN_CONFIG

    events = con.execute(
        """
        SELECT
            event_id, season, week, game_date, is_playoff,
            home_team_id, away_team_id, home_score, away_score, spread_line, total_line
        FROM dim_event
        WHERE season >= 2010
        """
    ).df()
    events = events[events["week"].notna()].copy()
    events["week"] = events["week"].astype(int)
    events["game_date"] = pd.to_datetime(events["game_date"])
    events["total_points"] = events["home_score"] + events["away_score"]
    events["home_margin"] = events["home_score"] - events["away_score"]
    events = events.sort_values(["season", "week", "game_date", "event_id"]).reset_index(drop=True)

    week_keys = (
        events[["season", "week"]]
        .drop_duplicates()
        .sort_values(["season", "week"])
        .itertuples(index=False, name=None)
    )
    week_keys = list(week_keys)
    events_by_week = {
        wk: events[(events["season"] == wk[0]) & (events["week"] == wk[1])].reset_index(drop=True)
        for wk in week_keys
    }

    bet_rows = []
    leg_rows = []
    cost_rows = []
    bet_id = 1
    leg_id = 1

    base_weekly_churn = churn_cfg["base_weekly_churn"]
    alpha = churn_cfg["alpha"]
    churn_plateau_mult = churn_cfg["churn_plateau_mult"]
    early_churn_mult = churn_cfg["early_churn_mult"]
    mid_churn_mult = churn_cfg["mid_churn_mult"]
    tail_churn_mult = churn_cfg["tail_churn_mult"]
    inactivity_mult = churn_cfg["inactivity_mult"]
    negative_net_mult = churn_cfg["negative_net_mult"]
    promo_hunter_post_promo_mult = churn_cfg["promo_hunter_post_promo_mult"]
    promo_hunter_post_promo_weeks = set(churn_cfg["promo_hunter_post_promo_weeks"])
    min_churn = churn_cfg["min_churn"]
    max_churn = churn_cfg["max_churn"]
    promo_eligibility = {
        "sharp": 0.45,
        "regular": 0.72,
        "casual": 0.68,
        "promo_hunter": 0.40,
    }
    promo_base_rate = {
        "sharp": 0.050,
        "regular": 0.095,
        "casual": 0.080,
        "promo_hunter": 0.080,
    }
    promo_cost_scale = {
        "sharp": 0.90,
        "regular": 1.00,
        "casual": 0.95,
        "promo_hunter": 0.70,
    }

    for _, c in customers.iterrows():
        if not week_keys:
            continue

        type_choices = ["moneyline", "spread", "total", "parlay"]
        type_weights = [
            c["pref_moneyline"],
            c["pref_spread"],
            c["pref_total"],
            c["pref_parlay"],
        ]
        segment = str(c["segment"])
        start_idx = int(rng.integers(0, max(1, len(week_keys) - 24)))
        weeks_active = 0
        last_bet_week_idx: int | None = None
        has_churned = False
        promo_eligible = rng.random() < promo_eligibility.get(segment, 0.50)
        last_promo_week_idx: int | None = None
        promo_week_history: list[int] = []
        negative_net_streak = 0
        promo_effect_weeks_remaining = 0

        for wk_idx in range(start_idx, len(week_keys)):
            if has_churned:
                break

            if weeks_active > 0:
                tenure_mult = churn_plateau_mult + (1.0 - churn_plateau_mult) * math.exp(-alpha * weeks_active)
                churn_prob = base_weekly_churn.get(segment, 0.06) * tenure_mult
                if weeks_active <= 1:
                    churn_prob *= early_churn_mult.get(segment, 1.8)
                elif 2 <= weeks_active <= 6:
                    churn_prob *= mid_churn_mult.get(segment, 1.0)
                elif 7 <= weeks_active <= 12:
                    churn_prob *= tail_churn_mult.get(segment, 1.0)
                if last_bet_week_idx is not None and (wk_idx - last_bet_week_idx) >= 2:
                    churn_prob *= inactivity_mult
                if negative_net_streak >= 2:
                    churn_prob *= negative_net_mult
                if (
                    segment == "promo_hunter"
                    and last_promo_week_idx is not None
                    and (wk_idx - last_promo_week_idx) in promo_hunter_post_promo_weeks
                ):
                    churn_prob *= promo_hunter_post_promo_mult
                # Promo campaigns are modeled as short-term engagement support.
                if promo_effect_weeks_remaining > 0:
                    churn_prob *= 0.93 if segment != "promo_hunter" else 0.97
                churn_prob = float(np.clip(churn_prob, min_churn, max_churn))
                if rng.random() < churn_prob:
                    has_churned = True
                    continue

            week_key = week_keys[wk_idx]
            week_events = events_by_week.get(week_key)
            if week_events is None or week_events.empty:
                weeks_active += 1
                continue

            week = int(week_key[1])
            intensity = 0.75 if week >= 19 else 1.0
            weekly_lambda = max(0.01, c["freq"] * intensity)
            if promo_effect_weeks_remaining > 0:
                weekly_lambda *= 1.09 if segment != "promo_hunter" else 1.04
            if weeks_active == 0:
                n_bets_week = max(1, int(rng.poisson(max(0.20, weekly_lambda))))
            else:
                n_bets_week = int(rng.poisson(weekly_lambda))
            if n_bets_week <= 0:
                if last_bet_week_idx is not None and (wk_idx - last_bet_week_idx) >= 2:
                    negative_net_streak = min(negative_net_streak + 1, 6)
                weeks_active += 1
                continue

            week_net_profit = 0.0
            promo_given_this_week = False
            for _ in range(n_bets_week):
                bet_type = random.choices(type_choices, weights=type_weights, k=1)[0]
                stake = float(np.clip(rng.lognormal(mean=math.log(c["avg_stake"]), sigma=0.55), 5, 2000))

                bet_time_event = week_events.iloc[int(rng.integers(0, len(week_events)))]
                event_id_base = int(bet_time_event["event_id"])
                bet_date = pd.Timestamp(bet_time_event["game_date"]) - pd.Timedelta(
                    hours=float(rng.uniform(1, 96))
                )

                if bet_type == "parlay":
                    n_legs = int(rng.integers(2, 5))
                    leg_sample_size = min(n_legs, len(week_events))
                    leg_events = week_events.sample(
                        n=leg_sample_size,
                        replace=False,
                        random_state=int(rng.integers(1, 10_000_000)),
                    )
                else:
                    leg_events = week_events[week_events["event_id"] == event_id_base]

                leg_results = []
                for leg_no, (_, ev) in enumerate(leg_events.iterrows(), start=1):
                    market_type = bet_type if bet_type != "parlay" else random.choices(
                        ["moneyline", "spread", "total"], weights=[0.25, 0.45, 0.30], k=1
                    )[0]
                    home_margin = int(ev["home_margin"])
                    total_points = int(ev["total_points"])
                    spread_line = ev["spread_line"]
                    total_line = ev["total_line"]

                    if pd.isna(total_line):
                        total_line = float(np.clip(43 + rng.normal(0, 6), 31, 59))
                    if pd.isna(spread_line):
                        spread_line = float(np.clip(rng.normal(0, 5), -14, 14))

                    if market_type == "spread":
                        favorite_side = "home" if spread_line < 0 else "away"
                        selection = favorite_side if rng.random() < 0.57 else ("away" if favorite_side == "home" else "home")
                        line_shift = float(np.clip(rng.normal(0, 0.55), -1.5, 1.5))
                        if selection == "home":
                            bet_line = spread_line + line_shift
                            margin_vs_line = home_margin + bet_line
                        else:
                            bet_line = -spread_line + line_shift
                            margin_vs_line = -home_margin + bet_line
                        if margin_vs_line > 0:
                            settled = "win"
                        elif margin_vs_line < 0:
                            settled = "lose"
                        else:
                            settled = "push"
                        american_odds = int(np.clip(round(-112 + rng.normal(0, 8)), -145, 116))
                    elif market_type == "total":
                        selection = "over" if rng.random() < 0.56 else "under"
                        bet_line = float(total_line + np.clip(rng.normal(0, 0.7), -1.8, 1.8))
                        delta = total_points - bet_line
                        if selection == "over":
                            settled = "win" if delta > 0 else ("lose" if delta < 0 else "push")
                        else:
                            settled = "win" if delta < 0 else ("lose" if delta > 0 else "push")
                        american_odds = int(np.clip(round(-114 + rng.normal(0, 9)), -155, 120))
                    else:  # moneyline
                        favorite_side = "home" if spread_line < 0 else "away"
                        selection = favorite_side if rng.random() < 0.58 else ("away" if favorite_side == "home" else "home")
                        abs_spread = abs(float(spread_line))
                        fav_odds = int(np.clip(round(-140 - abs_spread * 18 + rng.normal(0, 14)), -395, -113))
                        dog_odds = int(np.clip(round(109 + abs_spread * 19 + rng.normal(0, 15)), 100, 315))
                        american_odds = fav_odds if selection == favorite_side else dog_odds
                        home_won = home_margin > 0
                        if selection == "home":
                            settled = "win" if home_won else "lose"
                        else:
                            settled = "win" if not home_won else "lose"
                        bet_line = None

                    leg_rows.append(
                        {
                            "bet_leg_id": leg_id,
                            "bet_id": bet_id,
                            "leg_no": leg_no,
                            "event_id": int(ev["event_id"]),
                            "market_type": market_type,
                            "selection": selection,
                            "line_value": bet_line,
                            "american_odds": american_odds,
                            "decimal_odds": american_to_decimal(american_odds),
                            "is_win": settled == "win",
                            "is_push": settled == "push",
                            "settled_result": settled,
                        }
                    )
                    leg_results.append((settled, american_to_decimal(american_odds)))
                    leg_id += 1

                if bet_type == "parlay":
                    has_loss = any(r == "lose" for r, _ in leg_results)
                    if has_loss:
                        payout = 0.0
                        status = "lose"
                    else:
                        mult = 1.0
                        for r, dec in leg_results:
                            mult *= dec if r == "win" else 1.0
                        payout = stake * mult
                        status = "push" if all(r == "push" for r, _ in leg_results) else "win"
                    avg_odds = float(np.mean([od for _, od in leg_results]))
                else:
                    r, dec = leg_results[0]
                    if r == "win":
                        payout = stake * dec
                        status = "win"
                    elif r == "push":
                        payout = stake
                        status = "push"
                    else:
                        payout = 0.0
                        status = "lose"
                    avg_odds = dec

                gross_profit = stake - payout
                recent_promos_8w = sum(1 for p_wk in promo_week_history if (wk_idx - p_wk) <= 8)
                promo_prob = promo_base_rate.get(segment, 0.08) + 0.05 * float(c["promo_sensitivity"])
                promo_prob *= max(0.25, 1.0 - 0.22 * recent_promos_8w)
                # Late-tenure throttling reduces long-tail promo drag and stabilizes ROI curves.
                if weeks_active >= 20:
                    promo_prob *= 0.45
                elif weeks_active >= 8:
                    promo_prob *= 0.70
                if negative_net_streak >= 2:
                    promo_prob *= 0.75
                promo_prob = float(np.clip(promo_prob, 0.010, 0.45))
                promo_trigger = promo_eligible and (not promo_given_this_week) and (rng.random() < promo_prob)
                if promo_trigger and segment == "promo_hunter" and weeks_active >= 10:
                    promo_trigger = rng.random() < 0.45
                tenure_cost_mult = 1.00 if weeks_active < 8 else (0.45 if weeks_active < 20 else 0.30)
                promo_cost = (
                    stake
                    * float(
                        np.clip(
                            rng.uniform(0.05, 0.18)
                            * c["promo_sensitivity"]
                            * promo_cost_scale.get(segment, 1.0),
                            0.0,
                            0.20,
                        )
                    )
                    * tenure_cost_mult
                    if promo_trigger
                    else 0.0
                )
                # Cap promo cost relative to stake to keep realism under repeated exposure.
                promo_cost = min(promo_cost, stake * (0.14 if weeks_active < 8 else (0.09 if weeks_active < 20 else 0.06)))
                if promo_trigger and segment == "promo_hunter":
                    promo_cost = min(promo_cost, stake * 0.06)
                if promo_trigger:
                    last_promo_week_idx = wk_idx
                    promo_week_history.append(wk_idx)
                    promo_given_this_week = True
                    promo_effect_weeks_remaining = max(
                        promo_effect_weeks_remaining,
                        int(rng.integers(5, 11)) if segment != "promo_hunter" else int(rng.integers(3, 6)),
                    )
                payment_fee = stake * 0.012
                tax = max(0.0, gross_profit) * 0.03
                net_profit = gross_profit - promo_cost - payment_fee - tax
                week_net_profit += net_profit

                bet_rows.append(
                    {
                        "bet_id": bet_id,
                        "customer_id": int(c["customer_id"]),
                        "bet_datetime": bet_date,
                        "bet_type": bet_type,
                        "stake_amount": float(stake),
                        "odds_decimal_avg": float(avg_odds),
                        "gross_payout": float(payout),
                        "status": status,
                    }
                )
                cost_rows.append(
                    {
                        "cost_id": bet_id,
                        "bet_id": bet_id,
                        "promo_cost": float(promo_cost),
                        "payment_fee": float(payment_fee),
                        "tax": float(tax),
                    }
                )
                bet_id += 1
                last_bet_week_idx = wk_idx

            if week_net_profit < 0:
                negative_net_streak = min(negative_net_streak + 1, 8)
            else:
                negative_net_streak = 0
            promo_effect_weeks_remaining = max(0, promo_effect_weeks_remaining - 1)
            weeks_active += 1

    bets_df = pd.DataFrame(bet_rows)
    legs_df = pd.DataFrame(leg_rows)
    costs_df = pd.DataFrame(cost_rows)
    return bets_df, legs_df, costs_df


def compute_still_active_retention(bets: pd.DataFrame, customers: pd.DataFrame) -> dict:
    if bets.empty:
        return {}
    b = bets.copy()
    b["bet_date"] = pd.to_datetime(b["bet_datetime"]).dt.date
    first_bet = b.groupby("customer_id")["bet_date"].min().reset_index(name="anchor_date")
    customer_seg = customers[["customer_id", "segment"]].copy()
    joined = first_bet.merge(customer_seg, on="customer_id", how="left")
    anchor_map = dict(zip(joined["customer_id"], joined["anchor_date"]))
    segment_map = dict(zip(joined["customer_id"], joined["segment"]))
    bets_by_customer = b.groupby("customer_id")["bet_date"].apply(list).to_dict()
    segment_ids: dict[str, list[int]] = {"sharp": [], "regular": [], "casual": [], "promo_hunter": []}
    for c_id, seg in segment_map.items():
        seg_key = str(seg)
        if seg_key in segment_ids:
            segment_ids[seg_key].append(int(c_id))

    def active_in_window(c_id: int, start_day: int, end_day: int) -> int:
        anchor = anchor_map[c_id]
        dates = bets_by_customer.get(c_id, [])
        for d in dates:
            delta = (d - anchor).days
            if start_day < delta <= end_day:
                return 1
        return 0

    out: dict[str, dict[str, float]] = {}
    for seg, ids in segment_ids.items():
        n = len(ids)
        if n == 0:
            continue
        out[seg] = {
            "still_active_7d": float(sum(active_in_window(i, 0, 7) for i in ids) / n),
            "still_active_30d": float(sum(active_in_window(i, 23, 30) for i in ids) / n),
            "still_active_60d": float(sum(active_in_window(i, 53, 60) for i in ids) / n),
            "still_active_90d": float(sum(active_in_window(i, 83, 90) for i in ids) / n),
        }
    return out


def calibrate_churn_config(
    con: duckdb.DuckDBPyConnection,
    customers: pd.DataFrame,
    base_cfg: dict,
    seed: int = 7,
) -> tuple[dict, dict]:
    target = {
        "sharp": {"still_active_7d": 0.72, "still_active_30d": 0.58, "still_active_60d": 0.42, "still_active_90d": 0.28},
        "regular": {"still_active_7d": 0.40, "still_active_30d": 0.20, "still_active_60d": 0.08, "still_active_90d": 0.03},
        "casual": {"still_active_7d": 0.18, "still_active_30d": 0.06, "still_active_60d": 0.01, "still_active_90d": 0.00},
        "promo_hunter": {"still_active_7d": 0.14, "still_active_30d": 0.03, "still_active_60d": 0.00, "still_active_90d": 0.00},
    }
    sample = customers.sample(n=min(1000, len(customers)), random_state=seed).copy()
    grid = list(
        itertools.product(
            [0.95, 1.05],   # base rate scale
            [0.9, 1.1],     # mid multiplier scale
            [0.9, 1.1],     # tail multiplier scale
            [2.3, 2.7],     # inactivity multiplier
        )
    )
    best_score = float("inf")
    best_cfg = json.loads(json.dumps(base_cfg))
    best_ret = {}

    for base_scale, mid_scale, tail_scale, inactivity_mult in grid:
        cfg = json.loads(json.dumps(base_cfg))
        for seg in cfg["base_weekly_churn"]:
            cfg["base_weekly_churn"][seg] *= base_scale
        for seg in cfg["mid_churn_mult"]:
            cfg["mid_churn_mult"][seg] *= mid_scale
        for seg in cfg["tail_churn_mult"]:
            cfg["tail_churn_mult"][seg] *= tail_scale
        cfg["inactivity_mult"] = inactivity_mult
        bets, _, _ = simulate_ledger(con, sample, seed=seed, churn_cfg=cfg)
        ret = compute_still_active_retention(bets, sample)
        score = 0.0
        for seg, targets in target.items():
            for k, v in targets.items():
                sim = ret.get(seg, {}).get(k, 0.0)
                score += (sim - v) ** 2
        if score < best_score:
            best_score = score
            best_cfg = cfg
            best_ret = ret

    diagnostics = {"target": target, "sample_achieved": best_ret, "score": best_score}
    return best_cfg, diagnostics


def build_fact_tables(
    con: duckdb.DuckDBPyConnection,
    customers: pd.DataFrame,
    bets: pd.DataFrame,
    legs: pd.DataFrame,
    costs: pd.DataFrame,
) -> None:
    con.register("tmp_customers", customers)
    con.register("tmp_bets", bets)
    con.register("tmp_legs", legs)
    con.register("tmp_costs", costs)

    con.execute(
        """
        CREATE OR REPLACE TABLE dim_customer AS
        SELECT
            customer_id,
            segment,
            avg_stake AS avg_bet_size,
            freq AS bets_per_week,
            pref_moneyline,
            pref_spread,
            pref_total,
            pref_parlay,
            promo_sensitivity
        FROM tmp_customers;
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE fact_bet AS
        SELECT
            bet_id,
            customer_id,
            CAST(bet_datetime AS TIMESTAMP) AS bet_datetime,
            bet_type,
            stake_amount,
            odds_decimal_avg,
            gross_payout,
            status
        FROM tmp_bets;
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE fact_bet_leg AS
        SELECT
            bet_leg_id,
            bet_id,
            leg_no,
            event_id,
            market_type,
            selection,
            line_value,
            american_odds,
            decimal_odds,
            is_win,
            is_push,
            settled_result
        FROM tmp_legs;
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE fact_bet_cost AS
        SELECT
            cost_id,
            bet_id,
            promo_cost,
            payment_fee,
            tax,
            promo_cost + payment_fee + tax AS total_cost
        FROM tmp_costs;
        """
    )


def sql_to_df(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return con.execute(sql).df()


def plot_line(df: pd.DataFrame, x: str, ys: list[str], title: str, out_file: Path) -> None:
    plt.figure(figsize=(10, 5))
    for y in ys:
        plt.plot(df[x], df[y], marker="o", linewidth=2, label=y.replace("_", " ").title())
    plt.title(title)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_file, dpi=130)
    plt.close()


def plot_bar(df: pd.DataFrame, x: str, y: str, title: str, out_file: Path) -> None:
    plt.figure(figsize=(10, 5))
    plt.bar(df[x].astype(str), df[y])
    plt.title(title)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(out_file, dpi=130)
    plt.close()


def run_analyses(con: duckdb.DuckDBPyConnection) -> dict:
    queries = {}

    queries["q1_revenue_profit_over_time"] = """
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
    """
    queries["q2_profit_by_bet_type"] = """
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
    """
    queries["q3_profit_by_week_playoff"] = """
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
    """
    queries["q4_profit_by_team"] = """
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
    """
    queries["q5_promo_profitability"] = """
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
    """
    queries["q6_customer_group_differences"] = """
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
    """

    outputs = {}
    for k, q in queries.items():
        outputs[k] = sql_to_df(con, q)

    # Charts
    plot_line(
        outputs["q1_revenue_profit_over_time"],
        "month",
        ["handle", "gross_profit", "net_profit"],
        "Handle, Gross Profit, Net Profit Over Time",
        OUT_DIR / "q1_revenue_profit_over_time.png",
    )
    plot_bar(
        outputs["q2_profit_by_bet_type"],
        "bet_type",
        "net_profit",
        "Net Profit by Bet Type",
        OUT_DIR / "q2_profit_by_bet_type.png",
    )
    q3_plot = outputs["q3_profit_by_week_playoff"].copy()
    q3_plot["week_group"] = q3_plot.apply(
        lambda r: f"W{int(r['week'])}-{'PO' if r['is_playoff'] else 'REG'}", axis=1
    )
    plot_bar(
        q3_plot.sort_values(["week", "is_playoff"]),
        "week_group",
        "net_profit",
        "Net Profit by Week and Playoff",
        OUT_DIR / "q3_profit_by_week_playoff.png",
    )
    q4_top = outputs["q4_profit_by_team"].head(15)
    plot_bar(
        q4_top,
        "team_name",
        "net_profit",
        "Top Teams by Net Profit Association",
        OUT_DIR / "q4_profit_by_team.png",
    )
    plot_bar(
        outputs["q5_promo_profitability"],
        "promo_flag",
        "net_profit",
        "Net Profit: Promo vs No Promo Bets",
        OUT_DIR / "q5_promo_profitability.png",
    )
    plot_bar(
        outputs["q6_customer_group_differences"],
        "segment",
        "net_profit",
        "Net Profit by Customer Segment",
        OUT_DIR / "q6_customer_group_differences.png",
    )

    return {"queries": queries, "results": outputs}


def write_report(profile: dict, analysis: dict) -> None:
    q = analysis["queries"]
    r = analysis["results"]

    top_line = r["q1_revenue_profit_over_time"].agg(
        {"handle": "sum", "gross_profit": "sum", "net_profit": "sum"}
    )
    hold = float(top_line["gross_profit"] / top_line["handle"]) if top_line["handle"] else 0.0
    margin = float(top_line["net_profit"] / top_line["handle"]) if top_line["handle"] else 0.0

    lines = []
    lines.append("# NFL Sportsbook Analytics Report")
    lines.append("")
    lines.append("## Step 1 - Data Inspection")
    lines.append("")
    for table_key in ["raw_scores", "raw_teams"]:
        p = profile[table_key]
        lines.append(f"### {table_key}")
        lines.append(f"- Row count: {p['row_count']}")
        lines.append("- Columns and data types:")
        for c in p["columns"]:
            lines.append(f"  - `{c['name']}` ({c['type']})")
        lines.append(f"- Min/Max summary: `{json.dumps(p['min_max'])}`")
        lines.append(
            f"- Potential PKs: single={p['primary_key_candidates']['single_column']}, "
            f"composite={p['primary_key_candidates']['composite']}"
        )
        lines.append("- Null percentage by column:")
        for cn, npct in p["null_pct"].items():
            lines.append(f"  - `{cn}`: {fmt_pct(npct)}")
        lines.append("")

    lines.append("## Step 2 - Analytic Tables Built")
    lines.append("")
    lines.append("Created and populated tables:")
    lines.append("- `dim_team` from `nfl_teams.csv`")
    lines.append("- `dim_event` from `spreadspoke_scores.csv` with cleaned team mapping and playoff/week normalization")
    lines.append("- `dim_customer` (synthetic)")
    lines.append("- `fact_bet`, `fact_bet_leg`, `fact_bet_cost`")
    lines.append("")
    lines.append("## Step 3 - Synthetic Bet Ledger")
    lines.append("")
    lines.append("- Customer heterogeneity modeled with segment-specific frequency, stake size, market preference, and promo sensitivity")
    lines.append("- Bets include singles and parlays, realistic stake distribution, mild favorite/over bias, and price drift from closing numbers")
    lines.append("- Settlement logic uses real game results from `dim_event`")
    lines.append("- Costs attached per bet: promo, payment fee, tax")
    lines.append("")
    lines.append("## Step 4 - Core KPI Definitions")
    lines.append("")
    lines.append("- `handle = SUM(stake_amount)`")
    lines.append("- `payout = SUM(gross_payout)`")
    lines.append("- `gross_profit = handle - payout`")
    lines.append("- `net_profit = gross_profit - promo_cost - payment_fee - tax`")
    lines.append("- `hold_pct = gross_profit / handle`")
    lines.append("- `margin_pct = net_profit / handle`")
    lines.append("")
    lines.append(
        f"Portfolio totals: handle={fmt_money(float(top_line['handle']))}, "
        f"gross_profit={fmt_money(float(top_line['gross_profit']))}, "
        f"net_profit={fmt_money(float(top_line['net_profit']))}, "
        f"hold={fmt_pct(hold)}, margin={fmt_pct(margin)}"
    )
    lines.append("")

    sections = [
        (
            "1) Revenue and profit over time",
            "q1_revenue_profit_over_time",
            "q1_revenue_profit_over_time.png",
            "Track monthly trend in handle, gross profit, and net profit.",
            [
                "Set monthly net-profit guardrails by segment and channel.",
                "Flag months where margin compression is driven by promo over-spend.",
                "Use seasonality to pre-allocate risk and promo budgets.",
            ],
        ),
        (
            "2) Bet type contribution to profit",
            "q2_profit_by_bet_type",
            "q2_profit_by_bet_type.png",
            "Compare bet types on net profit and hold efficiency.",
            [
                "Scale acquisition into bet types with strong net-profit density.",
                "Adjust pricing/promos for low-hold bet types before scaling volume.",
                "Use parlay risk limits when payout volatility spikes.",
            ],
        ),
        (
            "3) Profit variation by week and playoffs",
            "q3_profit_by_week_playoff",
            "q3_profit_by_week_playoff.png",
            "Understand where weekly and playoff timing creates upside or risk.",
            [
                "Tighten limits in historically volatile playoff windows.",
                "Shift promos to stronger in-season weeks with stable hold.",
                "Use week-level dashboards for rapid pricing response.",
            ],
        ),
        (
            "4) Team-level profit association",
            "q4_profit_by_team",
            "q4_profit_by_team.png",
            "Identify team exposure where customer behavior impacts sportsbook profit.",
            [
                "Monitor high-handle teams with weak margin for exposure control.",
                "Tailor campaigns to teams with durable net-profit profile.",
                "Set dynamic limits for heavily biased one-sided markets.",
            ],
        ),
        (
            "5) Promotion profitability",
            "q5_promo_profitability",
            "q5_promo_profitability.png",
            "Compare promo-applied bets vs non-promo bets on margin.",
            [
                "Cap promo depth where incremental handle does not convert to net profit.",
                "Target promos to cohorts with positive post-cost margin.",
                "Measure promo ROI by campaign, not only by handle lift.",
            ],
        ),
        (
            "6) Customer group differences",
            "q6_customer_group_differences",
            "q6_customer_group_differences.png",
            "Segment economics by customer type to improve lifetime value and risk.",
            [
                "Increase retention spend on segments with sustained positive net margin.",
                "Reduce broad promos for promo-sensitive cohorts with weak ROI.",
                "Use segment-level risk and pricing policies instead of one-size-fits-all.",
            ],
        ),
    ]

    lines.append("## Step 5 and 6 - Business Questions, SQL, Charts, Explanations, Recommendations")
    lines.append("")
    for title, key, chart, explanation, recs in sections:
        lines.append(f"### {title}")
        lines.append("")
        lines.append("SQL:")
        lines.append("```sql")
        lines.append(q[key].strip())
        lines.append("```")
        lines.append("")
        lines.append(f"Chart: `output/{chart}`")
        lines.append("")
        lines.append(f"Business explanation: {explanation}")
        lines.append("")
        lines.append("Recommendations:")
        for rec in recs:
            lines.append(f"- {rec}")
        lines.append("")
        lines.append("Sample output (top rows):")
        lines.append("")
        lines.append("```")
        lines.append(r[key].head(10).to_string(index=False))
        lines.append("```")
        lines.append("")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="NFL sportsbook analytics pipeline")
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Skip data build/simulation and only regenerate analysis outputs and report from existing DB/profile.",
    )
    parser.add_argument(
        "--skip-calibration",
        action="store_true",
        help="Skip churn calibration and use default churn settings.",
    )
    parser.add_argument(
        "--customer-seed",
        type=int,
        default=42,
        help="Random seed for synthetic customer generation.",
    )
    parser.add_argument(
        "--ledger-seed",
        type=int,
        default=7,
        help="Random seed for ledger simulation.",
    )
    args = parser.parse_args()

    ensure_dirs()
    con = duckdb.connect(str(DB_PATH))

    if args.report_only:
        if PROFILE_PATH.exists():
            profile = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))
        else:
            profile = {
                "raw_scores": profile_table(con, "raw_scores"),
                "raw_teams": profile_table(con, "raw_teams"),
            }
            PROFILE_PATH.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    else:
        build_core_tables(con)

        profile = {
            "raw_scores": profile_table(con, "raw_scores"),
            "raw_teams": profile_table(con, "raw_teams"),
        }
        PROFILE_PATH.write_text(json.dumps(profile, indent=2), encoding="utf-8")

        customers = generate_customers(n_customers=1600, seed=args.customer_seed)
        churn_cfg = json.loads(json.dumps(DEFAULT_CHURN_CONFIG))
        calibration = {"target": {}, "sample_achieved": {}, "score": None}
        if not args.skip_calibration:
            churn_cfg, calibration = calibrate_churn_config(con, customers, churn_cfg, seed=args.ledger_seed)
            CALIBRATION_PATH.write_text(
                json.dumps({"churn_config": churn_cfg, "calibration": calibration}, indent=2),
                encoding="utf-8",
            )
        bets, legs, costs = simulate_ledger(con, customers, seed=args.ledger_seed, churn_cfg=churn_cfg)
        build_fact_tables(con, customers, bets, legs, costs)

    analysis = run_analyses(con)
    write_report(profile, analysis)

    counts = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM dim_team) AS dim_team_rows,
            (SELECT COUNT(*) FROM dim_event) AS dim_event_rows,
            (SELECT COUNT(*) FROM dim_customer) AS dim_customer_rows,
            (SELECT COUNT(*) FROM fact_bet) AS fact_bet_rows,
            (SELECT COUNT(*) FROM fact_bet_leg) AS fact_bet_leg_rows,
            (SELECT COUNT(*) FROM fact_bet_cost) AS fact_bet_cost_rows
        """
    ).fetchdf().iloc[0].to_dict()

    print("Build complete.")
    print(json.dumps(counts, indent=2))
    print(f"Report: {REPORT_PATH}")
    print(f"Profile: {PROFILE_PATH}")
    print(f"DB: {DB_PATH}")


if __name__ == "__main__":
    main()
