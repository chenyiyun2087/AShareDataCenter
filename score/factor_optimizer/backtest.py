"""Portfolio backtest engine.

Implements a generalized backtest that accepts arbitrary category weights.
Core logic reused from advanced_analysis.py with enhancements for:
- Arbitrary weight vectors
- Transaction cost modeling
- Top-N concentrated portfolio (default Top 5)
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from .config import OptimizerConfig, CATEGORY_NAMES
from .metrics import calc_turnover

logger = logging.getLogger(__name__)


def compute_weighted_score(
    scores_df: pd.DataFrame,
    weights: np.ndarray,
) -> pd.DataFrame:
    """Apply category weights to compute total weighted score.

    Args:
        scores_df: DataFrame with columns trade_date, ts_code, + CATEGORY_NAMES
        weights: array of 7 weights (summing to 1)

    Returns:
        scores_df with added 'total_score' column.
    """
    df = scores_df.copy()
    # Normalize each category to [0, 1] range per day before weighting
    for i, cat in enumerate(CATEGORY_NAMES):
        if cat in df.columns:
            grouped = df.groupby("trade_date")[cat]
            cat_min = grouped.transform("min")
            cat_max = grouped.transform("max")
            cat_range = cat_max - cat_min
            # Avoid division by zero
            df[f"{cat}_norm"] = np.where(cat_range > 0, (df[cat] - cat_min) / cat_range, 0.5)
        else:
            df[f"{cat}_norm"] = 0.0

    # Weighted sum
    df["total_score"] = sum(
        weights[i] * df[f"{cat}_norm"] for i, cat in enumerate(CATEGORY_NAMES)
    )
    return df


def run_backtest(
    scores_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    weights: np.ndarray,
    config: OptimizerConfig,
    start_date: Optional[int] = None,
    end_date: Optional[int] = None,
) -> Tuple[pd.DataFrame, Dict]:
    """Run a single backtest with given weights.

    Args:
        scores_df: category scores (trade_date, ts_code, 7 categories)
        returns_df: daily returns (trade_date, ts_code, pct_chg, is_limit_up, is_suspended)
        weights: 7-element weight array
        config: optimizer config
        start_date: override start (for fold testing)
        end_date: override end (for fold testing)

    Returns:
        (daily_nav_df, summary_metrics_dict)
    """
    sd = start_date or config.backtest_start
    ed = end_date or config.backtest_end

    # Filter to date range
    scores = scores_df[(scores_df["trade_date"] >= sd) & (scores_df["trade_date"] <= ed)].copy()
    returns = returns_df[(returns_df["trade_date"] >= sd) & (returns_df["trade_date"] <= ed)].copy()

    if scores.empty or returns.empty:
        logger.warning(f"No data for {sd}-{ed}")
        return pd.DataFrame(), {}

    # Compute weighted scores
    scores = compute_weighted_score(scores, weights)

    # Merge
    df = pd.merge(scores[["trade_date", "ts_code", "total_score"]],
                  returns, on=["trade_date", "ts_code"], how="inner")

    trade_dates = sorted(df["trade_date"].unique())
    if len(trade_dates) < 2:
        return pd.DataFrame(), {}

    # Build T-1 scoring dictionary
    date_to_prev_scores = {}
    for i in range(1, len(trade_dates)):
        prev = trade_dates[i - 1]
        curr = trade_dates[i]
        prev_scores = df[df["trade_date"] == prev][["ts_code", "total_score"]].copy()
        date_to_prev_scores[curr] = prev_scores

    current_portfolio: List[str] = []
    portfolio_value = float(config.initial_capital)
    daily_nav = []
    next_rebalance_idx = 1
    total_turnover_events = 0
    total_turnover_sum = 0.0

    for i, date in enumerate(trade_dates):
        day_return = 0.0
        is_rebalance_day = False
        transaction_cost = 0.0

        # --- Rebalance logic (use T-1 scores) ---
        if i == next_rebalance_idx and date in date_to_prev_scores:
            is_rebalance_day = True
            prev_scores = date_to_prev_scores[date]

            today_data = df[df["trade_date"] == date].copy()

            selection = pd.merge(
                prev_scores,
                today_data[["ts_code", "is_limit_up", "is_suspended"]],
                on="ts_code", how="inner",
            )

            # Filter: cannot buy limit-up or suspended stocks
            eligible = selection[
                (selection["is_limit_up"] == 0) & (selection["is_suspended"] == 0)
            ]

            if not eligible.empty:
                top_stocks = eligible.nlargest(config.num_stocks, "total_score")["ts_code"].tolist()

                old_set = set(current_portfolio)
                new_set = set(top_stocks)

                # Turnover tracking
                turnover = calc_turnover(old_set, new_set)
                total_turnover_events += 1
                total_turnover_sum += turnover

                # Sell cost (commission + stamp_tax + slippage)
                sell_stocks = old_set - new_set
                sell_ratio = len(sell_stocks) / max(len(old_set), 1)
                sell_cost = sell_ratio * (config.commission + config.stamp_tax + config.slippage)

                # Buy cost (commission + slippage)
                buy_stocks = new_set - old_set
                buy_ratio = len(buy_stocks) / max(len(new_set), 1)
                buy_cost = buy_ratio * (config.commission + config.slippage)

                transaction_cost = sell_cost + buy_cost
                current_portfolio = top_stocks
                next_rebalance_idx = min(i + config.holding_days, len(trade_dates))

        # --- Return calculation ---
        if current_portfolio:
            port_data = df[
                (df["trade_date"] == date) & (df["ts_code"].isin(current_portfolio))
            ]
            if not port_data.empty:
                if is_rebalance_day:
                    day_return = -transaction_cost
                else:
                    avg_chg = port_data["pct_chg"].mean()
                    day_return = avg_chg / 100.0

        portfolio_value *= (1 + day_return)

        daily_nav.append({
            "trade_date": date,
            "nav": portfolio_value,
            "daily_return": day_return,
            "holdings_count": len(current_portfolio),
            "is_rebalance": is_rebalance_day,
        })

    df_nav = pd.DataFrame(daily_nav)

    summary = {}
    if not df_nav.empty:
        from .metrics import calc_all_metrics
        returns_series = df_nav["daily_return"]
        nav_series = df_nav["nav"]
        summary = calc_all_metrics(returns_series, nav_series)
        summary["avg_turnover"] = total_turnover_sum / max(total_turnover_events, 1)
        summary["rebalance_count"] = total_turnover_events

    return df_nav, summary


def run_backtest_with_turnover_penalty(
    scores_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    weights: np.ndarray,
    config: OptimizerConfig,
    start_date: Optional[int] = None,
    end_date: Optional[int] = None,
) -> float:
    """Run backtest and return penalized Sharpe for optimizer.

    Returns negative Sharpe (for minimization) with turnover penalty.
    """
    df_nav, summary = run_backtest(scores_df, returns_df, weights, config, start_date, end_date)

    if not summary:
        return 0.0  # No data, return neutral

    sharpe = summary.get("sharpe", 0.0)
    avg_turnover = summary.get("avg_turnover", 0.0)

    # Penalize high turnover (especially important for Top-5 concentrated portfolio)
    penalized = sharpe - config.turnover_penalty * avg_turnover * 100

    return penalized
