"""Performance metrics for backtest evaluation."""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Dict


def calc_sharpe(returns: pd.Series, rf: float = 0.0, ann_factor: int = 252) -> float:
    """Annualized Sharpe ratio."""
    if returns.empty or returns.std() == 0:
        return 0.0
    excess = returns - rf / ann_factor
    return float(np.sqrt(ann_factor) * excess.mean() / excess.std())


def calc_sortino(returns: pd.Series, rf: float = 0.0, ann_factor: int = 252) -> float:
    """Annualized Sortino ratio (downside deviation)."""
    if returns.empty:
        return 0.0
    excess = returns - rf / ann_factor
    downside = excess[excess < 0]
    if downside.empty or downside.std() == 0:
        return float("inf") if excess.mean() > 0 else 0.0
    return float(np.sqrt(ann_factor) * excess.mean() / downside.std())


def calc_max_drawdown(nav: pd.Series) -> float:
    """Maximum drawdown from NAV series (returns negative value)."""
    if nav.empty:
        return 0.0
    peak = nav.cummax()
    dd = (nav - peak) / peak
    return float(dd.min())


def calc_calmar(returns: pd.Series, nav: pd.Series, ann_factor: int = 252) -> float:
    """Calmar ratio = annualized return / |max drawdown|."""
    if nav.empty or len(nav) < 2:
        return 0.0
    mdd = calc_max_drawdown(nav)
    if mdd == 0:
        return 0.0
    total_days = len(returns)
    ann_ret = (nav.iloc[-1] / nav.iloc[0]) ** (ann_factor / total_days) - 1
    return float(ann_ret / abs(mdd))


def calc_annual_return(nav: pd.Series, ann_factor: int = 252) -> float:
    """Annualized return from NAV series."""
    if nav.empty or len(nav) < 2:
        return 0.0
    total_days = len(nav)
    return float((nav.iloc[-1] / nav.iloc[0]) ** (ann_factor / total_days) - 1)


def calc_win_rate(returns: pd.Series) -> float:
    """Percentage of positive return days."""
    if returns.empty:
        return 0.0
    return float((returns > 0).sum() / len(returns))


def calc_turnover(old_portfolio: set, new_portfolio: set) -> float:
    """Single-side turnover ratio."""
    if not old_portfolio and not new_portfolio:
        return 0.0
    union = old_portfolio | new_portfolio
    if not union:
        return 0.0
    changed = len(old_portfolio.symmetric_difference(new_portfolio))
    return changed / (2 * max(len(old_portfolio), len(new_portfolio), 1))


def calc_all_metrics(returns: pd.Series, nav: pd.Series) -> Dict[str, float]:
    """Calculate all performance metrics."""
    return {
        "annual_return": calc_annual_return(nav),
        "sharpe": calc_sharpe(returns),
        "sortino": calc_sortino(returns),
        "max_drawdown": calc_max_drawdown(nav),
        "calmar": calc_calmar(returns, nav),
        "win_rate": calc_win_rate(returns),
        "total_return": float(nav.iloc[-1] / nav.iloc[0] - 1) if len(nav) > 0 else 0.0,
    }
