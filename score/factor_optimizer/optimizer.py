"""Walk-forward weight optimization.

Optimizes inter-category weights using expanding window walk-forward
to minimize overfitting. Uses scipy.optimize for constrained optimization.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.optimize import minimize, OptimizeResult

from .config import OptimizerConfig, CATEGORY_NAMES
from .backtest import run_backtest, run_backtest_with_turnover_penalty

logger = logging.getLogger(__name__)


def _objective(
    weights: np.ndarray,
    scores_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    config: OptimizerConfig,
    start_date: int,
    end_date: int,
) -> float:
    """Objective function: negative penalized Sharpe (we minimize)."""
    penalized_sharpe = run_backtest_with_turnover_penalty(
        scores_df, returns_df, weights, config, start_date, end_date,
    )
    return -penalized_sharpe  # scipy minimizes


def optimize_weights(
    scores_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    config: OptimizerConfig,
    train_start: int,
    train_end: int,
) -> Tuple[np.ndarray, float]:
    """Find optimal category weights on training period.

    Returns:
        (optimal_weights, best_sharpe)
    """
    n = len(CATEGORY_NAMES)

    # Start with equal weights
    w0 = np.ones(n) / n

    # Constraints: weights sum to 1
    constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]

    # Bounds: each weight in [weight_min, weight_max]
    bounds = [(config.weight_min, config.weight_max)] * n

    logger.info(f"Optimizing weights on {train_start} -> {train_end}")

    result: OptimizeResult = minimize(
        _objective,
        w0,
        args=(scores_df, returns_df, config, train_start, train_end),
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={"maxiter": 200, "ftol": 1e-6, "disp": False},
    )

    if result.success:
        optimal_weights = result.x
        best_sharpe = -result.fun
        logger.info(f"Optimization converged: Sharpe={best_sharpe:.4f}")
    else:
        logger.warning(f"Optimization did not converge: {result.message}")
        optimal_weights = w0
        best_sharpe = 0.0

    # Log weights
    for i, cat in enumerate(CATEGORY_NAMES):
        logger.info(f"  {cat}: {optimal_weights[i]:.4f}")

    return optimal_weights, best_sharpe


def round_weights(weights: np.ndarray, step: float = 0.05) -> np.ndarray:
    """Round weights to nearest step and re-normalize to sum to 1."""
    rounded = np.round(weights / step) * step
    rounded = np.clip(rounded, step, 1.0)  # Ensure min weight
    rounded /= rounded.sum()  # Re-normalize
    return rounded


def walk_forward_optimize(
    scores_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    config: OptimizerConfig,
) -> Dict:
    """Run walk-forward optimization across all folds.

    Returns dict with:
        - fold_results: list of per-fold dictionaries
        - avg_weights: average weights across folds
        - rounded_weights: average weights rounded to step
        - weight_stability: std of weights across folds
    """
    folds = config.get_default_folds()
    train_start = config.backtest_start

    fold_results = []

    for fold_idx, (train_end, test_start, test_end) in enumerate(folds):
        logger.info(f"\n{'='*60}")
        logger.info(f"Fold {fold_idx + 1}/{len(folds)}")
        logger.info(f"  Train: {train_start} -> {train_end}")
        logger.info(f"  Test:  {test_start} -> {test_end}")
        logger.info(f"{'='*60}")

        # 1. Optimize on train
        opt_weights, train_sharpe = optimize_weights(
            scores_df, returns_df, config, train_start, train_end,
        )

        # 2. Run backtest on train with optimal weights
        train_nav, train_metrics = run_backtest(
            scores_df, returns_df, opt_weights, config, train_start, train_end,
        )

        # 3. Test on out-of-sample period
        test_nav, test_metrics = run_backtest(
            scores_df, returns_df, opt_weights, config, test_start, test_end,
        )

        fold_result = {
            "fold": fold_idx + 1,
            "train_period": f"{train_start}-{train_end}",
            "test_period": f"{test_start}-{test_end}",
            "weights": opt_weights.copy(),
            "train_sharpe": train_metrics.get("sharpe", 0),
            "train_return": train_metrics.get("total_return", 0),
            "train_max_dd": train_metrics.get("max_drawdown", 0),
            "test_sharpe": test_metrics.get("sharpe", 0),
            "test_return": test_metrics.get("total_return", 0),
            "test_max_dd": test_metrics.get("max_drawdown", 0),
            "train_nav": train_nav,
            "test_nav": test_nav,
            "train_metrics": train_metrics,
            "test_metrics": test_metrics,
        }
        fold_results.append(fold_result)

        # Log fold summary
        logger.info(f"  Train: Sharpe={train_metrics.get('sharpe', 0):.3f}, "
                     f"Return={train_metrics.get('total_return', 0)*100:.1f}%")
        logger.info(f"  Test:  Sharpe={test_metrics.get('sharpe', 0):.3f}, "
                     f"Return={test_metrics.get('total_return', 0)*100:.1f}%")

    # Aggregate results
    all_weights = np.array([f["weights"] for f in fold_results])
    avg_weights = all_weights.mean(axis=0)
    weight_std = all_weights.std(axis=0)

    # Round
    rounded_weights = round_weights(avg_weights, config.weight_round_step)

    logger.info(f"\n{'='*60}")
    logger.info("Walk-Forward Summary")
    logger.info(f"{'='*60}")
    for i, cat in enumerate(CATEGORY_NAMES):
        logger.info(f"  {cat:12s}: avg={avg_weights[i]:.3f} +/- {weight_std[i]:.3f} "
                     f"-> rounded={rounded_weights[i]:.2f}")

    return {
        "fold_results": fold_results,
        "avg_weights": avg_weights,
        "rounded_weights": rounded_weights,
        "weight_stability": weight_std,
        "all_weights": all_weights,
    }


def run_oos_test(
    scores_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    weights: np.ndarray,
    config: OptimizerConfig,
) -> Tuple[pd.DataFrame, Dict]:
    """Run final out-of-sample test with fixed weights.

    This uses the OOS period that was NEVER seen during optimization.
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"Out-of-Sample Test: {config.oos_start} -> {config.oos_end}")
    logger.info(f"Using rounded weights: {dict(zip(CATEGORY_NAMES, weights))}")
    logger.info(f"{'='*60}")

    nav, metrics = run_backtest(
        scores_df, returns_df, weights, config, config.oos_start, config.oos_end,
    )

    if metrics:
        logger.info(f"  OOS Sharpe:  {metrics.get('sharpe', 0):.3f}")
        logger.info(f"  OOS Return:  {metrics.get('total_return', 0)*100:.1f}%")
        logger.info(f"  OOS MaxDD:   {metrics.get('max_drawdown', 0)*100:.1f}%")

    return nav, metrics
