"""Generate Excel report with optimization results."""
from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .config import CATEGORY_NAMES, OptimizerConfig

logger = logging.getLogger(__name__)


def generate_report(
    wf_results: Dict,
    oos_nav: pd.DataFrame,
    oos_metrics: Dict,
    benchmark_df: pd.DataFrame,
    config: OptimizerConfig,
    output_file: Optional[str] = None,
) -> str:
    """Generate comprehensive Excel report.

    Args:
        wf_results: walk-forward results dict
        oos_nav: out-of-sample NAV DataFrame
        oos_metrics: out-of-sample metrics dict
        benchmark_df: benchmark daily data
        config: optimizer config
        output_file: output path (default: factor_optimizer_report.xlsx)

    Returns:
        Path to generated file.
    """
    if output_file is None:
        output_file = "factor_optimizer_report.xlsx"

    logger.info(f"Generating report: {output_file}")

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # --- Sheet 1: Weight Summary ---
        _write_weight_summary(writer, wf_results)

        # --- Sheet 2: Fold Details ---
        _write_fold_details(writer, wf_results)

        # --- Sheet 3: OOS Results ---
        _write_oos_results(writer, oos_nav, oos_metrics)

        # --- Sheet 4: Equity Curves ---
        _write_equity_curves(writer, wf_results, oos_nav, benchmark_df, config)

        # --- Sheet 5: Stability Analysis ---
        _write_stability(writer, wf_results)

    abs_path = os.path.abspath(output_file)
    logger.info(f"Report saved: {abs_path}")
    return abs_path


def _write_weight_summary(writer, wf_results: Dict) -> None:
    """Write weight summary sheet."""
    avg_w = wf_results["avg_weights"]
    rounded_w = wf_results["rounded_weights"]
    std_w = wf_results["weight_stability"]

    rows = []
    for i, cat in enumerate(CATEGORY_NAMES):
        rows.append({
            "category": cat,
            "avg_weight": round(avg_w[i], 4),
            "std": round(std_w[i], 4),
            "rounded_weight": round(rounded_w[i], 2),
        })

    # Add per-fold weights
    for fold_r in wf_results["fold_results"]:
        fold_idx = fold_r["fold"]
        for i, cat in enumerate(CATEGORY_NAMES):
            rows[i][f"fold_{fold_idx}"] = round(fold_r["weights"][i], 4)

    df = pd.DataFrame(rows)
    df.to_excel(writer, sheet_name="Weight Summary", index=False)


def _write_fold_details(writer, wf_results: Dict) -> None:
    """Write per-fold performance comparison."""
    rows = []
    for f in wf_results["fold_results"]:
        rows.append({
            "fold": f["fold"],
            "train_period": f["train_period"],
            "test_period": f["test_period"],
            "train_sharpe": round(f["train_sharpe"], 3),
            "test_sharpe": round(f["test_sharpe"], 3),
            "sharpe_decay": round(f["train_sharpe"] - f["test_sharpe"], 3),
            "train_return": f"{f['train_return']*100:.1f}%",
            "test_return": f"{f['test_return']*100:.1f}%",
            "train_max_dd": f"{f['train_max_dd']*100:.1f}%",
            "test_max_dd": f"{f['test_max_dd']*100:.1f}%",
        })

    df = pd.DataFrame(rows)
    df.to_excel(writer, sheet_name="Fold Details", index=False)


def _write_oos_results(writer, oos_nav: pd.DataFrame, oos_metrics: Dict) -> None:
    """Write out-of-sample test results."""
    metrics_rows = [
        {"metric": k, "value": round(v, 4) if isinstance(v, float) else v}
        for k, v in oos_metrics.items()
    ]
    df = pd.DataFrame(metrics_rows)
    df.to_excel(writer, sheet_name="OOS Results", index=False)

    if not oos_nav.empty:
        oos_nav.to_excel(writer, sheet_name="OOS NAV", index=False)


def _write_equity_curves(
    writer, wf_results: Dict, oos_nav: pd.DataFrame,
    benchmark_df: pd.DataFrame, config: OptimizerConfig,
) -> None:
    """Write combined equity curves for comparison."""
    curves = []

    # Combine all test period NAVs
    for f in wf_results["fold_results"]:
        test_nav = f.get("test_nav")
        if test_nav is not None and not test_nav.empty:
            test_nav = test_nav.copy()
            test_nav["fold"] = f"fold_{f['fold']}_test"
            curves.append(test_nav[["trade_date", "nav", "fold"]])

    # Add OOS
    if not oos_nav.empty:
        oos = oos_nav.copy()
        oos["fold"] = "oos"
        curves.append(oos[["trade_date", "nav", "fold"]])

    if curves:
        combined = pd.concat(curves, ignore_index=True)

        # Add benchmark
        if not benchmark_df.empty:
            bm = benchmark_df.copy()
            bm = bm.sort_values("trade_date")
            bm["nav"] = bm["close"] / bm["close"].iloc[0]
            bm["fold"] = "benchmark_csi500"
            combined = pd.concat([combined, bm[["trade_date", "nav", "fold"]]], ignore_index=True)

        combined.to_excel(writer, sheet_name="Equity Curves", index=False)


def _write_stability(writer, wf_results: Dict) -> None:
    """Write weight stability analysis."""
    all_w = wf_results["all_weights"]
    stability_rows = []
    for i, cat in enumerate(CATEGORY_NAMES):
        col = all_w[:, i]
        stability_rows.append({
            "category": cat,
            "min": round(col.min(), 4),
            "max": round(col.max(), 4),
            "std": round(col.std(), 4),
            "cv": round(col.std() / col.mean(), 4) if col.mean() > 0 else 0,
            "stable": "YES" if col.std() < 0.05 else "NO",
        })

    df = pd.DataFrame(stability_rows)
    df.to_excel(writer, sheet_name="Stability Analysis", index=False)
