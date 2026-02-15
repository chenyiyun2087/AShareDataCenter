"""CLI entry point for factor optimizer.

Usage:
    python -m score.factor_optimizer.run_optimizer
    python -m score.factor_optimizer.run_optimizer --start 20220101 --end 20251231
    python -m score.factor_optimizer.run_optimizer --top-n 5 --holding-days 20
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

import numpy as np

from .config import OptimizerConfig, CATEGORY_NAMES
from .data_loader import load_all_data
from .optimizer import walk_forward_optimize, run_oos_test
from .backtest import run_backtest
from .report import generate_report


def main():
    parser = argparse.ArgumentParser(description="Factor Combination Optimizer")
    parser.add_argument("--start", type=int, default=20220101, help="Backtest start date")
    parser.add_argument("--end", type=int, default=20251231, help="Backtest end date")
    parser.add_argument("--top-n", type=int, default=5, help="Number of stocks to hold")
    parser.add_argument("--holding-days", type=int, default=20, help="Rebalance period in days")
    parser.add_argument("--output", type=str, default=None, help="Output report file")
    parser.add_argument("--dry-run", action="store_true", help="Quick test with limited date range")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.dry_run:
        logging.info("=== DRY RUN MODE ===")
        args.start = 20250101
        args.end = 20250228

    config = OptimizerConfig(
        backtest_start=args.start,
        backtest_end=args.end,
        top_n=args.top_n,
        holding_days=args.holding_days,
    )

    if args.dry_run:
        # Single fold for dry run
        config.folds = [(20250131, 20250201, 20250228)]
        config.oos_start = 20250201
        config.oos_end = 20250228

    output_file = args.output or f"factor_optimizer_report_{args.start}_{args.end}.xlsx"

    logging.info("=" * 60)
    logging.info("Factor Combination Optimizer")
    logging.info("=" * 60)
    logging.info(f"Period:       {config.backtest_start} -> {config.backtest_end}")
    logging.info(f"Top N:        {config.top_n}")
    logging.info(f"Holding Days: {config.holding_days}")
    logging.info(f"Benchmark:    {config.benchmark_code}")
    logging.info(f"Categories:   {CATEGORY_NAMES}")
    logging.info("=" * 60)

    t0 = time.time()

    # Step 1: Load data
    logging.info("\n[Step 1/4] Loading data...")
    scores_df, returns_df, benchmark_df = load_all_data(config)

    if scores_df.empty:
        logging.error("No factor scores found. Run DWS scoring first.")
        sys.exit(1)

    if returns_df.empty:
        logging.error("No daily return data found.")
        sys.exit(1)

    logging.info(f"Data loaded in {time.time()-t0:.1f}s")

    # Step 2: Walk-forward optimize
    logging.info("\n[Step 2/4] Walk-forward optimization...")
    t1 = time.time()
    wf_results = walk_forward_optimize(scores_df, returns_df, config)
    logging.info(f"Optimization completed in {time.time()-t1:.1f}s")

    # Step 3: OOS test
    logging.info("\n[Step 3/4] Out-of-sample test...")
    rounded_weights = wf_results["rounded_weights"]
    oos_nav, oos_metrics = run_oos_test(scores_df, returns_df, rounded_weights, config)

    # Step 4: Generate report
    logging.info("\n[Step 4/4] Generating report...")
    report_path = generate_report(
        wf_results, oos_nav, oos_metrics, benchmark_df, config, output_file,
    )

    # Final summary
    elapsed = time.time() - t0
    logging.info("\n" + "=" * 60)
    logging.info("FINAL RESULTS")
    logging.info("=" * 60)
    logging.info(f"Optimal weights (rounded to {config.weight_round_step*100:.0f}%):")
    for i, cat in enumerate(CATEGORY_NAMES):
        logging.info(f"  {cat:12s}: {rounded_weights[i]*100:.0f}%")
    logging.info(f"\nOOS Sharpe:    {oos_metrics.get('sharpe', 0):.3f}")
    logging.info(f"OOS Return:    {oos_metrics.get('total_return', 0)*100:.1f}%")
    logging.info(f"OOS Max DD:    {oos_metrics.get('max_drawdown', 0)*100:.1f}%")
    logging.info(f"\nReport:        {report_path}")
    logging.info(f"Total time:    {elapsed:.1f}s")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
