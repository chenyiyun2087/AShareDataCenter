#!/usr/bin/env python3
"""One-time backfill script for new DWD/DWS/ADS tables."""

import sys
import os
from pathlib import Path

# Add project scripts directory to sys.path to allow importing 'etl' package
scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base.runtime import get_env_config, get_mysql_connection
from etl.dwd.runner import (
    load_dwd_stock_daily_standard,
    load_dwd_fina_snapshot,
    load_dwd_margin_sentiment,
    load_dwd_chip_stability,
)
from etl.dws.runner import (
    _run_tech_pattern,
    _run_capital_flow,
    _run_leverage_sentiment,
    _run_chip_dynamics,
)
from etl.ads.runner import _run_stock_score

import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill new DWD/DWS/ADS tables")
    parser.add_argument("--config", required=True, help="Path to config file")
    parser.add_argument("--layer", choices=["dwd", "dws", "ads", "all"], default="all", help="Layer to backfill")
    parser.add_argument("--start-date", type=int, default=20250101, help="Start date for backfill")
    parser.add_argument("--end-date", type=int, default=20261231, help="End date for backfill")
    return parser.parse_args()


def backfill_dwd(conn, start_date: int, end_date: int):
    """Backfill new DWD tables by iterating through trade dates."""
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT DISTINCT cal_date FROM dim_trade_cal WHERE exchange='SSE' AND is_open=1 "
            "AND cal_date BETWEEN %s AND %s ORDER BY cal_date",
            (start_date, end_date),
        )
        trade_dates = [row[0] for row in cursor.fetchall()]
    
    total = len(trade_dates)
    logging.info(f"Backfilling DWD: {total} trade dates from {start_date} to {end_date}")
    
    for i, trade_date in enumerate(trade_dates):
        if i % 50 == 0:
            logging.info(f"DWD progress: {i}/{total} ({trade_date})")
        with conn.cursor() as cursor:
            try:
                load_dwd_stock_daily_standard(cursor, trade_date)
                load_dwd_fina_snapshot(cursor, trade_date)
                load_dwd_margin_sentiment(cursor, trade_date)
                load_dwd_chip_stability(cursor, trade_date)
                conn.commit()
            except Exception as e:
                logging.warning(f"DWD error at {trade_date}: {e}")
                conn.rollback()
    
    logging.info("DWD backfill completed")


def backfill_dws(conn):
    """Backfill new DWS tables (bulk insert)."""
    logging.info("Backfilling DWS tables (bulk)...")
    with conn.cursor() as cursor:
        _run_tech_pattern(cursor)
        logging.info("  - dws_tech_pattern done")
        _run_capital_flow(cursor)
        logging.info("  - dws_capital_flow done")
        _run_leverage_sentiment(cursor)
        logging.info("  - dws_leverage_sentiment done")
        _run_chip_dynamics(cursor)
        logging.info("  - dws_chip_dynamics done")
        conn.commit()
    logging.info("DWS backfill completed")


def backfill_ads(conn):
    """Backfill ADS scoring table (bulk insert)."""
    logging.info("Backfilling ADS scoring table...")
    with conn.cursor() as cursor:
        _run_stock_score(cursor)
        conn.commit()
    logging.info("ADS backfill completed")


def main():
    args = parse_args()
    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            # Try relative to CWD first
            cwd_path = (Path.cwd() / config_path).resolve()
            if cwd_path.exists():
                config_path = cwd_path
            else:
                # Fallback to project root
                root_path = (scripts_dir.parent / config_path).resolve()
                if root_path.exists():
                    config_path = root_path
                else:
                    config_path = cwd_path

        if not config_path.exists():
            raise RuntimeError(f"config file not found: {config_path}")
        os.environ["ETL_CONFIG_PATH"] = str(config_path)
    print(f"Using config: {os.environ.get('ETL_CONFIG_PATH')}")
    
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        if args.layer in ("dwd", "all"):
            backfill_dwd(conn, args.start_date, args.end_date)
        if args.layer in ("dws", "all"):
            backfill_dws(conn)
        if args.layer in ("ads", "all"):
            backfill_ads(conn)
    
    logging.info("Backfill complete!")


if __name__ == "__main__":
    main()
