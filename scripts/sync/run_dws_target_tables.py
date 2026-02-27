#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base.runtime import (
    get_env_config,
    get_mysql_session,
    list_trade_dates,
    log_run_end,
    log_run_start,
)
from etl.dws.runner import _run_capital_flow, _run_chip_dynamics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh only DWS target tables required by 08:30 completion."
    )
    parser.add_argument("--start-date", type=int, required=True)
    parser.add_argument("--end-date", type=int, required=True)
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    return parser.parse_args()


def _apply_config(config: str | None) -> None:
    if not config:
        return
    config_path = Path(config).expanduser()
    if not config_path.is_absolute():
        cwd_path = (Path.cwd() / config_path).resolve()
        if cwd_path.exists():
            config_path = cwd_path
        else:
            root_path = (scripts_dir.parent / config_path).resolve()
            config_path = root_path if root_path.exists() else cwd_path
    if not config_path.exists():
        raise RuntimeError(f"config file not found: {config_path}")
    os.environ["ETL_CONFIG_PATH"] = str(config_path)


def _ensure_sources(cursor, trade_date: int) -> None:
    checks = [
        ("dwd_daily", "dwd_daily missing"),
        ("ods_moneyflow", "ods_moneyflow missing"),
        ("dwd_chip_stability", "dwd_chip_stability missing"),
    ]
    missing = []
    for table_name, msg in checks:
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE trade_date=%s LIMIT 1", (trade_date,))
        if cursor.fetchone() is None:
            missing.append(msg)
    if missing:
        raise RuntimeError(f"trade_date={trade_date}: {', '.join(missing)}")


def main() -> None:
    args = parse_args()
    _apply_config(args.config)
    if args.start_date > args.end_date:
        raise RuntimeError("start_date cannot be greater than end_date")

    cfg = get_env_config()
    today_int = int(datetime.now().strftime("%Y%m%d"))

    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "dws_0830_targets", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                trade_dates = list_trade_dates(cursor, args.start_date, args.end_date)
            trade_dates = [d for d in trade_dates if d <= today_int]
            if not trade_dates:
                with conn.cursor() as cursor:
                    log_run_end(cursor, run_id, "SUCCESS")
                    conn.commit()
                print("No trade dates to process.")
                return

            for trade_date in trade_dates:
                with conn.cursor() as cursor:
                    _ensure_sources(cursor, trade_date)
                    _run_capital_flow(cursor, trade_date, trade_date)
                    _run_chip_dynamics(cursor, trade_date, trade_date)
                    conn.commit()
                print(f"Refreshed DWS target tables for {trade_date}")

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise


if __name__ == "__main__":
    main()
