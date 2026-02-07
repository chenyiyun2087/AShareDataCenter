#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

from etl.base.runtime import get_env_config, get_mysql_connection, get_tushare_token
from etl.ods import run_full


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill ODS tables if empty.")
    parser.add_argument("--start-date", type=int, default=19900101)
    parser.add_argument("--token", default=None)
    parser.add_argument("--rate-limit", type=int, default=500)
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--database", default=None)
    return parser.parse_args()


def _needs_backfill(cursor, table: str) -> bool:
    cursor.execute(f"SELECT MAX(trade_date), COUNT(*) FROM {table}")
    max_date, total_rows = cursor.fetchone()
    return not max_date or total_rows == 0


def main() -> None:
    args = parse_args()
    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            config_path = (Path.cwd() / config_path).resolve()
        if not config_path.exists():
            raise RuntimeError(f"config file not found: {config_path}")
        os.environ["ETL_CONFIG_PATH"] = str(config_path)
    if args.host:
        os.environ["MYSQL_HOST"] = args.host
    if args.port is not None:
        os.environ["MYSQL_PORT"] = str(args.port)
    if args.user:
        os.environ["MYSQL_USER"] = args.user
    if args.password:
        os.environ["MYSQL_PASSWORD"] = args.password
    if args.database:
        os.environ["MYSQL_DB"] = args.database
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")

    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM dim_trade_cal")
            cal_count = cursor.fetchone()[0]
            if cal_count == 0:
                raise RuntimeError("dim_trade_cal is empty; load trade calendar before backfill.")

            tables = ["ods_daily", "ods_daily_basic", "ods_adj_factor"]
            empty_tables = [table for table in tables if _needs_backfill(cursor, table)]

    if not empty_tables:
        print("ODS tables already populated; no backfill needed.")
        return

    print(f"Empty tables detected: {', '.join(empty_tables)}")
    print(f"Running full ODS backfill from {args.start_date}...")
    run_full(token, args.start_date, args.rate_limit)


if __name__ == "__main__":
    main()
