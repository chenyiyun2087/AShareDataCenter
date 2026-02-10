#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Add project scripts directory to sys.path to allow importing 'etl' package
scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base.runtime import get_env_config, get_mysql_connection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify ODS backfill status.")
    parser.add_argument("--min-date", type=int, default=20250829)
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--database", default=None)
    return parser.parse_args()


def main() -> None:
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
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT api_name, water_mark, status, last_run_at, last_err "
                "FROM meta_etl_watermark WHERE api_name IN "
                "('ods_daily','ods_daily_basic','ods_adj_factor') ORDER BY api_name"
            )
            watermarks = cursor.fetchall()
            print("Watermarks:")
            for row in watermarks:
                print(row)

            tables = ["ods_daily", "ods_daily_basic", "ods_adj_factor"]
            print("\nTable status:")
            for table in tables:
                cursor.execute(f"SELECT MAX(trade_date), COUNT(*) FROM {table}")
                max_date, total_rows = cursor.fetchone()
                status = "OK"
                if not max_date or int(max_date) < args.min_date:
                    status = "NEEDS_BACKFILL"
                print(f"{table}: max_date={max_date}, total_rows={total_rows}, status={status}")


if __name__ == "__main__":
    main()
