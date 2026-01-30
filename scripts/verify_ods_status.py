#!/usr/bin/env python3
from __future__ import annotations

import argparse

from etl.base.runtime import get_env_config, get_mysql_connection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify ODS backfill status.")
    parser.add_argument("--min-date", type=int, default=20250829)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
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
