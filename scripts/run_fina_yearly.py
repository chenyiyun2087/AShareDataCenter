#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from etl.base.runtime import get_env_config, get_mysql_connection, get_tushare_token, get_tushare_limit
from etl.ods import run_fina_incremental


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TuShare fina_indicator by year.")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--token", default=None)
    parser.add_argument("--rate-limit", type=int, default=None)
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from watermark in meta_etl_watermark (ods_fina_indicator).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()
    logging.info(f"Starting Fina Yearly ETL with args: {args}")
    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            config_path = (Path.cwd() / config_path).resolve()
        if not config_path.exists():
            raise RuntimeError(f"config file not found: {config_path}")
        os.environ["ETL_CONFIG_PATH"] = str(config_path)
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")
    if args.end_year < args.start_year:
        raise SystemExit("end-year must be >= start-year")

    rate_limit = args.rate_limit or get_tushare_limit()

    start_year = args.start_year
    if args.resume:
        cfg = get_env_config()
        with get_mysql_connection(cfg) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT water_mark FROM meta_etl_watermark WHERE api_name=%s",
                    ("ods_fina_indicator",),
                )
                row = cursor.fetchone()
        if row and row[0]:
            water_mark = int(row[0])
            start_year = max(start_year, int(str(water_mark)[:4]) + 1)

    for year in range(start_year, args.end_year + 1):
        start_date = int(f"{year}0101")
        end_date = int(f"{year}1231")
        print(f"Running fina_indicator for {year}: {start_date} -> {end_date}")
        run_fina_incremental(token, start_date, end_date, rate_limit)

    logging.info("Fina Yearly ETL completed successfully")


if __name__ == "__main__":
    main()
