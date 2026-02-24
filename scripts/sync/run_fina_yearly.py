#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Add project scripts directory to sys.path to allow importing 'etl' package
scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base.runtime import get_env_config, get_mysql_session, get_tushare_token, get_tushare_limit
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
    parser.add_argument(
        "--skip-balancesheet",
        action="store_true",
        help="Skip balancesheet API calls to improve speed when not backfilling asset/equity fields.",
    )
    parser.add_argument(
        "--yoy-only",
        action="store_true",
        help="Fetch and upsert only or_yoy/netprofit_yoy columns for fast backfill.",
    )
    parser.add_argument(
        "--only-missing-yoy",
        action="store_true",
        help="Process only ts_code with missing YoY rows in the selected date range.",
    )
    parser.add_argument(
        "--commit-every",
        type=int,
        default=1,
        help="Commit interval by ts_code count (default: 1).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1,
        help="Progress log interval by ts_code count (default: 1).",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue with next ts_code when one code fails instead of aborting the year batch.",
    )
    parser.add_argument(
        "--active-ts-only",
        action="store_true",
        help="Only process stocks active within [start_date, end_date] by list_date/delist_date.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()
    logging.info(f"Starting Fina Yearly ETL with args: {args}")
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
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")
    if args.end_year < args.start_year:
        raise SystemExit("end-year must be >= start-year")

    rate_limit = args.rate_limit or get_tushare_limit()

    start_year = args.start_year
    if args.resume:
        cfg = get_env_config()
        with get_mysql_session(cfg) as conn:
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
        run_fina_incremental(
            token,
            start_date,
            end_date,
            rate_limit,
            include_balancesheet=not args.skip_balancesheet,
            yoy_only=args.yoy_only,
            only_missing_yoy=args.only_missing_yoy,
            commit_every=args.commit_every,
            progress_every=args.progress_every,
            continue_on_error=args.continue_on_error,
            active_ts_only=args.active_ts_only,
        )

    logging.info("Fina Yearly ETL completed successfully")


if __name__ == "__main__":
    main()
