#!/usr/bin/env python3
import argparse
import logging
import os
import sys
from pathlib import Path

# Add project scripts directory to sys.path to allow importing 'etl' package
scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.ods import run_fina_incremental, run_full, run_incremental, run_dividend_incremental
from etl.base.runtime import get_tushare_token, get_tushare_limit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ODS layer ETL")
    parser.add_argument("--token", default=None)
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", type=int, default=20100101)
    parser.add_argument("--end-date", type=int)
    parser.add_argument("--fina-start", type=int)
    parser.add_argument("--fina-end", type=int)
    parser.add_argument("--dividend", action="store_true", help="Sync dividend data")
    parser.add_argument(
        "--only-dividend",
        action="store_true",
        help="Run dividend sync only (skip base incremental/full load). Requires --dividend.",
    )
    parser.add_argument(
        "--only-fina",
        action="store_true",
        help="Run fina sync only (skip base incremental/full load). Requires --fina-start/--fina-end.",
    )
    parser.add_argument("--limit", type=int, help="Limit number of stocks (for testing)")
    parser.add_argument("--rate-limit", type=int, default=None)
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--database", default=None)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()
    logging.info(f"Starting ODS ETL with args: {args}")
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
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")

    rate_limit = args.rate_limit or get_tushare_limit()

    if args.only_dividend and not args.dividend:
        raise RuntimeError("--only-dividend requires --dividend")
    if args.only_dividend and (args.fina_start or args.fina_end):
        raise RuntimeError("--only-dividend cannot be combined with --fina-start/--fina-end")
    if args.only_fina and not (args.fina_start and args.fina_end):
        raise RuntimeError("--only-fina requires --fina-start and --fina-end")
    if args.only_fina and args.dividend:
        raise RuntimeError("--only-fina cannot be combined with --dividend")
    if args.only_dividend and args.only_fina:
        raise RuntimeError("--only-dividend cannot be combined with --only-fina")

    if not args.only_dividend and not args.only_fina:
        if args.mode == "full":
            run_full(token, args.start_date, args.end_date, rate_limit)
        else:
            run_incremental(
                token,
                args.start_date if args.start_date != 20100101 else None,
                args.end_date,
                rate_limit,
            )

    if args.fina_start and args.fina_end:
        run_fina_incremental(token, args.fina_start, args.fina_end, rate_limit, args.limit)

    if args.dividend:
        run_dividend_incremental(token, rate_limit, args.limit)

    logging.info("ODS ETL completed successfully")


if __name__ == "__main__":
    main()
