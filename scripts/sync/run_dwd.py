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

from etl.dwd import run_fina_incremental, run_full, run_incremental


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DWD layer ETL")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", type=int, default=20100101)
    parser.add_argument("--end-date", type=int)
    parser.add_argument("--fina-start", type=int)

    parser.add_argument("--fina-end", type=int)
    parser.add_argument(
        "--only-fina",
        action="store_true",
        help="Only run fina incremental task (skip daily dwd tasks).",
    )
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
    logging.info(f"Starting DWD ETL with args: {args}")
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
    if args.only_fina:
        if not (args.fina_start and args.fina_end):
            raise RuntimeError("--only-fina requires --fina-start and --fina-end")
        run_fina_incremental(args.fina_start, args.fina_end)
        return

    if args.mode == "full":
        run_full(args.start_date, args.end_date)
    else:
        run_incremental(args.start_date if "start_date" in args and args.start_date != 20100101 else None, args.end_date)


    if args.fina_start and args.fina_end:
        run_fina_incremental(args.fina_start, args.fina_end)

    logging.info("DWD ETL completed successfully")


if __name__ == "__main__":
    main()
