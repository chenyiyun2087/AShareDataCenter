#!/usr/bin/env python3
import argparse
import logging
import os
from pathlib import Path

from etl.ads import run_full, run_incremental
from etl.base.runtime import ensure_watermark, get_env_config, get_mysql_connection, get_watermark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ADS layer ETL")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", type=int, default=20100101)
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument(
        "--init-watermark",
        action="store_true",
        help="Initialize ads watermark if missing (uses start-date - 1).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()
    logging.info(f"Starting ADS ETL with args: {args}")
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
    if args.init_watermark:
        cfg = get_env_config()
        with get_mysql_connection(cfg) as conn:
            with conn.cursor() as cursor:
                last_date = get_watermark(cursor, "ads")
                if last_date is None:
                    ensure_watermark(cursor, "ads", args.start_date - 1)
                    conn.commit()
    if args.mode == "full":
        run_full(args.start_date)
    else:
        run_incremental()

    logging.info("ADS ETL completed successfully")


if __name__ == "__main__":
    main()
