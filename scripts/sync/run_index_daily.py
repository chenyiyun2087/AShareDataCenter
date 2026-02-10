#!/usr/bin/env python3
"""
Script to sync index daily data from TuShare.
Usage:
    python scripts/sync/run_index_daily.py --start 20100101
    
Config is read from config/etl.ini by default.
"""
from __future__ import annotations

import argparse
import configparser
import logging
import os
import sys
from pathlib import Path

# Add scripts directory to path for imports
scripts_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(scripts_dir))

from etl.ods.runner import run_index_daily

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch index daily data from TuShare")
    parser.add_argument("--config", default="config/etl.ini", help="Path to etl.ini")
    parser.add_argument("--start", type=int, default=20200101, help="Start date (YYYYMMDD)")
    parser.add_argument("--rate-limit", type=int, default=None, help="API rate limit (ms)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    # Resolve config path relative to project root
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
    
    # Read config file
    if not config_path.exists():
        raise ValueError(f"Config file not found: {config_path}")
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # Get token from config
    token = config.get("tushare", "token", fallback=None)
    if not token:
        raise ValueError(f"Missing [tushare] token in {config_path}")
    
    # Get rate limit from config or command line
    rate_limit = args.rate_limit
    if rate_limit is None:
        rate_limit = config.getint("tushare", "rate_limit", fallback=500)
    
    logging.info(f"Starting index daily sync from {args.start}")
    logging.info(f"Config: {config_path}, Rate limit: {rate_limit}ms")
    run_index_daily(token, args.start, rate_limit)
    logging.info("Index daily sync completed")


if __name__ == "__main__":
    main()
