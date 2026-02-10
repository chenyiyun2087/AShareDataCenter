#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path

# Add project scripts directory to sys.path to allow importing 'etl' package
scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base import run_full, run_incremental
from etl.base.runtime import get_tushare_token


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Base layer ETL")
    parser.add_argument("--token", default=None)
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", type=int, default=20100101)
    parser.add_argument("--rate-limit", type=int, default=500)
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    return parser.parse_args()


def _apply_config(config: Optional[str]) -> None:
    if not config:
        return
    config_path = Path(config).expanduser()
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


def main() -> None:
    args = parse_args()
    _apply_config(args.config)
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")

    if args.mode == "full":
        run_full(token, args.start_date, args.rate_limit)
    else:
        run_incremental(token, args.start_date, args.rate_limit)


if __name__ == "__main__":
    main()
