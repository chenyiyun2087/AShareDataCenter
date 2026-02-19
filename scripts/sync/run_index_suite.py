#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import tushare as ts

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
scripts_dir = project_root / "scripts"

from scripts.etl.base.runtime import get_env_config, get_mysql_session, get_tushare_limit, get_tushare_token, RateLimiter
from scripts.etl.ods.index_suite import (
    TARGET_INDEXES,
    build_default_options,
    fetch_index_basic,
    fetch_index_daily,
    fetch_index_daily_basic,
    fetch_index_members,
    fetch_index_weight,
    fetch_sw_classify,
    fetch_sw_daily,
    load_index_suite,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync A-share index suite from TuShare.")
    parser.add_argument("--start-date", type=int, required=True)
    parser.add_argument("--end-date", type=int, required=True)
    parser.add_argument("--token", default=None)
    parser.add_argument("--rate-limit", type=int, default=None)
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument(
        "--index-codes",
        default=",".join(TARGET_INDEXES.keys()),
        help="Comma-separated index ts_code list.",
    )
    parser.add_argument("--sw-level", default="L1", help="SW classify level, default L1")
    parser.add_argument("--sw-src", default="SW2021", help="SW classify source, default SW2021")
    return parser.parse_args()


def _set_config_path(config_path: str | None) -> None:
    if not config_path:
        return
    path = Path(config_path).expanduser()
    if not path.is_absolute():
        cwd_path = (Path.cwd() / path).resolve()
        root_path = (scripts_dir.parent / path).resolve()
        path = cwd_path if cwd_path.exists() else root_path
    if not path.exists():
        raise RuntimeError(f"config file not found: {path}")
    os.environ["ETL_CONFIG_PATH"] = str(path)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()
    _set_config_path(args.config)

    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")

    rate_limit = args.rate_limit or get_tushare_limit()
    limiter = RateLimiter(rate_limit)
    pro = ts.pro_api(token)

    index_codes = [code.strip() for code in args.index_codes.split(",") if code.strip()]
    options = build_default_options(args.start_date, args.end_date, index_codes)

    payload = {
        "index_basic": fetch_index_basic(pro, limiter, options.index_codes),
        "index_member": fetch_index_members(pro, limiter, options.index_codes),
        "index_weight": fetch_index_weight(pro, limiter, options.index_codes, options.start_date, options.end_date),
        "index_daily": fetch_index_daily(pro, limiter, options.index_codes, options.start_date, options.end_date),
        "index_dailybasic": fetch_index_daily_basic(pro, limiter, options.index_codes, options.start_date, options.end_date),
    }
    payload["sw_classify"] = fetch_sw_classify(pro, limiter, args.sw_level, args.sw_src)
    payload["sw_daily"] = fetch_sw_daily(pro, limiter, options.start_date, options.end_date, args.sw_level, args.sw_src)

    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            metrics = load_index_suite(cursor, payload)
        conn.commit()

    logging.info("Index suite sync completed.")
    for table, count in metrics.items():
        logging.info("%s: %s rows", table, count)


if __name__ == "__main__":
    main()
