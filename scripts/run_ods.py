#!/usr/bin/env python3
import argparse
import os

from etl.ods import run_fina_incremental, run_full, run_incremental
from etl.base.runtime import get_tushare_token


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ODS layer ETL")
    parser.add_argument("--token", default=None)
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", type=int, default=20100101)
    parser.add_argument("--fina-start", type=int)
    parser.add_argument("--fina-end", type=int)
    parser.add_argument("--rate-limit", type=int, default=500)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")

    if args.mode == "full":
        run_full(token, args.start_date, args.rate_limit)
    else:
        run_incremental(token, args.rate_limit)

    if args.fina_start and args.fina_end:
        run_fina_incremental(token, args.fina_start, args.fina_end, args.rate_limit)


if __name__ == "__main__":
    main()
