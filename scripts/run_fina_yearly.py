#!/usr/bin/env python3
from __future__ import annotations

import argparse

from etl.base.runtime import get_tushare_token
from etl.ods import run_fina_incremental


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TuShare fina_indicator by year.")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--token", default=None)
    parser.add_argument("--rate-limit", type=int, default=500)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")
    if args.end_year < args.start_year:
        raise SystemExit("end-year must be >= start-year")

    for year in range(args.start_year, args.end_year + 1):
        start_date = int(f"{year}0101")
        end_date = int(f"{year}1231")
        print(f"Running fina_indicator for {year}: {start_date} -> {end_date}")
        run_fina_incremental(token, start_date, end_date, args.rate_limit)


if __name__ == "__main__":
    main()
