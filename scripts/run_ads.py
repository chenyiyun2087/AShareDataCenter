#!/usr/bin/env python3
import argparse

from etl.ads import run_full, run_incremental


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ADS layer ETL")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", type=int, default=20100101)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "full":
        run_full(args.start_date)
    else:
        run_incremental()


if __name__ == "__main__":
    main()
