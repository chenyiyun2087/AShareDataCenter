#!/usr/bin/env python3
import argparse

from etl.dwd import run_fina_incremental, run_full, run_incremental


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DWD layer ETL")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", type=int, default=20100101)
    parser.add_argument("--fina-start", type=int)
    parser.add_argument("--fina-end", type=int)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "full":
        run_full(args.start_date)
    else:
        run_incremental()

    if args.fina_start and args.fina_end:
        run_fina_incremental(args.fina_start, args.fina_end)


if __name__ == "__main__":
    main()
