#!/usr/bin/env python3
import argparse
import os

from etl import ads, base, dwd, dws, ods
from etl.base.runtime import get_tushare_token

DEFAULT_RATE_LIMIT = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TuShare daily ETL (layered)")
    parser.add_argument("--token", default=None)
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", type=int, default=20100101)
    parser.add_argument("--fina-start", type=int, default=None)
    parser.add_argument("--fina-end", type=int, default=None)
    parser.add_argument("--rate-limit", type=int, default=DEFAULT_RATE_LIMIT)
    parser.add_argument(
        "--layers",
        default="base,ods,dwd,dws,ads",
        help="Comma-separated layers to run",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    layers = [layer.strip() for layer in args.layers.split(",") if layer.strip()]

    token = args.token or get_tushare_token()

    if ("base" in layers or "ods" in layers) and not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")

    if "base" in layers:
        if args.mode == "full":
            base.run_full(token, args.start_date, args.rate_limit)
        else:
            base.run_incremental(token, args.start_date, args.rate_limit)

    if "ods" in layers:
        if args.mode == "full":
            ods.run_full(token, args.start_date, args.rate_limit)
        else:
            ods.run_incremental(token, args.rate_limit)
        if args.fina_start and args.fina_end:
            ods.run_fina_incremental(token, args.fina_start, args.fina_end, args.rate_limit)

    if "dwd" in layers:
        if args.mode == "full":
            dwd.run_full(args.start_date)
        else:
            dwd.run_incremental()
        if args.fina_start and args.fina_end:
            dwd.run_fina_incremental(args.fina_start, args.fina_end)

    if "dws" in layers:
        if args.mode == "full":
            dws.run_full(args.start_date)
        else:
            dws.run_incremental()

    if "ads" in layers:
        if args.mode == "full":
            ads.run_full(args.start_date)
        else:
            ads.run_incremental()


if __name__ == "__main__":
    main()
