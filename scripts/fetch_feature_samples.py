#!/usr/bin/env python3
from __future__ import annotations

import argparse
from typing import Callable, Dict

import pandas as pd
import tushare as ts

from etl.base.runtime import RateLimiter, get_tushare_token


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch TuShare feature sample data.")
    parser.add_argument("--trade-date", type=int, required=True)
    parser.add_argument("--ts-code", default=None)
    parser.add_argument("--token", default=None)
    parser.add_argument("--rate-limit", type=int, default=500)
    parser.add_argument(
        "--apis",
        default="margin,margin_detail,margin_target,moneyflow,moneyflow_ths,cyq_chips,stk_factor",
        help="Comma-separated API list.",
    )
    return parser.parse_args()


def _fetch_margin(pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: str | None) -> pd.DataFrame:
    limiter.wait()
    return pro.margin(trade_date=str(trade_date))


def _fetch_margin_detail(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: str | None
) -> pd.DataFrame:
    limiter.wait()
    return pro.margin_detail(trade_date=str(trade_date), ts_code=ts_code or "")


def _fetch_margin_target(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: str | None
) -> pd.DataFrame:
    limiter.wait()
    return pro.margin_target(trade_date=str(trade_date), ts_code=ts_code or "")


def _fetch_moneyflow(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: str | None
) -> pd.DataFrame:
    limiter.wait()
    return pro.moneyflow(trade_date=str(trade_date), ts_code=ts_code or "")


def _fetch_moneyflow_ths(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: str | None
) -> pd.DataFrame:
    limiter.wait()
    return pro.moneyflow_ths(trade_date=str(trade_date), ts_code=ts_code or "")


def _fetch_cyq_chips(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: str | None
) -> pd.DataFrame:
    limiter.wait()
    return pro.cyq_chips(trade_date=str(trade_date), ts_code=ts_code or "")


def _fetch_stk_factor(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: str | None
) -> pd.DataFrame:
    limiter.wait()
    return pro.stk_factor(trade_date=str(trade_date), ts_code=ts_code or "")


def main() -> None:
    args = parse_args()
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")
    limiter = RateLimiter(args.rate_limit)
    pro = ts.pro_api(token)
    apis = [api.strip() for api in args.apis.split(",") if api.strip()]
    api_map: Dict[str, Callable[[ts.pro_api, RateLimiter, int, str | None], pd.DataFrame]] = {
        "margin": _fetch_margin,
        "margin_detail": _fetch_margin_detail,
        "margin_target": _fetch_margin_target,
        "moneyflow": _fetch_moneyflow,
        "moneyflow_ths": _fetch_moneyflow_ths,
        "cyq_chips": _fetch_cyq_chips,
        "stk_factor": _fetch_stk_factor,
    }

    for api in apis:
        fetcher = api_map.get(api)
        if not fetcher:
            print(f"Skipping unknown api: {api}")
            continue
        df = fetcher(pro, limiter, args.trade_date, args.ts_code)
        print(f"\n=== {api} sample ({len(df)} rows) ===")
        if df.empty:
            print("No data.")
            continue
        print(df.head(5).to_string(index=False))
        print(f"columns: {list(df.columns)}")


if __name__ == "__main__":
    main()
