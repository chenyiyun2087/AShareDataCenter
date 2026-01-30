#!/usr/bin/env python3
from __future__ import annotations

import argparse
from typing import Callable, Dict, List

import pandas as pd
import tushare as ts

from etl.base.runtime import (
    RateLimiter,
    get_env_config,
    get_mysql_connection,
    get_tushare_token,
    list_trade_dates,
    to_records,
    upsert_rows,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch TuShare feature datasets into ODS raw tables.")
    parser.add_argument("--start-date", type=int, required=True)
    parser.add_argument("--end-date", type=int, required=True)
    parser.add_argument("--token", default=None)
    parser.add_argument("--rate-limit", type=int, default=500)
    parser.add_argument(
        "--apis",
        default="margin,margin_detail,margin_target,moneyflow,moneyflow_ths,cyq_chips,stk_factor",
        help="Comma-separated API list.",
    )
    return parser.parse_args()


API_COLUMNS: Dict[str, List[str]] = {
    "margin": [
        "trade_date",
        "exchange",
        "rzye",
        "rzmre",
        "rzche",
        "rqye",
        "rqmre",
        "rqyl",
        "rzrqye",
        "rzrqye_chg",
    ],
    "margin_detail": [
        "trade_date",
        "ts_code",
        "rzye",
        "rzmre",
        "rzche",
        "rqye",
        "rqmre",
        "rqyl",
        "rzrqye",
        "rzrqye_chg",
    ],
    "margin_target": ["trade_date", "ts_code", "exchange", "is_target"],
    "moneyflow": [
        "trade_date",
        "ts_code",
        "buy_sm",
        "sell_sm",
        "buy_md",
        "sell_md",
        "buy_lg",
        "sell_lg",
        "buy_elg",
        "sell_elg",
        "net_mf_vol",
        "net_mf_amount",
    ],
    "moneyflow_ths": [
        "trade_date",
        "ts_code",
        "net_mf_amount",
        "net_mf_vol",
        "buy_sm_amount",
        "sell_sm_amount",
        "buy_md_amount",
        "sell_md_amount",
        "buy_lg_amount",
        "sell_lg_amount",
        "buy_elg_amount",
        "sell_elg_amount",
    ],
    "cyq_chips": ["trade_date", "ts_code", "avg_cost", "winner_rate", "pct90", "pct10"],
    "stk_factor": [
        "trade_date",
        "ts_code",
        "close",
        "pct_chg",
        "turnover_rate",
        "volume_ratio",
        "pe",
        "pb",
        "ps",
        "dv_ratio",
        "total_mv",
        "circ_mv",
        "score",
    ],
}


def _fetch_margin(pro: ts.pro_api, limiter: RateLimiter, trade_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.margin(trade_date=str(trade_date))


def _fetch_margin_detail(pro: ts.pro_api, limiter: RateLimiter, trade_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.margin_detail(trade_date=str(trade_date))


def _fetch_margin_target(pro: ts.pro_api, limiter: RateLimiter, trade_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.margin_target(trade_date=str(trade_date))


def _fetch_moneyflow(pro: ts.pro_api, limiter: RateLimiter, trade_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.moneyflow(trade_date=str(trade_date))


def _fetch_moneyflow_ths(pro: ts.pro_api, limiter: RateLimiter, trade_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.moneyflow_ths(trade_date=str(trade_date))


def _fetch_cyq_chips(pro: ts.pro_api, limiter: RateLimiter, trade_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.cyq_chips(trade_date=str(trade_date))


def _fetch_stk_factor(pro: ts.pro_api, limiter: RateLimiter, trade_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.stk_factor(trade_date=str(trade_date))


def _table_for_api(api_name: str) -> str:
    table_map = {
        "margin": "ods_margin",
        "margin_detail": "ods_margin_detail",
        "margin_target": "ods_margin_target",
        "moneyflow": "ods_moneyflow",
        "moneyflow_ths": "ods_moneyflow_ths",
        "cyq_chips": "ods_cyq_chips",
        "stk_factor": "ods_stk_factor",
    }
    return table_map[api_name]


def _prepare_rows(api_name: str, df: pd.DataFrame, trade_date: int):
    columns = API_COLUMNS[api_name]
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]
    if "trade_date" in columns and "trade_date" not in df.columns:
        df["trade_date"] = trade_date
    if "ts_code" in columns and "ts_code" not in df.columns:
        df["ts_code"] = ""
    if "exchange" in columns and "exchange" not in df.columns:
        df["exchange"] = ""
    df = df.reindex(columns=columns)
    df = df.where(pd.notnull(df), None)
    return to_records(df, columns), columns


def main() -> None:
    args = parse_args()
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")
    apis = [api.strip() for api in args.apis.split(",") if api.strip()]
    api_map: dict[str, Callable[[ts.pro_api, RateLimiter, int], pd.DataFrame]] = {
        "margin": _fetch_margin,
        "margin_detail": _fetch_margin_detail,
        "margin_target": _fetch_margin_target,
        "moneyflow": _fetch_moneyflow,
        "moneyflow_ths": _fetch_moneyflow_ths,
        "cyq_chips": _fetch_cyq_chips,
        "stk_factor": _fetch_stk_factor,
    }
    cfg = get_env_config()
    limiter = RateLimiter(args.rate_limit)
    pro = ts.pro_api(token)

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            trade_dates = list_trade_dates(cursor, args.start_date)
        trade_dates = [d for d in trade_dates if d <= args.end_date]

        for trade_date in trade_dates:
            for api_name in apis:
                fetcher = api_map.get(api_name)
                if not fetcher:
                    continue
                df = fetcher(pro, limiter, trade_date)
                rows, columns = _prepare_rows(api_name, df, trade_date)
                if not rows:
                    continue
                table = _table_for_api(api_name)
                with conn.cursor() as cursor:
                    upsert_rows(cursor, table, columns, rows)
                    conn.commit()
            print(f"Completed feature APIs for trade_date={trade_date}")


if __name__ == "__main__":
    main()
