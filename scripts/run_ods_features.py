#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Callable, Dict, List, Optional

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
        "--cyq-rate-limit",
        type=int,
        default=180,
        help="Requests per minute for cyq_chips (default: 180).",
    )
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument(
        "--apis",
        default="margin,margin_detail,margin_target,moneyflow,moneyflow_ths,cyq_chips,stk_factor",
        help="Comma-separated API list.",
    )
    return parser.parse_args()


API_COLUMNS: Dict[str, List[str]] = {
    "margin": [
        "trade_date",
        "exchange_id",
        "rzye",
        "rzmre",
        "rzche",
        "rqye",
        "rqmcl",
        "rzrqye",
        "rqyl",
    ],
    "margin_detail": [
        "trade_date",
        "ts_code",
        "rzye",
        "rqye",
        "rzmre",
        "rqyl",
        "rzche",
        "rqchl",
        "rqmcl",
        "rzrqye",
    ],
    "margin_target": ["ts_code", "mg_type", "is_new", "in_date", "out_date", "ann_date"],
    "moneyflow": [
        "trade_date",
        "ts_code",
        "buy_sm_vol",
        "buy_sm_amount",
        "sell_sm_vol",
        "sell_sm_amount",
        "buy_md_vol",
        "buy_md_amount",
        "sell_md_vol",
        "sell_md_amount",
        "buy_lg_vol",
        "buy_lg_amount",
        "sell_lg_vol",
        "sell_lg_amount",
        "buy_elg_vol",
        "buy_elg_amount",
        "sell_elg_vol",
        "sell_elg_amount",
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
    "cyq_chips": ["trade_date", "ts_code", "price", "percent"],
    "stk_factor": [
        "trade_date",
        "ts_code",
        "close",
        "open",
        "high",
        "low",
        "pre_close",
        "change",
        "pct_change",
        "vol",
        "amount",
        "adj_factor",
        "open_hfq",
        "open_qfq",
        "close_hfq",
        "close_qfq",
        "high_hfq",
        "high_qfq",
        "low_hfq",
        "low_qfq",
        "pre_close_hfq",
        "pre_close_qfq",
        "macd_dif",
        "macd_dea",
        "macd",
        "kdj_k",
        "kdj_d",
        "kdj_j",
        "rsi_6",
        "rsi_12",
        "rsi_24",
        "boll_upper",
        "boll_mid",
        "boll_lower",
        "cci",
        "score",
    ],
}


def _fetch_margin(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.margin(trade_date=str(trade_date))


def _fetch_margin_detail(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.margin_detail(trade_date=str(trade_date))


def _fetch_margin_target(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.margin_target(trade_date=str(trade_date))


def _fetch_moneyflow(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.moneyflow(trade_date=str(trade_date))


def _fetch_moneyflow_ths(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.moneyflow_ths(trade_date=str(trade_date))


def _fetch_cyq_chips(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    if not ts_code:
        raise RuntimeError("cyq_chips requires ts_code")
    return pro.cyq_chips(trade_date=str(trade_date), ts_code=ts_code)


def _fetch_stk_factor(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
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
    if "exchange_id" in columns and "exchange_id" not in df.columns:
        df["exchange_id"] = ""
    df = df.reindex(columns=columns)
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None, "nan": None, "NaN": None})
    return to_records(df, columns), columns


def _list_ts_codes(cursor) -> List[str]:
    cursor.execute("SELECT ts_code FROM dim_stock ORDER BY ts_code")
    return [row[0] for row in cursor.fetchall()]


def _existing_tables(cursor, schema: str, tables: List[str]) -> set[str]:
    if not tables:
        return set()
    placeholders = ",".join(["%s"] * len(tables))
    sql = (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema=%s AND table_name IN ({placeholders})"
    )
    cursor.execute(sql, [schema, *tables])
    return {row[0] for row in cursor.fetchall()}


def main() -> None:
    args = parse_args()
    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            config_path = (Path.cwd() / config_path).resolve()
        if not config_path.exists():
            raise RuntimeError(f"config file not found: {config_path}")
        os.environ["ETL_CONFIG_PATH"] = str(config_path)
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")
    apis = [api.strip() for api in args.apis.split(",") if api.strip()]
    api_map: dict[str, Callable[[ts.pro_api, RateLimiter, int, Optional[str]], pd.DataFrame]] = {
        "margin": _fetch_margin,
        "margin_detail": _fetch_margin_detail,
        "margin_target": _fetch_margin_target,
        "moneyflow": _fetch_moneyflow,
        "moneyflow_ths": _fetch_moneyflow_ths,
        "cyq_chips": _fetch_cyq_chips,
        "stk_factor": _fetch_stk_factor,
    }
    apis_require_ts_code = {"cyq_chips"}
    cfg = get_env_config()
    limiter = RateLimiter(args.rate_limit)
    limiter_map = {"cyq_chips": RateLimiter(args.cyq_rate_limit)}
    pro = ts.pro_api(token)

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            table_map = {api: _table_for_api(api) for api in apis if api in api_map}
            existing = _existing_tables(cursor, cfg.database, list(table_map.values()))
            missing_tables = {table for table in table_map.values() if table not in existing}
            if missing_tables:
                print(
                    "Missing tables detected: "
                    f"{', '.join(sorted(missing_tables))}. "
                    "Run sql/ddl.sql to create them."
                )
            apis = [api for api in apis if table_map.get(api) in existing]
            trade_dates = list_trade_dates(cursor, args.start_date)
            ts_codes = _list_ts_codes(cursor) if apis_require_ts_code.intersection(apis) else []
        trade_dates = [d for d in trade_dates if d <= args.end_date]
        if apis_require_ts_code.intersection(apis) and not ts_codes:
            print("No ts_code found in dim_stock; skipping APIs that require ts_code.")

        for trade_date in trade_dates:
            for api_name in apis:
                fetcher = api_map.get(api_name)
                if not fetcher:
                    continue
                if api_name in apis_require_ts_code:
                    for ts_code in ts_codes:
                        try:
                            api_limiter = limiter_map.get(api_name, limiter)
                            df = fetcher(pro, api_limiter, trade_date, ts_code)
                        except Exception as exc:
                            print(f"Fetch failed for {api_name} on {trade_date} {ts_code}: {exc}")
                            continue
                        rows, columns = _prepare_rows(api_name, df, trade_date)
                        if not rows:
                            continue
                        table = _table_for_api(api_name)
                        try:
                            with conn.cursor() as cursor:
                                upsert_rows(cursor, table, columns, rows)
                                conn.commit()
                        except Exception as exc:
                            conn.rollback()
                            print(f"Insert failed for {api_name} on {trade_date} {ts_code}: {exc}")
                            continue
                else:
                    try:
                        df = fetcher(pro, limiter, trade_date)
                    except Exception as exc:
                        print(f"Fetch failed for {api_name} on {trade_date}: {exc}")
                        continue
                    rows, columns = _prepare_rows(api_name, df, trade_date)
                    if not rows:
                        continue
                    table = _table_for_api(api_name)
                    try:
                        with conn.cursor() as cursor:
                            upsert_rows(cursor, table, columns, rows)
                            conn.commit()
                    except Exception as exc:
                        conn.rollback()
                        print(f"Insert failed for {api_name} on {trade_date}: {exc}")
                        continue
            print(f"Completed feature APIs for trade_date={trade_date}")


if __name__ == "__main__":
    main()
