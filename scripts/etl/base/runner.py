from __future__ import annotations

from typing import Optional

import tushare as ts

from .runtime import (
    RateLimiter,
    ensure_watermark,
    get_mysql_connection,
    get_watermark,
    list_run_logs,
    log_run_end,
    log_run_start,
    update_watermark,
)
from .runtime import get_env_config
from .runtime import list_trade_dates
from .runtime import to_records
from .runtime import upsert_rows

DEFAULT_RATE_LIMIT = 500


def fetch_dim_trade_cal(pro: ts.pro_api, limiter: RateLimiter, start_date: int):
    limiter.wait()
    return pro.trade_cal(start_date=str(start_date))


def fetch_dim_stock(pro: ts.pro_api, limiter: RateLimiter):
    limiter.wait()
    return pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,is_hs",
    )


def load_dim_trade_cal(cursor, df) -> None:
    columns = ["exchange", "cal_date", "is_open", "pretrade_date"]
    rows = to_records(df, columns)
    upsert_rows(cursor, "dim_trade_cal", columns, rows)


def load_dim_stock(cursor, df) -> None:
    columns = [
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "market",
        "list_date",
        "delist_date",
        "is_hs",
    ]
    rows = to_records(df, columns)
    upsert_rows(cursor, "dim_stock", columns, rows)


def run_full(token: str, start_date: int, rate_limit: int = DEFAULT_RATE_LIMIT) -> None:
    cfg = get_env_config()
    limiter = RateLimiter(rate_limit)
    pro = ts.pro_api(token)

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "base", "full")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                trade_cal = fetch_dim_trade_cal(pro, limiter, start_date)
                load_dim_trade_cal(cursor, trade_cal)
                dim_stock = fetch_dim_stock(pro, limiter)
                load_dim_stock(cursor, dim_stock)
                conn.commit()

            with conn.cursor() as cursor:
                trade_dates = list_trade_dates(cursor, start_date)
                latest_trade_date = trade_dates[-1] if trade_dates else start_date - 1
                ensure_watermark(cursor, "base_trade_cal", latest_trade_date)
                ensure_watermark(cursor, "base_stock", latest_trade_date)
                conn.commit()

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise


def run_incremental(
    token: str,
    start_date: int,
    rate_limit: int = DEFAULT_RATE_LIMIT,
) -> None:
    cfg = get_env_config()
    limiter = RateLimiter(rate_limit)
    pro = ts.pro_api(token)

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "base", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                last_trade_date = get_watermark(cursor, "base_trade_cal")
                if last_trade_date is None:
                    last_trade_date = start_date - 1
                trade_cal = fetch_dim_trade_cal(pro, limiter, last_trade_date + 1)
                load_dim_trade_cal(cursor, trade_cal)
                dim_stock = fetch_dim_stock(pro, limiter)
                load_dim_stock(cursor, dim_stock)
                conn.commit()

            with conn.cursor() as cursor:
                trade_dates = list_trade_dates(cursor, last_trade_date + 1)
                latest_trade_date = trade_dates[-1] if trade_dates else last_trade_date
                update_watermark(cursor, "base_trade_cal", latest_trade_date, "SUCCESS")
                update_watermark(cursor, "base_stock", latest_trade_date, "SUCCESS")
                conn.commit()

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise


def list_runs(limit: int = 50):
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            return list_run_logs(cursor, limit)
