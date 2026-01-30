from __future__ import annotations

from typing import Optional

import tushare as ts
import pandas as pd

from ..base.runtime import (
    RateLimiter,
    ensure_watermark,
    get_env_config,
    get_mysql_connection,
    get_watermark,
    list_trade_dates,
    list_trade_dates_after,
    log_run_end,
    log_run_start,
    to_records,
    update_watermark,
    upsert_rows,
)

DEFAULT_RATE_LIMIT = 500


def fetch_daily(pro: ts.pro_api, limiter: RateLimiter, trade_date: int):
    limiter.wait()
    return pro.daily(trade_date=str(trade_date))


def fetch_daily_basic(pro: ts.pro_api, limiter: RateLimiter, trade_date: int):
    limiter.wait()
    return pro.daily_basic(trade_date=str(trade_date))


def fetch_adj_factor(pro: ts.pro_api, limiter: RateLimiter, trade_date: int):
    limiter.wait()
    return pro.adj_factor(trade_date=str(trade_date))


def fetch_fina_indicator(
    pro: ts.pro_api,
    limiter: RateLimiter,
    ts_code: str,
    start_date: int,
    end_date: int,
):
    limiter.wait()
    return pro.fina_indicator(ts_code=ts_code, start_date=str(start_date), end_date=str(end_date))


def load_ods_daily(cursor, df) -> None:
    data_columns = [
        "trade_date",
        "ts_code",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]
    db_columns = [
        "trade_date",
        "ts_code",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "`change`",
        "pct_chg",
        "vol",
        "amount",
    ]
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    rows = to_records(df, data_columns)
    upsert_rows(cursor, "ods_daily", db_columns, rows)


def load_ods_daily_basic(cursor, df) -> None:
    columns = [
        "trade_date",
        "ts_code",
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
    ]
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    rows = to_records(df, columns)
    upsert_rows(cursor, "ods_daily_basic", columns, rows)


def load_ods_adj_factor(cursor, df) -> None:
    columns = ["trade_date", "ts_code", "adj_factor"]
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    rows = to_records(df, columns)
    upsert_rows(cursor, "ods_adj_factor", columns, rows)


def load_ods_fina_indicator(cursor, df) -> None:
    columns = [
        "ts_code",
        "ann_date",
        "end_date",
        "report_type",
        "roe",
        "grossprofit_margin",
        "debt_to_assets",
        "netprofit_margin",
        "op_income",
        "total_assets",
        "total_hldr_eqy",
    ]
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]
    if "report_type" not in df.columns and "report_type_name" in df.columns:
        df = df.rename(columns={"report_type_name": "report_type"})
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    rows = to_records(df, columns)
    upsert_rows(cursor, "ods_fina_indicator", columns, rows)


def run_full(token: str, start_date: int, rate_limit: int = DEFAULT_RATE_LIMIT) -> None:
    cfg = get_env_config()
    limiter = RateLimiter(rate_limit)
    pro = ts.pro_api(token)

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ods", "full")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                trade_dates = list_trade_dates(cursor, start_date)
            for trade_date in trade_dates:
                with conn.cursor() as cursor:
                    daily = fetch_daily(pro, limiter, trade_date)
                    load_ods_daily(cursor, daily)
                    daily_basic = fetch_daily_basic(pro, limiter, trade_date)
                    load_ods_daily_basic(cursor, daily_basic)
                    adj_factor = fetch_adj_factor(pro, limiter, trade_date)
                    load_ods_adj_factor(cursor, adj_factor)
                    conn.commit()

            with conn.cursor() as cursor:
                ensure_watermark(cursor, "ods_daily", start_date - 1)
                ensure_watermark(cursor, "ods_daily_basic", start_date - 1)
                ensure_watermark(cursor, "ods_adj_factor", start_date - 1)
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
    rate_limit: int = DEFAULT_RATE_LIMIT,
) -> None:
    cfg = get_env_config()
    limiter = RateLimiter(rate_limit)
    pro = ts.pro_api(token)

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ods", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                last_date = get_watermark(cursor, "ods_daily")
                if last_date is None:
                    raise RuntimeError("missing watermark for ods_daily")
                trade_dates = list_trade_dates_after(cursor, last_date)

            for trade_date in trade_dates:
                with conn.cursor() as cursor:
                    try:
                        daily = fetch_daily(pro, limiter, trade_date)
                        load_ods_daily(cursor, daily)
                        daily_basic = fetch_daily_basic(pro, limiter, trade_date)
                        load_ods_daily_basic(cursor, daily_basic)
                        adj_factor = fetch_adj_factor(pro, limiter, trade_date)
                        load_ods_adj_factor(cursor, adj_factor)
                        update_watermark(cursor, "ods_daily", trade_date, "SUCCESS")
                        update_watermark(cursor, "ods_daily_basic", trade_date, "SUCCESS")
                        update_watermark(cursor, "ods_adj_factor", trade_date, "SUCCESS")
                        conn.commit()
                    except Exception as exc:
                        update_watermark(cursor, "ods_daily", last_date, "FAILED", str(exc))
                        update_watermark(cursor, "ods_daily_basic", last_date, "FAILED", str(exc))
                        update_watermark(cursor, "ods_adj_factor", last_date, "FAILED", str(exc))
                        conn.rollback()
                        raise

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise


def run_fina_incremental(
    token: str,
    start_date: int,
    end_date: int,
    rate_limit: int = DEFAULT_RATE_LIMIT,
) -> None:
    cfg = get_env_config()
    limiter = RateLimiter(rate_limit)
    pro = ts.pro_api(token)

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ods_fina_indicator", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT ts_code FROM dim_stock ORDER BY ts_code")
                ts_codes = [row[0] for row in cursor.fetchall()]

            for ts_code in ts_codes:
                with conn.cursor() as cursor:
                    df = fetch_fina_indicator(pro, limiter, ts_code, start_date, end_date)
                    load_ods_fina_indicator(cursor, df)
                    conn.commit()

            with conn.cursor() as cursor:
                update_watermark(cursor, "ods_fina_indicator", end_date, "SUCCESS")
                conn.commit()

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise
