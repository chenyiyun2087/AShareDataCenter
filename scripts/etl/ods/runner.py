from __future__ import annotations

import logging
import time
from typing import Callable, Optional, TypeVar

import tushare as ts
import pandas as pd
from requests import exceptions as requests_exceptions

from ..base.runtime import (
    RateLimiter,
    ensure_watermark,
    get_env_config,
    get_mysql_session,
    get_watermark,
    list_trade_dates,
    list_trade_dates_after,
    log_run_end,
    log_run_start,
    to_records,
    update_watermark,
    upsert_rows,
)

DEFAULT_RATE_LIMIT = 400
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_S = 1.0

T = TypeVar("T")


logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def call_with_retry(
    action: Callable[[], T],
    *,
    attempts: int = DEFAULT_RETRY_ATTEMPTS,
    base_delay_s: float = DEFAULT_RETRY_DELAY_S,
) -> T:
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except requests_exceptions.RequestException as exc:
            last_exc = exc
            if attempt >= attempts:
                raise
            time.sleep(base_delay_s * (2 ** (attempt - 1)))
    if last_exc:
        raise last_exc
    raise RuntimeError("retry attempts exhausted without exception")


def log_progress(label: str, current: int, total: int) -> None:
    if total <= 0:
        return
    percent = (current / total) * 100
    logger.info("%s progress: %s/%s (%.1f%%)", label, current, total, percent)


def fetch_daily(pro: ts.pro_api, limiter: RateLimiter, trade_date: int):
    limiter.wait()
    return call_with_retry(lambda: pro.daily(trade_date=str(trade_date)))


def fetch_weekly(pro: ts.pro_api, limiter: RateLimiter, trade_date: int):
    limiter.wait()
    return call_with_retry(lambda: pro.weekly(trade_date=str(trade_date)))


def fetch_monthly(pro: ts.pro_api, limiter: RateLimiter, trade_date: int):
    limiter.wait()
    return call_with_retry(lambda: pro.monthly(trade_date=str(trade_date)))


def fetch_weekly(pro: ts.pro_api, limiter: RateLimiter, trade_date: int):
    limiter.wait()
    return call_with_retry(lambda: pro.weekly(trade_date=str(trade_date)))


def fetch_monthly(pro: ts.pro_api, limiter: RateLimiter, trade_date: int):
    limiter.wait()
    return call_with_retry(lambda: pro.monthly(trade_date=str(trade_date)))


def fetch_daily_basic(pro: ts.pro_api, limiter: RateLimiter, trade_date: int):
    limiter.wait()
    return call_with_retry(lambda: pro.daily_basic(trade_date=str(trade_date)))


def fetch_adj_factor(pro: ts.pro_api, limiter: RateLimiter, trade_date: int):
    limiter.wait()
    return call_with_retry(lambda: pro.adj_factor(trade_date=str(trade_date)))


def fetch_fina_indicator(
    pro: ts.pro_api,
    limiter: RateLimiter,
    ts_code: str,
    start_date: int,
    end_date: int,
):
    limiter.wait()
    return call_with_retry(
        lambda: pro.fina_indicator(ts_code=ts_code, start_date=str(start_date), end_date=str(end_date))
    )


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
        # NOTE: `change` is a MySQL keyword; upsert_rows will quote identifiers safely.
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    rows = to_records(df, data_columns)
    upsert_rows(cursor, "ods_daily", db_columns, rows)


def load_ods_weekly(cursor, df) -> None:
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
    # NOTE: Reuse the same DB columns as daily, just different table name
    db_columns = [
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
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    rows = to_records(df, data_columns)
    upsert_rows(cursor, "ods_weekly", db_columns, rows)


def load_ods_monthly(cursor, df) -> None:
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
    # NOTE: Reuse the same DB columns as daily, just different table name
    db_columns = [
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
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    rows = to_records(df, data_columns)
    upsert_rows(cursor, "ods_monthly", db_columns, rows)


def load_ods_weekly(cursor, df) -> None:
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
    # NOTE: Reuse the same DB columns as daily, just different table name
    db_columns = [
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
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    rows = to_records(df, data_columns)
    upsert_rows(cursor, "ods_weekly", db_columns, rows)


def load_ods_monthly(cursor, df) -> None:
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
    # NOTE: Reuse the same DB columns as daily, just different table name
    db_columns = [
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
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    rows = to_records(df, data_columns)
    upsert_rows(cursor, "ods_monthly", db_columns, rows)


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
    if "report_type" not in df.columns:
        df["report_type"] = None
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df.copy()
    for col in ("grossprofit_margin", "netprofit_margin"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[df[col].abs() > 999999.999999, col] = None
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    df = df[df["ann_date"].notnull() & df["end_date"].notnull() & df["ts_code"].notnull()]
    rows = to_records(df, columns)
    upsert_rows(cursor, "ods_fina_indicator", columns, rows)


def fetch_index_daily_range(pro: ts.pro_api, limiter: RateLimiter, ts_code: str, start_date: int, end_date: int):
    """Fetch index daily data for a specific index code and date range."""
    limiter.wait()
    return call_with_retry(lambda: pro.index_daily(
        ts_code=ts_code, 
        start_date=str(start_date), 
        end_date=str(end_date)
    ))


def load_ods_index_daily(cursor, df) -> None:
    """Load index daily data into ods_index_daily table."""
    if df is None or df.empty:
        return
    columns = [
        "trade_date", "ts_code", "open", "high", "low", "close", 
        "pre_close", "pct_chg", "vol", "amount"
    ]
    df = df.copy()
    # Rename change to pct_chg if needed
    if "change" in df.columns and "pct_chg" not in df.columns:
        df["pct_chg"] = df["change"]
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})
    rows = to_records(df, columns)
    upsert_rows(cursor, "ods_index_daily", columns, rows)


# Major index codes for fetching
INDEX_CODES = [
    "000300.SH",  # 沪深300
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000688.SH",  # 科创50
]


def run_index_daily(
    token: str,
    start_date: int,
    rate_limit: int = DEFAULT_RATE_LIMIT,
) -> None:
    """Fetch index daily data for major indices using batch date range mode."""
    cfg = get_env_config()
    pro = ts.pro_api(token)
    limiter = RateLimiter(rate_limit)
    
    # Get today's date as end date
    import datetime
    end_date = int(datetime.date.today().strftime("%Y%m%d"))
    
    with get_mysql_connection(cfg) as conn:
        for idx, ts_code in enumerate(INDEX_CODES, 1):
            logger.info(f"[{idx}/{len(INDEX_CODES)}] Fetching {ts_code} from {start_date} to {end_date}")
            df = fetch_index_daily_range(pro, limiter, ts_code, start_date, end_date)
            if df is not None and not df.empty:
                logger.info(f"  Got {len(df)} records")
                with conn.cursor() as cursor:
                    load_ods_index_daily(cursor, df)
                    conn.commit()
            else:
                logger.warning(f"  No data for {ts_code}")
        
        logger.info(f"Completed index daily sync for {len(INDEX_CODES)} indices")


def run_full(
    token: str,
    start_date: int,
    end_date: Optional[int] = None,
    rate_limit: int = DEFAULT_RATE_LIMIT,
) -> None:
    cfg = get_env_config()
    limiter = RateLimiter(rate_limit)
    pro = ts.pro_api(token)

    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ods", "full")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                trade_dates = list_trade_dates(cursor, start_date, end_date)
            total_dates = len(trade_dates)
            logger.info("ODS full load: %s trade dates to process", total_dates)
            for index, trade_date in enumerate(trade_dates, start=1):
                log_progress("ODS full load", index, total_dates)
                with conn.cursor() as cursor:
                    daily = fetch_daily(pro, limiter, trade_date)
                    load_ods_daily(cursor, daily)
                    daily_basic = fetch_daily_basic(pro, limiter, trade_date)
                    load_ods_daily_basic(cursor, daily_basic)
                    adj_factor = fetch_adj_factor(pro, limiter, trade_date)
                    load_ods_adj_factor(cursor, adj_factor)
                    weekly = fetch_weekly(pro, limiter, trade_date)
                    load_ods_weekly(cursor, weekly)
                    monthly = fetch_monthly(pro, limiter, trade_date)
                    load_ods_monthly(cursor, monthly)
                    conn.commit()

            with conn.cursor() as cursor:
                ensure_watermark(cursor, "ods_daily", start_date - 1)
                ensure_watermark(cursor, "ods_daily_basic", start_date - 1)
                ensure_watermark(cursor, "ods_adj_factor", start_date - 1)
                ensure_watermark(cursor, "ods_weekly", start_date - 1)
                ensure_watermark(cursor, "ods_monthly", start_date - 1)
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
    start_date: Optional[int] = None,
    end_date: Optional[int] = None,
    rate_limit: int = DEFAULT_RATE_LIMIT,
) -> None:
    cfg = get_env_config()
    limiter = RateLimiter(rate_limit)
    pro = ts.pro_api(token)

    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ods", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                if start_date:
                    last_date = start_date - 1
                else:
                    last_date = get_watermark(cursor, "ods_daily")
                
                if last_date is None:
                    raise RuntimeError("missing watermark for ods_daily")
                
                if start_date:
                    trade_dates = list_trade_dates(cursor, start_date, end_date)
                else:
                    trade_dates = list_trade_dates_after(cursor, last_date, end_date)
                
                # Cap dates at today to avoid processing future calendar dates
                from datetime import datetime
                today_int = int(datetime.now().strftime('%Y%m%d'))
                trade_dates = [d for d in trade_dates if d <= today_int]
            
            total_dates = len(trade_dates)
            logger.info("ODS incremental load: %s trade dates to process", total_dates)

            for index, trade_date in enumerate(trade_dates, start=1):
                log_progress("ODS incremental load", index, total_dates)
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
                        
                        weekly = fetch_weekly(pro, limiter, trade_date)
                        load_ods_weekly(cursor, weekly)
                        update_watermark(cursor, "ods_weekly", trade_date, "SUCCESS")

                        monthly = fetch_monthly(pro, limiter, trade_date)
                        load_ods_monthly(cursor, monthly)
                        update_watermark(cursor, "ods_monthly", trade_date, "SUCCESS")
                        
                        conn.commit()
                    except Exception as exc:
                        update_watermark(cursor, "ods_daily", last_date, "FAILED", str(exc))
                        update_watermark(cursor, "ods_daily_basic", last_date, "FAILED", str(exc))
                        update_watermark(cursor, "ods_adj_factor", last_date, "FAILED", str(exc))
                        update_watermark(cursor, "ods_weekly", last_date, "FAILED", str(exc))
                        update_watermark(cursor, "ods_monthly", last_date, "FAILED", str(exc))
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

    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ods_fina_indicator", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT ts_code FROM dim_stock ORDER BY ts_code")
                ts_codes = [row[0] for row in cursor.fetchall()]
            total_codes = len(ts_codes)
            logger.info("ODS fina indicator load: %s ts_code items to process", total_codes)

            for index, ts_code in enumerate(ts_codes, start=1):
                log_progress("ODS fina indicator load", index, total_codes)
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
