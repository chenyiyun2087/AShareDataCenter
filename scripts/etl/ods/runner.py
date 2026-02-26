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
    fields: Optional[str] = None,
):
    limiter.wait()
    kwargs = {
        "ts_code": ts_code,
        "start_date": str(start_date),
        "end_date": str(end_date),
    }
    if fields:
        kwargs["fields"] = fields
    return call_with_retry(
        lambda: pro.fina_indicator(**kwargs)
    )


def fetch_balancesheet(
    pro: ts.pro_api,
    limiter: RateLimiter,
    ts_code: str,
    start_date: int,
    end_date: int,
):
    limiter.wait()
    return call_with_retry(
        lambda: pro.balancesheet(
            ts_code=ts_code,
            start_date=str(start_date),
            end_date=str(end_date),
            fields="ts_code,ann_date,end_date,total_assets,total_hldr_eqy_exc_min_int",
        )
    )


def fetch_dividend(pro: ts.pro_api, limiter: RateLimiter, ts_code: str):
    limiter.wait()
    return call_with_retry(lambda: pro.dividend(ts_code=ts_code))


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


def _coerce_fina_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("ann_date", "end_date"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _sanitize_ratio_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[df[col].abs() > 999999.999999, col] = None
    return df


def _fill_or_yoy_from_op_income(df: pd.DataFrame) -> pd.DataFrame:
    if "or_yoy" not in df.columns or "op_income" not in df.columns:
        return df
    if df["or_yoy"].notnull().all():
        return df

    tmp = df[["ts_code", "end_date", "op_income"]].copy()
    tmp["end_date"] = pd.to_numeric(tmp["end_date"], errors="coerce")
    tmp["op_income"] = pd.to_numeric(tmp["op_income"], errors="coerce")
    tmp = tmp[tmp["ts_code"].notnull() & tmp["end_date"].notnull()]
    if tmp.empty:
        return df

    tmp["prev_end_date"] = tmp["end_date"] - 10000
    prev = tmp[["ts_code", "end_date", "op_income"]].rename(
        columns={"end_date": "prev_end_date", "op_income": "op_income_prev"}
    )
    merged = tmp.merge(prev, on=["ts_code", "prev_end_date"], how="left")
    mask = (
        merged["op_income_prev"].notnull()
        & (merged["op_income_prev"] != 0)
        & merged["op_income"].notnull()
    )
    merged["or_yoy_calc"] = None
    merged.loc[mask, "or_yoy_calc"] = (
        (merged.loc[mask, "op_income"] - merged.loc[mask, "op_income_prev"])
        / merged.loc[mask, "op_income_prev"]
    )
    lookup = (
        merged[["ts_code", "end_date", "or_yoy_calc"]]
        .dropna(subset=["or_yoy_calc"])
        .drop_duplicates(subset=["ts_code", "end_date"])
    )
    if lookup.empty:
        return df

    df = df.merge(lookup, on=["ts_code", "end_date"], how="left")
    df["or_yoy"] = df["or_yoy"].fillna(df["or_yoy_calc"])
    return df.drop(columns=["or_yoy_calc"], errors="ignore")


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
        "or_yoy",
        "netprofit_yoy",
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
    df = _coerce_fina_dates(df)
    for col in ("grossprofit_margin", "netprofit_margin", "or_yoy", "netprofit_yoy"):
        df = _sanitize_ratio_column(df, col)
    if "op_income" in df.columns:
        df["op_income"] = pd.to_numeric(df["op_income"], errors="coerce")

    df = df[df["ann_date"].notnull() & df["end_date"].notnull() & df["ts_code"].notnull()]
    df = _fill_or_yoy_from_op_income(df)

    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})

    rows = to_records(df, columns)
    upsert_rows(cursor, "ods_fina_indicator", columns, rows)


def load_ods_fina_yoy(cursor, df) -> None:
    columns = ["ts_code", "ann_date", "end_date", "or_yoy", "netprofit_yoy"]
    if df is None or df.empty:
        return

    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]
    for col in columns:
        if col not in df.columns:
            df[col] = None

    df = _coerce_fina_dates(df)
    for col in ("or_yoy", "netprofit_yoy"):
        df = _sanitize_ratio_column(df, col)
    if "op_income" in df.columns:
        df["op_income"] = pd.to_numeric(df["op_income"], errors="coerce")
        df = _fill_or_yoy_from_op_income(df)

    df = df[df["ann_date"].notnull() & df["end_date"].notnull() & df["ts_code"].notnull()]
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})

    rows = to_records(df, columns)
    upsert_rows(cursor, "ods_fina_indicator", columns, rows)


def load_ods_dividend(cursor, df) -> None:
    if df is None or df.empty:
        return
    columns = [
        "ts_code",
        "ann_date",
        "end_date",
        "div_proc",
        "stk_div",
        "stk_chl_div",
        "stk_img_div",
        "cash_div",
        "cash_div_tax",
        "record_date",
        "ex_date",
        "pay_date",
        "div_listdate",
        "imp_ann_date",
        "base_date",
        "base_share",
    ]
    df = df.copy()

    # Map possible TuShare column names to our DB schema
    mapping = {
        "stk_co_rate": "stk_chl_div",
        "stk_bo_rate": "stk_img_div",
    }
    for old_col, new_col in mapping.items():
        if old_col in df.columns and (new_col not in df.columns or df[new_col].isnull().all()):
            df[new_col] = df[old_col]

    # Ensure all columns exist in the dataframe before records conversion
    for col in columns:
        if col not in df.columns:
            df[col] = None

    # Handle NaNs
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None})

    # Ensure date columns are safe for MySQL
    date_cols = [
        "ann_date",
        "end_date",
        "record_date",
        "ex_date",
        "pay_date",
        "div_listdate",
        "imp_ann_date",
        "base_date",
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int).replace(0, None)

    # Skip 0 dividend rows if needed, but here we just upsert what TuShare gives
    rows = to_records(df, columns)
    upsert_rows(cursor, "ods_dividend", columns, rows)


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
                def _table_max_date(table_name: str) -> Optional[int]:
                    cursor.execute(f"SELECT MAX(trade_date) FROM {table_name}")
                    row = cursor.fetchone()
                    return int(row[0]) if row and row[0] else None

                if start_date:
                    last_date = start_date - 1
                else:
                    watermark_daily = get_watermark(cursor, "ods_daily")
                    daily_max_date = _table_max_date("ods_daily")
                    # Self-heal when watermark is ahead of real table data.
                    if daily_max_date is not None and (
                        watermark_daily is None or watermark_daily > daily_max_date
                    ):
                        logger.warning(
                            "ods_daily watermark (%s) is inconsistent with table max (%s), "
                            "resetting watermark baseline.",
                            watermark_daily,
                            daily_max_date,
                        )
                        daily_basic_max_date = _table_max_date("ods_daily_basic") or daily_max_date
                        adj_factor_max_date = _table_max_date("ods_adj_factor") or daily_max_date
                        ensure_watermark(cursor, "ods_daily", daily_max_date)
                        ensure_watermark(cursor, "ods_daily_basic", daily_basic_max_date)
                        ensure_watermark(cursor, "ods_adj_factor", adj_factor_max_date)
                        conn.commit()
                        last_date = daily_max_date
                    else:
                        last_date = watermark_daily

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
                        if daily is None or daily.empty:
                            raise RuntimeError(f"ods_daily returned empty for trade_date={trade_date}")
                        load_ods_daily(cursor, daily)
                        daily_basic = fetch_daily_basic(pro, limiter, trade_date)
                        if daily_basic is None or daily_basic.empty:
                            raise RuntimeError(f"ods_daily_basic returned empty for trade_date={trade_date}")
                        load_ods_daily_basic(cursor, daily_basic)
                        adj_factor = fetch_adj_factor(pro, limiter, trade_date)
                        if adj_factor is None or adj_factor.empty:
                            raise RuntimeError(f"ods_adj_factor returned empty for trade_date={trade_date}")
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
                        last_date = trade_date
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
    limit_count: Optional[int] = None,
    include_balancesheet: bool = True,
    yoy_only: bool = False,
    only_missing_yoy: bool = False,
    commit_every: int = 1,
    progress_every: int = 1,
    continue_on_error: bool = False,
    active_ts_only: bool = False,
) -> None:
    cfg = get_env_config()
    limiter = RateLimiter(rate_limit)
    pro = ts.pro_api(token)
    commit_every = max(1, commit_every)
    progress_every = max(1, progress_every)

    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ods_fina_indicator", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                if only_missing_yoy:
                    cursor.execute(
                        """
                        SELECT DISTINCT ts_code
                        FROM ods_fina_indicator
                        WHERE ann_date BETWEEN %s AND %s
                          AND (or_yoy IS NULL OR netprofit_yoy IS NULL)
                        ORDER BY ts_code
                        """,
                        (start_date, end_date),
                    )
                else:
                    if active_ts_only:
                        cursor.execute(
                            """
                            SELECT ts_code
                            FROM dim_stock
                            WHERE (list_date IS NULL OR list_date <= %s)
                              AND (delist_date IS NULL OR delist_date = 0 OR delist_date >= %s)
                            ORDER BY ts_code
                            """,
                            (end_date, start_date),
                        )
                    else:
                        cursor.execute("SELECT ts_code FROM dim_stock ORDER BY ts_code")
                ts_codes = [row[0] for row in cursor.fetchall()]

            if limit_count:
                ts_codes = ts_codes[:limit_count]

            total_codes = len(ts_codes)
            logger.info("ODS fina indicator load: %s ts_code items to process", total_codes)
            if total_codes == 0:
                logger.info("No ts_code to process for range %s - %s", start_date, end_date)

            failed_codes: list[str] = []
            fina_fields = None
            if yoy_only:
                # Keep payload small in YoY-only backfill mode.
                fina_fields = "ts_code,ann_date,end_date,op_income,or_yoy,netprofit_yoy"

            for index, ts_code in enumerate(ts_codes, start=1):
                if index == 1 or index == total_codes or (index % progress_every == 0):
                    log_progress("ODS fina indicator load", index, total_codes)
                try:
                    with conn.cursor() as cursor:
                        fina_df = fetch_fina_indicator(
                            pro,
                            limiter,
                            ts_code,
                            start_date,
                            end_date,
                            fields=fina_fields,
                        )
                        if yoy_only:
                            if fina_df is not None and not fina_df.empty:
                                load_ods_fina_yoy(cursor, fina_df)
                        else:
                            bs_df = None
                            if include_balancesheet:
                                bs_df = fetch_balancesheet(pro, limiter, ts_code, start_date, end_date)

                            if fina_df is not None and not fina_df.empty:
                                if bs_df is not None and not bs_df.empty:
                                    # Merge asset/equity from bs_df into fina_df
                                    bs_df = bs_df.rename(columns={"total_hldr_eqy_exc_min_int": "total_hldr_eqy"})
                                    # Ensure date types match for merging
                                    for df in (fina_df, bs_df):
                                        for col in ("ann_date", "end_date"):
                                            if col in df.columns:
                                                df[col] = pd.to_numeric(df[col], errors="coerce")

                                    # Identify columns to select from BS
                                    bs_cols = ["ts_code", "ann_date", "end_date"]
                                    for col in ("total_assets", "total_hldr_eqy"):
                                        if col in bs_df.columns:
                                            bs_cols.append(col)

                                    combined = fina_df.merge(
                                        bs_df[bs_cols],
                                        on=["ts_code", "ann_date", "end_date"],
                                        how="left",
                                        suffixes=("", "_bs"),
                                    )

                                    # Fill missing from BS if columns exist
                                    for col in ("total_assets", "total_hldr_eqy"):
                                        col_bs = f"{col}_bs"
                                        if col_bs in combined.columns:
                                            combined[col] = combined[col].fillna(combined[col_bs])
                                    load_ods_fina_indicator(cursor, combined)
                                else:
                                    load_ods_fina_indicator(cursor, fina_df)
                            elif bs_df is not None and not bs_df.empty:
                                # Even if indicator API fails, we can partially fill the row from BS
                                bs_df = bs_df.rename(columns={"total_hldr_eqy_exc_min_int": "total_hldr_eqy"})
                                load_ods_fina_indicator(cursor, bs_df)

                        if index % commit_every == 0:
                            conn.commit()
                except Exception as exc:
                    conn.rollback()
                    failed_codes.append(ts_code)
                    logger.exception("Failed processing ts_code=%s: %s", ts_code, exc)
                    if not continue_on_error:
                        raise

            conn.commit()

            if failed_codes:
                sample = ", ".join(failed_codes[:20])
                logger.warning(
                    "ODS fina indicator had %s failed ts_code(s). sample=%s%s",
                    len(failed_codes),
                    sample,
                    " ..." if len(failed_codes) > 20 else "",
                )
                if len(failed_codes) >= total_codes:
                    raise RuntimeError("all ts_code requests failed for ods_fina_indicator")

            with conn.cursor() as cursor:
                update_watermark(cursor, "ods_fina_indicator", end_date, "SUCCESS")
                conn.commit()

            with conn.cursor() as cursor:
                log_run_end(
                    cursor,
                    run_id,
                    "SUCCESS",
                    request_count=total_codes,
                    fail_count=len(failed_codes),
                )
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc), fail_count=1)
                conn.commit()
            raise


def run_dividend_incremental(
    token: str,
    rate_limit: int = DEFAULT_RATE_LIMIT,
    limit_count: Optional[int] = None,
) -> None:
    cfg = get_env_config()
    limiter = RateLimiter(rate_limit)
    pro = ts.pro_api(token)

    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ods_dividend", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT ts_code FROM dim_stock ORDER BY ts_code")
                ts_codes = [row[0] for row in cursor.fetchall()]
            if limit_count:
                ts_codes = ts_codes[:limit_count]

            total_codes = len(ts_codes)
            logger.info("ODS dividend load: %s ts_code items to process", total_codes)

            for index, ts_code in enumerate(ts_codes, start=1):
                if index % 100 == 0 or index == 1:
                    log_progress("ODS dividend load", index, total_codes)
                with conn.cursor() as cursor:
                    df = fetch_dividend(pro, limiter, ts_code)
                    load_ods_dividend(cursor, df)
                    if index % 200 == 0:
                        conn.commit()
            conn.commit()

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise
