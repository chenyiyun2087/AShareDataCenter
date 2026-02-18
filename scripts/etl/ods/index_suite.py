from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import tushare as ts

from scripts.etl.base.runtime import RateLimiter, to_records, upsert_rows


TARGET_INDEXES: Dict[str, str] = {
    "000001.SH": "Shanghai Composite Index",
    "399001.SZ": "SZSE COMPONENT INDEX",
    "000300.SH": "CSI 300 Index",
    "000905.SH": "CSI 500 Index",
    "000016.SH": "SSE 50 Index",
    "000688.SH": "STAR 50",
    "000133.SH": "S&P China A 1500 Index",
    "000029.SH": "FTSE China A50 Index",
    "399330.SZ": "SZSE 100 Price Index",
    "399006.SZ": "ChiNext Index",
}


@dataclass(frozen=True)
class FetchOptions:
    start_date: int
    end_date: int
    index_codes: Sequence[str]
    sw_level: str = "L1"
    sw_src: str = "SW2021"


def _ensure_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=list(columns))
    normalized = df.copy()
    normalized.columns = [str(c).strip() for c in normalized.columns]
    for col in columns:
        if col not in normalized.columns:
            normalized[col] = None
    normalized = normalized.astype(object).where(pd.notnull(normalized), None)
    return normalized[list(columns)]


def fetch_index_basic(pro: ts.pro_api, limiter: RateLimiter, index_codes: Sequence[str]) -> pd.DataFrame:
    limiter.wait()
    basic = pro.index_basic(fields="ts_code,name,market,publisher,category,base_date,base_point,list_date,fullname,index_type")
    if basic is None or basic.empty:
        return pd.DataFrame()
    return basic[basic["ts_code"].isin(index_codes)].copy()


def fetch_index_members(pro: ts.pro_api, limiter: RateLimiter, index_codes: Sequence[str]) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for code in index_codes:
        limiter.wait()
        df = pro.index_member(index_code=code, fields="index_code,con_code,in_date,out_date,is_new")
        if df is not None and not df.empty:
            rows.append(df)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def fetch_index_weight(
    pro: ts.pro_api,
    limiter: RateLimiter,
    index_codes: Sequence[str],
    start_date: int,
    end_date: int,
) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    # Fetch weights in yearly chunks to avoid TuShare API single-request limits
    start_yr = start_date // 10000
    end_yr = end_date // 10000
    
    for code in index_codes:
        for yr in range(start_yr, end_yr + 1):
            yr_start = max(start_date, yr * 10000 + 101)
            yr_end = min(end_date, yr * 10000 + 1231)
            if yr_start > yr_end:
                continue
            
            limiter.wait()
            df = pro.index_weight(index_code=code, start_date=str(yr_start), end_date=str(yr_end))
            if df is not None and not df.empty:
                rows.append(df)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def fetch_index_daily(pro: ts.pro_api, limiter: RateLimiter, index_codes: Sequence[str], start_date: int, end_date: int) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for code in index_codes:
        limiter.wait()
        df = pro.index_daily(ts_code=code, start_date=str(start_date), end_date=str(end_date))
        if df is not None and not df.empty:
            rows.append(df)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def fetch_index_daily_basic(
    pro: ts.pro_api,
    limiter: RateLimiter,
    index_codes: Sequence[str],
    start_date: int,
    end_date: int,
) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for code in index_codes:
        limiter.wait()
        df = pro.index_dailybasic(ts_code=code, start_date=str(start_date), end_date=str(end_date))
        if df is not None and not df.empty:
            rows.append(df)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def fetch_sw_classify(pro: ts.pro_api, limiter: RateLimiter, level: str, src: str) -> pd.DataFrame:
    limiter.wait()
    return pro.index_classify(level=level, src=src)


def fetch_sw_daily(
    pro: ts.pro_api,
    limiter: RateLimiter,
    start_date: int,
    end_date: int,
    level: str,
    src: str,
) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    classify = fetch_sw_classify(pro, limiter, level=level, src=src)
    if classify is None or classify.empty:
        return pd.DataFrame()
    for code in classify["index_code"].dropna().unique().tolist():
        limiter.wait()
        df = pro.sw_daily(ts_code=code, start_date=str(start_date), end_date=str(end_date))
        if df is not None and not df.empty:
            rows.append(df)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def load_index_suite(cursor, payload: Dict[str, pd.DataFrame]) -> Dict[str, int]:
    metrics: Dict[str, int] = {}

    datasets: List[Tuple[str, Sequence[str], str]] = [
        (
            "ods_index_basic",
            ["ts_code", "name", "market", "publisher", "category", "base_date", "base_point", "list_date", "fullname", "index_type"],
            "index_basic",
        ),
        (
            "ods_index_member",
            ["index_code", "con_code", "in_date", "out_date", "is_new"],
            "index_member",
        ),
        (
            "ods_index_weight",
            ["trade_date", "index_code", "con_code", "weight"],
            "index_weight",
        ),
        (
            "ods_index_daily",
            ["trade_date", "ts_code", "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"],
            "index_daily",
        ),
        (
            "ods_index_tech_factor",
            ["trade_date", "ts_code", "turnover_rate", "pe", "pe_ttm", "pb", "total_mv", "float_mv", "total_share", "float_share", "free_share", "turnover_rate5", "turnover_rate10"],
            "index_dailybasic",
        ),
        (
            "ods_sw_index_classify",
            ["index_code", "industry_name", "level", "industry_code", "is_pub", "parent_code", "src"],
            "sw_classify",
        ),
        (
            "ods_sw_index_daily",
            ["trade_date", "ts_code", "name", "open", "low", "high", "close", "change", "pct_change", "vol", "amount", "pe", "pb"],
            "sw_daily",
        ),
    ]

    for table, columns, key in datasets:
        normalized = _ensure_columns(payload.get(key, pd.DataFrame()), columns)
        rows = to_records(normalized, list(columns))
        upsert_rows(cursor, table, list(columns), rows)
        metrics[table] = len(rows)

    return metrics


def build_default_options(start_date: int, end_date: int, index_codes: Optional[Iterable[str]] = None) -> FetchOptions:
    codes = list(index_codes) if index_codes else list(TARGET_INDEXES.keys())
    return FetchOptions(start_date=start_date, end_date=end_date, index_codes=codes)
