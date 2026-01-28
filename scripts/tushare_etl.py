#!/usr/bin/env python3
import argparse
import os
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import pandas as pd
import pymysql
import tushare as ts

DEFAULT_RATE_LIMIT = 500  # requests per minute
BATCH_SIZE = 2000


@dataclass
class MysqlConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


class RateLimiter:
    def __init__(self, max_per_minute: int) -> None:
        self.interval = 60.0 / max_per_minute
        self.last = 0.0

    def wait(self) -> None:
        now = time.time()
        sleep_for = self.interval - (now - self.last)
        if sleep_for > 0:
            time.sleep(sleep_for)
        self.last = time.time()


def get_env_config() -> MysqlConfig:
    return MysqlConfig(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", ""),
        database=os.environ.get("MYSQL_DB", "tushare_stock"),
    )


def get_mysql_connection(cfg: MysqlConfig) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        autocommit=False,
    )


def to_records(df: pd.DataFrame, columns: List[str]) -> List[Tuple]:
    if df.empty:
        return []
    return [tuple(df[col].iloc[i] for col in columns) for i in range(len(df))]


def chunked(items: List[Tuple], size: int) -> Iterable[List[Tuple]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def upsert_rows(
    cursor: pymysql.cursors.Cursor,
    table: str,
    columns: List[str],
    rows: List[Tuple],
) -> None:
    if not rows:
        return
    placeholders = ",".join(["%s"] * len(columns))
    columns_sql = ",".join(columns)
    update_sql = ",".join([f"{col}=VALUES({col})" for col in columns])
    sql = (
        f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )
    for batch in chunked(rows, BATCH_SIZE):
        cursor.executemany(sql, batch)


def ensure_watermark(cursor: pymysql.cursors.Cursor, api_name: str, water_mark: int) -> None:
    sql = (
        "INSERT INTO meta_etl_watermark (api_name, water_mark, status, last_run_at) "
        "VALUES (%s, %s, 'SUCCESS', NOW()) "
        "ON DUPLICATE KEY UPDATE water_mark=VALUES(water_mark), status='SUCCESS', last_run_at=NOW()"
    )
    cursor.execute(sql, (api_name, water_mark))


def update_watermark(
    cursor: pymysql.cursors.Cursor,
    api_name: str,
    water_mark: int,
    status: str,
    last_err: Optional[str] = None,
) -> None:
    sql = (
        "UPDATE meta_etl_watermark SET water_mark=%s, status=%s, last_run_at=NOW(), last_err=%s "
        "WHERE api_name=%s"
    )
    cursor.execute(sql, (water_mark, status, last_err, api_name))


def get_watermark(cursor: pymysql.cursors.Cursor, api_name: str) -> Optional[int]:
    cursor.execute("SELECT water_mark FROM meta_etl_watermark WHERE api_name=%s", (api_name,))
    row = cursor.fetchone()
    if row:
        return int(row[0])
    return None


def list_trade_dates(cursor: pymysql.cursors.Cursor, start_date: int) -> List[int]:
    sql = (
        "SELECT cal_date FROM dim_trade_cal "
        "WHERE exchange='SSE' AND is_open=1 AND cal_date >= %s ORDER BY cal_date"
    )
    cursor.execute(sql, (start_date,))
    return [int(row[0]) for row in cursor.fetchall()]


def list_trade_dates_after(cursor: pymysql.cursors.Cursor, last_date: int) -> List[int]:
    sql = (
        "SELECT cal_date FROM dim_trade_cal "
        "WHERE exchange='SSE' AND is_open=1 AND cal_date > %s ORDER BY cal_date"
    )
    cursor.execute(sql, (last_date,))
    return [int(row[0]) for row in cursor.fetchall()]


def fetch_dim_trade_cal(pro: ts.pro_api, limiter: RateLimiter, start_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.trade_cal(start_date=str(start_date))


def fetch_dim_stock(pro: ts.pro_api, limiter: RateLimiter) -> pd.DataFrame:
    limiter.wait()
    return pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,is_hs",
    )


def fetch_daily(pro: ts.pro_api, limiter: RateLimiter, trade_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.daily(trade_date=str(trade_date))


def fetch_daily_basic(pro: ts.pro_api, limiter: RateLimiter, trade_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.daily_basic(trade_date=str(trade_date))


def fetch_adj_factor(pro: ts.pro_api, limiter: RateLimiter, trade_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.adj_factor(trade_date=str(trade_date))


def fetch_fina_indicator(pro: ts.pro_api, limiter: RateLimiter, start_date: int, end_date: int) -> pd.DataFrame:
    limiter.wait()
    return pro.fina_indicator(start_date=str(start_date), end_date=str(end_date))


def load_dim_trade_cal(cursor: pymysql.cursors.Cursor, df: pd.DataFrame) -> None:
    columns = ["exchange", "cal_date", "is_open", "pretrade_date"]
    rows = to_records(df, columns)
    upsert_rows(cursor, "dim_trade_cal", columns, rows)


def load_dim_stock(cursor: pymysql.cursors.Cursor, df: pd.DataFrame) -> None:
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


def load_daily(cursor: pymysql.cursors.Cursor, df: pd.DataFrame) -> None:
    df = df.rename(columns={"change": "change_amount"})
    columns = [
        "trade_date",
        "ts_code",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change_amount",
        "pct_chg",
        "vol",
        "amount",
    ]
    rows = to_records(df, columns)
    upsert_rows(cursor, "dwd_daily", columns, rows)


def load_daily_basic(cursor: pymysql.cursors.Cursor, df: pd.DataFrame) -> None:
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
    rows = to_records(df, columns)
    upsert_rows(cursor, "dwd_daily_basic", columns, rows)


def load_adj_factor(cursor: pymysql.cursors.Cursor, df: pd.DataFrame) -> None:
    columns = ["trade_date", "ts_code", "adj_factor"]
    rows = to_records(df, columns)
    upsert_rows(cursor, "dwd_adj_factor", columns, rows)


def load_fina_indicator(cursor: pymysql.cursors.Cursor, df: pd.DataFrame) -> None:
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
    rows = to_records(df, columns)
    upsert_rows(cursor, "dwd_fina_indicator", columns, rows)


def run_full_load(
    pro: ts.pro_api,
    limiter: RateLimiter,
    conn: pymysql.connections.Connection,
    start_date: int,
) -> None:
    with conn.cursor() as cursor:
        trade_cal = fetch_dim_trade_cal(pro, limiter, start_date)
        load_dim_trade_cal(cursor, trade_cal)
        dim_stock = fetch_dim_stock(pro, limiter)
        load_dim_stock(cursor, dim_stock)
        trade_dates = list_trade_dates(cursor, start_date)
        conn.commit()

    for trade_date in trade_dates:
        with conn.cursor() as cursor:
            daily = fetch_daily(pro, limiter, trade_date)
            load_daily(cursor, daily)
            daily_basic = fetch_daily_basic(pro, limiter, trade_date)
            load_daily_basic(cursor, daily_basic)
            adj_factor = fetch_adj_factor(pro, limiter, trade_date)
            load_adj_factor(cursor, adj_factor)
            conn.commit()

    with conn.cursor() as cursor:
        ensure_watermark(cursor, "daily", start_date - 1)
        ensure_watermark(cursor, "daily_basic", start_date - 1)
        ensure_watermark(cursor, "adj_factor", start_date - 1)
        conn.commit()


def run_incremental(
    pro: ts.pro_api,
    limiter: RateLimiter,
    conn: pymysql.connections.Connection,
    api_name: str,
    fetcher,
    loader,
) -> None:
    with conn.cursor() as cursor:
        last_date = get_watermark(cursor, api_name)
        if last_date is None:
            raise RuntimeError(f"missing watermark for {api_name}")
        trade_dates = list_trade_dates_after(cursor, last_date)

    for trade_date in trade_dates:
        with conn.cursor() as cursor:
            try:
                data = fetcher(pro, limiter, trade_date)
                loader(cursor, data)
                update_watermark(cursor, api_name, trade_date, "SUCCESS")
                conn.commit()
            except Exception as exc:
                update_watermark(cursor, api_name, last_date, "FAILED", str(exc))
                conn.rollback()
                raise


def run_fina_incremental(
    pro: ts.pro_api,
    limiter: RateLimiter,
    conn: pymysql.connections.Connection,
    start_date: int,
    end_date: int,
) -> None:
    with conn.cursor() as cursor:
        df = fetch_fina_indicator(pro, limiter, start_date, end_date)
        load_fina_indicator(cursor, df)
        update_watermark(cursor, "fina_indicator", end_date, "SUCCESS")
        conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TuShare daily ETL")
    parser.add_argument("--token", default=os.environ.get("TUSHARE_TOKEN"))
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", type=int, default=20100101)
    parser.add_argument("--fina-start", type=int, default=None)
    parser.add_argument("--fina-end", type=int, default=None)
    parser.add_argument("--rate-limit", type=int, default=DEFAULT_RATE_LIMIT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")

    cfg = get_env_config()
    limiter = RateLimiter(args.rate_limit)
    pro = ts.pro_api(args.token)

    with get_mysql_connection(cfg) as conn:
        if args.mode == "full":
            run_full_load(pro, limiter, conn, args.start_date)
        else:
            run_incremental(pro, limiter, conn, "daily", fetch_daily, load_daily)
            run_incremental(pro, limiter, conn, "daily_basic", fetch_daily_basic, load_daily_basic)
            run_incremental(pro, limiter, conn, "adj_factor", fetch_adj_factor, load_adj_factor)

        if args.fina_start and args.fina_end:
            run_fina_incremental(pro, limiter, conn, args.fina_start, args.fina_end)


if __name__ == "__main__":
    main()
