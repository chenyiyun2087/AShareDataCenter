from __future__ import annotations

import os
from pathlib import Path
import time
from dataclasses import dataclass
from configparser import ConfigParser
from typing import Iterable, List, Optional, Tuple

import pandas as pd
import pymysql

BATCH_SIZE = 2000

_DEFAULT_REL_CONFIG = Path("config") / "etl.ini"
_MODULE_ROOT = Path(__file__).resolve().parents[2]
_MODULE_DEFAULT_CONFIG = _MODULE_ROOT / _DEFAULT_REL_CONFIG


def _resolve_default_config_path() -> str:
    env_path = os.environ.get("ETL_CONFIG_PATH")
    if env_path:
        return env_path
    cwd_path = Path.cwd() / _DEFAULT_REL_CONFIG
    if cwd_path.exists():
        return str(cwd_path)
    if _MODULE_DEFAULT_CONFIG.exists():
        return str(_MODULE_DEFAULT_CONFIG)
    return str(_DEFAULT_REL_CONFIG)


DEFAULT_CONFIG_PATH = _resolve_default_config_path()


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


def _load_config() -> ConfigParser:
    parser = ConfigParser()
    print(f"Using ETL config: {DEFAULT_CONFIG_PATH}")
    parser.read(DEFAULT_CONFIG_PATH)
    return parser


def get_env_config() -> MysqlConfig:
    cfg = _load_config()
    host = os.environ.get("MYSQL_HOST") or cfg.get("mysql", "host", fallback="127.0.0.1")
    port = os.environ.get("MYSQL_PORT") or cfg.get("mysql", "port", fallback="3306")
    user = os.environ.get("MYSQL_USER") or cfg.get("mysql", "user", fallback="root")
    password = os.environ.get("MYSQL_PASSWORD") or cfg.get("mysql", "password", fallback="")
    database = os.environ.get("MYSQL_DB") or cfg.get("mysql", "database", fallback="tushare_stock")
    return MysqlConfig(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=database,
    )


def get_tushare_token() -> Optional[str]:
    cfg = _load_config()
    token = os.environ.get("TUSHARE_TOKEN")
    if token:
        return token
    return cfg.get("tushare", "token", fallback=None)


def get_mysql_connection(cfg: MysqlConfig) -> pymysql.connections.Connection:
    try:
        return pymysql.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            password=cfg.password,
            database=cfg.database,
            charset="utf8mb4",
            autocommit=False,
        )
    except pymysql.err.OperationalError as exc:
        raise RuntimeError(
            "Failed to connect to MySQL. Check MYSQL_HOST/MYSQL_PORT/MYSQL_USER/"
            "MYSQL_PASSWORD/MYSQL_DB and verify credentials."
        ) from exc


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


def get_latest_trade_date(cursor: pymysql.cursors.Cursor) -> Optional[int]:
    cursor.execute("SELECT MAX(trade_date) FROM dwd_daily")
    row = cursor.fetchone()
    if row and row[0]:
        return int(row[0])
    return None


def log_run_start(cursor: pymysql.cursors.Cursor, api_name: str, run_type: str) -> int:
    sql = (
        "INSERT INTO meta_etl_run_log (api_name, run_type, start_at, status) "
        "VALUES (%s, %s, NOW(), 'RUNNING')"
    )
    cursor.execute(sql, (api_name, run_type))
    return cursor.lastrowid


def log_run_end(
    cursor: pymysql.cursors.Cursor,
    run_id: int,
    status: str,
    err_msg: Optional[str] = None,
    request_count: int = 0,
    fail_count: int = 0,
) -> None:
    sql = (
        "UPDATE meta_etl_run_log SET end_at=NOW(), status=%s, err_msg=%s, "
        "request_count=%s, fail_count=%s WHERE id=%s"
    )
    cursor.execute(sql, (status, err_msg, request_count, fail_count, run_id))


def list_run_logs(cursor: pymysql.cursors.Cursor, limit: int = 50) -> List[dict]:
    cursor.execute(
        "SELECT id, api_name, run_type, start_at, end_at, status, err_msg "
        "FROM meta_etl_run_log ORDER BY id DESC LIMIT %s",
        (limit,),
    )
    rows = cursor.fetchall()
    return [
        {
            "id": row[0],
            "api_name": row[1],
            "run_type": row[2],
            "start_at": row[3],
            "end_at": row[4],
            "status": row[5],
            "err_msg": row[6],
        }
        for row in rows
    ]
