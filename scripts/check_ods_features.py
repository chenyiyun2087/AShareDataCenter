#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

from etl.base.runtime import get_env_config, get_mysql_connection, list_trade_dates


FEATURE_TABLES: Dict[str, Dict[str, Optional[str]]] = {
    "ods_margin": {"date_col": "trade_date"},
    "ods_margin_detail": {"date_col": "trade_date"},
    "ods_margin_target": {"date_col": None},
    "ods_moneyflow": {"date_col": "trade_date"},
    "ods_moneyflow_ths": {"date_col": "trade_date"},
    "ods_cyq_chips": {"date_col": "trade_date"},
    "ods_cyq_perf": {"date_col": "trade_date"},
    "ods_stk_factor": {"date_col": "trade_date"},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check ODS feature tables for missing data.")
    parser.add_argument("--start-date", type=int, required=True)
    parser.add_argument("--end-date", type=int, required=True)
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table list (default: all feature tables).",
    )
    return parser.parse_args()


def _table_exists(cursor, schema: str, table: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        [schema, table],
    )
    return cursor.fetchone() is not None


def _fetch_dates(cursor, table: str, date_col: str, start_date: int, end_date: int) -> List[int]:
    cursor.execute(
        f"SELECT DISTINCT {date_col} FROM {table} "
        f"WHERE {date_col} BETWEEN %s AND %s ORDER BY {date_col}",
        [start_date, end_date],
    )
    return [row[0] for row in cursor.fetchall()]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()

    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            config_path = (Path.cwd() / config_path).resolve()
        if not config_path.exists():
            raise RuntimeError(f"config file not found: {config_path}")
        os.environ["ETL_CONFIG_PATH"] = str(config_path)

    cfg = get_env_config()
    tables = [t.strip() for t in args.tables.split(",") if t.strip()] or list(FEATURE_TABLES.keys())

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            expected_dates = [
                d for d in list_trade_dates(cursor, args.start_date) if d <= args.end_date
            ]

        for table in tables:
            if table not in FEATURE_TABLES:
                print(f"Skip unknown table: {table}")
                continue
            date_col = FEATURE_TABLES[table]["date_col"]
            with conn.cursor() as cursor:
                if not _table_exists(cursor, cfg.database, table):
                    print(f"Missing table: {table}")
                    continue
                if not date_col:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    total = cursor.fetchone()[0]
                    print(f"{table}: total_rows={total} (no trade_date column)")
                    continue
                present_dates = set(_fetch_dates(cursor, table, date_col, args.start_date, args.end_date))
                missing_dates = [d for d in expected_dates if d not in present_dates]
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {date_col} BETWEEN %s AND %s",
                    [args.start_date, args.end_date],
                )
                total = cursor.fetchone()[0]
                if missing_dates:
                    print(
                        f"{table}: total_rows={total}, missing_dates={len(missing_dates)} "
                        f"({missing_dates[:10]}{'...' if len(missing_dates) > 10 else ''})"
                    )
                else:
                    print(f"{table}: total_rows={total}, missing_dates=0")


if __name__ == "__main__":
    main()
