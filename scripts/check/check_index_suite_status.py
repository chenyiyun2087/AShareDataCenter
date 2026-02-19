#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Tuple

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
scripts_dir = project_root / "scripts"

from scripts.etl.base.runtime import get_env_config, get_mysql_connection


TABLE_DATE_COLUMNS: Dict[str, str | None] = {
    "ods_index_basic": None,
    "ods_index_member": None,
    "ods_index_weight": "trade_date",
    "ods_index_daily": "trade_date",
    "ods_index_tech_factor": "trade_date",
    "ods_sw_index_classify": None,
    "ods_sw_index_daily": "trade_date",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check index suite table status.")
    parser.add_argument("--start-date", type=int, required=True)
    parser.add_argument("--end-date", type=int, required=True)
    parser.add_argument("--config", default=None)
    parser.add_argument("--fail-on-empty", action="store_true")
    return parser.parse_args()


def _set_config_path(config_path: str | None) -> None:
    if not config_path:
        return
    path = Path(config_path).expanduser()
    if not path.is_absolute():
        cwd_path = (Path.cwd() / path).resolve()
        root_path = (scripts_dir.parent / path).resolve()
        path = cwd_path if cwd_path.exists() else root_path
    if not path.exists():
        raise RuntimeError(f"config file not found: {path}")
    os.environ["ETL_CONFIG_PATH"] = str(path)


def _count_rows(cursor, table: str, date_col: str | None, start: int, end: int) -> int:
    if not date_col:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
    else:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {date_col} BETWEEN %s AND %s", [start, end])
    return int(cursor.fetchone()[0])


def _min_max_dates(cursor, table: str, date_col: str) -> Tuple[int | None, int | None]:
    cursor.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}")
    row = cursor.fetchone()
    return (row[0], row[1])


def main() -> None:
    args = parse_args()
    _set_config_path(args.config)
    cfg = get_env_config()

    empty_tables = []
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            for table, date_col in TABLE_DATE_COLUMNS.items():
                rows = _count_rows(cursor, table, date_col, args.start_date, args.end_date)
                if date_col:
                    min_date, max_date = _min_max_dates(cursor, table, date_col)
                    print(f"{table}: rows={rows}, min_date={min_date}, max_date={max_date}")
                else:
                    print(f"{table}: rows={rows}")
                if rows == 0:
                    empty_tables.append(table)

    if empty_tables and args.fail_on_empty:
        raise SystemExit(f"Empty index suite tables: {', '.join(empty_tables)}")


if __name__ == "__main__":
    main()
