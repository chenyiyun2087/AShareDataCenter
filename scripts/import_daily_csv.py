from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from etl.base.runtime import (
    DEFAULT_CONFIG_PATH,
    get_env_config,
    get_mysql_connection,
    to_records,
    upsert_rows,
)


TABLE_NAME = "ods_daily"
EXPECTED_COLUMNS = [
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


def _iter_csv_files(root: Path) -> Iterable[Path]:
    for path in sorted(root.glob("*.csv")):
        if path.is_file():
            yield path


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]
    if "change_amount" in df.columns and "change" not in df.columns:
        df = df.rename(columns={"change_amount": "change"})
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"Missing required columns: {missing_str}")
    return df


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["trade_date"] = pd.to_numeric(df["trade_date"], errors="coerce").astype("Int64")
    df["ts_code"] = df["ts_code"].astype(str).str.strip()
    for col in EXPECTED_COLUMNS:
        if col in {"trade_date", "ts_code"}:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.where(pd.notnull(df), None)
    return df


def load_csv(path: Path) -> List[tuple]:
    df = pd.read_csv(path, sep="\t", encoding="utf-8-sig")
    df = _normalize_columns(df)
    df = _coerce_types(df)
    return to_records(df, EXPECTED_COLUMNS)


def import_folder(folder: Path) -> None:
    cfg = get_env_config()
    try:
        conn = get_mysql_connection(cfg)
    except RuntimeError as exc:
        raise RuntimeError(
            "Failed to connect to MySQL. Provide credentials via --host/--port/--user/--password/--database "
            "or set MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DB, or configure ETL_CONFIG_PATH."
        ) from exc
    with conn:
        with conn.cursor() as cursor:
            for csv_file in _iter_csv_files(folder):
                rows = load_csv(csv_file)
                if not rows:
                    continue
                upsert_rows(cursor, TABLE_NAME, EXPECTED_COLUMNS, rows)
                conn.commit()
                print(f"Imported {len(rows):,} rows from {csv_file.name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import daily CSV files into MySQL.")
    parser.add_argument(
        "--folder",
        default=os.environ.get("DAILY_CSV_FOLDER", "/Users/chenyiyun/Downloads/daily"),
        help="Folder containing daily CSV files (default: %(default)s).",
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("ETL_CONFIG_PATH", "config/etl.ini"),
        help="Path to etl.ini (default: %(default)s).",
    )
    parser.add_argument("--host", default=os.environ.get("MYSQL_HOST"))
    parser.add_argument("--port", type=int, default=os.environ.get("MYSQL_PORT"))
    parser.add_argument("--user", default=os.environ.get("MYSQL_USER"))
    parser.add_argument("--password", default=os.environ.get("MYSQL_PASSWORD"))
    parser.add_argument("--database", default=os.environ.get("MYSQL_DB"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    folder = Path(args.folder)
    if not folder.exists():
        raise SystemExit(f"Folder not found: {folder}")
    if args.config:
        os.environ["ETL_CONFIG_PATH"] = str(args.config)
    overrides = {
        "MYSQL_HOST": args.host,
        "MYSQL_PORT": args.port,
        "MYSQL_USER": args.user,
        "MYSQL_PASSWORD": args.password,
        "MYSQL_DB": args.database,
    }
    for key, value in overrides.items():
        if value is not None:
            os.environ[key] = str(value)
    import_folder(folder)


if __name__ == "__main__":
    main()
