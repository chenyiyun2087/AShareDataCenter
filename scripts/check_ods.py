#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

from etl.base.runtime import get_env_config, get_mysql_connection


DEFAULT_EXPECTED_TRADE_DATE = 20260206


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate ODS load completeness")
    parser.add_argument(
        "--expected-trade-date",
        type=int,
        default=DEFAULT_EXPECTED_TRADE_DATE,
        help="Expected latest trade date (YYYYMMDD)",
    )
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--database", default=None)
    return parser.parse_args()


def apply_env_overrides(args: argparse.Namespace) -> None:
    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            config_path = (Path.cwd() / config_path).resolve()
        if not config_path.exists():
            raise RuntimeError(f"config file not found: {config_path}")
        os.environ["ETL_CONFIG_PATH"] = str(config_path)
    if args.host:
        os.environ["MYSQL_HOST"] = args.host
    if args.port is not None:
        os.environ["MYSQL_PORT"] = str(args.port)
    if args.user:
        os.environ["MYSQL_USER"] = args.user
    if args.password:
        os.environ["MYSQL_PASSWORD"] = args.password
    if args.database:
        os.environ["MYSQL_DB"] = args.database


def fetch_single_value(cursor, sql: str, params: tuple = ()) -> int | None:
    cursor.execute(sql, params)
    row = cursor.fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


def main() -> None:
    args = parse_args()
    apply_env_overrides(args)
    cfg = get_env_config()

    checks = []
    expected_date = args.expected_trade_date

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            latest_trade_date = fetch_single_value(
                cursor,
                "SELECT MAX(cal_date) FROM dim_trade_cal WHERE exchange='SSE' AND is_open=1",
            )
            checks.append(("dim_trade_cal", latest_trade_date))

            ods_daily = fetch_single_value(cursor, "SELECT MAX(trade_date) FROM ods_daily")
            ods_daily_basic = fetch_single_value(cursor, "SELECT MAX(trade_date) FROM ods_daily_basic")
            ods_adj_factor = fetch_single_value(cursor, "SELECT MAX(trade_date) FROM ods_adj_factor")
            ods_fina_indicator = fetch_single_value(cursor, "SELECT MAX(end_date) FROM ods_fina_indicator")

            checks.extend(
                [
                    ("ods_daily.trade_date", ods_daily),
                    ("ods_daily_basic.trade_date", ods_daily_basic),
                    ("ods_adj_factor.trade_date", ods_adj_factor),
                    ("ods_fina_indicator.end_date", ods_fina_indicator),
                ]
            )

    print(f"Expected latest trade date: {expected_date}")
    print("Latest dates:")
    failures = []
    for label, value in checks:
        print(f"  - {label}: {value}")
        if label == "ods_fina_indicator.end_date":
            continue
        if label == "dim_trade_cal":
            if value is None or value < expected_date:
                failures.append(label)
            continue
        if value != expected_date:
            failures.append(label)

    if failures:
        print("FAILED: ODS tables not up to expected trade date.")
        for label in failures:
            if label == "dim_trade_cal":
                print(f"  * {label} is behind {expected_date}")
            else:
                print(f"  * {label} does not match {expected_date}")
        raise SystemExit(1)

    print("SUCCESS: ODS tables match expected trade date.")


if __name__ == "__main__":
    main()
