#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path

import pymysql


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check ODS/DWD/DWS/ADS component status.")
    parser.add_argument("--config", default="config/etl.ini", help="Path to etl.ini")
    parser.add_argument(
        "--expected-trade-date",
        type=int,
        default=None,
        help="Expected latest trade date YYYYMMDD. Default: latest open date in dim_trade_cal.",
    )
    parser.add_argument(
        "--strict-financial-ann-date",
        action="store_true",
        help="Apply expected date check on ann_date-based financial tables.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    parser.add_argument("--fail-on-issues", action="store_true", help="Exit non-zero on stale/empty status.")
    return parser.parse_args()


def load_db_config(config_path: str) -> dict:
    cfg = ConfigParser()
    path = Path(config_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"config not found: {path}")
    cfg.read(path)
    return {
        "host": cfg.get("mysql", "host", fallback="127.0.0.1"),
        "port": cfg.getint("mysql", "port", fallback=3306),
        "user": cfg.get("mysql", "user", fallback="root"),
        "password": cfg.get("mysql", "password", fallback=""),
        "database": cfg.get("mysql", "database", fallback="tushare_stock"),
    }


def resolve_expected_trade_date(cursor, expected_trade_date: int | None) -> int:
    if expected_trade_date:
        return expected_trade_date
    cursor.execute(
        """
        SELECT MAX(cal_date) AS d
        FROM dim_trade_cal
        WHERE exchange='SSE' AND is_open=1
          AND cal_date <= DATE_FORMAT(CURDATE(), '%Y%m%d')
        """
    )
    row = cursor.fetchone()
    if not row or not row["d"]:
        raise RuntimeError("cannot resolve expected trade date from dim_trade_cal")
    return int(row["d"])


def table_status(cursor, table: str, date_col: str, threshold: int | None) -> dict:
    cursor.execute(f"SELECT MAX({date_col}) AS max_date, COUNT(*) AS rows_cnt, MAX(updated_at) AS updated_at FROM {table}")
    row = cursor.fetchone()
    max_date = int(row["max_date"]) if row["max_date"] is not None else None
    rows_cnt = int(row["rows_cnt"] or 0)
    updated_at = row["updated_at"].isoformat(sep=" ") if row["updated_at"] else None
    if rows_cnt == 0 or max_date is None:
        status = "EMPTY"
    elif threshold is None:
        status = "INFO"
    elif max_date >= threshold:
        status = "OK"
    else:
        status = "STALE"
    return {
        "table": table,
        "date_col": date_col,
        "max_date": max_date,
        "rows_cnt": rows_cnt,
        "updated_at": updated_at,
        "threshold": threshold,
        "status": status,
    }


def main() -> int:
    args = parse_args()
    db = load_db_config(args.config)

    layers: dict[str, list[tuple[str, str]]] = {
        "ODS": [
            ("ods_daily", "trade_date"),
            ("ods_daily_basic", "trade_date"),
            ("ods_adj_factor", "trade_date"),
            ("ods_fina_indicator", "ann_date"),
        ],
        "DWD": [
            ("dwd_daily", "trade_date"),
            ("dwd_daily_basic", "trade_date"),
            ("dwd_adj_factor", "trade_date"),
            ("dwd_stock_daily_standard", "trade_date"),
            ("dwd_fina_snapshot", "trade_date"),
            ("dwd_fina_indicator", "ann_date"),
        ],
        "DWS": [
            ("dws_price_adj_daily", "trade_date"),
            ("dws_fina_pit_daily", "trade_date"),
            ("dws_tech_pattern", "trade_date"),
            ("dws_capital_flow", "trade_date"),
            ("dws_chip_dynamics", "trade_date"),
        ],
        "ADS": [
            ("ads_features_stock_daily", "trade_date"),
            ("ads_universe_daily", "trade_date"),
            ("ads_stock_score_daily", "trade_date"),
        ],
    }

    conn = pymysql.connect(
        host=db["host"],
        port=db["port"],
        user=db["user"],
        password=db["password"],
        database=db["database"],
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cursor:
            expected_trade_date = resolve_expected_trade_date(cursor, args.expected_trade_date)
            result: dict = {
                "checked_at": datetime.now().isoformat(sep=" ", timespec="seconds"),
                "expected_trade_date": expected_trade_date,
                "layers": {},
                "issues": [],
            }

            for layer_name, tables in layers.items():
                layer_items = []
                for table, date_col in tables:
                    threshold = expected_trade_date
                    if date_col == "ann_date" and not args.strict_financial_ann_date:
                        threshold = None
                    item = table_status(cursor, table, date_col, threshold)
                    layer_items.append(item)
                    if item["status"] in {"EMPTY", "STALE"}:
                        result["issues"].append(item)
                result["layers"][layer_name] = layer_items

            result["healthy"] = len(result["issues"]) == 0

        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        else:
            print(
                f"Component status (expected_trade_date={result['expected_trade_date']}, "
                f"strict_financial_ann_date={args.strict_financial_ann_date})"
            )
            for layer_name, items in result["layers"].items():
                print(f"\n[{layer_name}]")
                for item in items:
                    print(
                        f"- {item['table']}: max={item['max_date']} rows={item['rows_cnt']} "
                        f"status={item['status']} threshold={item['threshold']}"
                    )
            if result["issues"]:
                print("\nIssues:")
                for item in result["issues"]:
                    print(f"- {item['table']} ({item['status']})")
            else:
                print("\nAll checked components are healthy.")

        if args.fail_on_issues and result["issues"]:
            return 2
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

