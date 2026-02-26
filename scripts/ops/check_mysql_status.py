#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from configparser import ConfigParser
from pathlib import Path

import pymysql


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check MySQL health and potential connection leaks.")
    parser.add_argument(
        "--config",
        default="config/etl.ini",
        help="Path to etl.ini (default: config/etl.ini)",
    )
    parser.add_argument(
        "--sleep-threshold-sec",
        type=int,
        default=300,
        help="Threshold for long sleep connections (default: 300s)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output.",
    )
    parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        help="Exit non-zero if warning conditions are met.",
    )
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


def main() -> int:
    args = parse_args()
    db = load_db_config(args.config)

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
            cursor.execute(
                """
                SHOW GLOBAL STATUS
                WHERE Variable_name IN (
                    'Threads_connected',
                    'Threads_running',
                    'Threads_created',
                    'Connections',
                    'Aborted_clients',
                    'Aborted_connects',
                    'Max_used_connections',
                    'Uptime'
                )
                """
            )
            status_rows = cursor.fetchall()

            cursor.execute(
                """
                SHOW GLOBAL VARIABLES
                WHERE Variable_name IN (
                    'max_connections',
                    'wait_timeout',
                    'interactive_timeout',
                    'thread_cache_size'
                )
                """
            )
            var_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 120) AS INFO
                FROM INFORMATION_SCHEMA.PROCESSLIST
                ORDER BY TIME DESC
                LIMIT 30
                """
            )
            process_rows = cursor.fetchall()

            cursor.execute(
                "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.PROCESSLIST WHERE COMMAND='Sleep' AND TIME > %s",
                (args.sleep_threshold_sec,),
            )
            sleep_over_threshold = int(cursor.fetchone()["c"])

            cursor.execute(
                "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.PROCESSLIST WHERE COMMAND='Sleep' AND TIME > 1800"
            )
            sleep_over_30m = int(cursor.fetchone()["c"])

            cursor.execute(
                "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.PROCESSLIST WHERE COMMAND != 'Sleep'"
            )
            non_sleep_processes = int(cursor.fetchone()["c"])

        status = {row["Variable_name"]: int(row["Value"]) for row in status_rows}
        variables = {row["Variable_name"]: int(row["Value"]) for row in var_rows}

        max_connections = max(1, variables.get("max_connections", 1))
        threads_connected = status.get("Threads_connected", 0)
        connection_usage_pct = round((threads_connected / max_connections) * 100, 2)

        warnings: list[str] = []
        if connection_usage_pct >= 80:
            warnings.append(f"High connection usage: {connection_usage_pct}%")
        if sleep_over_threshold > 20:
            warnings.append(
                f"Too many long Sleep sessions > {args.sleep_threshold_sec}s: {sleep_over_threshold}"
            )
        if sleep_over_30m > 5:
            warnings.append(f"Potential leaked sessions > 1800s: {sleep_over_30m}")

        payload = {
            "mysql": {
                "host": db["host"],
                "port": db["port"],
                "database": db["database"],
            },
            "status": status,
            "variables": variables,
            "connection_usage_pct": connection_usage_pct,
            "sleep_over_threshold": sleep_over_threshold,
            "sleep_over_30m": sleep_over_30m,
            "non_sleep_processes": non_sleep_processes,
            "processlist_top30": process_rows,
            "warnings": warnings,
            "healthy": len(warnings) == 0,
        }

        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        else:
            print("MySQL status:")
            print(
                f"- host={db['host']}:{db['port']} db={db['database']} "
                f"threads_connected={threads_connected}/{max_connections} ({connection_usage_pct}%)"
            )
            print(
                f"- sleep>{args.sleep_threshold_sec}s={sleep_over_threshold}, "
                f"sleep>1800s={sleep_over_30m}, non_sleep={non_sleep_processes}"
            )
            print(f"- aborted_clients={status.get('Aborted_clients', 0)}, aborted_connects={status.get('Aborted_connects', 0)}")
            print(f"- max_used_connections={status.get('Max_used_connections', 0)}, uptime={status.get('Uptime', 0)}s")
            if warnings:
                print("Warnings:")
                for w in warnings:
                    print(f"  - {w}")
            else:
                print("No obvious connection leak symptoms.")

        if args.fail_on_warn and warnings:
            return 2
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

