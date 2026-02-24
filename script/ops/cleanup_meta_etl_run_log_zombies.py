#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter
from configparser import ConfigParser
from pathlib import Path

import pymysql


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cleanup stale RUNNING rows in meta_etl_run_log.")
    parser.add_argument("--config", default="config/etl.ini", help="Path to etl.ini")
    parser.add_argument(
        "--threshold-minutes",
        type=int,
        default=120,
        help="Mark RUNNING rows older than this as FAILED (default: 120).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually update rows. Default is dry-run.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max rows to update (0 means no limit).",
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
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT id, api_name, run_type, start_at,
                       TIMESTAMPDIFF(MINUTE, start_at, NOW()) AS age_min
                FROM meta_etl_run_log
                WHERE status='RUNNING'
                  AND start_at < NOW() - INTERVAL %s MINUTE
                ORDER BY id
            """
            cursor.execute(sql, (args.threshold_minutes,))
            rows = cursor.fetchall()
            if args.limit > 0:
                rows = rows[: args.limit]

            print(f"stale_running_rows={len(rows)} threshold_minutes={args.threshold_minutes}")
            if not rows:
                conn.rollback()
                return 0

            by_api = Counter(r["api_name"] for r in rows)
            print(f"by_api={dict(by_api)}")
            print("sample_first_10:")
            for r in rows[:10]:
                print(r)

            if not args.apply:
                print("dry-run only. use --apply to update.")
                conn.rollback()
                return 0

            ids = [r["id"] for r in rows]
            placeholders = ",".join(["%s"] * len(ids))
            mark = f"[AUTO_CLEANUP {args.threshold_minutes}m stale RUNNING at SQL NOW()]"
            update_sql = f"""
                UPDATE meta_etl_run_log
                SET status='FAILED',
                    end_at=COALESCE(end_at, NOW()),
                    err_msg=CASE
                        WHEN err_msg IS NULL OR err_msg='' THEN %s
                        ELSE CONCAT(err_msg, ' ', %s)
                    END
                WHERE id IN ({placeholders})
            """
            cursor.execute(update_sql, [mark, mark, *ids])
            print(f"updated_rows={cursor.rowcount}")
            conn.commit()

            cursor.execute("SELECT COUNT(*) AS c FROM meta_etl_run_log WHERE status='RUNNING'")
            print(f"running_after={cursor.fetchone()['c']}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

