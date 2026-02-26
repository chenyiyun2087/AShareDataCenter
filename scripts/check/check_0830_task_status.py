#!/usr/bin/env python3
from __future__ import annotations

import argparse
import configparser
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pymysql


SUCCESS_MARKER = "SUCCESS: Completed in "
FAILED_MARKER = "Max retries reached. Task failed."


@dataclass
class RunStatus:
    target_date: str
    state: str
    reason: str
    log_file: str
    log_start_line: int | None
    success_line: int | None
    failed_line: int | None
    active_process_count: int
    active_processes: list[str]
    latest_log_timestamp: str | None
    db_running_count: int | None
    db_latest_rows: list[dict[str, Any]] | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check whether the 08:30 cron task has completed.")
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Target date in YYYY-MM-DD format. Default: today.",
    )
    parser.add_argument(
        "--log-file",
        default="logs/cron_0830.log",
        help="Path to the 08:30 cron log file.",
    )
    parser.add_argument(
        "--config",
        default="config/etl.ini",
        help="Path to etl.ini used for DB status checks.",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip DB checks and only rely on log + process.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON.",
    )
    parser.add_argument(
        "--running-grace-minutes",
        type=int,
        default=30,
        help="If no terminal marker, infer RUNNING when latest log activity is within this window.",
    )
    return parser.parse_args()


def _resolve_path(path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    return (Path.cwd() / path).resolve()


def _find_log_segment(lines: list[str], target_date: str) -> tuple[int | None, int | None, int | None]:
    start_line_no: int | None = None
    success_line_no: int | None = None
    failed_line_no: int | None = None

    start_re = re.compile(rf"^{re.escape(target_date)} 08:30:\d{{2}},\d+ ")
    for idx, line in enumerate(lines, start=1):
        if start_re.match(line):
            start_line_no = idx
            break

    if start_line_no is None:
        return None, None, None

    for idx in range(start_line_no - 1, len(lines)):
        line = lines[idx]
        line_no = idx + 1
        if SUCCESS_MARKER in line:
            success_line_no = line_no
        if FAILED_MARKER in line:
            failed_line_no = line_no

    return start_line_no, success_line_no, failed_line_no


def _extract_latest_timestamp(lines: list[str], start_line_no: int | None) -> datetime | None:
    if start_line_no is None:
        return None

    ts_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:,\d+)?\b")
    latest: datetime | None = None
    for line in lines[start_line_no - 1 :]:
        matched = ts_re.match(line)
        if not matched:
            continue
        try:
            latest = datetime.strptime(matched.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return latest


def _detect_active_processes() -> list[str]:
    try:
        proc = subprocess.run(["ps", "-ef"], capture_output=True, text=True, check=False)
    except Exception:
        return []
    if proc.returncode != 0:
        return []

    matches: list[str] = []
    for line in proc.stdout.splitlines():
        if "check_0830_task_status.py" in line:
            continue
        if "cron_0830.log" in line and "run_with_retry.py" in line:
            matches.append(line.strip())
            continue
        if "scripts/sync/run_daily_pipeline.py" in line and "--config config/etl.ini" in line:
            matches.append(line.strip())
            continue
        if "scripts/sync/run_0830_pipeline.py" in line and "--config config/etl.ini" in line:
            matches.append(line.strip())
            continue
    return matches


def _load_mysql_config(config_path: Path) -> dict[str, Any]:
    parser = configparser.ConfigParser()
    parser.read(config_path)
    return {
        "host": parser.get("mysql", "host", fallback="127.0.0.1"),
        "port": parser.getint("mysql", "port", fallback=3306),
        "user": parser.get("mysql", "user", fallback="root"),
        "password": parser.get("mysql", "password", fallback=""),
        "database": parser.get("mysql", "database", fallback="tushare_stock"),
    }


def _fetch_db_rows(config_path: Path, target_date: str) -> tuple[int | None, list[dict[str, Any]] | None]:
    try:
        cfg = _load_mysql_config(config_path)
        conn = pymysql.connect(
            host=cfg["host"],
            port=int(cfg["port"]),
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
    except Exception:
        return None, None

    try:
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        start_ts = f"{target_date} 08:30:00"
        end_ts = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d 17:00:00")
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, api_name, run_type, status, start_at, end_at
                FROM meta_etl_run_log
                WHERE start_at >= %s AND start_at < %s
                ORDER BY id DESC
                LIMIT 20
                """,
                (start_ts, end_ts),
            )
            rows = cursor.fetchall()
    finally:
        conn.close()

    running_count = sum(1 for row in rows if str(row.get("status")) == "RUNNING")
    for row in rows:
        for key in ("start_at", "end_at"):
            value = row.get(key)
            if isinstance(value, datetime):
                row[key] = value.strftime("%Y-%m-%d %H:%M:%S")
    return running_count, rows


def _decide_state(
    *,
    start_line: int | None,
    success_line: int | None,
    failed_line: int | None,
    active_processes: list[str],
    latest_log_ts: datetime | None,
    running_grace_minutes: int,
) -> tuple[str, str]:
    if start_line is None:
        return "NOT_TRIGGERED", "No 08:30 log entry found for target date."

    if success_line and (not failed_line or success_line > failed_line):
        return "COMPLETED", "Found success marker after the 08:30 run started."

    if failed_line and (not success_line or failed_line > success_line):
        return "FAILED", "Found terminal failure marker for the 08:30 run."

    if active_processes:
        return "RUNNING", "No terminal marker yet, and related pipeline process is active."

    if latest_log_ts:
        elapsed = datetime.now() - latest_log_ts
        if elapsed <= timedelta(minutes=max(1, running_grace_minutes)):
            return "RUNNING", "No terminal marker yet; inferred RUNNING from recent log activity."

    return "UNKNOWN", "Run started but no terminal marker and no active process."


def _print_text(result: RunStatus) -> None:
    print(f"target_date: {result.target_date}")
    print(f"state: {result.state}")
    print(f"reason: {result.reason}")
    print(f"log_file: {result.log_file}")
    print(f"log_start_line: {result.log_start_line}")
    print(f"success_line: {result.success_line}")
    print(f"failed_line: {result.failed_line}")
    print(f"active_process_count: {result.active_process_count}")
    if result.active_processes:
        print("active_processes:")
        for line in result.active_processes:
            print(f"  - {line}")
    print(f"latest_log_timestamp: {result.latest_log_timestamp}")
    if result.db_running_count is not None:
        print(f"db_running_count: {result.db_running_count}")
    if result.db_latest_rows:
        print("db_latest_rows:")
        for row in result.db_latest_rows[:10]:
            print(
                "  - "
                f"id={row.get('id')}, api={row.get('api_name')}, type={row.get('run_type')}, "
                f"status={row.get('status')}, start={row.get('start_at')}, end={row.get('end_at')}"
            )


def main() -> int:
    args = parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid --date: {args.date}. Expected YYYY-MM-DD.", file=sys.stderr)
        return 2

    log_file = _resolve_path(args.log_file)
    if not log_file.exists():
        result = RunStatus(
            target_date=args.date,
            state="NOT_TRIGGERED",
            reason="Log file does not exist.",
            log_file=str(log_file),
            log_start_line=None,
            success_line=None,
            failed_line=None,
            active_process_count=0,
            active_processes=[],
            latest_log_timestamp=None,
            db_running_count=None,
            db_latest_rows=None,
        )
        if args.json:
            print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        else:
            _print_text(result)
        return 1

    lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
    start_line, success_line, failed_line = _find_log_segment(lines, args.date)
    latest_ts = _extract_latest_timestamp(lines, start_line)
    active_processes = _detect_active_processes()
    state, reason = _decide_state(
        start_line=start_line,
        success_line=success_line,
        failed_line=failed_line,
        active_processes=active_processes,
        latest_log_ts=latest_ts,
        running_grace_minutes=args.running_grace_minutes,
    )

    db_running_count: int | None = None
    db_rows: list[dict[str, Any]] | None = None
    if not args.skip_db:
        db_cfg = _resolve_path(args.config)
        db_running_count, db_rows = _fetch_db_rows(db_cfg, args.date)

    result = RunStatus(
        target_date=args.date,
        state=state,
        reason=reason,
        log_file=str(log_file),
        log_start_line=start_line,
        success_line=success_line,
        failed_line=failed_line,
        active_process_count=len(active_processes),
        active_processes=active_processes,
        latest_log_timestamp=latest_ts.strftime("%Y-%m-%d %H:%M:%S") if latest_ts else None,
        db_running_count=db_running_count,
        db_latest_rows=db_rows,
    )

    if args.json:
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    else:
        _print_text(result)

    if result.state == "COMPLETED":
        return 0
    if result.state in {"RUNNING", "UNKNOWN"}:
        return 3
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
