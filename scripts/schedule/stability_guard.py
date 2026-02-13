#!/usr/bin/env python3
"""带重试、超时、幂等保护的跑批执行器。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
import subprocess
import time
from datetime import datetime
from typing import Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.etl.base.runtime import get_env_config, get_mysql_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="跑批稳定性执行器")
    parser.add_argument("--task-name", required=True, help="任务名（如 ods_incremental）")
    parser.add_argument("--idempotency-key", required=True, help="幂等键（建议 task+date）")
    parser.add_argument("--retries", type=int, default=2, help="失败后重试次数")
    parser.add_argument("--retry-delay", type=int, default=120, help="重试间隔秒")
    parser.add_argument("--timeout", type=int, default=3600, help="单次执行超时秒")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="真实执行命令，使用 -- 分隔")
    return parser.parse_args()


def ensure_guard_table() -> None:
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS meta_etl_retry_guard (
                  id BIGINT NOT NULL AUTO_INCREMENT,
                  task_name VARCHAR(128) NOT NULL,
                  idempotency_key VARCHAR(128) NOT NULL,
                  status VARCHAR(16) NOT NULL,
                  attempt INT NOT NULL DEFAULT 0,
                  started_at DATETIME NOT NULL,
                  finished_at DATETIME NULL,
                  timeout_sec INT NOT NULL,
                  err_msg TEXT NULL,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (id),
                  UNIQUE KEY uk_task_key (task_name, idempotency_key),
                  KEY idx_status_started (status, started_at)
                ) ENGINE=InnoDB COMMENT='跑批重试幂等保护表'
                """
            )
            conn.commit()


def load_guard(task_name: str, idempotency_key: str):
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, status, attempt
                FROM meta_etl_retry_guard
                WHERE task_name=%s AND idempotency_key=%s
                """,
                (task_name, idempotency_key),
            )
            row = cursor.fetchone()
            return row


def upsert_guard(task_name: str, idempotency_key: str, status: str, attempt: int, timeout_sec: int, err_msg: str | None = None):
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO meta_etl_retry_guard
                  (task_name, idempotency_key, status, attempt, started_at, finished_at, timeout_sec, err_msg)
                VALUES
                  (%s, %s, %s, %s, NOW(), NULL, %s, %s)
                ON DUPLICATE KEY UPDATE
                  status=VALUES(status),
                  attempt=VALUES(attempt),
                  timeout_sec=VALUES(timeout_sec),
                  err_msg=VALUES(err_msg),
                  started_at=IF(VALUES(status)='RUNNING', NOW(), started_at),
                  finished_at=IF(VALUES(status) IN ('SUCCESS', 'FAILED'), NOW(), finished_at)
                """,
                (task_name, idempotency_key, status, attempt, timeout_sec, err_msg),
            )
            conn.commit()


def run_command(command: Sequence[str], timeout: int) -> tuple[int, str]:
    started = datetime.now()
    process = subprocess.run(command, timeout=timeout, capture_output=True, text=True)
    duration = datetime.now() - started
    output_tail = (process.stdout or "")[-1000:] + (process.stderr or "")[-1000:]
    print(f"[stability_guard] command finished in {duration}")
    return process.returncode, output_tail


def main() -> int:
    args = parse_args()
    cmd = list(args.command)
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        print("[ERROR] 缺少 command，例如: -- python scripts/sync/run_daily_pipeline.py")
        return 2

    ensure_guard_table()

    existing = load_guard(args.task_name, args.idempotency_key)
    if existing and existing[1] == "SUCCESS":
        print(
            f"[SKIP] task={args.task_name} key={args.idempotency_key} 已成功执行，"
            "命中幂等保护。"
        )
        return 0

    for attempt in range(args.retries + 1):
        upsert_guard(args.task_name, args.idempotency_key, "RUNNING", attempt, args.timeout)
        try:
            code, output_tail = run_command(cmd, timeout=args.timeout)
            if code == 0:
                upsert_guard(args.task_name, args.idempotency_key, "SUCCESS", attempt, args.timeout)
                print(f"[OK] task={args.task_name} 执行成功。")
                return 0

            err = f"exit_code={code}; output_tail={output_tail}"
            upsert_guard(args.task_name, args.idempotency_key, "FAILED", attempt, args.timeout, err)
            print(f"[WARN] attempt={attempt + 1} 失败：{err[:300]}")
        except subprocess.TimeoutExpired as exc:
            err = f"timeout_after={args.timeout}s; output_tail={(exc.stdout or '')[-800:]}{(exc.stderr or '')[-800:]}"
            upsert_guard(args.task_name, args.idempotency_key, "FAILED", attempt, args.timeout, err)
            print(f"[WARN] attempt={attempt + 1} 超时：{args.timeout}s")

        if attempt < args.retries:
            print(f"[INFO] {args.retry_delay}s 后重试...")
            time.sleep(args.retry_delay)

    print(f"[ERROR] task={args.task_name} 重试耗尽，最终失败。")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
