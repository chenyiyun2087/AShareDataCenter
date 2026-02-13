#!/usr/bin/env python3
"""批处理失败类型统计：按任务ID/错误码/模块聚合。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.etl.base.runtime import get_env_config, get_mysql_session

ERROR_CODE_RE = re.compile(r"\b([A-Z_]+\d{3,5}|ERR[_-]?\d+|\d{3,5})\b")
TASK_ID_RE = re.compile(r"\b(?:task[_-]?id|run[_-]?id|job[_-]?id)[:=\s]+([A-Za-z0-9_-]+)", re.IGNORECASE)
MODULE_RE = re.compile(r"\b(module|api|stage)[:=\s]+([A-Za-z0-9_.-]+)", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="统计批处理失败类型")
    parser.add_argument("--hours", type=int, default=24, help="统计时间窗（小时）")
    parser.add_argument("--limit", type=int, default=2000, help="最多扫描失败记录数")
    return parser.parse_args()


def extract_error_code(err_msg: Optional[str]) -> str:
    if not err_msg:
        return "UNKNOWN"
    m = ERROR_CODE_RE.search(err_msg)
    return m.group(1) if m else "UNKNOWN"


def extract_task_id(err_msg: Optional[str], fallback: str) -> str:
    if err_msg:
        m = TASK_ID_RE.search(err_msg)
        if m:
            return m.group(1)
    return fallback


def extract_module(api_name: str, err_msg: Optional[str]) -> str:
    if err_msg:
        m = MODULE_RE.search(err_msg)
        if m:
            return m.group(2)
    return api_name.split("_")[0] if "_" in api_name else api_name


def main() -> int:
    args = parse_args()
    since = datetime.now() - timedelta(hours=args.hours)

    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, api_name, err_msg
                FROM meta_etl_run_log
                WHERE status='FAILED' AND start_at >= %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (since, args.limit),
            )
            rows = cursor.fetchall()

    if not rows:
        print(f"[OK] 最近 {args.hours} 小时没有失败任务。")
        return 0

    by_task = Counter()
    by_code = Counter()
    by_module = Counter()
    matrix = defaultdict(Counter)

    for run_id, api_name, err_msg in rows:
        task_id = extract_task_id(err_msg, str(run_id))
        error_code = extract_error_code(err_msg)
        module = extract_module(api_name, err_msg)

        by_task[task_id] += 1
        by_code[error_code] += 1
        by_module[module] += 1
        matrix[module][error_code] += 1

    print(f"\n=== 失败统计（最近 {args.hours}h，共 {len(rows)} 条）===")

    print("\n[按任务ID Top10]")
    for task_id, count in by_task.most_common(10):
        print(f"- task_id={task_id:>12}  failures={count}")

    print("\n[按错误码 Top10]")
    for code, count in by_code.most_common(10):
        print(f"- error_code={code:>10}  failures={count}")

    print("\n[按模块 Top10]")
    for module, count in by_module.most_common(10):
        print(f"- module={module:>12}  failures={count}")

    print("\n[模块 x 错误码 Top20]")
    pairs = []
    for module, code_counter in matrix.items():
        for code, count in code_counter.items():
            pairs.append((count, module, code))
    for count, module, code in sorted(pairs, reverse=True)[:20]:
        print(f"- module={module:>12} | error_code={code:>10} | failures={count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
