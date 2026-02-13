#!/usr/bin/env python3
"""跑批看板指标与阈值报警：成功率、P95耗时、积压。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
from statistics import quantiles

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.etl.base.runtime import get_env_config, get_mysql_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="批任务SLO看板")
    parser.add_argument("--hours", type=int, default=24, help="统计窗口（小时）")
    parser.add_argument("--success-rate-threshold", type=float, default=99.0)
    parser.add_argument("--p95-threshold-sec", type=float, default=1800.0)
    parser.add_argument("--backlog-threshold", type=int, default=3, help="RUNNING积压阈值")
    return parser.parse_args()


def p95(values: list[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return quantiles(values, n=20, method="inclusive")[-1]


def main() -> int:
    args = parse_args()
    since = datetime.now() - timedelta(hours=args.hours)

    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT api_name, status,
                       TIMESTAMPDIFF(SECOND, start_at, COALESCE(end_at, NOW())) AS duration_sec
                FROM meta_etl_run_log
                WHERE start_at >= %s
                """,
                (since,),
            )
            rows = cursor.fetchall()

    total = len(rows)
    success = sum(1 for _, status, _ in rows if status == "SUCCESS")
    running = sum(1 for _, status, _ in rows if status == "RUNNING")
    durations = [float(duration) for _, status, duration in rows if status in {"SUCCESS", "FAILED"} and duration is not None]

    success_rate = (success / total * 100.0) if total else 100.0
    p95_duration = p95(durations)

    print(f"\n=== ETL SLO Dashboard ({args.hours}h) ===")
    print(f"- total_runs: {total}")
    print(f"- success_runs: {success}")
    print(f"- success_rate: {success_rate:.2f}%")
    print(f"- p95_duration_sec: {p95_duration:.1f}")
    print(f"- backlog_running: {running}")

    alerts = []
    if success_rate < args.success_rate_threshold:
        alerts.append(f"成功率告警: {success_rate:.2f}% < {args.success_rate_threshold}%")
    if p95_duration > args.p95_threshold_sec:
        alerts.append(f"耗时告警: P95 {p95_duration:.1f}s > {args.p95_threshold_sec}s")
    if running > args.backlog_threshold:
        alerts.append(f"积压告警: RUNNING {running} > {args.backlog_threshold}")

    if alerts:
        print("\n[ALERT]")
        for item in alerts:
            print(f"- {item}")
        return 2

    print("\n[OK] 所有指标在阈值内")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
