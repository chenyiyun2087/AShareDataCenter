#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Optional

scripts_dir = Path(__file__).resolve().parents[1]
project_root = scripts_dir.parent
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base.runtime import get_env_config, get_mysql_session, get_tushare_limit, get_tushare_token


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run 08:30 completion ETL pipeline (repair lagging feature sources + rebuild downstream tables)."
    )
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument("--token", default=None)
    parser.add_argument("--rate-limit", type=int, default=None)
    parser.add_argument("--expected-trade-date", type=int, default=None)
    parser.add_argument("--start-date", type=int, default=None)
    parser.add_argument("--end-date", type=int, default=None)
    parser.add_argument(
        "--feature-apis",
        default="moneyflow,moneyflow_ths,cyq_perf,stk_factor",
        help="Comma-separated ODS feature APIs to repair before DWD/DWS rebuild.",
    )
    parser.add_argument(
        "--feature-rate-limit",
        type=int,
        default=None,
        help="Rate limit for feature API calls. Defaults to --rate-limit or config default.",
    )
    parser.add_argument("--feature-cyq-rate-limit", type=int, default=180)
    parser.add_argument("--feature-cyq-chunk-days", type=int, default=5)
    parser.add_argument("--feature-stk-factor-rate-limit", type=int, default=200)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def _apply_config(config: Optional[str]) -> None:
    if not config:
        return
    config_path = Path(config).expanduser()
    if not config_path.is_absolute():
        cwd_path = (Path.cwd() / config_path).resolve()
        if cwd_path.exists():
            config_path = cwd_path
        else:
            root_path = (scripts_dir.parent / config_path).resolve()
            config_path = root_path if root_path.exists() else cwd_path
    if not config_path.exists():
        raise RuntimeError(f"config file not found: {config_path}")
    os.environ["ETL_CONFIG_PATH"] = str(config_path)


def _latest_trade_date() -> Optional[int]:
    cfg = get_env_config()
    now = datetime.now()
    today_int = int(now.strftime("%Y%m%d"))
    include_today = now.time() >= dtime(16, 0)
    op = "<=" if include_today else "<"

    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT MAX(cal_date) FROM dim_trade_cal "
                f"WHERE exchange='SSE' AND is_open=1 AND cal_date {op} %s",
                (today_int,),
            )
            row = cursor.fetchone()
            if row and row[0]:
                return int(row[0])
            cursor.execute("SELECT MAX(trade_date) FROM ods_daily")
            row = cursor.fetchone()
            if row and row[0]:
                return int(row[0])
    return None


def _run_step(label: str, cmd: list[str], debug: bool) -> None:
    print(f"\n[{label}] {' '.join(cmd)}")
    start = time.perf_counter()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(scripts_dir) + os.pathsep + env.get("PYTHONPATH", "")
    subprocess.run(cmd, check=True, env=env)
    elapsed = time.perf_counter() - start
    if debug:
        print(f"[{label}] elapsed={elapsed:.2f}s")


def _assert_table_has_date(table_name: str, trade_date: int) -> None:
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*), MAX(trade_date) FROM {table_name} WHERE trade_date=%s",
                (trade_date,),
            )
            row = cursor.fetchone()
            row_count = int(row[0]) if row and row[0] is not None else 0
            max_date = int(row[1]) if row and row[1] else None
            if row_count <= 0:
                raise RuntimeError(
                    f"{table_name} has no rows for trade_date={trade_date} "
                    f"(table max for date filter={max_date})"
                )


def main() -> None:
    args = parse_args()
    _apply_config(args.config)

    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")

    expected_trade_date = args.expected_trade_date or _latest_trade_date()
    if expected_trade_date is None:
        raise RuntimeError("failed to resolve expected trade_date")

    start_date = args.start_date or expected_trade_date
    end_date = args.end_date or expected_trade_date
    if start_date > end_date:
        raise RuntimeError("start_date cannot be greater than end_date")

    rate_limit = args.rate_limit or get_tushare_limit()
    feature_rate_limit = args.feature_rate_limit or rate_limit

    python = sys.executable
    base_cmd = [python]
    base_config = ["--config", args.config] if args.config else []

    def script(rel_path: str) -> str:
        return str((project_root / rel_path).resolve())

    print(
        f"Resolved expected_trade_date={expected_trade_date}, "
        f"start_date={start_date}, end_date={end_date}, "
        f"rate_limit={rate_limit}, feature_rate_limit={feature_rate_limit}"
    )

    # 08:30 should focus on T+1 completion; rely on prior ODS success rather than forcing refetch.
    _assert_table_has_date("ods_daily", end_date)
    _assert_table_has_date("ods_daily_basic", end_date)
    _assert_table_has_date("ods_adj_factor", end_date)

    _run_step(
        "ODS market features backfill",
        base_cmd
        + [
            script("scripts/sync/run_ods_features.py"),
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--apis",
            args.feature_apis,
            "--rate-limit",
            str(feature_rate_limit),
            "--cyq-rate-limit",
            str(args.feature_cyq_rate_limit),
            "--cyq-chunk-days",
            str(args.feature_cyq_chunk_days),
            "--stk-factor-rate-limit",
            str(args.feature_stk_factor_rate_limit),
        ]
        + base_config
        + ["--token", token],
        args.debug,
    )

    _run_step(
        "Check ODS market features",
        base_cmd
        + [
            script("scripts/check/check_ods_features.py"),
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--tables",
            "ods_moneyflow,ods_moneyflow_ths,ods_cyq_perf,ods_stk_factor",
            "--fail-on-missing",
        ]
        + base_config,
        args.debug,
    )

    _run_step(
        "ODS margin features",
        base_cmd
        + [
            script("scripts/sync/run_ods_features.py"),
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--apis",
            "margin,margin_detail",
            "--no-skip-existing",
            "--rate-limit",
            str(rate_limit),
        ]
        + base_config
        + ["--token", token],
        args.debug,
    )

    _assert_table_has_date("ods_margin", end_date)
    _assert_table_has_date("ods_margin_detail", end_date)
    _assert_table_has_date("ods_moneyflow", end_date)
    _assert_table_has_date("ods_cyq_perf", end_date)

    _run_step(
        "DWD incremental",
        base_cmd
        + [
            script("scripts/sync/run_dwd.py"),
            "--mode",
            "incremental",
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
        ]
        + base_config,
        args.debug,
    )
    _assert_table_has_date("dwd_margin_sentiment", end_date)
    _assert_table_has_date("dwd_chip_stability", end_date)

    _run_step(
        "DWS leverage sentiment",
        base_cmd
        + [
            script("scripts/sync/run_dws.py"),
            "--mode",
            "incremental",
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--only-leverage-sentiment",
        ]
        + base_config,
        args.debug,
    )

    _run_step(
        "DWS target tables (capital/chip)",
        base_cmd
        + [
            script("scripts/sync/run_dws_target_tables.py"),
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
        ]
        + base_config,
        args.debug,
    )

    _run_step(
        "DWS full status check",
        base_cmd
        + [
            script("scripts/check/check_data_status.py"),
            "--categories",
            "dws",
            "--fail-on-stale",
            "--expected-trade-date",
            str(end_date),
        ]
        + base_config,
        args.debug,
    )

    _run_step(
        "ADS incremental",
        base_cmd
        + [
            script("scripts/sync/run_ads.py"),
            "--mode",
            "incremental",
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
        ]
        + base_config,
        args.debug,
    )
    _assert_table_has_date("dws_leverage_sentiment", end_date)
    _assert_table_has_date("dws_capital_flow", end_date)
    _assert_table_has_date("dws_chip_dynamics", end_date)

    _run_step(
        "Check DWD/DWS/ADS",
        base_cmd
        + [
            script("scripts/check/check_data_status.py"),
            "--categories",
            "dwd,dws,ads",
            "--fail-on-stale",
            "--expected-trade-date",
            str(end_date),
        ]
        + base_config,
        args.debug,
    )

    print("08:30 completion pipeline completed successfully.")


if __name__ == "__main__":
    main()
