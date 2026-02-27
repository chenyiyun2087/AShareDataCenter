#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Optional

# Add project scripts directory to sys.path to allow importing 'etl' package
scripts_dir = Path(__file__).resolve().parents[1]
project_root = scripts_dir.parent
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base.runtime import get_env_config, get_mysql_session, get_tushare_limit, get_tushare_token


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run 20:00 enhancement ETL pipeline (dividend + fina + integrity).")
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument("--token", default=None)
    parser.add_argument("--rate-limit", type=int, default=None)
    parser.add_argument(
        "--feature-apis",
        default="moneyflow,cyq_chips,cyq_perf,stk_factor",
        help="Comma-separated ODS feature APIs to enforce at 20:00.",
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
    parser.add_argument(
        "--dividend-rate-limit",
        type=int,
        default=480,
        help="Rate limit for dividend endpoint (per minute). Keep <= 500.",
    )
    parser.add_argument("--start-date", type=int, default=None)
    parser.add_argument("--end-date", type=int, default=None)
    parser.add_argument(
        "--expected-trade-date",
        type=int,
        default=None,
        help="Override expected latest trade_date (YYYYMMDD).",
    )
    parser.add_argument("--fina-start", type=int, default=None)
    parser.add_argument("--fina-end", type=int, default=None)
    parser.add_argument(
        "--lenient",
        action="store_true",
        help="Ignore missing today's data in checks (handles TuShare data lag).",
    )
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


def _latest_fina_ann_date_in_range(start_date: int, end_date: int) -> Optional[int]:
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT MAX(ann_date) FROM ods_fina_indicator WHERE ann_date BETWEEN %s AND %s",
                (start_date, end_date),
            )
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
    dividend_rate_limit = args.dividend_rate_limit
    if dividend_rate_limit <= 0:
        raise RuntimeError("dividend_rate_limit must be > 0")
    if dividend_rate_limit > 500:
        print(
            f"Warning: dividend_rate_limit={dividend_rate_limit} exceeds endpoint cap 500; clamping to 500."
        )
        dividend_rate_limit = 500

    fina_start = args.fina_start
    fina_end = args.fina_end
    if fina_start is None or fina_end is None:
        end_dt = datetime.strptime(str(end_date), "%Y%m%d")
        start_dt = end_dt - timedelta(days=7)
        if fina_start is None:
            fina_start = int(start_dt.strftime("%Y%m%d"))
        if fina_end is None:
            fina_end = end_date

    python = sys.executable
    base_cmd = [python]
    base_config = ["--config", args.config] if args.config else []
    lenient_config = base_config + (["--ignore-today"] if args.lenient else [])

    def script(rel_path: str) -> str:
        return str((project_root / rel_path).resolve())

    print(
        f"Resolved expected_trade_date={expected_trade_date}, start_date={start_date}, "
        f"end_date={end_date}, fina_start={fina_start}, fina_end={fina_end}, "
        f"rate_limit={rate_limit}, feature_rate_limit={feature_rate_limit}, "
        f"dividend_rate_limit={dividend_rate_limit}"
    )

    _run_step(
        "ODS dividend incremental",
        base_cmd
        + [
            script("scripts/sync/run_ods.py"),
            "--mode",
            "incremental",
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--dividend",
            "--only-dividend",
            "--rate-limit",
            str(dividend_rate_limit),
        ]
        + base_config
        + ["--token", token],
        args.debug,
    )

    _run_step(
        "ODS fina incremental",
        base_cmd
        + [
            script("scripts/sync/run_ods.py"),
            "--mode",
            "incremental",
            "--only-fina",
            "--fina-start",
            str(fina_start),
            "--fina-end",
            str(fina_end),
            "--rate-limit",
            str(rate_limit),
        ]
        + base_config
        + ["--token", token],
        args.debug,
    )

    _run_step(
        "DWD fina incremental",
        base_cmd
        + [
            script("scripts/sync/run_dwd.py"),
            "--mode",
            "incremental",
            "--only-fina",
            "--fina-start",
            str(fina_start),
            "--fina-end",
            str(fina_end),
        ]
        + base_config,
        args.debug,
    )

    observed_fina_ann_date = _latest_fina_ann_date_in_range(fina_start, fina_end)
    check_fina_cmd = base_cmd + [
        script("scripts/check/check_data_status.py"),
        "--categories",
        "financial",
        "--fail-on-stale",
        "--min-fina-trade-date",
        str(expected_trade_date),
    ]
    if observed_fina_ann_date is not None:
        check_fina_cmd += ["--min-fina-ann-date", str(observed_fina_ann_date)]
    check_fina_cmd += lenient_config

    _run_step(
        "Check fina status",
        check_fina_cmd,
        args.debug,
    )

    _run_step(
        "ODS features incremental",
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

    # Strict check for 20:00 retry gate: if today's feature data is not ready, fail this run.
    _run_step(
        "Check ODS features strict",
        base_cmd
        + [
            script("scripts/check/check_ods_features.py"),
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--tables",
            "ods_moneyflow,ods_cyq_chips,ods_cyq_perf,ods_stk_factor",
            "--fail-on-missing",
        ]
        + base_config,
        args.debug,
    )

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

    _run_step(
        "DWS incremental",
        base_cmd
        + [
            script("scripts/sync/run_dws.py"),
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

    # Strict end-to-end validation for 20:00 batch.
    _run_step(
        "Check DWD/DWS/ADS strict",
        base_cmd
        + [
            script("scripts/check/check_data_status.py"),
            "--categories",
            "dwd,dws,ads",
            "--fail-on-stale",
            "--expected-trade-date",
            str(expected_trade_date),
        ]
        + base_config,
        args.debug,
    )

    _run_step(
        "Data integrity overview",
        base_cmd
        + [
            script("scripts/check/inspect_data_completeness.py"),
            "--days",
            "5",
        ]
        + base_config,
        args.debug,
    )

    print("20:00 enhancement pipeline completed successfully.")


if __name__ == "__main__":
    main()
