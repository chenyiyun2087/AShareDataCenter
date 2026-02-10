#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# Add project scripts directory to sys.path to allow importing 'etl' package
scripts_dir = Path(__file__).resolve().parents[1]
project_root = scripts_dir.parent
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base.runtime import get_env_config, get_mysql_connection, get_tushare_limit, get_tushare_token


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily incremental ETL pipeline with checks.")
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument("--token", default=None)
    parser.add_argument("--rate-limit", type=int, default=None)
    parser.add_argument("--start-date", type=int, default=None)
    parser.add_argument("--end-date", type=int, default=None)
    parser.add_argument("--fina-start", type=int, default=None)
    parser.add_argument("--fina-end", type=int, default=None)
    parser.add_argument("--cyq-rate-limit", type=int, default=180)
    parser.add_argument("--cyq-chunk-days", type=int, default=5)
    parser.add_argument(
        "--expected-trade-date",
        type=int,
        default=None,
        help="Override expected latest trade_date (YYYYMMDD).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug output for each step.",
    )
    parser.add_argument(
        "--lenient",
        action="store_true",
        help="Ignore missing today's data in checks (handles TuShare data lag).",
    )
    return parser.parse_args()


def _apply_config(config: Optional[str]) -> None:
    if not config:
        return
    config_path = Path(config).expanduser()
    if not config_path.is_absolute():
        # Try relative to CWD first
        cwd_path = (Path.cwd() / config_path).resolve()
        if cwd_path.exists():
            config_path = cwd_path
        else:
            # Fallback to project root
            root_path = (scripts_dir.parent / config_path).resolve()
            if root_path.exists():
                config_path = root_path
            else:
                config_path = cwd_path # Let it fail with CWD path if both missing

    if not config_path.exists():
        raise RuntimeError(f"config file not found: {config_path}")
    os.environ["ETL_CONFIG_PATH"] = str(config_path)


def _latest_trade_date() -> Optional[int]:
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT MAX(cal_date) FROM dim_trade_cal "
                "WHERE exchange='SSE' AND is_open=1 "
                "AND cal_date <= DATE_FORMAT(CURDATE(), '%Y%m%d')"
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
    # Ensure subprocess can find 'etl' package
    env = os.environ.copy()
    env["PYTHONPATH"] = str(scripts_dir) + os.pathsep + env.get("PYTHONPATH", "")
    
    try:
        result = subprocess.run(cmd, check=True, env=env)
    except subprocess.CalledProcessError as exc:
        elapsed = time.perf_counter() - start
        print(f"[{label}] failed in {elapsed:.2f}s with exit code {exc.returncode}")
        raise
    elapsed = time.perf_counter() - start
    if debug:
        print(f"[{label}] exit code: {result.returncode}")
    print(f"[{label}] completed in {elapsed:.2f}s")


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

    rate_limit = args.rate_limit or get_tushare_limit()
    python = sys.executable
    base_cmd = [python]
    if args.config:
        base_config = ["--config", args.config]
    else:
        base_config = []

    if args.debug:
        print("Debug mode enabled")
        print(f"Resolved expected_trade_date={expected_trade_date}")
        print(f"Resolved start_date={start_date} end_date={end_date}")
        print(f"Resolved rate_limit={rate_limit}")

    def get_script(rel_path: str) -> str:
        return str((project_root / rel_path).resolve())

    _run_step(
        "ODS incremental",
        base_cmd
        + [get_script("scripts/sync/run_ods.py"), "--mode", "incremental", "--rate-limit", str(rate_limit)]
        + base_config
        + ["--token", token],
        args.debug,
    )
    lenient_config = base_config + (["--ignore-today"] if args.lenient else [])

    _run_step(
        "Check ODS",
        base_cmd
        + [get_script("scripts/check/check_ods.py"), "--expected-trade-date", str(expected_trade_date)]
        + base_config,
        args.debug,
    )

    _run_step(
        "ODS features incremental",
        base_cmd
        + [
            get_script("scripts/sync/run_ods_features.py"),
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--rate-limit",
            str(rate_limit),
            "--cyq-rate-limit",
            str(args.cyq_rate_limit),
            "--cyq-chunk-days",
            str(args.cyq_chunk_days),
        ]
        + base_config
        + ["--token", token],
        args.debug,
    )
    _run_step(
        "Check ODS features",
        base_cmd
        + [
            get_script("scripts/check/check_ods_features.py"),
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--fail-on-missing",
        ]
        + lenient_config,
        args.debug,
    )

    if args.fina_start and args.fina_end:
        _run_step(
            "ODS fina incremental",
            base_cmd
            + [
                get_script("scripts/sync/run_ods.py"),
                "--mode",
                "incremental",
                "--fina-start",
                str(args.fina_start),
                "--fina-end",
                str(args.fina_end),
                "--rate-limit",
                str(rate_limit),
            ]
            + base_config
            + ["--token", token],
            args.debug,
        )
        _run_step(
            "Check fina status",
            base_cmd
            + [
                get_script("scripts/check/check_data_status.py"),
                "--categories",
                "financial",
                "--fail-on-stale",
                "--min-fina-ann-date",
                str(args.fina_end),
            ]
            + lenient_config,
            args.debug,
        )

    # 5. DWD incremental
    dwd_args = ["--mode", "incremental"]
    if args.start_date:
        dwd_args += ["--start-date", str(args.start_date)]
    if args.end_date:
        dwd_args += ["--end-date", str(args.end_date)]

    _run_step(
        "DWD incremental",
        base_cmd + [get_script("scripts/sync/run_dwd.py")] + dwd_args + base_config,
        args.debug,
    )

    _run_step(
        "Check DWD",
        base_cmd
        + [
            get_script("scripts/check/check_data_status.py"),
            "--categories",
            "dwd",
            "--fail-on-stale",
            "--expected-trade-date",
            str(expected_trade_date),
        ]
        + lenient_config,
        args.debug,
    )

    # 6. DWS incremental
    dws_args = ["--mode", "incremental"]
    if args.start_date:
        dws_args += ["--start-date", str(args.start_date)]
    if args.end_date:
        dws_args += ["--end-date", str(args.end_date)]

    _run_step(
        "DWS incremental",
        base_cmd + [get_script("scripts/sync/run_dws.py")] + dws_args + base_config,
        args.debug,
    )

    _run_step(
        "Check DWS",
        base_cmd
        + [
            get_script("scripts/check/check_data_status.py"),
            "--categories",
            "dws",
            "--fail-on-stale",
            "--expected-trade-date",
            str(expected_trade_date),
        ]
        + lenient_config,
        args.debug,
    )

    # 7. ADS incremental
    ads_args = ["--mode", "incremental"]
    if args.start_date:
        ads_args += ["--start-date", str(args.start_date)]
    if args.end_date:
        ads_args += ["--end-date", str(args.end_date)]

    _run_step(
        "ADS incremental",
        base_cmd + [get_script("scripts/sync/run_ads.py")] + ads_args + base_config,
        args.debug,
    )

    _run_step(
        "Check ADS",
        base_cmd
        + [
            get_script("scripts/check/check_data_status.py"),
            "--categories",
            "ads",
            "--fail-on-stale",
            "--expected-trade-date",
            str(expected_trade_date),
        ]
        + lenient_config,
        args.debug,
    )

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
