#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

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
    return parser.parse_args()


def _apply_config(config: Optional[str]) -> None:
    if not config:
        return
    config_path = Path(config).expanduser()
    if not config_path.is_absolute():
        config_path = (Path.cwd() / config_path).resolve()
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


def _run_step(label: str, cmd: list[str]) -> None:
    print(f"\n[{label}] {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


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

    _run_step(
        "ODS incremental",
        base_cmd
        + ["scripts/run_ods.py", "--mode", "incremental", "--rate-limit", str(rate_limit)]
        + base_config
        + ["--token", token],
    )
    _run_step(
        "Check ODS",
        base_cmd
        + ["scripts/check_ods.py", "--expected-trade-date", str(expected_trade_date)]
        + base_config,
    )

    _run_step(
        "ODS features incremental",
        base_cmd
        + [
            "scripts/run_ods_features.py",
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
    )
    _run_step(
        "Check ODS features",
        base_cmd
        + [
            "scripts/check_ods_features.py",
            "--start-date",
            str(start_date),
            "--end-date",
            str(end_date),
            "--fail-on-missing",
        ]
        + base_config,
    )

    if args.fina_start and args.fina_end:
        _run_step(
            "ODS fina incremental",
            base_cmd
            + [
                "scripts/run_ods.py",
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
        )
        _run_step(
            "Check fina status",
            base_cmd
            + [
                "scripts/check_data_status.py",
                "--categories",
                "financial",
                "--fail-on-stale",
                "--min-fina-ann-date",
                str(args.fina_end),
            ]
            + base_config,
        )

    _run_step("DWD incremental", base_cmd + ["scripts/run_dwd.py", "--mode", "incremental"] + base_config)
    _run_step(
        "Check DWD",
        base_cmd
        + [
            "scripts/check_data_status.py",
            "--categories",
            "dwd",
            "--fail-on-stale",
            "--expected-trade-date",
            str(expected_trade_date),
        ]
        + base_config,
    )

    _run_step("DWS incremental", base_cmd + ["scripts/run_dws.py", "--mode", "incremental"] + base_config)
    _run_step(
        "Check DWS",
        base_cmd
        + [
            "scripts/check_data_status.py",
            "--categories",
            "dws",
            "--fail-on-stale",
            "--expected-trade-date",
            str(expected_trade_date),
        ]
        + base_config,
    )

    _run_step("ADS incremental", base_cmd + ["scripts/run_ads.py", "--mode", "incremental"] + base_config)
    _run_step(
        "Check ADS",
        base_cmd
        + [
            "scripts/check_data_status.py",
            "--categories",
            "ads",
            "--fail-on-stale",
            "--expected-trade-date",
            str(expected_trade_date),
        ]
        + base_config,
    )

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
