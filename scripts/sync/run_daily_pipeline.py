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

from etl.base.runtime import get_env_config, get_mysql_session, get_tushare_limit, get_tushare_token, list_trade_dates
from utils.notify import send_imessage

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
    parser.add_argument(
        "--notify-recipient",
        default=None,
        help="iMessage recipient (phone/email) for execution summary.",
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
    with get_mysql_session(cfg) as conn:
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


def _run_step(label: str, cmd: list[str], debug: bool, stats: list = None) -> None:
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
        if stats is not None:
            stats.append({"step": label, "status": "FAILED", "duration": elapsed})
        raise
    elapsed = time.perf_counter() - start
    if debug:
        print(f"[{label}] exit code: {result.returncode}")
    print(f"[{label}] completed in {elapsed:.2f}s")
    if stats is not None:
        stats.append({"step": label, "status": "SUCCESS", "duration": elapsed})


def main() -> None:
    args = parse_args()
    _apply_config(args.config)

    # Execution stats container
    stats = []
    start_time = time.time()
    pipeline_status = "SUCCESS"
    recipient = None

    try:
        # Resolve notification recipient: Arg > Config > None
        recipient = args.notify_recipient
        if not recipient:
            try:
                # Import ConfigParser here to avoid global import if possible, or just use it
                from configparser import ConfigParser
                config_path = os.environ.get("ETL_CONFIG_PATH")
                if config_path:
                    parser = ConfigParser()
                    parser.read(config_path)
                    if parser.has_option("notification", "recipient"):
                        recipient = parser.get("notification", "recipient")
            except Exception as e:
                # We haven't enabled debug printing globally yet (it's in args), but if we get here we can print if args.debug
                if args.debug:
                     print(f"Failed to read notification config: {e}")

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

        ods_args = ["--mode", "incremental"]
        if args.start_date:
            ods_args += ["--start-date", str(args.start_date)]
        if args.end_date:
            ods_args += ["--end-date", str(args.end_date)]

        _run_step(
            "ODS incremental",
            base_cmd
            + [get_script("scripts/sync/run_ods.py")]
            + ods_args
            + ["--rate-limit", str(rate_limit)]
            + base_config
            + ["--token", token],
            args.debug,
            stats
        )
        lenient_config = base_config + (["--ignore-today"] if args.lenient else [])

        _run_step(
            "Check ODS",
            base_cmd
            + [get_script("scripts/check/check_ods.py"), "--expected-trade-date", str(expected_trade_date)]
            + lenient_config,
            args.debug,
            stats
        )

        # Determine T-1 date for Margin data (which is T+1 available)
        # This ensures that if we run today (T), we also check/backfill T-1 data which might have been missed yesterday.
        # run_ods_features has skip_existing logic, so this is efficient.
        ods_features_start_date = start_date
        if start_date:
            with get_mysql_session(get_env_config()) as conn:
                with conn.cursor() as cursor:
                     # Find dates <= start_date (limit 2 to get current and prev)
                     dates = list_trade_dates(cursor, 20100101, start_date)
                     if len(dates) >= 2:
                         prev_date = dates[-2]
                         ods_features_start_date = min(start_date, prev_date)
                         if args.debug:
                             print(f"Expanded ODS stats start_date to {ods_features_start_date} (T-1) for margin data")

        _run_step(
            "ODS features incremental",
            base_cmd
            + [
                get_script("scripts/sync/run_ods_features.py"),
                "--start-date",
                str(ods_features_start_date),
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
            stats
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
            stats
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
                stats
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
                stats
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
            stats
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
            stats
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
            stats
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
            stats
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
            stats
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
            stats
        )

        print("\nPipeline completed successfully.")

    except Exception as exc:
        pipeline_status = "FAILED"
        print(f"\nPipeline failed: {exc}")
        raise
    finally:
        ifRecipient = recipient  # Capture local variable
        if ifRecipient:
            total_duration = time.time() - start_time
            # Construct message
            from datetime import datetime
            date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
            msg_lines = [
                f"[{'✅' if pipeline_status == 'SUCCESS' else '❌'} ETL Report] {date_str}",
                f"Status: {pipeline_status}",
                f"Total Duration: {total_duration:.1f}s",
                "",
                "Steps:"
            ]
            for s in stats:
                icon = "OK" if s['status'] == 'SUCCESS' else "ERR"
                msg_lines.append(f"- {s['step']}: {icon} ({s['duration']:.1f}s)")
            
            summary = "\n".join(msg_lines)
            print(f"Sending notification to {ifRecipient}...")
            send_imessage(summary, ifRecipient)


if __name__ == "__main__":
    main()
