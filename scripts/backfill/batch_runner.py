#!/usr/bin/env python3
import argparse
import subprocess
import sys
from datetime import datetime
from typing import List

def run_command(cmd: List[str], description: str) -> None:
    print(f"Starting: {description}")
    print(f"Command: {' '.join(cmd)}")
    start_time = datetime.now()
    try:
        subprocess.check_call(cmd)
        duration = datetime.now() - start_time
        print(f"Completed: {description} in {duration}")
    except subprocess.CalledProcessError as e:
        print(f"Failed: {description} with error {e}")
        # Decide whether to stop or continue. For backfill, stopping is usually safer to diagnose.
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Batch Backfill Runner")
    parser.add_argument("--start-year", type=int, required=True, help="Start year (e.g., 2010)")
    parser.add_argument("--end-year", type=int, required=True, help="End year (e.g., 2019)")
    parser.add_argument("--layer", choices=["ods", "dwd", "dws", "ads", "all"], required=True)
    parser.add_argument("--config", default="config/etl.ini")
    args = parser.parse_args()

    for year in range(args.start_year, args.end_year + 1):
        start_date = f"{year}0101"
        end_date = f"{year}1231"
        print(f"\n=== Processing Year {year} ({start_date}-{end_date}) ===")

        # ODS Layer
        if args.layer in ["ods", "all"]:
            # Financial Indicators
            run_command(
                [sys.executable, "scripts/sync/run_ods.py", "--table", "ods_fina_indicator", "--start-date", start_date, "--end-date", end_date, "--config", args.config],
                f"ODS Fina Indicator {year}"
            )
            # Moneyflow & Margin & Target
            run_command(
                [sys.executable, "scripts/sync/run_ods_features.py", "--apis", "moneyflow,margin_detail,margin,margin_target", "--start-date", start_date, "--end-date", end_date, "--config", args.config, "--skip-existing"],
                f"ODS Moneyflow/Margin {year}"
            )
            # Stock Factor
            run_command(
                [sys.executable, "scripts/sync/run_ods_features.py", "--apis", "stk_factor", "--start-date", start_date, "--end-date", end_date, "--config", args.config, "--skip-existing"],
                f"ODS Stock Factor {year}"
            )

        # DWD Layer
        if args.layer in ["dwd", "all"]:
             run_command(
                [sys.executable, "scripts/sync/run_dwd.py", "--mode", "incremental", "--start-date", start_date, "--end-date", end_date, "--config", args.config],
                f"DWD Sync {year}"
            )

        # DWS Layer
        if args.layer in ["dws", "all"]:
             run_command(
                [sys.executable, "scripts/sync/run_dws.py", "--mode", "incremental", "--start-date", start_date, "--end-date", end_date, "--config", args.config],
                f"DWS Sync {year}"
            )

        # ADS Layer
        if args.layer in ["ads", "all"]:
             run_command(
                [sys.executable, "scripts/sync/run_ads.py", "--mode", "incremental", "--start-date", start_date, "--end-date", end_date, "--config", args.config],
                f"ADS Sync {year}"
            )

if __name__ == "__main__":
    main()
