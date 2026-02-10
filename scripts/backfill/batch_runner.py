import argparse
import subprocess
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import List

# Calculate project root relative to this script's location (scripts/backfill/batch_runner.py)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

def run_command(cmd: List[str], description: str) -> None:
    print(f"Starting: {description}")
    print(f"Command: {' '.join(cmd)}")
    start_time = datetime.now()
    try:
        # Resolve script path if it's the second element in the list
        if len(cmd) > 1 and cmd[1].startswith("scripts/"):
            cmd[1] = str(PROJECT_ROOT / cmd[1])
        
        subprocess.check_call(cmd, cwd=str(PROJECT_ROOT))
        duration = datetime.now() - start_time
        print(f"Completed: {description} in {duration}")
    except subprocess.CalledProcessError as e:
        print(f"Failed: {description} with error {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Batch Backfill Runner")
    parser.add_argument("--start-year", type=int, required=True, help="Start year (e.g., 2010)")
    parser.add_argument("--end-year", type=int, required=True, help="End year (e.g., 2019)")
    parser.add_argument("--layer", choices=["ods", "dwd", "dws", "ads", "all"], required=True)
    parser.add_argument("--config", default="config/etl.ini")
    parser.add_argument("--rate-limit", type=int, default=None, help="TuShare rate limit (requests per minute)")
    args = parser.parse_args()

    for year in range(args.start_year, args.end_year + 1):
        start_date = f"{year}0101"
        end_date = f"{year}1231"
        print(f"\n=== Processing Year {year} ({start_date}-{end_date}) ===")

        rate_limit_args = ["--rate-limit", str(args.rate_limit)] if args.rate_limit else []

        # ODS Layer
        if args.layer in ["ods", "all"]:
            # Financial Indicators
            run_command(
                [sys.executable, "scripts/sync/run_ods.py", "--fina-start", start_date, "--fina-end", end_date, "--config", args.config] + rate_limit_args,
                f"ODS Fina Indicator {year}"
            )
            # Moneyflow & Margin (excluding margin_target due to low rate limit)
            run_command(
                [sys.executable, "scripts/sync/run_ods_features.py", "--apis", "moneyflow,margin_detail,margin", "--start-date", start_date, "--end-date", end_date, "--config", args.config, "--skip-existing"] + rate_limit_args,
                f"ODS Moneyflow/Margin {year}"
            )
            # Stock Factor
            run_command(
                [sys.executable, "scripts/sync/run_ods_features.py", "--apis", "stk_factor", "--start-date", start_date, "--end-date", end_date, "--config", args.config, "--skip-existing"] + rate_limit_args,
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
