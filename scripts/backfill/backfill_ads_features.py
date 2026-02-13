#!/usr/bin/env python3
import argparse
import logging
import os
import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

def main():
    parser = argparse.ArgumentParser(description="Backfill ADS Features data.")
    parser.add_argument("--start-date", type=int, default=20251001, help="Start date (YYYYMMDD)")
    parser.add_argument("--end-date", type=int, default=20260213, help="End date (YYYYMMDD)")
    parser.add_argument("--config", default="config/etl.ini", help="Path to etl.ini")
    args = parser.parse_args()

    # Construct the command to call run_ads.py
    # ADS run is usually fast enough to just rerun for the range
    
    script_path = project_root / "scripts" / "sync" / "run_ads.py"
    
    cmd = [
        sys.executable,
        str(script_path),
        "--mode", "incremental", # incremental mode respects start/end dates
        "--start-date", str(args.start_date),
        "--end-date", str(args.end_date),
        "--config", args.config,
    ]
    
    print(f"Executing: {' '.join(cmd)}")
    os.execv(sys.executable, cmd)

if __name__ == "__main__":
    main()
