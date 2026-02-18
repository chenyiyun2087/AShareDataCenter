#!/usr/bin/env python3
import sys
import os
import logging
from pathlib import Path
from datetime import datetime

# Add project scripts directory to sys.path
scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base.runtime import get_env_config, get_mysql_session, list_trade_dates
from etl.dwd.runner import load_dwd_stock_label_daily, load_dwd_fina_indicator
from etl.dws.enhanced_factors import run_liquidity_factor

def verify_ods_completeness(cursor, start_date, end_date):
    """Verify if ODS and DWD source data is sufficient."""
    print("Checking source data completeness...")
    
    # 1. dwd_daily (Price data)
    cursor.execute("SELECT COUNT(DISTINCT trade_date) FROM dwd_daily WHERE trade_date BETWEEN %s AND %s", (start_date, end_date))
    daily_count = cursor.fetchone()[0]
    
    # 2. ods_fina_indicator (Financial data)
    cursor.execute("SELECT COUNT(*) FROM ods_fina_indicator WHERE ann_date BETWEEN %s AND %s", (start_date, end_date))
    fina_count = cursor.fetchone()[0]
    
    print(f"Source Trade Dates (dwd_daily): {daily_count}")
    print(f"Source Fina Records (ods_fina_indicator): {fina_count}")
    
    if daily_count == 0 or fina_count < 100:
        print("WARNING: Source data seems very low or missing. Refill might be incomplete.")
        return False
    return True

def refill_liquidity_factor(cursor, start_date, end_date):
    print(f"Refilling dws_liquidity_factor for {start_date} - {end_date}...")
    # run_liquidity_factor supports batch date range
    run_liquidity_factor(cursor, start_date, end_date)
    print("Completed dws_liquidity_factor.")

def refill_stock_labels(cursor, start_date, end_date):
    print(f"Refilling dwd_stock_label_daily for {start_date} - {end_date}...")
    trade_dates = list_trade_dates(cursor, start_date, end_date)
    total = len(trade_dates)
    for i, trade_date in enumerate(trade_dates):
        if i % 50 == 0:
            print(f"  Progress: {i}/{total} dates processed...")
        load_dwd_stock_label_daily(cursor, trade_date)
    print("Completed dwd_stock_label_daily.")

def refill_fina_indicator(cursor, start_date, end_date):
    print(f"Refilling dwd_fina_indicator for {start_date} - {end_date}...")
    load_dwd_fina_indicator(cursor, start_date, end_date)
    print("Completed dwd_fina_indicator.")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto-approve", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    cfg = get_env_config()
    
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            # Date Ranges
            liq_start, liq_end = 20200101, 20260131
            label_start, label_end = 20210501, 20260131
            fina_start, fina_end = 20200101, 20260217
            
            # Step 1: ODS Verification
            if not verify_ods_completeness(cursor, 20200101, 20260217):
                if not args.auto_approve:
                    confirm = input("Source data might be missing. Continue anyway? (y/n): ")
                    if confirm.lower() != 'y':
                        print("Aborted.")
                        return
                else:
                    print("Source data missing, but --auto-approve is set. Continuing...")
            
            # Step 2: DWD Fina Indicator (Dependencies might be needed by others)
            refill_fina_indicator(cursor, fina_start, fina_end)
            conn.commit()
            
            # Step 3: DWD Stock Labels
            refill_stock_labels(cursor, label_start, label_end)
            conn.commit()
            
            # Step 4: DWS Liquidity Factor (Depends on dwd_daily_basic)
            refill_liquidity_factor(cursor, liq_start, liq_end)
            conn.commit()
            
    print("\nAll refill operations completed successfully.")

if __name__ == "__main__":
    main()
