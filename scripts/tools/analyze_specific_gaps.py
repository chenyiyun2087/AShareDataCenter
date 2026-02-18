import pymysql
import sys
import os
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

from scripts.etl.base.runtime import get_env_config

def get_trade_dates(cursor, start_date, end_date):
    cursor.execute(
        "SELECT DISTINCT trade_date FROM ods_daily WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date",
        (start_date, end_date)
    )
    return [row[0] for row in cursor.fetchall()]

def analyze_gaps():
    cfg = get_env_config()
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database
    )
    
    # Target tables to analyze
    targets = {
        "dws_chip_score": "dws_chip_score",
        "dws_quality_score": "dws_quality_score",
        "dws_technical_score": "dws_technical_score",
        "ads_stock_score_daily": "ads_stock_score_daily",
        "ads_features_stock_daily": "ads_features_stock_daily"
    }
    
    # Source tables mapping for dependency check
    # Chip score likely depends on dws_chip_dynamics or similar
    # Quality score depends on fin indicators
    # Technical score depends on price data
    
    start_date = 20200101
    end_date = 20260213
    
    try:
        with conn.cursor() as cursor:
            # Get expected trade dates
            print(f"Fetching trade dates from {start_date} to {end_date}...")
            expected_dates = get_trade_dates(cursor, start_date, end_date)
            expected_set = set(expected_dates)
            print(f"Total expected trade days: {len(expected_dates)}")
            
            for table_name in targets:
                print(f"\nAnalyzing {table_name}...")
                
                # Get actual dates present in the table
                cursor.execute(f"SELECT DISTINCT trade_date FROM {table_name} WHERE trade_date BETWEEN %s AND %s", (start_date, end_date))
                actual_dates = set(row[0] for row in cursor.fetchall())
                
                missing_dates = sorted(list(expected_set - actual_dates))
                
                if not missing_dates:
                    print(f"  Result: COMPLETE (No missing dates)")
                else:
                    print(f"  Result: MISSING {len(missing_dates)} dates")
                    # Group consecutive dates for cleaner output
                    ranges = []
                    if missing_dates:
                        start = missing_dates[0]
                        prev = missing_dates[0]
                        
                        # Use index to find non-consecutive dates in the sorted list of missing dates is tricky because they are trade dates (gaps naturally exist like weekends).
                        # Better approach: Iterate through missing_dates. If next missing date != next expected trade date after current, it's a break.
                        # But simpler: just look for gaps > 10 days or just list first/last 5.
                        
                        print(f"  First 5 missing: {missing_dates[:5]}")
                        print(f"  Last 5 missing: {missing_dates[-5:]}")
                        
                        # Let's try to identify large blocks
                        # Simple clustering: if date[i] and date[i+1] are far apart (more than 10 days), print range
                        
                        # Determine dependency availability for the first missing date to see WHY it is missing.
                        sample_missing = missing_dates[0]
                        print(f"  Checking sources for sample missing date: {sample_missing}")
                        
                        if table_name == "dws_chip_score":
                            # Check ods_cyq_perf or dws_chip_dynamics
                            cursor.execute("SELECT count(*) FROM dws_chip_dynamics WHERE trade_date=%s", (sample_missing,))
                            cnt = cursor.fetchone()[0]
                            print(f"    Source `dws_chip_dynamics` count: {cnt}")
                            
                        elif table_name == "dws_quality_score":
                            # Check dws_fina_pit_daily
                            cursor.execute("SELECT count(*) FROM dws_fina_pit_daily WHERE trade_date=%s", (sample_missing,))
                            cnt = cursor.fetchone()[0]
                            print(f"    Source `dws_fina_pit_daily` count: {cnt}")

                        elif table_name == "dws_technical_score":
                             # Check dws_price_adj_daily
                            cursor.execute("SELECT count(*) FROM dws_price_adj_daily WHERE trade_date=%s", (sample_missing,))
                            cnt = cursor.fetchone()[0]
                            print(f"    Source `dws_price_adj_daily` count: {cnt}")
                            
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_gaps()
