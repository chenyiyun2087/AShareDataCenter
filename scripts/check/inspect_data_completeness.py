#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add project root to sys.path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scripts.etl.base.runtime import get_mysql_session, get_env_config, list_trade_dates

def check_completeness(end_date: int, days: int):
    cfg = get_env_config()
    
    # Tables to check for each layer
    layer_tables = {
        'ODS': ['ods_daily', 'ods_daily_basic', 'ods_moneyflow'],
        'DWD': ['dwd_daily', 'dwd_daily_basic'],
        'DWS': ['dws_momentum_score', 'dws_value_score'],
        'ADS': ['ads_features_stock_daily', 'ads_stock_score_daily']
    }
    
    print(f"Checking data completeness backwards from {end_date} for {days} trading days...")
    
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            # 1. Get last N trade dates
            cursor.execute(f"""
                SELECT cal_date FROM dim_trade_cal 
                WHERE is_open=1 AND cal_date <= {end_date} 
                ORDER BY cal_date DESC LIMIT {days}
            """)
            trade_dates = [row[0] for row in cursor.fetchall()]
            
            if not trade_dates:
                print("No trade dates found.")
                return

            results = []
            
            # 2. Check each date
            total_dates = len(trade_dates)
            for idx, date in enumerate(trade_dates, 1):
                print(f"[{idx}/{total_dates}] Checking {date}...", end="\r")
                row_data = {'date': date}
                
                for layer, tables in layer_tables.items():
                    layer_status = "OK"
                    details = []
                    for table in tables:
                        # For some tables, trade_date might be named different or we check specific logic
                        # But standard tables use trade_date
                        cursor.execute(f"SELECT count(*) FROM {table} WHERE trade_date={date}")
                        count = cursor.fetchone()[0]
                        if count == 0:
                            layer_status = "MISSING"
                            details.append(f"{table}(0)")
                        elif count < 1000: # Simple heuristic for "too few rows"
                            layer_status = "PARTIAL"
                            details.append(f"{table}({count})")
                    
                    if layer_status == "OK":
                        row_data[layer] = "✅"
                    elif layer_status == "PARTIAL":
                        row_data[layer] = f"⚠️ {' '.join(details)}"
                    else:
                        row_data[layer] = f"❌ {' '.join(details)}"
                
                results.append(row_data)
            
            print("\n" + "="*80)
            df = pd.DataFrame(results)
            # Reorder columns
            cols = ['date', 'ODS', 'DWD', 'DWS', 'ADS']
            print(df[cols].to_string(index=False))
            print("="*80)

            # Generate report file
            report_path = project_root / "data_completeness_report.md"
            with open(report_path, "w") as f:
                f.write(f"# Data Completeness Report\n")
                f.write(f"Generated at: {datetime.now()}\n")
                f.write(f"Ref Date: {end_date}\n\n")
                f.write(df[cols].to_string(index=False))
            print(f"Report saved to {report_path}")

def main():
    parser = argparse.ArgumentParser(description="Inspect data completeness.")
    parser.add_argument("--end-date", type=int, default=int(datetime.now().strftime("%Y%m%d")))
    parser.add_argument("--days", type=int, default=10, help="Number of trade days to look back")
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    args = parser.parse_args()
    
    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            # Try relative to CWD first
            cwd_path = (Path.cwd() / config_path).resolve()
            if cwd_path.exists():
                config_path = cwd_path
            else:
                # Fallback to project root
                root_path = (project_root / config_path).resolve()
                if root_path.exists():
                    config_path = root_path
        
        if config_path.exists():
            os.environ["ETL_CONFIG_PATH"] = str(config_path)
            print(f"Using config: {config_path}")

    check_completeness(args.end_date, args.days)

if __name__ == "__main__":
    main()
