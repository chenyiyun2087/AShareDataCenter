
import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scripts.etl.base.runtime import get_mysql_session, get_env_config
from scripts.etl.base.runtime import list_trade_dates_after


def main():
    cfg = get_env_config()
    print(f"Using host: {cfg.host}, db: {cfg.database}")
    
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            # Check Watermark
            cursor.execute("SELECT * FROM meta_etl_watermark WHERE api_name='ods_daily'")
            watermark = cursor.fetchone()
            print(f"Current Watermark for ods_daily: {watermark}")
             
            if watermark:
                last_date = watermark[1] # value is at index 1 based on schema
                print(f"Listing trade dates after {last_date}...")
                dates = list_trade_dates_after(cursor, last_date)
                print(f"Trade dates found: {dates}")
                
            # Check calendar for Feb 10-13 explicitly
            cursor.execute("SELECT cal_date, is_open FROM dim_trade_cal WHERE cal_date BETWEEN 20260210 AND 20260214 ORDER BY cal_date")
            print("Calendar entries 20260210-20260214:")
            for row in cursor.fetchall():
                print(row)

if __name__ == "__main__":
    main()
