import pymysql
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from scripts.etl.base.runtime import get_env_config

def check_ods_source():
    cfg = get_env_config()
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database
    )
    
    try:
        with conn.cursor() as cursor:
            # Check ODS CYQ Perf for 2025-01-02
            cursor.execute("SELECT count(*) FROM ods_cyq_perf WHERE trade_date=20250102")
            cyq_count = cursor.fetchone()[0]
            print(f"ods_cyq_perf count for 2025-01-02: {cyq_count}")
            
            # Check ODS Financial Indicator for Q1 2020 (source for 2020 quality scores)
            cursor.execute("SELECT count(*) FROM ods_fina_indicator WHERE end_date>=20191231 AND end_date<=20200331")
            fina_count = cursor.fetchone()[0]
            print(f"ods_fina_indicator count for period 20191231-20200331: {fina_count}")
            
             # Check ODS Financial Indicator for entire 2020-2024 period
            cursor.execute("SELECT MIN(end_date), MAX(end_date), COUNT(*) FROM ods_fina_indicator WHERE end_date BETWEEN 20200101 AND 20241231")
            min_date, max_date, total_count = cursor.fetchone()
            print(f"ods_fina_indicator range 2020-2024: {min_date} to {max_date}, Count: {total_count}")

    finally:
        conn.close()

if __name__ == "__main__":
    check_ods_source()
