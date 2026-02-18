import pymysql
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from scripts.etl.base.runtime import get_env_config

def check_max_dates():
    cfg = get_env_config()
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database
    )
    
    tables = [
        "ods_daily",
        "dws_price_adj_daily",
        "dws_momentum_score",
        "ads_stock_score_daily"
    ]
    
    print("Checking max trade_date for tables...")
    try:
        with conn.cursor() as cursor:
            for table in tables:
                cursor.execute(f"SELECT MAX(trade_date) FROM {table}")
                max_date = cursor.fetchone()[0]
                print(f"{table}: {max_date}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_max_dates()
