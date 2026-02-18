from etl.base.runtime import get_env_config, get_mysql_session
import sys
import os

def check_table(cursor, table_name):
    print(f"--- Table: {table_name} ---")
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"Total count: {count}")
    
    if count > 0:
        cursor.execute(f"SELECT MIN(trade_date), MAX(trade_date) FROM {table_name}")
        min_date, max_date = cursor.fetchone()
        print(f"Date range (trade_date): {min_date} - {max_date}")
    print()

def check_fina_table(cursor, table_name):
    print(f"--- Table: {table_name} ---")
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"Total count: {count}")
    
    if count > 0:
        cursor.execute(f"SELECT MIN(ann_date), MAX(ann_date) FROM {table_name}")
        min_date, max_date = cursor.fetchone()
        print(f"Date range (ann_date): {min_date} - {max_date}")
    print()

def main():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            tables = [
                "dws_liquidity_factor",
                "dwd_stock_label_daily",
                "dwd_daily",
                "dwd_daily_basic"
            ]
            for table in tables:
                check_table(cursor, table)
            
            fina_tables = [
                "dwd_fina_indicator",
                "ods_fina_indicator"
            ]
            for table in fina_tables:
                check_fina_table(cursor, table)

if __name__ == "__main__":
    scripts_dir = os.getcwd() + "/scripts"
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    main()
