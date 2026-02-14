from etl.base.runtime import get_env_config, get_mysql_session
import sys
import os

def main():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            print("--- Table Indices: dws_price_adj_daily ---")
            cursor.execute("SHOW INDEX FROM dws_price_adj_daily")
            for row in cursor.fetchall():
                print(row)
            
            print("\n--- System Variables ---")
            cursor.execute("SHOW VARIABLES LIKE 'innodb_lock_wait_timeout'")
            print(cursor.fetchone())
            cursor.execute("SHOW VARIABLES LIKE 'max_allowed_packet'")
            print(cursor.fetchone())

if __name__ == "__main__":
    # Add project scripts directory to sys.path
    scripts_dir = os.getcwd() + "/scripts"
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    main()
