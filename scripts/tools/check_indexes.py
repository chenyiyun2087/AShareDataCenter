import sys
from etl.base.runtime import get_env_config, get_mysql_session

def check_indexes(tables=None):
    cfg = get_env_config()
    if not tables:
        tables = ['dws_price_adj_daily', 'dwd_daily', 'dwd_adj_factor']
    
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            for t in tables:
                print(f"--- Indexes for {t} ---")
                try:
                    cursor.execute(f"SHOW INDEX FROM {t}")
                    for row in cursor.fetchall():
                        print(row)
                except Exception as e:
                    print(f"Error checking {t}: {e}")

if __name__ == "__main__":
    check_indexes(sys.argv[1:])
