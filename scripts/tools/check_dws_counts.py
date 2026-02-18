from __future__ import annotations
import pymysql
from etl.base.runtime import get_env_config

def check_counts():
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
        "dwd_daily",
        "dws_price_adj_daily",
        "dws_momentum_score",
        "ads_features_stock_daily",
        "ads_stock_score_daily"
    ]
    try:
        with conn.cursor() as cursor:
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"{table}: {count}")
                except Exception as e:
                    print(f"{table}: Error {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_counts()
