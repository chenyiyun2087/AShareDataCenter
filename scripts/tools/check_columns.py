from etl.base.runtime import get_env_config, get_mysql_session
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def check_columns():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            tables = [
                'ods_daily', 'ods_moneyflow', 'ods_margin', 'ods_margin_detail', 
                'ods_fina_indicator', 'ods_adj_factor', 'ods_daily_basic',
                'dwd_daily', 'dwd_daily_basic', 'dwd_adj_factor', 
                'dwd_fina_indicator', 'dwd_margin_sentiment', 'dwd_chip_stability'
            ]
            
            for t in tables:
                cursor.execute(f"DESCRIBE {t}")
                cols = [row[0] for row in cursor.fetchall()]
                print(f"{t}: {cols}")

if __name__ == "__main__":
    check_columns()
