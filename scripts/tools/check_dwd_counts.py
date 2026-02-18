from etl.base.runtime import get_env_config, get_mysql_session

def check_counts():
    cfg = get_env_config()
    tables = ['ods_margin_detail', 'dwd_daily', 'dwd_adj_factor', 'ods_cyq_perf']
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            for t in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {t}")
                print(f"{t}: {cursor.fetchone()[0]}")

if __name__ == "__main__":
    check_counts()
