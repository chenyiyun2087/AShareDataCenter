from etl.base.runtime import get_env_config, get_mysql_session

def check_feature_counts():
    cfg = get_env_config()
    tables = ['ods_moneyflow_ths', 'ods_cyq_perf', 'ods_margin', 'ods_stk_factor']
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            for t in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {t}")
                print(f"{t}: {cursor.fetchone()[0]}")

if __name__ == "__main__":
    check_feature_counts()
