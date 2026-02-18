from etl.base.runtime import get_env_config, get_mysql_session

def get_count():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM ods_stk_factor")
            print(f"Total rows in ods_stk_factor: {cursor.fetchone()[0]}")

if __name__ == "__main__":
    get_count()
