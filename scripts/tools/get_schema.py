from etl.base.runtime import get_env_config, get_mysql_session

def get_full_schema():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DESC ods_stk_factor")
            columns = cursor.fetchall()
            for col in columns:
                print(col)

if __name__ == "__main__":
    get_full_schema()
