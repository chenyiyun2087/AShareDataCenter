from etl.base.runtime import get_env_config, get_mysql_session

def check_processlist():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW FULL PROCESSLIST")
            columns = [col[0] for col in cursor.description]
            for row in cursor.fetchall():
                print(dict(zip(columns, row)))

if __name__ == "__main__":
    check_processlist()
