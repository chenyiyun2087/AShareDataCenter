from etl.base.runtime import get_env_config, get_mysql_session
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def list_tables():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            print(tables)

if __name__ == "__main__":
    list_tables()
