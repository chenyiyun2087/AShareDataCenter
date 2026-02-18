from __future__ import annotations
import pymysql
from etl.base.runtime import get_env_config

def kill_ghost_queries():
    cfg = get_env_config()
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute('SHOW FULL PROCESSLIST')
            procs = cursor.fetchall()
            for p in procs:
                # p[0] is Id, p[4] is Command, p[7] is Info
                proc_id = p[0]
                command = p[4]
                info = str(p[7])
                if command == 'Query' and ('dws_' in info or 'ads_' in info or 'dwd_' in info):
                    if 'SHOW FULL PROCESSLIST' in info:
                        continue
                    print(f"Killing process {proc_id}: {info[:100]}...")
                    try:
                        cursor.execute(f"KILL {proc_id}")
                    except Exception as e:
                        print(f"Failed to kill {proc_id}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    kill_ghost_queries()
