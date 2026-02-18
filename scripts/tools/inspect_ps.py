from etl.base.runtime import get_env_config, get_mysql_session
import pandas as pd

def inspect_ps_schema():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            # Check schema
            cursor.execute("DESC ods_stk_factor")
            columns = cursor.fetchall()
            for col in columns:
                if col[0] == 'ps':
                    print(f"PS Column Definition: {col}")
            
            # Check for existing ps values to see if they are near limit
            cursor.execute("SELECT MAX(ps), MIN(ps) FROM ods_stk_factor")
            max_ps, min_ps = cursor.fetchone()
            print(f"Current Max PS: {max_ps}, Min PS: {min_ps}")

if __name__ == "__main__":
    inspect_ps_schema()
