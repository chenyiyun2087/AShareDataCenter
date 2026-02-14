from etl.base.runtime import get_env_config, get_mysql_session
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def check_locks():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            # 1. Check for running transactions
            sql_trx = """
            SELECT trx_id, trx_state, trx_started, trx_query 
            FROM information_schema.innodb_trx
            """
            cursor.execute(sql_trx)
            trx_rows = cursor.fetchall()
            
            print("\n=== Active Transactions (INNODB_TRX) ===")
            if not trx_rows:
                print("No active transactions found.")
            else:
                for row in trx_rows:
                    print(f"Trx ID: {row[0]}, State: {row[1]}, Started: {row[2]}")
                    print(f"Query: {row[3]}")
                    print("-" * 30)

            # 2. Check for waiting locks (MySQL 8.0+ uses performance_schema.data_locks, 5.7 uses information_schema.innodb_locks)
            # Try to infer version or just try both queries safely
            
            try:
                # Try performance_schema first (MySQL 8.0)
                sql_locks = """
                SELECT * FROM performance_schema.data_lock_waits
                """
                cursor.execute(sql_locks)
                lock_waits = cursor.fetchall()
                if lock_waits:
                    print("\n=== Lock Waits (performance_schema.data_lock_waits) ===")
                    for row in lock_waits:
                        print(row)
                else:
                    print("\nNo lock waits found (performance_schema).")
            except Exception:
                # Fallback to MySQL 5.7 information_schema
                try:
                    sql_locks_57 = """
                    SELECT * FROM information_schema.innodb_lock_waits
                    """
                    cursor.execute(sql_locks_57)
                    lock_waits_57 = cursor.fetchall()
                    if lock_waits_57:
                         print("\n=== Lock Waits (information_schema.innodb_lock_waits) ===")
                         for row in lock_waits_57:
                             print(row)
                    else:
                        print("\nNo lock waits found (information_schema).")
                except Exception as e:
                    print(f"\nCould not query lock waits: {e}")

            # 3. Process List
            print("\n=== Process List (Top 10) ===")
            cursor.execute("SHOW FULL PROCESSLIST")
            procs = cursor.fetchall()
            for p in procs[:10]:
                print(p)

if __name__ == "__main__":
    check_locks()
