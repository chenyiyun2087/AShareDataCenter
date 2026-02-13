
import configparser
import pymysql
from datetime import datetime, timedelta

def cleanup():
    config = configparser.ConfigParser()
    config.read('/Users/chenyiyun/PycharmProjects/AShareDataCenter/config/etl.ini')
    
    try:
        conn = pymysql.connect(
            host=config['mysql']['host'],
            port=int(config['mysql']['port']),
            user=config['mysql']['user'],
            password=config['mysql']['password'],
            database=config['mysql']['database'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
        with conn.cursor() as cursor:
            # Mark tasks older than 4 hours as FAILED
            four_hours_ago = (datetime.now() - timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S')
            
            # First, see what we're about to change
            cursor.execute("""
                SELECT id, api_name, start_at FROM meta_etl_run_log 
                WHERE status = 'RUNNING' AND start_at < %s
            """, (four_hours_ago,))
            stale_tasks = cursor.fetchall()
            
            if not stale_tasks:
                print("No stale tasks found.")
                return

            print(f"Found {len(stale_tasks)} stale tasks. Cleaning up...")
            for task in stale_tasks:
                print(f"Cleaning task: ID={task['id']}, API={task['api_name']}, Start={task['start_at']}")

            # Perform update
            affected = cursor.execute("""
                UPDATE meta_etl_run_log 
                SET status = 'FAILED', 
                    end_at = NOW(), 
                    err_msg = 'Auto-cleaned: Process likely died or timed out.'
                WHERE status = 'RUNNING' AND start_at < %s
            """, (four_hours_ago,))
            
            print(f"Update complete. Affected rows: {affected}")
            
    except Exception as e:
        print(f"Error during cleanup: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    cleanup()
