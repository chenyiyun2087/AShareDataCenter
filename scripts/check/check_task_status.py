
import configparser
import pymysql
from pathlib import Path

def check_db():
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
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            # Check latest run logs
            cursor.execute("SELECT * FROM meta_etl_run_log ORDER BY id DESC LIMIT 10")
            logs = cursor.fetchall()
            print("\nLatest Task Logs:")
            for log in logs:
                print(f"ID: {log['id']}, API: {log['api_name']}, Type: {log['run_type']}, Status: {log['status']}, Start: {log['start_at']}, End: {log['end_at']}")
            
            # Check watermarks
            cursor.execute("SELECT * FROM meta_etl_watermark")
            watermarks = cursor.fetchall()
            print("\nWatermarks:")
            for wm in watermarks:
                print(f"API: {wm['api_name']}, Watermark: {wm['water_mark']}, Status: {wm['status']}, Last Run: {wm['last_run_at']}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_db()
