import sys
from pathlib import Path

# Add project root to sys.path
scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base.runtime import get_env_config, get_mysql_session

def check_connections():
    cfg = get_env_config()
    print(f"Connecting to MySQL at {cfg.host}:{cfg.port} as {cfg.user}...")
    
    try:
        with get_mysql_session(cfg) as conn:
            with conn.cursor() as cursor:
                # Get current threads connected
                cursor.execute("SHOW STATUS LIKE 'Threads_connected';")
                threads_connected = cursor.fetchone()[1]
                
                # Get max connections
                cursor.execute("SHOW VARIABLES LIKE 'max_connections';")
                max_connections = cursor.fetchone()[1]
                
                # Get process list count
                cursor.execute("SHOW PROCESSLIST;")
                processes = cursor.fetchall()
                
                print("\n" + "="*40)
                print("MySQL Connection Status")
                print("="*40)
                print(f"Threads Connected: {threads_connected}")
                print(f"Max Connections:   {max_connections}")
                print(f"Usage Percentage:  {(int(threads_connected)/int(max_connections))*100:.2f}%")
                print("-" * 40)
                print(f"Active Processes:  {len(processes)}")
                
                # Show top consumers or details if needed
                print("\nTop Processes:")
                print(f"{'Id':<10} | {'User':<15} | {'Host':<20} | {'db':<15} | {'Command':<10} | {'Time':<5}")
                print("-" * 85)
                for p in processes[:10]: # Show first 10
                    print(f"{p[0]:<10} | {p[1]:<15} | {p[2]:<20} | {str(p[3]):<15} | {p[4]:<10} | {p[5]:<5}")
                
                if len(processes) > 10:
                    print(f"... and {len(processes) - 10} more")
                
    except Exception as e:
        print(f"Error checking database: {e}")

if __name__ == "__main__":
    check_connections()
