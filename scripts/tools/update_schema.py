
import os
import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from scripts.etl.base.runtime import get_env_config, get_mysql_connection

def main():
    cfg = get_env_config()
    print(f"Connecting to database: {cfg.host}:{cfg.port}/{cfg.database}")
    
    drop_sql = "DROP TABLE IF EXISTS ods_stk_factor"
    
    ddl_path = project_root / "sql" / "ddl.sql"
    with open(ddl_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Extract ods_stk_factor definition
    start_marker = "CREATE TABLE IF NOT EXISTS ods_stk_factor ("
    end_marker = ") ENGINE=InnoDB COMMENT='股票技术因子表(Pro版)';"
    
    start_idx = content.find(start_marker)
    if start_idx == -1:
        print("Could not find table definition in ddl.sql")
        return
        
    end_idx = content.find(end_marker, start_idx)
    if end_idx == -1:
        print("Could not find end of table definition")
        return
        
    create_sql = content[start_idx : end_idx + len(end_marker)]
    
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            print("Dropping old table...")
            cursor.execute(drop_sql)
            print("Creating new table...")
            cursor.execute(create_sql)
            conn.commit()
    
    print("Schema update completed successfully.")

if __name__ == "__main__":
    main()
