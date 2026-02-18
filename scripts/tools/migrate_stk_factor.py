from etl.base.runtime import get_env_config, get_mysql_session

def migrate_ods_stk_factor():
    cfg = get_env_config()
    # Aligning with ddl.sql intended schema: pe, pb, ps, ps_ttm, pe_ttm, dv_ratio, dv_ttm -> DECIMAL(20,4)
    # Wait, dv_ratio and dv_ttm in ddl.sql are actually DECIMAL(12,6) in some places? 
    # Let's check ddl.sql again for stk_factor.
    
    # In ddl.sql:
    # pe DECIMAL(20,4), pe_ttm DECIMAL(20,4), pb DECIMAL(20,4), ps DECIMAL(20,4), ps_ttm DECIMAL(20,4)
    # dv_ratio DECIMAL(12,6), dv_ttm DECIMAL(12,6)
    
    sqls = [
        "ALTER TABLE ods_stk_factor MODIFY pe DECIMAL(20,4) NULL COMMENT 'PE'",
        "ALTER TABLE ods_stk_factor MODIFY pe_ttm DECIMAL(20,4) NULL COMMENT 'PE_TTM'",
        "ALTER TABLE ods_stk_factor MODIFY pb DECIMAL(20,4) NULL COMMENT 'PB'",
        "ALTER TABLE ods_stk_factor MODIFY ps DECIMAL(20,4) NULL COMMENT 'PS'",
        "ALTER TABLE ods_stk_factor MODIFY ps_ttm DECIMAL(20,4) NULL COMMENT 'PS_TTM'"
    ]
    
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            for sql in sqls:
                print(f"Executing: {sql}")
                cursor.execute(sql)
            conn.commit()
    print("Migration completed successfully.")

if __name__ == "__main__":
    migrate_ods_stk_factor()
