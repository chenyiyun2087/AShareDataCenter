from etl.base.runtime import get_env_config, get_mysql_session

def create_missing_tables():
    cfg = get_env_config()
    ddls = [
        """
        CREATE TABLE IF NOT EXISTS ods_weekly (
          trade_date INT NOT NULL COMMENT '交易日期',
          ts_code CHAR(9) NOT NULL COMMENT '股票代码',
          open DECIMAL(20,4) NULL COMMENT '开盘价',
          high DECIMAL(20,4) NULL COMMENT '最高价',
          low DECIMAL(20,4) NULL COMMENT '最低价',
          close DECIMAL(20,4) NULL COMMENT '收盘价',
          pre_close DECIMAL(20,4) NULL COMMENT '昨收价',
          `change` DECIMAL(20,4) NULL COMMENT '涨跌额',
          pct_chg DECIMAL(12,6) NULL COMMENT '涨跌幅',
          vol DECIMAL(20,4) NULL COMMENT '成交量(手)',
          amount DECIMAL(20,4) NULL COMMENT '成交额(千元)',
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (trade_date, ts_code),
          KEY idx_ts_date (ts_code, trade_date)
        ) ENGINE=InnoDB COMMENT='周频行情原始表';
        """,
        """
        CREATE TABLE IF NOT EXISTS ods_monthly (
          trade_date INT NOT NULL COMMENT '交易日期',
          ts_code CHAR(9) NOT NULL COMMENT '股票代码',
          open DECIMAL(20,4) NULL COMMENT '开盘价',
          high DECIMAL(20,4) NULL COMMENT '最高价',
          low DECIMAL(20,4) NULL COMMENT '最低价',
          close DECIMAL(20,4) NULL COMMENT '收盘价',
          pre_close DECIMAL(20,4) NULL COMMENT '昨收价',
          `change` DECIMAL(20,4) NULL COMMENT '涨跌额',
          pct_chg DECIMAL(12,6) NULL COMMENT '涨跌幅',
          vol DECIMAL(20,4) NULL COMMENT '成交量(手)',
          amount DECIMAL(20,4) NULL COMMENT '成交额(千元)',
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (trade_date, ts_code),
          KEY idx_ts_date (ts_code, trade_date)
        ) ENGINE=InnoDB COMMENT='月频行情原始表';
        """
    ]
    
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            for sql in ddls:
                print(f"Executing: {sql[:50]}...")
                cursor.execute(sql)
            conn.commit()
    print("Missing tables created successfully.")

if __name__ == "__main__":
    create_missing_tables()
