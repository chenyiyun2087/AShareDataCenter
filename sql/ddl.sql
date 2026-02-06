-- TuShare 日频选股平台数据底座（MySQL 8.0）DDL
-- 统一约定：trade_date INT(YYYYMMDD)，ts_code CHAR(9)，金额/市值 DECIMAL(20,4)，比率 DECIMAL(12,6)

CREATE DATABASE IF NOT EXISTS tushare_stock
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE tushare_stock;

-- ========== 采集水位与运行日志 ==========
--采集水位表
CREATE TABLE IF NOT EXISTS meta_etl_watermark (
  api_name VARCHAR(64) NOT NULL COMMENT '接口名称',
  water_mark INT NOT NULL COMMENT '当前水位(YYYYMMDD)',
  status VARCHAR(16) NOT NULL COMMENT '状态(SUCCESS/FAILED)',
  last_run_at DATETIME NULL COMMENT '上次运行时间',
  last_err TEXT NULL COMMENT '上次错误信息',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (api_name)
) ENGINE=InnoDB COMMENT='ETL采集水位表';
--运行日志表
CREATE TABLE IF NOT EXISTS meta_etl_run_log (
  id BIGINT NOT NULL AUTO_INCREMENT,
  api_name VARCHAR(64) NOT NULL COMMENT '接口名称',
  run_type VARCHAR(16) NOT NULL COMMENT '运行类型(full/incremental)',
  start_at DATETIME NOT NULL COMMENT '开始时间',
  end_at DATETIME NULL COMMENT '结束时间',
  request_count INT NOT NULL DEFAULT 0 COMMENT '请求次数',
  fail_count INT NOT NULL DEFAULT 0 COMMENT '失败次数',
  status VARCHAR(16) NOT NULL COMMENT '状态',
  err_msg TEXT NULL COMMENT '错误信息',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_api_start (api_name, start_at)
) ENGINE=InnoDB COMMENT='ETL运行日志表';

-- ========== 维表 ==========

CREATE TABLE IF NOT EXISTS dim_stock (
  ts_code CHAR(9) NOT NULL COMMENT 'TS代码',
  symbol VARCHAR(10) NOT NULL COMMENT '股票代码',
  name VARCHAR(50) NOT NULL COMMENT '股票名称',
  area VARCHAR(20) NULL COMMENT '地域',
  industry VARCHAR(50) NULL COMMENT '所属行业',
  market VARCHAR(20) NULL COMMENT '市场类型(主板/创业板/科创板)',
  list_date INT NULL COMMENT '上市日期',
  delist_date INT NULL COMMENT '退市日期',
  is_hs CHAR(1) NULL COMMENT '是否沪深港通标的(N/H/S)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ts_code)
) ENGINE=InnoDB COMMENT='股票基础信息表';
CREATE TABLE IF NOT EXISTS dim_trade_cal (
  exchange VARCHAR(8) NOT NULL COMMENT '交易所 SSE上交所 SZSE深交所',
  cal_date INT NOT NULL COMMENT '日历日期',
  is_open TINYINT NOT NULL COMMENT '是否交易 0休市 1交易',
  pretrade_date INT NULL COMMENT '上一个交易日',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (exchange, cal_date),
  KEY idx_cal_date (cal_date)
) ENGINE=InnoDB COMMENT='交易日历表';

-- ========== ODS 原始表 ==========
CREATE TABLE IF NOT EXISTS ods_daily (
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
) ENGINE=InnoDB COMMENT='日频行情原始表';
CREATE TABLE IF NOT EXISTS ods_daily_basic (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  close DECIMAL(20,4) NULL COMMENT '收盘价',
  turnover_rate DECIMAL(12,6) NULL COMMENT '换手率',
  turnover_rate_f DECIMAL(12,6) NULL COMMENT '换手率(自由流通股)',
  volume_ratio DECIMAL(12,6) NULL COMMENT '量比',
  pe DECIMAL(20,4) NULL COMMENT '市盈率(总市值/净利润)',
  pe_ttm DECIMAL(20,4) NULL COMMENT '市盈率(TTM)',
  pb DECIMAL(20,4) NULL COMMENT '市净率',
  ps DECIMAL(20,4) NULL COMMENT '市销率',
  ps_ttm DECIMAL(20,4) NULL COMMENT '市销率(TTM)',
  dv_ratio DECIMAL(12,6) NULL COMMENT '股息率',
  dv_ttm DECIMAL(12,6) NULL COMMENT '股息率(TTM)',
  total_share DECIMAL(20,4) NULL COMMENT '总股本',
  float_share DECIMAL(20,4) NULL COMMENT '流通股本',
  free_share DECIMAL(20,4) NULL COMMENT '自由流通股本',
  total_mv DECIMAL(20,4) NULL COMMENT '总市值',
  circ_mv DECIMAL(20,4) NULL COMMENT '流通市值',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='每日指标原始表';
--复权因子表
CREATE TABLE IF NOT EXISTS ods_adj_factor (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  adj_factor DECIMAL(20,4) NULL COMMENT '复权因子',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='复权因子表';
--财务指标表
CREATE TABLE IF NOT EXISTS ods_fina_indicator (
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  ann_date INT NOT NULL COMMENT '公告日期',
  end_date INT NOT NULL COMMENT '报告期',
  report_type VARCHAR(8) NULL COMMENT '报告类型',
  roe DECIMAL(12,6) NULL COMMENT 'ROE',
  grossprofit_margin DECIMAL(12,6) NULL COMMENT '毛利率',
  debt_to_assets DECIMAL(12,6) NULL COMMENT '资产负债率',
  netprofit_margin DECIMAL(12,6) NULL COMMENT '净利率',
  op_income DECIMAL(20,4) NULL COMMENT '营业收入',
  total_assets DECIMAL(20,4) NULL COMMENT '总资产',
  total_hldr_eqy DECIMAL(20,4) NULL COMMENT '股东权益',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ts_code, ann_date, end_date),
  KEY idx_ts_ann (ts_code, ann_date)
) ENGINE=InnoDB COMMENT='财务指标原始表';

-- ========== ODS 特色数据 ==========
--融资融券余额表
CREATE TABLE IF NOT EXISTS ods_margin (
  trade_date INT NOT NULL COMMENT '交易日期',
  exchange_id VARCHAR(8) NOT NULL COMMENT '交易所代码',
  rzye DECIMAL(20,4) NULL COMMENT '融资余额(元)',
  rzmre DECIMAL(20,4) NULL COMMENT '融资买入额(元)',
  rzche DECIMAL(20,4) NULL COMMENT '融资偿还额(元)',
  rqye DECIMAL(20,4) NULL COMMENT '融券余额(元)',
  rqmcl DECIMAL(20,4) NULL COMMENT '融券卖出量(股)',
  rzrqye DECIMAL(20,4) NULL COMMENT '融资融券余额(元)',
  rqyl DECIMAL(20,4) NULL COMMENT '融券余量(股)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, exchange_id)
) ENGINE=InnoDB COMMENT='融资融券交易汇总表';

CREATE TABLE IF NOT EXISTS ods_margin_detail (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT 'TS代码',
  rzye DECIMAL(20,4) NULL COMMENT '融资余额(元)',
  rqye DECIMAL(20,4) NULL COMMENT '融券余额(元)',
  rzmre DECIMAL(20,4) NULL COMMENT '融资买入额(元)',
  rqyl DECIMAL(20,4) NULL COMMENT '融券余量(股)',
  rzche DECIMAL(20,4) NULL COMMENT '融资偿还额(元)',
  rqchl DECIMAL(20,4) NULL COMMENT '融券偿还量(股)',
  rqmcl DECIMAL(20,4) NULL COMMENT '融券卖出量(股)',
  rzrqye DECIMAL(20,4) NULL COMMENT '融资融券余额(元)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_margin_detail_date (trade_date)
) ENGINE=InnoDB COMMENT='融资融券交易明细表';

CREATE TABLE IF NOT EXISTS ods_margin_target (
  ts_code CHAR(9) NOT NULL COMMENT '标的代码',
  mg_type VARCHAR(4) NOT NULL COMMENT '标的类型(B融资/S融券)',
  is_new CHAR(1) NULL COMMENT '最新标记(Y/N)',
  in_date INT NULL COMMENT '纳入日期',
  out_date INT NULL COMMENT '剔除日期',
  ann_date INT NOT NULL COMMENT '公告日期',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ts_code, mg_type, ann_date)
) ENGINE=InnoDB COMMENT='融资融券标的表';

CREATE TABLE IF NOT EXISTS ods_moneyflow (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  buy_sm_vol DECIMAL(20,4) NULL COMMENT '小单买入量(手)',
  buy_sm_amount DECIMAL(20,4) NULL COMMENT '小单买入额(万元)',
  sell_sm_vol DECIMAL(20,4) NULL COMMENT '小单卖出量(手)',
  sell_sm_amount DECIMAL(20,4) NULL COMMENT '小单卖出额(万元)',
  buy_md_vol DECIMAL(20,4) NULL COMMENT '中单买入量(手)',
  buy_md_amount DECIMAL(20,4) NULL COMMENT '中单买入额(万元)',
  sell_md_vol DECIMAL(20,4) NULL COMMENT '中单卖出量(手)',
  sell_md_amount DECIMAL(20,4) NULL COMMENT '中单卖出额(万元)',
  buy_lg_vol DECIMAL(20,4) NULL COMMENT '大单买入量(手)',
  buy_lg_amount DECIMAL(20,4) NULL COMMENT '大单买入额(万元)',
  sell_lg_vol DECIMAL(20,4) NULL COMMENT '大单卖出量(手)',
  sell_lg_amount DECIMAL(20,4) NULL COMMENT '大单卖出额(万元)',
  buy_elg_vol DECIMAL(20,4) NULL COMMENT '特大单买入量(手)',
  buy_elg_amount DECIMAL(20,4) NULL COMMENT '特大单买入额(万元)',
  sell_elg_vol DECIMAL(20,4) NULL COMMENT '特大单卖出量(手)',
  sell_elg_amount DECIMAL(20,4) NULL COMMENT '特大单卖出额(万元)',
  net_mf_vol DECIMAL(20,4) NULL COMMENT '净流入量(手)',
  net_mf_amount DECIMAL(20,4) NULL COMMENT '净流入额(万元)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_moneyflow_date (trade_date)
) ENGINE=InnoDB COMMENT='个股资金流向表';

CREATE TABLE IF NOT EXISTS ods_moneyflow_ths (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  net_mf_amount DECIMAL(20,4) NULL COMMENT '净流入额(万元)',
  net_mf_vol DECIMAL(20,4) NULL COMMENT '净流入量(万股)',
  buy_sm_amount DECIMAL(20,4) NULL COMMENT '小单买入额',
  sell_sm_amount DECIMAL(20,4) NULL COMMENT '小单卖出额',
  buy_md_amount DECIMAL(20,4) NULL COMMENT '中单买入额',
  sell_md_amount DECIMAL(20,4) NULL COMMENT '中单卖出额',
  buy_lg_amount DECIMAL(20,4) NULL COMMENT '大单买入额',
  sell_lg_amount DECIMAL(20,4) NULL COMMENT '大单卖出额',
  buy_elg_amount DECIMAL(20,4) NULL COMMENT '特大单买入额',
  sell_elg_amount DECIMAL(20,4) NULL COMMENT '特大单卖出额',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_moneyflow_ths_date (trade_date)
) ENGINE=InnoDB COMMENT='个股资金流向表(同花顺)';

CREATE TABLE IF NOT EXISTS ods_cyq_chips (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  price DECIMAL(20,4) NULL COMMENT '成本价格',
  percent DECIMAL(12,6) NULL COMMENT '占比',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_cyq_date (trade_date)
) ENGINE=InnoDB COMMENT='每日筹码分布表';

CREATE TABLE IF NOT EXISTS ods_stk_factor (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  close DECIMAL(20,4) NULL COMMENT '收盘价',
  open DECIMAL(20,4) NULL COMMENT '开盘价',
  high DECIMAL(20,4) NULL COMMENT '最高价',
  low DECIMAL(20,4) NULL COMMENT '最低价',
  pre_close DECIMAL(20,4) NULL COMMENT '昨收价',
  `change` DECIMAL(20,4) NULL COMMENT '涨跌额',
  pct_change DECIMAL(12,6) NULL COMMENT '涨跌幅',
  vol DECIMAL(20,4) NULL COMMENT '成交量',
  amount DECIMAL(20,4) NULL COMMENT '成交额',
  adj_factor DECIMAL(20,4) NULL COMMENT '复权因子',
  open_hfq DECIMAL(20,4) NULL COMMENT '后复权开盘价',
  open_qfq DECIMAL(20,4) NULL COMMENT '前复权开盘价',
  close_hfq DECIMAL(20,4) NULL COMMENT '后复权收盘价',
  close_qfq DECIMAL(20,4) NULL COMMENT '前复权收盘价',
  high_hfq DECIMAL(20,4) NULL COMMENT '后复权最高价',
  high_qfq DECIMAL(20,4) NULL COMMENT '前复权最高价',
  low_hfq DECIMAL(20,4) NULL COMMENT '后复权最低价',
  low_qfq DECIMAL(20,4) NULL COMMENT '前复权最低价',
  pre_close_hfq DECIMAL(20,4) NULL COMMENT '后复权昨收',
  pre_close_qfq DECIMAL(20,4) NULL COMMENT '前复权昨收',
  macd_dif DECIMAL(12,6) NULL COMMENT 'MACD_DIF',
  macd_dea DECIMAL(12,6) NULL COMMENT 'MACD_DEA',
  macd DECIMAL(12,6) NULL COMMENT 'MACD',
  kdj_k DECIMAL(12,6) NULL COMMENT 'KDJ_K',
  kdj_d DECIMAL(12,6) NULL COMMENT 'KDJ_D',
  kdj_j DECIMAL(12,6) NULL COMMENT 'KDJ_J',
  rsi_6 DECIMAL(12,6) NULL COMMENT 'RSI_6',
  rsi_12 DECIMAL(12,6) NULL COMMENT 'RSI_12',
  rsi_24 DECIMAL(12,6) NULL COMMENT 'RSI_24',
  boll_upper DECIMAL(20,4) NULL COMMENT 'BOLL_UPPER',
  boll_mid DECIMAL(20,4) NULL COMMENT 'BOLL_MID',
  boll_lower DECIMAL(20,4) NULL COMMENT 'BOLL_LOWER',
  cci DECIMAL(12,6) NULL COMMENT 'CCI',
  score DECIMAL(12,6) NULL COMMENT '技术评分',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_stk_factor_date (trade_date)
) ENGINE=InnoDB COMMENT='每日技术指标表';

CREATE TABLE IF NOT EXISTS ods_margin_raw (
  api_name VARCHAR(32) NOT NULL COMMENT 'API接口名称',
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL DEFAULT '' COMMENT '股票代码',
  exchange VARCHAR(8) NOT NULL DEFAULT '' COMMENT '交易所',
  payload JSON NOT NULL COMMENT '原始响应数据',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (api_name, trade_date, ts_code, exchange),
  KEY idx_margin_date (trade_date)
) ENGINE=InnoDB COMMENT='融资融券原始JSON数据表';

CREATE TABLE IF NOT EXISTS ods_moneyflow_raw (
  api_name VARCHAR(32) NOT NULL COMMENT 'API接口名称',
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL DEFAULT '' COMMENT '股票代码',
  payload JSON NOT NULL COMMENT '原始响应数据',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (api_name, trade_date, ts_code),
  KEY idx_moneyflow_date (trade_date)
) ENGINE=InnoDB COMMENT='资金流向原始JSON数据表';

CREATE TABLE IF NOT EXISTS ods_cyq_raw (
  api_name VARCHAR(32) NOT NULL COMMENT 'API接口名称',
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL DEFAULT '' COMMENT '股票代码',
  payload JSON NOT NULL COMMENT '原始响应数据',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (api_name, trade_date, ts_code),
  KEY idx_cyq_date (trade_date)
) ENGINE=InnoDB COMMENT='筹码分布原始JSON数据表';

CREATE TABLE IF NOT EXISTS ods_tech_score_raw (
  api_name VARCHAR(32) NOT NULL COMMENT 'API接口名称',
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL DEFAULT '' COMMENT '股票代码',
  payload JSON NOT NULL COMMENT '原始响应数据',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (api_name, trade_date, ts_code),
  KEY idx_tech_date (trade_date)
) ENGINE=InnoDB COMMENT='技术指标原始JSON数据表';

-- ========== DWD 日频事实表 ==========
CREATE TABLE IF NOT EXISTS dwd_daily (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  open DECIMAL(20,4) NULL COMMENT '开盘价',
  high DECIMAL(20,4) NULL COMMENT '最高价',
  low DECIMAL(20,4) NULL COMMENT '最低价',
  close DECIMAL(20,4) NULL COMMENT '收盘价',
  pre_close DECIMAL(20,4) NULL COMMENT '昨收价',
  change_amount DECIMAL(20,4) NULL COMMENT '涨跌额',
  pct_chg DECIMAL(12,6) NULL COMMENT '涨跌幅',
  vol DECIMAL(20,4) NULL COMMENT '成交量(手)',
  amount DECIMAL(20,4) NULL COMMENT '成交额(千元)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='日频行情事实表';

CREATE TABLE IF NOT EXISTS dwd_daily_basic (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  close DECIMAL(20,4) NULL COMMENT '收盘价',
  turnover_rate DECIMAL(12,6) NULL COMMENT '换手率',
  turnover_rate_f DECIMAL(12,6) NULL COMMENT '换手率(自由流通股)',
  volume_ratio DECIMAL(12,6) NULL COMMENT '量比',
  pe DECIMAL(12,6) NULL COMMENT '市盈率',
  pe_ttm DECIMAL(12,6) NULL COMMENT '市盈率(TTM)',
  pb DECIMAL(12,6) NULL COMMENT '市净率',
  ps DECIMAL(12,6) NULL COMMENT '市销率',
  ps_ttm DECIMAL(12,6) NULL COMMENT '市销率(TTM)',
  dv_ratio DECIMAL(12,6) NULL COMMENT '股息率',
  dv_ttm DECIMAL(12,6) NULL COMMENT '股息率(TTM)',
  total_share DECIMAL(20,4) NULL COMMENT '总股本',
  float_share DECIMAL(20,4) NULL COMMENT '流通股本',
  free_share DECIMAL(20,4) NULL COMMENT '自由流通股本',
  total_mv DECIMAL(20,4) NULL COMMENT '总市值',
  circ_mv DECIMAL(20,4) NULL COMMENT '流通市值',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='每日指标事实表';

CREATE TABLE IF NOT EXISTS dwd_adj_factor (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  adj_factor DECIMAL(20,6) NULL COMMENT '复权因子',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='复权因子事实表';

CREATE TABLE IF NOT EXISTS dwd_fina_indicator (
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  ann_date INT NOT NULL COMMENT '公告日期',
  end_date INT NOT NULL COMMENT '报告期',
  report_type VARCHAR(20) NULL COMMENT '报告类型',
  roe DECIMAL(12,6) NULL COMMENT 'ROE',
  grossprofit_margin DECIMAL(12,6) NULL COMMENT '毛利率',
  debt_to_assets DECIMAL(12,6) NULL COMMENT '资产负债率',
  netprofit_margin DECIMAL(12,6) NULL COMMENT '净利率',
  op_income DECIMAL(20,4) NULL COMMENT '营业收入',
  total_assets DECIMAL(20,4) NULL COMMENT '总资产',
  total_hldr_eqy DECIMAL(20,4) NULL COMMENT '股东权益',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ts_code, ann_date, end_date),
  KEY idx_ann_date (ann_date),
  KEY idx_ts_ann (ts_code, ann_date)
) ENGINE=InnoDB COMMENT='财务指标事实表';

-- ========== DWS 主题层 ==========
CREATE TABLE IF NOT EXISTS dws_price_adj_daily (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  qfq_close DECIMAL(20,4) NULL COMMENT '前复权收盘价',
  qfq_ret_1 DECIMAL(12,6) NULL COMMENT '前复权日收益率',
  qfq_ret_5 DECIMAL(12,6) NULL COMMENT '前复权5日收益率',
  qfq_ret_20 DECIMAL(12,6) NULL COMMENT '前复权20日收益率',
  qfq_ret_60 DECIMAL(12,6) NULL COMMENT '前复权60日收益率',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='价格复权日频主题表';

CREATE TABLE IF NOT EXISTS dws_fina_pit_daily (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  ann_date INT NOT NULL COMMENT '公告日期',
  end_date INT NOT NULL COMMENT '报告期',
  roe DECIMAL(12,6) NULL COMMENT 'ROE(PIT)',
  grossprofit_margin DECIMAL(12,6) NULL COMMENT '毛利率(PIT)',
  debt_to_assets DECIMAL(12,6) NULL COMMENT '资产负债率(PIT)',
  netprofit_margin DECIMAL(12,6) NULL COMMENT '净利率(PIT)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date),
  KEY idx_ann_date (ann_date)
) ENGINE=InnoDB COMMENT='财务PIT日频主题表';

-- ========== ADS 服务层 ==========
CREATE TABLE IF NOT EXISTS ads_features_stock_daily (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  ret_5 DECIMAL(12,6) NULL COMMENT '5日收益率',
  ret_20 DECIMAL(12,6) NULL COMMENT '20日收益率',
  ret_60 DECIMAL(12,6) NULL COMMENT '60日收益率',
  vol_20 DECIMAL(12,6) NULL COMMENT '20日平均成交量',
  vol_60 DECIMAL(12,6) NULL COMMENT '60日平均成交量',
  amt_ma20 DECIMAL(20,4) NULL COMMENT '20日成交额均值',
  turnover_rate DECIMAL(12,6) NULL COMMENT '换手率',
  pe_ttm DECIMAL(12,6) NULL COMMENT 'PE(TTM)',
  pb DECIMAL(12,6) NULL COMMENT 'PB',
  total_mv DECIMAL(20,4) NULL COMMENT '总市值',
  circ_mv DECIMAL(20,4) NULL COMMENT '流通市值',
  roe DECIMAL(12,6) NULL COMMENT 'ROE',
  grossprofit_margin DECIMAL(12,6) NULL COMMENT '毛利率',
  debt_to_assets DECIMAL(12,6) NULL COMMENT '资产负债率',
  industry_code VARCHAR(20) NULL COMMENT '行业代码',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='股票日频因子服务表';

CREATE TABLE IF NOT EXISTS ads_universe_daily (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  is_tradable TINYINT NOT NULL COMMENT '是否可交易(1是/0否)',
  is_listed TINYINT NOT NULL COMMENT '是否在市(1是/0否)',
  is_suspended TINYINT NOT NULL COMMENT '是否停牌(1是/0否)',
  no_amount TINYINT NOT NULL COMMENT '是否无成交(1是/0否)',
  filter_flags VARCHAR(200) NULL COMMENT '过滤原因',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='每日股票池(交易宇宙)';

-- ========== 数据质量校验（日志级） ==========
CREATE TABLE IF NOT EXISTS meta_quality_check_log (
  id BIGINT NOT NULL AUTO_INCREMENT,
  check_date INT NOT NULL COMMENT '校验日期',
  check_name VARCHAR(100) NOT NULL COMMENT '校验规则名称',
  status VARCHAR(16) NOT NULL COMMENT '状态',
  detail TEXT NULL COMMENT '详情',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_check_date (check_date)
) ENGINE=InnoDB COMMENT='数据质量校验日志';
