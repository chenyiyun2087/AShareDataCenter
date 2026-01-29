-- TuShare 日频选股平台数据底座（MySQL 8.0）DDL
-- 统一约定：trade_date INT(YYYYMMDD)，ts_code CHAR(9)，金额/市值 DECIMAL(20,4)，比率 DECIMAL(12,6)

CREATE DATABASE IF NOT EXISTS tushare_stock
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE tushare_stock;

-- ========== 采集水位与运行日志 ==========
CREATE TABLE IF NOT EXISTS meta_etl_watermark (
  api_name VARCHAR(64) NOT NULL,
  water_mark INT NOT NULL,
  status VARCHAR(16) NOT NULL,
  last_run_at DATETIME NULL,
  last_err TEXT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (api_name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS meta_etl_run_log (
  id BIGINT NOT NULL AUTO_INCREMENT,
  api_name VARCHAR(64) NOT NULL,
  run_type VARCHAR(16) NOT NULL,
  start_at DATETIME NOT NULL,
  end_at DATETIME NULL,
  request_count INT NOT NULL DEFAULT 0,
  fail_count INT NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL,
  err_msg TEXT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_api_start (api_name, start_at)
) ENGINE=InnoDB;

-- ========== 维表 ==========
CREATE TABLE IF NOT EXISTS dim_stock (
  ts_code CHAR(9) NOT NULL,
  symbol VARCHAR(10) NOT NULL,
  name VARCHAR(50) NOT NULL,
  area VARCHAR(20) NULL,
  industry VARCHAR(50) NULL,
  market VARCHAR(20) NULL,
  list_date INT NULL,
  delist_date INT NULL,
  is_hs CHAR(1) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ts_code)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dim_trade_cal (
  exchange VARCHAR(8) NOT NULL,
  cal_date INT NOT NULL,
  is_open TINYINT NOT NULL,
  pretrade_date INT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (exchange, cal_date),
  KEY idx_cal_date (cal_date)
) ENGINE=InnoDB;

-- ========== ODS 原始表 ==========
CREATE TABLE IF NOT EXISTS ods_daily (
  trade_date INT NOT NULL,
  ts_code CHAR(9) NOT NULL,
  open DECIMAL(20,4) NULL,
  high DECIMAL(20,4) NULL,
  low DECIMAL(20,4) NULL,
  close DECIMAL(20,4) NULL,
  pre_close DECIMAL(20,4) NULL,
  `change` DECIMAL(20,4) NULL,
  pct_chg DECIMAL(12,6) NULL,
  vol DECIMAL(20,4) NULL,
  amount DECIMAL(20,4) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ods_daily_basic (
  trade_date INT NOT NULL,
  ts_code CHAR(9) NOT NULL,
  close DECIMAL(20,4) NULL,
  turnover_rate DECIMAL(12,6) NULL,
  turnover_rate_f DECIMAL(12,6) NULL,
  volume_ratio DECIMAL(12,6) NULL,
  pe DECIMAL(20,4) NULL,
  pe_ttm DECIMAL(20,4) NULL,
  pb DECIMAL(20,4) NULL,
  ps DECIMAL(20,4) NULL,
  ps_ttm DECIMAL(20,4) NULL,
  dv_ratio DECIMAL(12,6) NULL,
  dv_ttm DECIMAL(12,6) NULL,
  total_share DECIMAL(20,4) NULL,
  float_share DECIMAL(20,4) NULL,
  free_share DECIMAL(20,4) NULL,
  total_mv DECIMAL(20,4) NULL,
  circ_mv DECIMAL(20,4) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ods_adj_factor (
  trade_date INT NOT NULL,
  ts_code CHAR(9) NOT NULL,
  adj_factor DECIMAL(20,4) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ods_fina_indicator (
  ts_code CHAR(9) NOT NULL,
  ann_date INT NOT NULL,
  end_date INT NOT NULL,
  report_type VARCHAR(8) NULL,
  roe DECIMAL(12,6) NULL,
  grossprofit_margin DECIMAL(12,6) NULL,
  debt_to_assets DECIMAL(12,6) NULL,
  netprofit_margin DECIMAL(12,6) NULL,
  op_income DECIMAL(20,4) NULL,
  total_assets DECIMAL(20,4) NULL,
  total_hldr_eqy DECIMAL(20,4) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ts_code, ann_date, end_date),
  KEY idx_ts_ann (ts_code, ann_date)
) ENGINE=InnoDB;

-- ========== DWD 日频事实表 ==========
CREATE TABLE IF NOT EXISTS dwd_daily (
  trade_date INT NOT NULL,
  ts_code CHAR(9) NOT NULL,
  open DECIMAL(20,4) NULL,
  high DECIMAL(20,4) NULL,
  low DECIMAL(20,4) NULL,
  close DECIMAL(20,4) NULL,
  pre_close DECIMAL(20,4) NULL,
  change_amount DECIMAL(20,4) NULL,
  pct_chg DECIMAL(12,6) NULL,
  vol DECIMAL(20,4) NULL,
  amount DECIMAL(20,4) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dwd_daily_basic (
  trade_date INT NOT NULL,
  ts_code CHAR(9) NOT NULL,
  close DECIMAL(20,4) NULL,
  turnover_rate DECIMAL(12,6) NULL,
  turnover_rate_f DECIMAL(12,6) NULL,
  volume_ratio DECIMAL(12,6) NULL,
  pe DECIMAL(12,6) NULL,
  pe_ttm DECIMAL(12,6) NULL,
  pb DECIMAL(12,6) NULL,
  ps DECIMAL(12,6) NULL,
  ps_ttm DECIMAL(12,6) NULL,
  dv_ratio DECIMAL(12,6) NULL,
  dv_ttm DECIMAL(12,6) NULL,
  total_share DECIMAL(20,4) NULL,
  float_share DECIMAL(20,4) NULL,
  free_share DECIMAL(20,4) NULL,
  total_mv DECIMAL(20,4) NULL,
  circ_mv DECIMAL(20,4) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dwd_adj_factor (
  trade_date INT NOT NULL,
  ts_code CHAR(9) NOT NULL,
  adj_factor DECIMAL(20,6) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dwd_fina_indicator (
  ts_code CHAR(9) NOT NULL,
  ann_date INT NOT NULL,
  end_date INT NOT NULL,
  report_type VARCHAR(20) NULL,
  roe DECIMAL(12,6) NULL,
  grossprofit_margin DECIMAL(12,6) NULL,
  debt_to_assets DECIMAL(12,6) NULL,
  netprofit_margin DECIMAL(12,6) NULL,
  op_income DECIMAL(20,4) NULL,
  total_assets DECIMAL(20,4) NULL,
  total_hldr_eqy DECIMAL(20,4) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ts_code, ann_date, end_date),
  KEY idx_ann_date (ann_date),
  KEY idx_ts_ann (ts_code, ann_date)
) ENGINE=InnoDB;

-- ========== DWS 主题层 ==========
CREATE TABLE IF NOT EXISTS dws_price_adj_daily (
  trade_date INT NOT NULL,
  ts_code CHAR(9) NOT NULL,
  qfq_close DECIMAL(20,4) NULL,
  qfq_ret_1 DECIMAL(12,6) NULL,
  qfq_ret_5 DECIMAL(12,6) NULL,
  qfq_ret_20 DECIMAL(12,6) NULL,
  qfq_ret_60 DECIMAL(12,6) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dws_fina_pit_daily (
  trade_date INT NOT NULL,
  ts_code CHAR(9) NOT NULL,
  ann_date INT NOT NULL,
  end_date INT NOT NULL,
  roe DECIMAL(12,6) NULL,
  grossprofit_margin DECIMAL(12,6) NULL,
  debt_to_assets DECIMAL(12,6) NULL,
  netprofit_margin DECIMAL(12,6) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date),
  KEY idx_ann_date (ann_date)
) ENGINE=InnoDB;

-- ========== ADS 服务层 ==========
CREATE TABLE IF NOT EXISTS ads_features_stock_daily (
  trade_date INT NOT NULL,
  ts_code CHAR(9) NOT NULL,
  ret_5 DECIMAL(12,6) NULL,
  ret_20 DECIMAL(12,6) NULL,
  ret_60 DECIMAL(12,6) NULL,
  vol_20 DECIMAL(12,6) NULL,
  vol_60 DECIMAL(12,6) NULL,
  amt_ma20 DECIMAL(20,4) NULL,
  turnover_rate DECIMAL(12,6) NULL,
  pe_ttm DECIMAL(12,6) NULL,
  pb DECIMAL(12,6) NULL,
  total_mv DECIMAL(20,4) NULL,
  circ_mv DECIMAL(20,4) NULL,
  roe DECIMAL(12,6) NULL,
  grossprofit_margin DECIMAL(12,6) NULL,
  debt_to_assets DECIMAL(12,6) NULL,
  industry_code VARCHAR(20) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ads_universe_daily (
  trade_date INT NOT NULL,
  ts_code CHAR(9) NOT NULL,
  is_tradable TINYINT NOT NULL,
  is_listed TINYINT NOT NULL,
  is_suspended TINYINT NOT NULL,
  no_amount TINYINT NOT NULL,
  filter_flags VARCHAR(200) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB;

-- ========== 数据质量校验（日志级） ==========
CREATE TABLE IF NOT EXISTS meta_quality_check_log (
  id BIGINT NOT NULL AUTO_INCREMENT,
  check_date INT NOT NULL,
  check_name VARCHAR(100) NOT NULL,
  status VARCHAR(16) NOT NULL,
  detail TEXT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_check_date (check_date)
) ENGINE=InnoDB;
