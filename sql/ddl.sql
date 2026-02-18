-- TuShare 日频选股平台数据底座（MySQL 8.0）DDL
-- 统一约定：trade_date INT(YYYYMMDD)，ts_code CHAR(9)，金额/市值 DECIMAL(20,4)，比率 DECIMAL(12,6)

CREATE DATABASE IF NOT EXISTS tushare_stock
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE tushare_stock;

-- ========== 采集水位与运行日志 ==========
-- 采集采集水位表
CREATE TABLE IF NOT EXISTS meta_etl_watermark (
  api_name VARCHAR(64) NOT NULL COMMENT '接口名称',
  water_mark INT NOT NULL COMMENT '当前水位(YYYYMMDD)',
  status VARCHAR(16) NOT NULL COMMENT '状态(SUCCESS/FAILED)',
  last_run_at DATETIME NULL COMMENT '上次运行时间',
  last_err TEXT NULL COMMENT '上次错误信息',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (api_name)
) ENGINE=InnoDB COMMENT='ETL采集水位表';
-- ETL运行日志表
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
-- 股票维度表
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
-- 交易日历表
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
-- 日频行情表
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
-- 日频指标表
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
-- 复权因子表
CREATE TABLE IF NOT EXISTS ods_adj_factor (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  adj_factor DECIMAL(20,4) NULL COMMENT '复权因子',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='复权因子表';
-- 财务指标表
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
-- 融资融券余额表
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
-- 融资融券明细表
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
-- 融资融券标的表
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
-- 资金流向表
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

-- 个股资金流向表
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

-- 每日筹码分布表
CREATE TABLE IF NOT EXISTS ods_cyq_chips (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  price DECIMAL(20,4) NULL COMMENT '成本价格',
  percent DECIMAL(12,6) NULL COMMENT '占比',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_cyq_date (trade_date)
) ENGINE=InnoDB COMMENT='每日筹码分布表';

-- 每日筹码绩效表
CREATE TABLE IF NOT EXISTS ods_cyq_perf (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  his_low DECIMAL(20,4) NULL COMMENT '历史最低价',
  his_high DECIMAL(20,4) NULL COMMENT '历史最高价',
  cost_5pct DECIMAL(20,4) NULL COMMENT '5分位成本',
  cost_15pct DECIMAL(20,4) NULL COMMENT '15分位成本',
  cost_50pct DECIMAL(20,4) NULL COMMENT '50分位成本',
  cost_85pct DECIMAL(20,4) NULL COMMENT '85分位成本',
  cost_95pct DECIMAL(20,4) NULL COMMENT '95分位成本',
  weight_avg DECIMAL(20,4) NULL COMMENT '加权平均成本',
  winner_rate DECIMAL(12,6) NULL COMMENT '获利比例',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_cyq_perf_date (trade_date)
) ENGINE=InnoDB COMMENT='每日筹码绩效表';

-- 股票因子表
CREATE TABLE IF NOT EXISTS ods_stk_factor (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  
  -- 基础行情
  open DECIMAL(20,4) NULL COMMENT '开盘价',
  high DECIMAL(20,4) NULL COMMENT '最高价',
  low DECIMAL(20,4) NULL COMMENT '最低价',
  close DECIMAL(20,4) NULL COMMENT '收盘价',
  pre_close DECIMAL(20,4) NULL COMMENT '昨收价',
  `change` DECIMAL(20,4) NULL COMMENT '涨跌额',
  pct_chg DECIMAL(12,6) NULL COMMENT '涨跌幅',
  vol DECIMAL(20,4) NULL COMMENT '成交量',
  amount DECIMAL(20,4) NULL COMMENT '成交额',
  turnover_rate DECIMAL(12,6) NULL COMMENT '换手率',
  turnover_rate_f DECIMAL(12,6) NULL COMMENT '换手率(自由流通)',
  volume_ratio DECIMAL(12,6) NULL COMMENT '量比',
  pe DECIMAL(20,4) NULL COMMENT 'PE',
  pe_ttm DECIMAL(20,4) NULL COMMENT 'PE_TTM',
  pb DECIMAL(20,4) NULL COMMENT 'PB',
  ps DECIMAL(20,4) NULL COMMENT 'PS',
  ps_ttm DECIMAL(20,4) NULL COMMENT 'PS_TTM',
  dv_ratio DECIMAL(12,6) NULL COMMENT '股息率',
  dv_ttm DECIMAL(12,6) NULL COMMENT '股息率TTM',
  total_share DECIMAL(20,4) NULL COMMENT '总股本',
  float_share DECIMAL(20,4) NULL COMMENT '流通股本',
  free_share DECIMAL(20,4) NULL COMMENT '自由流通股本',
  total_mv DECIMAL(20,4) NULL COMMENT '总市值',
  circ_mv DECIMAL(20,4) NULL COMMENT '流通市值',
  adj_factor DECIMAL(20,4) NULL COMMENT '复权因子',

  -- 复权行情 (hfq=后复权, qfq=前复权)
  open_hfq DECIMAL(20,4) NULL, open_qfq DECIMAL(20,4) NULL,
  high_hfq DECIMAL(20,4) NULL, high_qfq DECIMAL(20,4) NULL,
  low_hfq DECIMAL(20,4) NULL, low_qfq DECIMAL(20,4) NULL,
  close_hfq DECIMAL(20,4) NULL, close_qfq DECIMAL(20,4) NULL,
  pre_close_hfq DECIMAL(20,4) NULL, pre_close_qfq DECIMAL(20,4) NULL,
  
  -- 技术指标 (bfq=不复权, hfq=后复权, qfq=前复权)
  -- MACD
  macd_bfq DECIMAL(12,6) NULL, macd_hfq DECIMAL(12,6) NULL, macd_qfq DECIMAL(12,6) NULL,
  macd_dea_bfq DECIMAL(12,6) NULL, macd_dea_hfq DECIMAL(12,6) NULL, macd_dea_qfq DECIMAL(12,6) NULL,
  macd_dif_bfq DECIMAL(12,6) NULL, macd_dif_hfq DECIMAL(12,6) NULL, macd_dif_qfq DECIMAL(12,6) NULL,
  
  -- KDJ
  kdj_bfq DECIMAL(12,6) NULL, kdj_hfq DECIMAL(12,6) NULL, kdj_qfq DECIMAL(12,6) NULL,
  kdj_d_bfq DECIMAL(12,6) NULL, kdj_d_hfq DECIMAL(12,6) NULL, kdj_d_qfq DECIMAL(12,6) NULL,
  kdj_k_bfq DECIMAL(12,6) NULL, kdj_k_hfq DECIMAL(12,6) NULL, kdj_k_qfq DECIMAL(12,6) NULL,
  
  -- RSI
  rsi_bfq_6 DECIMAL(12,6) NULL, rsi_hfq_6 DECIMAL(12,6) NULL, rsi_qfq_6 DECIMAL(12,6) NULL,
  rsi_bfq_12 DECIMAL(12,6) NULL, rsi_hfq_12 DECIMAL(12,6) NULL, rsi_qfq_12 DECIMAL(12,6) NULL,
  rsi_bfq_24 DECIMAL(12,6) NULL, rsi_hfq_24 DECIMAL(12,6) NULL, rsi_qfq_24 DECIMAL(12,6) NULL,
  
  -- BOLL
  boll_upper_bfq DECIMAL(20,4) NULL, boll_upper_hfq DECIMAL(20,4) NULL, boll_upper_qfq DECIMAL(20,4) NULL,
  boll_mid_bfq DECIMAL(20,4) NULL, boll_mid_hfq DECIMAL(20,4) NULL, boll_mid_qfq DECIMAL(20,4) NULL,
  boll_lower_bfq DECIMAL(20,4) NULL, boll_lower_hfq DECIMAL(20,4) NULL, boll_lower_qfq DECIMAL(20,4) NULL,
  
  -- CCI
  cci_bfq DECIMAL(12,6) NULL, cci_hfq DECIMAL(12,6) NULL, cci_qfq DECIMAL(12,6) NULL,
  
  -- 其他指标 (按首字母排序)
  asi_bfq DECIMAL(20,4) NULL, asi_hfq DECIMAL(20,4) NULL, asi_qfq DECIMAL(20,4) NULL,
  asit_bfq DECIMAL(20,4) NULL, asit_hfq DECIMAL(20,4) NULL, asit_qfq DECIMAL(20,4) NULL,
  atr_bfq DECIMAL(12,6) NULL, atr_hfq DECIMAL(12,6) NULL, atr_qfq DECIMAL(12,6) NULL,
  bbi_bfq DECIMAL(12,6) NULL, bbi_hfq DECIMAL(12,6) NULL, bbi_qfq DECIMAL(12,6) NULL,
  bias1_bfq DECIMAL(12,6) NULL, bias1_hfq DECIMAL(12,6) NULL, bias1_qfq DECIMAL(12,6) NULL,
  bias2_bfq DECIMAL(12,6) NULL, bias2_hfq DECIMAL(12,6) NULL, bias2_qfq DECIMAL(12,6) NULL,
  bias3_bfq DECIMAL(12,6) NULL, bias3_hfq DECIMAL(12,6) NULL, bias3_qfq DECIMAL(12,6) NULL,
  brar_ar_bfq DECIMAL(12,6) NULL, brar_ar_hfq DECIMAL(12,6) NULL, brar_ar_qfq DECIMAL(12,6) NULL,
  brar_br_bfq DECIMAL(12,6) NULL, brar_br_hfq DECIMAL(12,6) NULL, brar_br_qfq DECIMAL(12,6) NULL,
  cr_bfq DECIMAL(12,6) NULL, cr_hfq DECIMAL(12,6) NULL, cr_qfq DECIMAL(12,6) NULL,
  dfma_dif_bfq DECIMAL(12,6) NULL, dfma_dif_hfq DECIMAL(12,6) NULL, dfma_dif_qfq DECIMAL(12,6) NULL,
  dfma_difma_bfq DECIMAL(12,6) NULL, dfma_difma_hfq DECIMAL(12,6) NULL, dfma_difma_qfq DECIMAL(12,6) NULL,
  dmi_adx_bfq DECIMAL(12,6) NULL, dmi_adx_hfq DECIMAL(12,6) NULL, dmi_adx_qfq DECIMAL(12,6) NULL,
  dmi_adxr_bfq DECIMAL(12,6) NULL, dmi_adxr_hfq DECIMAL(12,6) NULL, dmi_adxr_qfq DECIMAL(12,6) NULL,
  dmi_mdi_bfq DECIMAL(12,6) NULL, dmi_mdi_hfq DECIMAL(12,6) NULL, dmi_mdi_qfq DECIMAL(12,6) NULL,
  dmi_pdi_bfq DECIMAL(12,6) NULL, dmi_pdi_hfq DECIMAL(12,6) NULL, dmi_pdi_qfq DECIMAL(12,6) NULL,
  dpo_bfq DECIMAL(12,6) NULL, dpo_hfq DECIMAL(12,6) NULL, dpo_qfq DECIMAL(12,6) NULL,
  madpo_bfq DECIMAL(12,6) NULL, madpo_hfq DECIMAL(12,6) NULL, madpo_qfq DECIMAL(12,6) NULL,
  emv_bfq DECIMAL(12,6) NULL, emv_hfq DECIMAL(12,6) NULL, emv_qfq DECIMAL(12,6) NULL,
  maemv_bfq DECIMAL(12,6) NULL, maemv_hfq DECIMAL(12,6) NULL, maemv_qfq DECIMAL(12,6) NULL,
  ktn_down_bfq DECIMAL(12,6) NULL, ktn_down_hfq DECIMAL(12,6) NULL, ktn_down_qfq DECIMAL(12,6) NULL,
  ktn_mid_bfq DECIMAL(12,6) NULL, ktn_mid_hfq DECIMAL(12,6) NULL, ktn_mid_qfq DECIMAL(12,6) NULL,
  ktn_upper_bfq DECIMAL(12,6) NULL, ktn_upper_hfq DECIMAL(12,6) NULL, ktn_upper_qfq DECIMAL(12,6) NULL,
  mass_bfq DECIMAL(12,6) NULL, mass_hfq DECIMAL(12,6) NULL, mass_qfq DECIMAL(12,6) NULL,
  ma_mass_bfq DECIMAL(12,6) NULL, ma_mass_hfq DECIMAL(12,6) NULL, ma_mass_qfq DECIMAL(12,6) NULL,
  mfi_bfq DECIMAL(12,6) NULL, mfi_hfq DECIMAL(12,6) NULL, mfi_qfq DECIMAL(12,6) NULL,
  mtm_bfq DECIMAL(12,6) NULL, mtm_hfq DECIMAL(12,6) NULL, mtm_qfq DECIMAL(12,6) NULL,
  mtmma_bfq DECIMAL(12,6) NULL, mtmma_hfq DECIMAL(12,6) NULL, mtmma_qfq DECIMAL(12,6) NULL,
  obv_bfq DECIMAL(20,4) NULL, obv_hfq DECIMAL(20,4) NULL, obv_qfq DECIMAL(20,4) NULL,
  psy_bfq DECIMAL(12,6) NULL, psy_hfq DECIMAL(12,6) NULL, psy_qfq DECIMAL(12,6) NULL,
  psyma_bfq DECIMAL(12,6) NULL, psyma_hfq DECIMAL(12,6) NULL, psyma_qfq DECIMAL(12,6) NULL,
  roc_bfq DECIMAL(12,6) NULL, roc_hfq DECIMAL(12,6) NULL, roc_qfq DECIMAL(12,6) NULL,
  maroc_bfq DECIMAL(12,6) NULL, maroc_hfq DECIMAL(12,6) NULL, maroc_qfq DECIMAL(12,6) NULL,
  taq_down_bfq DECIMAL(12,6) NULL, taq_down_hfq DECIMAL(12,6) NULL, taq_down_qfq DECIMAL(12,6) NULL,
  taq_mid_bfq DECIMAL(12,6) NULL, taq_mid_hfq DECIMAL(12,6) NULL, taq_mid_qfq DECIMAL(12,6) NULL,
  taq_up_bfq DECIMAL(12,6) NULL, taq_up_hfq DECIMAL(12,6) NULL, taq_up_qfq DECIMAL(12,6) NULL,
  trix_bfq DECIMAL(12,6) NULL, trix_hfq DECIMAL(12,6) NULL, trix_qfq DECIMAL(12,6) NULL,
  trma_bfq DECIMAL(12,6) NULL, trma_hfq DECIMAL(12,6) NULL, trma_qfq DECIMAL(12,6) NULL,
  vr_bfq DECIMAL(12,6) NULL, vr_hfq DECIMAL(12,6) NULL, vr_qfq DECIMAL(12,6) NULL,
  wr_bfq DECIMAL(12,6) NULL, wr_hfq DECIMAL(12,6) NULL, wr_qfq DECIMAL(12,6) NULL,
  wr1_bfq DECIMAL(12,6) NULL, wr1_hfq DECIMAL(12,6) NULL, wr1_qfq DECIMAL(12,6) NULL,
  xsii_td1_bfq DECIMAL(12,6) NULL, xsii_td1_hfq DECIMAL(12,6) NULL, xsii_td1_qfq DECIMAL(12,6) NULL,
  xsii_td2_bfq DECIMAL(12,6) NULL, xsii_td2_hfq DECIMAL(12,6) NULL, xsii_td2_qfq DECIMAL(12,6) NULL,
  xsii_td3_bfq DECIMAL(12,6) NULL, xsii_td3_hfq DECIMAL(12,6) NULL, xsii_td3_qfq DECIMAL(12,6) NULL,
  xsii_td4_bfq DECIMAL(12,6) NULL, xsii_td4_hfq DECIMAL(12,6) NULL, xsii_td4_qfq DECIMAL(12,6) NULL,

  -- 均线 (MA/EMA/EXPMA) - 这里简化只存部分关键周期，或由用户决定是否全存。
  -- 鉴于字段限制，我先保留常用周期：5, 10, 20, 60, 250
  ma_bfq_5 DECIMAL(20,4) NULL, ma_bfq_10 DECIMAL(20,4) NULL, ma_bfq_20 DECIMAL(20,4) NULL, ma_bfq_60 DECIMAL(20,4) NULL, ma_bfq_250 DECIMAL(20,4) NULL,
  ma_qfq_5 DECIMAL(20,4) NULL, ma_qfq_10 DECIMAL(20,4) NULL, ma_qfq_20 DECIMAL(20,4) NULL, ma_qfq_60 DECIMAL(20,4) NULL, ma_qfq_250 DECIMAL(20,4) NULL,
  ma_hfq_5 DECIMAL(20,4) NULL, ma_hfq_10 DECIMAL(20,4) NULL, ma_hfq_20 DECIMAL(20,4) NULL, ma_hfq_60 DECIMAL(20,4) NULL, ma_hfq_250 DECIMAL(20,4) NULL,
  
  -- 统计天数
  updays INT NULL COMMENT '连涨天数',
  downdays INT NULL COMMENT '连跌天数',
  lowdays INT NULL COMMENT '近期新低天数',
  topdays INT NULL COMMENT '近期新高天数',

  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_stk_factor_date (trade_date)
) ENGINE=InnoDB COMMENT='股票技术因子表(Pro版)';

-- 融资融券原始表
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

-- 资金流向原始表
CREATE TABLE IF NOT EXISTS ods_moneyflow_raw (
  api_name VARCHAR(32) NOT NULL COMMENT 'API接口名称',
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL DEFAULT '' COMMENT '股票代码',
  payload JSON NOT NULL COMMENT '原始响应数据',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (api_name, trade_date, ts_code),
  KEY idx_moneyflow_date (trade_date)
) ENGINE=InnoDB COMMENT='资金流向原始JSON数据表';

-- 筹码分布原始表
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

-- 日线行情表
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

-- 日线行情表
CREATE TABLE IF NOT EXISTS dwd_daily_basic (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  close DECIMAL(20,4) NULL COMMENT '收盘价',
  turnover_rate DECIMAL(12,6) NULL COMMENT '换手率',
  turnover_rate_f DECIMAL(12,6) NULL COMMENT '换手率(自由流通股)',
  volume_ratio DECIMAL(12,6) NULL COMMENT '量比',
  pe DECIMAL(20,4) NULL COMMENT '市盈率',
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
) ENGINE=InnoDB COMMENT='每日指标事实表';

-- 复权因子表
CREATE TABLE IF NOT EXISTS dwd_adj_factor (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  adj_factor DECIMAL(20,6) NULL COMMENT '复权因子',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='复权因子事实表';

-- 财务指标表
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

-- 行情数据标准化表
CREATE TABLE IF NOT EXISTS dwd_stock_daily_standard (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  adj_open DECIMAL(20,4) NULL COMMENT '前复权开盘价',
  adj_high DECIMAL(20,4) NULL COMMENT '前复权最高价',
  adj_low DECIMAL(20,4) NULL COMMENT '前复权最低价',
  adj_close DECIMAL(20,4) NULL COMMENT '前复权收盘价',
  turnover_rate_f DECIMAL(12,6) NULL COMMENT '自由流通换手率',
  vol DECIMAL(20,4) NULL COMMENT '成交量(手)',
  amount DECIMAL(20,4) NULL COMMENT '成交额(千元)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='行情数据标准化表';

-- 财务指标快照表(按交易日)
CREATE TABLE IF NOT EXISTS dwd_fina_snapshot (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  roe_ttm DECIMAL(12,6) NULL COMMENT 'ROE(TTM)',
  netprofit_margin DECIMAL(12,6) NULL COMMENT '净利率',
  grossprofit_margin DECIMAL(12,6) NULL COMMENT '毛利率',
  debt_to_assets DECIMAL(12,6) NULL COMMENT '资产负债率',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='财务指标快照表';

-- 融资情绪表
CREATE TABLE IF NOT EXISTS dwd_margin_sentiment (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  rz_net_buy DECIMAL(20,4) NULL COMMENT '融资净买入(元)',
  rz_net_buy_ratio DECIMAL(12,6) NULL COMMENT '融资净买入占比',
  rz_change_rate DECIMAL(12,6) NULL COMMENT '融资余额变化率',
  rq_pressure DECIMAL(12,6) NULL COMMENT '融券压力',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='融资情绪表';

-- 筹码稳定性表
CREATE TABLE IF NOT EXISTS dwd_chip_stability (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  avg_cost DECIMAL(20,4) NULL COMMENT '平均成本',
  winner_rate DECIMAL(12,6) NULL COMMENT '获利比例',
  chip_concentration DECIMAL(12,6) NULL COMMENT '筹码集中度',
  cost_deviation DECIMAL(12,6) NULL COMMENT '成本偏离度',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='筹码稳定性表';

-- ========== DWS 主题层 ==========


-- 日线行情主题表
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

-- 财务指标主题表
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

-- 技术形态主题表
CREATE TABLE IF NOT EXISTS dws_tech_pattern (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  hma_5 DECIMAL(20,4) NULL COMMENT '5日赫尔移动平均',
  hma_slope DECIMAL(12,6) NULL COMMENT 'HMA斜率',
  rsi_14 DECIMAL(12,6) NULL COMMENT '14日RSI',
  boll_upper DECIMAL(20,4) NULL COMMENT '布林上轨',
  boll_mid DECIMAL(20,4) NULL COMMENT '布林中轨',
  boll_lower DECIMAL(20,4) NULL COMMENT '布林下轨',
  boll_width DECIMAL(12,6) NULL COMMENT '布林带宽',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='技术形态主题表';

-- 资金驱动主题表
CREATE TABLE IF NOT EXISTS dws_capital_flow (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  main_net_inflow DECIMAL(20,4) NULL COMMENT '主力净流入(万元)',
  main_net_ratio DECIMAL(12,6) NULL COMMENT '主力净流入占比',
  main_net_ma5 DECIMAL(20,4) NULL COMMENT '5日主力净流入均值',
  vol_price_corr DECIMAL(12,6) NULL COMMENT '量价协同因子',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='资金驱动主题表';

-- 情绪杠杆主题表
CREATE TABLE IF NOT EXISTS dws_leverage_sentiment (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  rz_buy_intensity DECIMAL(12,6) NULL COMMENT '融资买入强度',
  rz_concentration DECIMAL(12,6) NULL COMMENT '融资集中度',
  rq_pressure_factor DECIMAL(12,6) NULL COMMENT '融券压力因子',
  turnover_spike DECIMAL(12,6) NULL COMMENT '换手率异常脉冲',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='情绪杠杆主题表';

-- 筹码分布主题表
CREATE TABLE IF NOT EXISTS dws_chip_dynamics (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  profit_ratio DECIMAL(12,6) NULL COMMENT '获利比例',
  profit_pressure DECIMAL(12,6) NULL COMMENT '获利回吐压力',
  support_strength DECIMAL(12,6) NULL COMMENT '支撑强度',
  chip_peak_cross DECIMAL(12,6) NULL COMMENT '筹码峰穿越因子',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='筹码分布主题表';

-- 动量评分表
CREATE TABLE IF NOT EXISTS dws_momentum_score (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  ret_5_score DECIMAL(4,2) NULL COMMENT '5日收益评分(0-5)',
  ret_20_score DECIMAL(4,2) NULL COMMENT '20日收益评分(0-5)',
  ret_60_score DECIMAL(4,2) NULL COMMENT '60日收益评分(0-5)',
  vol_ratio_score DECIMAL(4,2) NULL COMMENT '量比评分(0-5)',
  turnover_score DECIMAL(4,2) NULL COMMENT '换手率评分(0-5)',
  momentum_score DECIMAL(5,2) NULL COMMENT '动量总分(0-25)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='动量评分表';

-- 价值评分表
CREATE TABLE IF NOT EXISTS dws_value_score (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  pe_score DECIMAL(4,2) NULL COMMENT 'PE评分(0-7)',
  pb_score DECIMAL(4,2) NULL COMMENT 'PB评分(0-7)',
  ps_score DECIMAL(4,2) NULL COMMENT 'PS评分(0-6)',
  value_score DECIMAL(5,2) NULL COMMENT '价值总分(0-20)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='价值评分表';

-- 质量评分表
CREATE TABLE IF NOT EXISTS dws_quality_score (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  roe_score DECIMAL(4,2) NULL COMMENT 'ROE评分(0-8)',
  margin_score DECIMAL(4,2) NULL COMMENT '毛利率评分(0-6)',
  leverage_score DECIMAL(4,2) NULL COMMENT '负债率评分(0-6)',
  quality_score DECIMAL(5,2) NULL COMMENT '质量总分(0-20)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='质量评分表';

-- 技术评分表
CREATE TABLE IF NOT EXISTS dws_technical_score (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  macd_score DECIMAL(4,2) NULL COMMENT 'MACD评分(0-5)',
  kdj_score DECIMAL(4,2) NULL COMMENT 'KDJ评分(0-5)',
  rsi_score DECIMAL(4,2) NULL COMMENT 'RSI评分(0-5)',
  technical_score DECIMAL(5,2) NULL COMMENT '技术总分(0-15)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='技术评分表';

-- 资金评分表
CREATE TABLE IF NOT EXISTS dws_capital_score (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  elg_net DECIMAL(18,2) NULL COMMENT '特大单净流入(万元)',
  lg_net DECIMAL(18,2) NULL COMMENT '大单净流入(万元)',
  elg_score DECIMAL(4,2) NULL COMMENT '特大单评分(0-6)',
  lg_score DECIMAL(4,2) NULL COMMENT '大单评分(0-4)',
  capital_score DECIMAL(5,2) NULL COMMENT '资金总分(0-10)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='资金评分表';

-- 筹码评分表
CREATE TABLE IF NOT EXISTS dws_chip_score (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  winner_score DECIMAL(4,2) NULL COMMENT '获利比例评分(0-6)',
  cost_score DECIMAL(4,2) NULL COMMENT '成本偏离评分(0-4)',
  chip_score DECIMAL(5,2) NULL COMMENT '筹码总分(0-10)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='筹码评分表';

-- ========== ADS 服务层 ==========
-- 股票日频因子服务表
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
  pe_ttm DECIMAL(20,4) NULL COMMENT 'PE(TTM)',
  pb DECIMAL(20,4) NULL COMMENT 'PB',
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
-- 每日股票池
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

-- 股票综合评分表
CREATE TABLE IF NOT EXISTS ads_stock_score_daily (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  tech_score DECIMAL(12,6) NULL COMMENT '技术形态得分(40%)',
  capital_score DECIMAL(12,6) NULL COMMENT '资金流向得分(25%)',
  sentiment_score DECIMAL(12,6) NULL COMMENT '情绪指标得分(20%)',
  chip_score DECIMAL(12,6) NULL COMMENT '筹码结构得分(15%)',
  total_score DECIMAL(12,6) NULL COMMENT '综合得分',
  score_rank INT NULL COMMENT '当日排名',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date),
  KEY idx_date_rank (trade_date, score_rank)
) ENGINE=InnoDB COMMENT='股票综合评分表';


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

-- 股票标签日表 (维表集成)
CREATE TABLE IF NOT EXISTS dwd_stock_label_daily (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  name VARCHAR(50) NULL COMMENT '股票名称',
  is_st TINYINT NOT NULL DEFAULT 0 COMMENT '是否ST(1是/0否)',
  is_new TINYINT NOT NULL DEFAULT 0 COMMENT '是否次新股<60天(1是/0否)',
  limit_type TINYINT NOT NULL DEFAULT 10 COMMENT '涨跌幅限制类型(10主板/20创业板科创板/30北交所)',
  market VARCHAR(10) NULL COMMENT '市场(主板/创业板/科创板/北交所)',
  industry VARCHAR(50) NULL COMMENT '所属行业',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date),
  KEY idx_label (trade_date, is_st, is_new, limit_type)
) ENGINE=InnoDB COMMENT='股票标签日表(维表集成)';

-- ========== Phase 1: 增强因子表 ==========

-- 流动性因子主题表
CREATE TABLE IF NOT EXISTS dws_liquidity_factor (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  turnover_vol_20 DECIMAL(12,6) NULL COMMENT '20日换手率波动',
  amihud_20 DECIMAL(18,10) NULL COMMENT '20日Amihud非流动性',
  vol_concentration DECIMAL(12,6) NULL COMMENT '成交量集中度(5日MAX/20日AVG)',
  bid_ask_spread DECIMAL(12,6) NULL COMMENT '买卖价差代理(2*(H-L)/(H+L))',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='流动性因子主题表';

-- 扩展动量因子表
CREATE TABLE IF NOT EXISTS dws_momentum_extended (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  high_52w_dist DECIMAL(12,6) NULL COMMENT '52周高点距离',
  reversal_5 DECIMAL(12,6) NULL COMMENT '5日反转因子(-ret_5)',
  mom_12m_1m DECIMAL(12,6) NULL COMMENT '12月去1月动量',
  vol_price_corr DECIMAL(12,6) NULL COMMENT '20日量价相关系数',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='扩展动量因子表';

-- 扩展质量因子表
CREATE TABLE IF NOT EXISTS dws_quality_extended (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  dupont_margin DECIMAL(12,6) NULL COMMENT '杜邦-净利率',
  dupont_turnover DECIMAL(12,6) NULL COMMENT '杜邦-资产周转率',
  dupont_leverage DECIMAL(12,6) NULL COMMENT '杜邦-财务杠杆',
  roe_trend DECIMAL(12,6) NULL COMMENT 'ROE同比变化',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='扩展质量因子表';

-- 风险因子表
CREATE TABLE IF NOT EXISTS dws_risk_factor (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  downside_vol_60 DECIMAL(12,6) NULL COMMENT '60日下行波动率',
  max_drawdown_60 DECIMAL(12,6) NULL COMMENT '60日最大回撤',
  var_5pct_60 DECIMAL(12,6) NULL COMMENT '60日VaR(5%分位)',
  beta_60 DECIMAL(12,6) NULL COMMENT '60日Beta系数',
  ivol_20 DECIMAL(12,6) NULL COMMENT '20日特质波动率',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='风险因子表';

-- 指数日线表 (Phase 2)
CREATE TABLE IF NOT EXISTS ods_index_daily (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '指数代码',
  open DECIMAL(20,4) NULL COMMENT '开盘点位',
  high DECIMAL(20,4) NULL COMMENT '最高点位',
  low DECIMAL(20,4) NULL COMMENT '最低点位',
  close DECIMAL(20,4) NULL COMMENT '收盘点位',
  pre_close DECIMAL(20,4) NULL COMMENT '昨收点位',
  pct_chg DECIMAL(12,6) NULL COMMENT '涨跌幅',
  vol DECIMAL(20,4) NULL COMMENT '成交量(手)',
  amount DECIMAL(20,4) NULL COMMENT '成交额(千元)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='指数日线表';


-- 指数基础信息表
CREATE TABLE IF NOT EXISTS ods_index_basic (
  ts_code CHAR(9) NOT NULL COMMENT '指数代码',
  name VARCHAR(128) NULL COMMENT '指数简称',
  market VARCHAR(32) NULL COMMENT '交易所/市场',
  publisher VARCHAR(128) NULL COMMENT '发布方',
  category VARCHAR(64) NULL COMMENT '指数类别',
  base_date INT NULL COMMENT '基日(YYYYMMDD)',
  base_point DECIMAL(20,4) NULL COMMENT '基点',
  list_date INT NULL COMMENT '发布日期(YYYYMMDD)',
  fullname VARCHAR(255) NULL COMMENT '指数全称',
  index_type VARCHAR(64) NULL COMMENT '指数风格类型',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ts_code)
) ENGINE=InnoDB COMMENT='A股指数基础信息';

-- 指数成分股信息表
CREATE TABLE IF NOT EXISTS ods_index_member (
  index_code CHAR(9) NOT NULL COMMENT '指数代码',
  con_code CHAR(9) NOT NULL COMMENT '成分股代码',
  in_date INT NULL COMMENT '纳入日期(YYYYMMDD)',
  out_date INT NULL COMMENT '剔除日期(YYYYMMDD)',
  is_new CHAR(1) NULL COMMENT '是否最新成分(N最新/Y历史)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (index_code, con_code),
  KEY idx_con_code (con_code)
) ENGINE=InnoDB COMMENT='A股指数成分信息';

-- 指数成分权重表
CREATE TABLE IF NOT EXISTS ods_index_weight (
  trade_date INT NOT NULL COMMENT '交易日期',
  index_code CHAR(9) NOT NULL COMMENT '指数代码',
  con_code CHAR(9) NOT NULL COMMENT '成分股代码',
  weight DECIMAL(12,6) NULL COMMENT '权重(%)',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, index_code, con_code),
  KEY idx_index_date (index_code, trade_date),
  KEY idx_con_date (con_code, trade_date)
) ENGINE=InnoDB COMMENT='A股指数成分权重';

-- 指数技术指标表（TuShare index_dailybasic）
CREATE TABLE IF NOT EXISTS ods_index_tech_factor (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '指数代码',
  turnover_rate DECIMAL(12,6) NULL COMMENT '换手率(%)',
  pe DECIMAL(20,6) NULL COMMENT '市盈率',
  pe_ttm DECIMAL(20,6) NULL COMMENT '市盈率TTM',
  pb DECIMAL(20,6) NULL COMMENT '市净率',
  total_mv DECIMAL(20,4) NULL COMMENT '总市值',
  float_mv DECIMAL(20,4) NULL COMMENT '流通市值',
  total_share DECIMAL(20,4) NULL COMMENT '总股本',
  float_share DECIMAL(20,4) NULL COMMENT '流通股本',
  free_share DECIMAL(20,4) NULL COMMENT '自由流通股本',
  turnover_rate5 DECIMAL(12,6) NULL COMMENT '5日换手率',
  turnover_rate10 DECIMAL(12,6) NULL COMMENT '10日换手率',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='指数技术因子信息';

-- 申万行业指数分类信息表
CREATE TABLE IF NOT EXISTS ods_sw_index_classify (
  index_code CHAR(9) NOT NULL COMMENT '申万行业指数代码',
  industry_name VARCHAR(128) NULL COMMENT '行业名称',
  level VARCHAR(16) NULL COMMENT '行业级别',
  industry_code VARCHAR(32) NULL COMMENT '行业编码',
  is_pub CHAR(1) NULL COMMENT '是否发布',
  parent_code VARCHAR(32) NULL COMMENT '父级行业编码',
  src VARCHAR(32) NULL COMMENT '分类标准',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (index_code)
) ENGINE=InnoDB COMMENT='申万行业指数分类';

-- 申万行业指数日线表
CREATE TABLE IF NOT EXISTS ods_sw_index_daily (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '申万行业指数代码',
  name VARCHAR(128) NULL COMMENT '指数名称',
  open DECIMAL(20,4) NULL COMMENT '开盘点位',
  low DECIMAL(20,4) NULL COMMENT '最低点位',
  high DECIMAL(20,4) NULL COMMENT '最高点位',
  close DECIMAL(20,4) NULL COMMENT '收盘点位',
  change DECIMAL(20,4) NULL COMMENT '涨跌点',
  pct_change DECIMAL(12,6) NULL COMMENT '涨跌幅(%)',
  vol DECIMAL(20,4) NULL COMMENT '成交量(万股)',
  amount DECIMAL(20,4) NULL COMMENT '成交额(万元)',
  pe DECIMAL(20,6) NULL COMMENT '市盈率',
  pb DECIMAL(20,6) NULL COMMENT '市净率',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_ts_date (ts_code, trade_date)
) ENGINE=InnoDB COMMENT='申万行业指数日线';
