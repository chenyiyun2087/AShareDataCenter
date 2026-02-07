-- Fama-French 评分系统 DDL
-- 新增评分表，与 claude_score 独立

-- 市值因子评分 (SMB)
CREATE TABLE IF NOT EXISTS dws_fama_size_score (
    trade_date INT NOT NULL COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    circ_mv DECIMAL(20,4) COMMENT '流通市值(万元)',
    size_rank DECIMAL(10,4) COMMENT '市值排名百分位',
    size_score DECIMAL(5,2) COMMENT '市值评分(0-10)',
    PRIMARY KEY (trade_date, ts_code),
    INDEX idx_trade_date (trade_date),
    INDEX idx_size_score (trade_date, size_score)
) COMMENT='Fama-French SMB因子评分';

-- 动量因子评分 (MOM)
CREATE TABLE IF NOT EXISTS dws_fama_momentum_score (
    trade_date INT NOT NULL COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    pct_chg_252d DECIMAL(10,4) COMMENT '12月涨幅(%)',
    pct_chg_21d DECIMAL(10,4) COMMENT '1月涨幅(%)',
    momentum_12m DECIMAL(10,4) COMMENT '12个月动量(12月-1月)',
    volume_ratio DECIMAL(10,4) COMMENT '量比',
    turnover_rate DECIMAL(10,4) COMMENT '换手率(%)',
    mtm DECIMAL(10,4) COMMENT 'MTM指标',
    mtmma DECIMAL(10,4) COMMENT 'MTMMA指标',
    momentum_score DECIMAL(5,2) COMMENT '动量评分(0-22)',
    PRIMARY KEY (trade_date, ts_code),
    INDEX idx_trade_date (trade_date)
) COMMENT='Fama-French MOM动量因子评分';

-- 价值因子评分 (HML)
CREATE TABLE IF NOT EXISTS dws_fama_value_score (
    trade_date INT NOT NULL COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    pb DECIMAL(10,4) COMMENT '市净率',
    bm_ratio DECIMAL(10,4) COMMENT 'B/M比率(1/PB)',
    bm_rank DECIMAL(10,4) COMMENT 'B/M排名百分位',
    pe_ttm DECIMAL(10,4) COMMENT 'PE(TTM)',
    ps_ttm DECIMAL(10,4) COMMENT 'PS(TTM)',
    value_score DECIMAL(5,2) COMMENT '价值评分(0-18)',
    PRIMARY KEY (trade_date, ts_code),
    INDEX idx_trade_date (trade_date)
) COMMENT='Fama-French HML价值因子评分';

-- 质量因子评分 (RMW + CMA)
CREATE TABLE IF NOT EXISTS dws_fama_quality_score (
    trade_date INT NOT NULL COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    roe_ttm DECIMAL(10,4) COMMENT 'ROE(TTM)%',
    grossprofit_margin DECIMAL(10,4) COMMENT '毛利率%',
    debt_to_assets DECIMAL(10,4) COMMENT '资产负债率%',
    asset_growth_rate DECIMAL(10,4) COMMENT '资产同比增长率%(CMA因子)',
    rwm_score DECIMAL(5,2) COMMENT 'RMW盈利评分',
    cma_score DECIMAL(5,2) COMMENT 'CMA投资评分',
    quality_score DECIMAL(5,2) COMMENT '质量评分(0-22)',
    PRIMARY KEY (trade_date, ts_code),
    INDEX idx_trade_date (trade_date)
) COMMENT='Fama-French RMW+CMA质量因子评分';

-- 技术因子评分 (精简版)
CREATE TABLE IF NOT EXISTS dws_fama_technical_score (
    trade_date INT NOT NULL COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    macd DECIMAL(10,4) COMMENT 'MACD',
    macd_dif DECIMAL(10,4) COMMENT 'DIF',
    macd_dea DECIMAL(10,4) COMMENT 'DEA',
    kdj_j DECIMAL(10,4) COMMENT 'KDJ-J值',
    rsi_6 DECIMAL(10,4) COMMENT 'RSI(6)',
    technical_score DECIMAL(5,2) COMMENT '技术评分(0-10)',
    PRIMARY KEY (trade_date, ts_code),
    INDEX idx_trade_date (trade_date)
) COMMENT='Fama-French技术因子评分';

-- 资金因子评分
CREATE TABLE IF NOT EXISTS dws_fama_capital_score (
    trade_date INT NOT NULL COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    elg_net DECIMAL(20,4) COMMENT '特大单净额(万元)',
    lg_net DECIMAL(20,4) COMMENT '大单净额(万元)',
    margin_net_pct DECIMAL(10,4) COMMENT '融资净买入比例(%)',
    capital_score DECIMAL(5,2) COMMENT '资金评分(0-10)',
    PRIMARY KEY (trade_date, ts_code),
    INDEX idx_trade_date (trade_date)
) COMMENT='Fama-French资金因子评分';

-- 筹码因子评分
CREATE TABLE IF NOT EXISTS dws_fama_chip_score (
    trade_date INT NOT NULL COMMENT '交易日期',
    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    winner_rate DECIMAL(10,4) COMMENT '获利比例(%)',
    cost_deviation DECIMAL(10,4) COMMENT '成本偏离度',
    chip_score DECIMAL(5,2) COMMENT '筹码评分(0-8)',
    PRIMARY KEY (trade_date, ts_code),
    INDEX idx_trade_date (trade_date)
) COMMENT='Fama-French筹码因子评分';

-- 综合评分视图
CREATE OR REPLACE VIEW v_fama_total_score AS
SELECT 
    s.trade_date,
    s.ts_code,
    s.size_score,
    m.momentum_score,
    v.value_score,
    q.quality_score,
    t.technical_score,
    c.capital_score,
    ch.chip_score,
    COALESCE(s.size_score, 0) + COALESCE(m.momentum_score, 0) + 
    COALESCE(v.value_score, 0) + COALESCE(q.quality_score, 0) + 
    COALESCE(t.technical_score, 0) + COALESCE(c.capital_score, 0) + 
    COALESCE(ch.chip_score, 0) AS total_score
FROM dws_fama_size_score s
LEFT JOIN dws_fama_momentum_score m ON s.trade_date = m.trade_date AND s.ts_code = m.ts_code
LEFT JOIN dws_fama_value_score v ON s.trade_date = v.trade_date AND s.ts_code = v.ts_code
LEFT JOIN dws_fama_quality_score q ON s.trade_date = q.trade_date AND s.ts_code = q.ts_code
LEFT JOIN dws_fama_technical_score t ON s.trade_date = t.trade_date AND s.ts_code = t.ts_code
LEFT JOIN dws_fama_capital_score c ON s.trade_date = c.trade_date AND s.ts_code = c.ts_code
LEFT JOIN dws_fama_chip_score ch ON s.trade_date = ch.trade_date AND s.ts_code = ch.ts_code;
