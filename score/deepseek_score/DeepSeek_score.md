# 股票综合打分算法设计

## 一、打分框架设计
采用加权综合评分模型，包含4个维度：

*   **技术面（35%）** - 价格动量、成交量、技术指标
*   **基本面（35%）** - 估值、财务质量、盈利能力
*   **资金面（20%）** - 资金流向、融资融券
*   **筹码面（10%）** - 筹码集中度、获利比例

## 二、具体评分指标

### 1. 技术面指标（35分）
*可从 ods_stk_factor、ods_daily 获取*

*   **短期动量**：5日收益率（10分）
*   **中期动量**：20日收益率（8分）
*   **成交量能**：20日成交量均值/60日成交量均值（5分）
*   **技术指标**：
    *   MACD金叉状态（4分）
    *   RSI(6)超卖反弹（3分）
    *   KDJ金叉状态（3分）
    *   布林带位置（2分）

### 2. 基本面指标（35分）
*可从 ods_daily_basic、ods_fina_indicator、dwd_fina_indicator 获取*

*   **估值合理性**：
    *   PE_TTM分位数（10分）
    *   PB分位数（8分）
*   **财务质量**：
    *   ROE（6分）
    *   毛利率（4分）
    *   净利率（3分）
*   **成长性**：
    *   营业收入同比增长（4分）

### 3. 资金面指标（20分）
*可从 ods_moneyflow、ods_margin_detail 获取*

*   **主力资金**：
    *   大单+特大单净流入额（8分）
    *   连续净流入天数（5分）
*   **融资融券**：
    *   融资余额变化率（4分）
    *   融券余额变化率（3分）

### 4. 筹码面指标（10分）
*可从 ods_cyq_perf、ods_cyq_chips 获取*

*   **筹码集中度**：
    *   获利比例（5分）
    *   筹码集中度（85-15分位成本差）（3分）
    *   近期筹码变化（2分）

## 三、打分算法SQL实现

### 步骤1：创建评分中间表

```sql
-- 创建日频打分中间表
CREATE TABLE IF NOT EXISTS ads_stock_score_intermediate (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  
  -- 技术面得分
  tech_score DECIMAL(10,4) NULL COMMENT '技术面得分(0-35)',
  tech_ret_5 DECIMAL(12,6) NULL COMMENT '5日收益率',
  tech_ret_20 DECIMAL(12,6) NULL COMMENT '20日收益率',
  tech_vol_ratio DECIMAL(12,6) NULL COMMENT '量比',
  tech_macd_score TINYINT NULL COMMENT 'MACD评分(0-4)',
  tech_rsi_score TINYINT NULL COMMENT 'RSI评分(0-3)',
  
  -- 基本面得分
  funda_score DECIMAL(10,4) NULL COMMENT '基本面得分(0-35)',
  funda_pe_percentile DECIMAL(12,6) NULL COMMENT 'PE分位数',
  funda_roe DECIMAL(12,6) NULL COMMENT 'ROE',
  funda_gross_margin DECIMAL(12,6) NULL COMMENT '毛利率',
  
  -- 资金面得分
  money_score DECIMAL(10,4) NULL COMMENT '资金面得分(0-20)',
  money_net_inflow DECIMAL(20,4) NULL COMMENT '净流入额',
  money_margin_ratio DECIMAL(12,6) NULL COMMENT '融资变化率',
  
  -- 筹码面得分
  chips_score DECIMAL(10,4) NULL COMMENT '筹码面得分(0-10)',
  chips_winner_rate DECIMAL(12,6) NULL COMMENT '获利比例',
  chips_concentration DECIMAL(12,6) NULL COMMENT '筹码集中度',
  
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_trade_date (trade_date),
  KEY idx_score (tech_score, funda_score, money_score, chips_score)
) ENGINE=InnoDB COMMENT='股票打分中间表';
```

### 步骤2：创建最终打分结果表

```sql
-- 创建日频综合打分表
CREATE TABLE IF NOT EXISTS ads_stock_score_daily (
  trade_date INT NOT NULL COMMENT '交易日期',
  ts_code CHAR(9) NOT NULL COMMENT '股票代码',
  
  total_score DECIMAL(10,4) NOT NULL COMMENT '综合得分',
  rank_percent DECIMAL(12,6) NULL COMMENT '排名分位数',
  score_breakdown JSON NULL COMMENT '得分明细',
  
  -- 各维度得分
  tech_score DECIMAL(10,4) NULL COMMENT '技术面得分',
  funda_score DECIMAL(10,4) NULL COMMENT '基本面得分',
  money_score DECIMAL(10,4) NULL COMMENT '资金面得分',
  chips_score DECIMAL(10,4) NULL COMMENT '筹码面得分',
  
  -- 标签
  tags VARCHAR(200) NULL COMMENT '标签(如:高动量,低估值,主力流入)',
  risk_level TINYINT NULL COMMENT '风险等级(1-5)',
  
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ts_code),
  KEY idx_total_score (trade_date, total_score DESC),
  KEY idx_rank (trade_date, rank_percent DESC),
  KEY idx_tags (tags(50))
) ENGINE=InnoDB COMMENT='股票日频综合评分表';
```

### 步骤3：打分计算存储过程

```sql
DELIMITER //

CREATE PROCEDURE sp_calculate_stock_score(
    IN p_trade_date INT
)
BEGIN
    -- 清空当日中间数据
    DELETE FROM ads_stock_score_intermediate 
    WHERE trade_date = p_trade_date;
    
    -- 1. 计算技术面得分
    INSERT INTO ads_stock_score_intermediate (
        trade_date, ts_code, tech_score, 
        tech_ret_5, tech_ret_20, tech_vol_ratio
    )
    SELECT 
        p_trade_date,
        ts_code,
        -- 技术面得分计算（简化示例）
        (CASE 
            WHEN ret_5 > 0.05 THEN 10
            WHEN ret_5 > 0.02 THEN 7
            WHEN ret_5 > 0 THEN 4
            ELSE 1
        END) +
        (CASE 
            WHEN ret_20 > 0.1 THEN 8
            WHEN ret_20 > 0.05 THEN 6
            WHEN ret_20 > 0 THEN 3
            ELSE 1
        END) +
        (CASE 
            WHEN volume_ratio > 1.5 THEN 5
            WHEN volume_ratio > 1.2 THEN 3
            WHEN volume_ratio > 0.8 THEN 1
            ELSE 0
        END) as tech_score,
        ret_5,
        ret_20,
        volume_ratio
    FROM (
        -- 获取技术指标数据（简化示例）
        SELECT 
            ts_code,
            pct_chg as ret_5,  -- 这里需要计算真实5日收益率
            -- 实际应该使用窗口函数计算
            ROUND(AVG(vol) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS 19 PRECEDING) / 
                  AVG(vol) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS 59 PRECEDING), 4) as volume_ratio
        FROM ods_daily 
        WHERE trade_date = p_trade_date
    ) t;
    
    -- 2. 计算基本面得分（简化示例）
    UPDATE ads_stock_score_intermediate i
    JOIN (
        SELECT 
            ts_code,
            -- PE分位数计算
            CASE 
                WHEN pe_ttm < 0 THEN 0  -- 亏损企业
                WHEN pe_ttm <= 15 THEN 10
                WHEN pe_ttm <= 25 THEN 7
                WHEN pe_ttm <= 40 THEN 4
                ELSE 1
            END as pe_score,
            
            -- ROE评分
            CASE 
                WHEN roe > 0.15 THEN 6
                WHEN roe > 0.10 THEN 4
                WHEN roe > 0.05 THEN 2
                ELSE 0
            END as roe_score,
            
            -- 毛利率评分
            CASE 
                WHEN grossprofit_margin > 0.4 THEN 4
                WHEN grossprofit_margin > 0.3 THEN 3
                WHEN grossprofit_margin > 0.2 THEN 2
                ELSE 1
            END as margin_score
        FROM ods_daily_basic d
        LEFT JOIN (
            -- 获取最新财务指标
            SELECT ts_code, MAX(end_date) as max_end_date
            FROM ods_fina_indicator 
            WHERE end_date <= p_trade_date
            GROUP BY ts_code
        ) f ON d.ts_code = f.ts_code
        LEFT JOIN ods_fina_indicator fi ON f.ts_code = fi.ts_code AND f.max_end_date = fi.end_date
        WHERE d.trade_date = p_trade_date
    ) f ON i.ts_code = f.ts_code
    SET i.funda_score = f.pe_score + f.roe_score + f.margin_score,
        i.funda_pe_percentile = f.pe_score / 10.0,
        i.funda_roe = f.roe_score / 6.0,
        i.funda_gross_margin = f.margin_score / 4.0;
    
    -- 3. 计算资金面得分
    UPDATE ads_stock_score_intermediate i
    LEFT JOIN ods_moneyflow m ON i.ts_code = m.ts_code AND i.trade_date = m.trade_date
    LEFT JOIN ods_margin_detail md ON i.ts_code = md.ts_code AND i.trade_date = md.trade_date
    SET 
        i.money_score = 
            CASE 
                WHEN (m.buy_lg_amount + m.buy_elg_amount - m.sell_lg_amount - m.sell_elg_amount) > 0 THEN 8
                ELSE 4
            END +
            CASE 
                WHEN md.rzye IS NOT NULL AND md.rzye > 0 THEN 4
                ELSE 2
            END,
        i.money_net_inflow = IFNULL(m.net_mf_amount, 0),
        i.money_margin_ratio = IFNULL(md.rzye, 0);
    
    -- 4. 计算筹码面得分
    UPDATE ads_stock_score_intermediate i
    LEFT JOIN ods_cyq_perf cp ON i.ts_code = cp.ts_code AND i.trade_date = cp.trade_date
    SET 
        i.chips_score = 
            CASE 
                WHEN cp.winner_rate > 0.7 THEN 5
                WHEN cp.winner_rate > 0.5 THEN 3
                WHEN cp.winner_rate > 0.3 THEN 1
                ELSE 0
            END +
            CASE 
                WHEN (cp.cost_85pct - cp.cost_15pct) / cp.weight_avg < 0.3 THEN 3
                WHEN (cp.cost_85pct - cp.cost_15pct) / cp.weight_avg < 0.5 THEN 2
                ELSE 1
            END,
        i.chips_winner_rate = cp.winner_rate,
        i.chips_concentration = (cp.cost_85pct - cp.cost_15pct) / cp.weight_avg;
    
    -- 5. 计算综合得分并插入最终表
    INSERT INTO ads_stock_score_daily (
        trade_date, ts_code, total_score, 
        tech_score, funda_score, money_score, chips_score,
        rank_percent, score_breakdown, tags, risk_level
    )
    SELECT 
        p_trade_date,
        i.ts_code,
        -- 综合得分 = 各维度得分加权求和
        ROUND(
            COALESCE(i.tech_score, 0) + 
            COALESCE(i.funda_score, 0) + 
            COALESCE(i.money_score, 0) + 
            COALESCE(i.chips_score, 0), 4
        ) as total_score,
        
        i.tech_score,
        i.funda_score,
        i.money_score,
        i.chips_score,
        
        -- 排名分位数
        RANK() OVER (ORDER BY 
            COALESCE(i.tech_score, 0) + 
            COALESCE(i.funda_score, 0) + 
            COALESCE(i.money_score, 0) + 
            COALESCE(i.chips_score, 0) DESC
        ) / COUNT(*) OVER () as rank_percent,
        
        -- 得分明细（JSON格式）
        JSON_OBJECT(
            'tech', JSON_OBJECT(
                'score', i.tech_score,
                'ret_5', i.tech_ret_5,
                'ret_20', i.tech_ret_20
            ),
            'funda', JSON_OBJECT(
                'score', i.funda_score,
                'pe_percentile', i.funda_pe_percentile,
                'roe', i.funda_roe
            ),
            'money', JSON_OBJECT(
                'score', i.money_score,
                'net_inflow', i.money_net_inflow
            ),
            'chips', JSON_OBJECT(
                'score', i.chips_score,
                'winner_rate', i.chips_winner_rate
            )
        ) as score_breakdown,
        
        -- 标签生成
        CONCAT_WS(',',
            CASE WHEN i.tech_ret_5 > 0.05 THEN '高动量' END,
            CASE WHEN i.funda_pe_percentile > 0.7 THEN '低估值' END,
            CASE WHEN i.money_net_inflow > 1000 THEN '主力流入' END,
            CASE WHEN i.chips_winner_rate > 0.7 THEN '筹码集中' END
        ) as tags,
        
        -- 风险等级（1-5，1为低风险）
        CASE 
            WHEN i.tech_ret_20 > 0.3 THEN 4  -- 短期涨幅过大
            WHEN i.funda_pe_percentile < 0.3 THEN 2  -- 估值偏高
            WHEN i.chips_winner_rate > 0.9 THEN 3  -- 获利盘过多
            ELSE 2
        END as risk_level
        
    FROM ads_stock_score_intermediate i
    WHERE i.trade_date = p_trade_date;
    
END //

DELIMITER ;
```

### 步骤4：创建查询接口视图

```sql
-- 创建高分股票视图
CREATE VIEW vw_top_scored_stocks AS
SELECT 
    s.trade_date,
    s.ts_code,
    d.name,
    d.industry,
    s.total_score,
    s.rank_percent,
    s.tech_score,
    s.funda_score,
    s.money_score,
    s.chips_score,
    s.tags,
    s.risk_level,
    db.close,
    db.total_mv,
    db.pe_ttm,
    db.pb
FROM ads_stock_score_daily s
JOIN dim_stock d ON s.ts_code = d.ts_code
LEFT JOIN ods_daily_basic db ON s.ts_code = db.ts_code AND s.trade_date = db.trade_date
WHERE s.rank_percent <= 0.2  -- 前20%
ORDER BY s.trade_date DESC, s.total_score DESC;

-- 创建按行业分组评分视图
CREATE VIEW vw_industry_score_summary AS
SELECT 
    trade_date,
    industry,
    COUNT(*) as stock_count,
    AVG(total_score) as avg_score,
    AVG(tech_score) as avg_tech,
    AVG(funda_score) as avg_funda,
    AVG(money_score) as avg_money,
    AVG(chips_score) as avg_chips,
    GROUP_CONCAT(
        JSON_OBJECT('code', ts_code, 'score', total_score, 'name', name)
        ORDER BY total_score DESC 
        LIMIT 5
    ) as top_5_stocks
FROM ads_stock_score_daily s
JOIN dim_stock d ON s.ts_code = d.ts_code
WHERE industry IS NOT NULL
GROUP BY trade_date, industry;
```

## 四、算法优化建议

### 动态权重调整
*   根据市场风格（牛市/熊市）调整各维度权重
*   实现季节性因子调整

### 行业中性化
```sql
-- 行业内的相对排名，消除行业偏差
rank() OVER (PARTITION BY industry ORDER BY total_score DESC)
```

### 动量因子加强
*   加入12个月动量因子
*   加入日内动量（开盘-收盘）

### 风险控制因子
*   波动率调整（高波动降分）
*   流动性评分（成交额分位数）

## 五、使用示例

```sql
-- 获取今日高分股票
SELECT * FROM vw_top_scored_stocks 
WHERE trade_date = 20231215
ORDER BY total_score DESC
LIMIT 20;

-- 按行业查看评分
SELECT * FROM vw_industry_score_summary
WHERE trade_date = 20231215
ORDER BY avg_score DESC;

-- 查看个股评分历史
SELECT trade_date, total_score, rank_percent, tags
FROM ads_stock_score_daily
WHERE ts_code = '000001.SZ'
ORDER BY trade_date DESC
LIMIT 30;
```

## 六、后续扩展建议

### 机器学习集成
*   使用历史数据训练XGBoost/LightGBM模型
*   加入特征重要性分析

### 多因子模型
*   实现Fama-French三因子
*   加入质量因子、低波动因子

### 回测框架
*   建立基于评分的策略回测
*   优化调仓频率和持仓数量

这个打分算法框架具有良好的可扩展性，您可以根据实际需求调整权重、添加新因子或优化评分逻辑。