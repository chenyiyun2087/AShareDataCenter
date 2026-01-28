-- DWS/ADS 计算示例（MySQL 8.0）
USE tushare_stock;

-- 复权价与收益（前复权示例，基准日=最新交易日）
-- 1) 计算基准因子（最新交易日）
WITH latest_trade AS (
  SELECT MAX(trade_date) AS base_date
  FROM dwd_daily
),
base_factor AS (
  SELECT a.ts_code, a.adj_factor AS base_adj
  FROM dwd_adj_factor a
  JOIN latest_trade lt ON a.trade_date = lt.base_date
)
INSERT INTO dws_price_adj_daily (
  trade_date,
  ts_code,
  qfq_close,
  qfq_ret_1,
  qfq_ret_5,
  qfq_ret_20,
  qfq_ret_60
)
SELECT
  d.trade_date,
  d.ts_code,
  ROUND(d.close * b.base_adj / a.adj_factor, 4) AS qfq_close,
  NULL AS qfq_ret_1,
  NULL AS qfq_ret_5,
  NULL AS qfq_ret_20,
  NULL AS qfq_ret_60
FROM dwd_daily d
JOIN dwd_adj_factor a
  ON a.trade_date = d.trade_date AND a.ts_code = d.ts_code
JOIN base_factor b
  ON b.ts_code = d.ts_code
ON DUPLICATE KEY UPDATE
  qfq_close = VALUES(qfq_close),
  qfq_ret_1 = VALUES(qfq_ret_1),
  qfq_ret_5 = VALUES(qfq_ret_5),
  qfq_ret_20 = VALUES(qfq_ret_20),
  qfq_ret_60 = VALUES(qfq_ret_60),
  updated_at = CURRENT_TIMESTAMP;

-- 2) 计算复权收益（示例：1/5/20/60日收益）
UPDATE dws_price_adj_daily cur
JOIN dws_price_adj_daily prev_1
  ON prev_1.ts_code = cur.ts_code
  AND prev_1.trade_date = (
    SELECT MAX(t.trade_date)
    FROM dws_price_adj_daily t
    WHERE t.ts_code = cur.ts_code AND t.trade_date < cur.trade_date
  )
LEFT JOIN dws_price_adj_daily prev_5
  ON prev_5.ts_code = cur.ts_code
  AND prev_5.trade_date = (
    SELECT MAX(t.trade_date)
    FROM dws_price_adj_daily t
    WHERE t.ts_code = cur.ts_code AND t.trade_date < cur.trade_date
    ORDER BY t.trade_date DESC
    LIMIT 1 OFFSET 4
  )
LEFT JOIN dws_price_adj_daily prev_20
  ON prev_20.ts_code = cur.ts_code
  AND prev_20.trade_date = (
    SELECT MAX(t.trade_date)
    FROM dws_price_adj_daily t
    WHERE t.ts_code = cur.ts_code AND t.trade_date < cur.trade_date
    ORDER BY t.trade_date DESC
    LIMIT 1 OFFSET 19
  )
LEFT JOIN dws_price_adj_daily prev_60
  ON prev_60.ts_code = cur.ts_code
  AND prev_60.trade_date = (
    SELECT MAX(t.trade_date)
    FROM dws_price_adj_daily t
    WHERE t.ts_code = cur.ts_code AND t.trade_date < cur.trade_date
    ORDER BY t.trade_date DESC
    LIMIT 1 OFFSET 59
  )
SET
  cur.qfq_ret_1 = (cur.qfq_close / prev_1.qfq_close) - 1,
  cur.qfq_ret_5 = CASE WHEN prev_5.qfq_close IS NULL THEN NULL ELSE (cur.qfq_close / prev_5.qfq_close) - 1 END,
  cur.qfq_ret_20 = CASE WHEN prev_20.qfq_close IS NULL THEN NULL ELSE (cur.qfq_close / prev_20.qfq_close) - 1 END,
  cur.qfq_ret_60 = CASE WHEN prev_60.qfq_close IS NULL THEN NULL ELSE (cur.qfq_close / prev_60.qfq_close) - 1 END,
  cur.updated_at = CURRENT_TIMESTAMP;

-- 财务点时快照（ann_date <= trade_date 的最新值）
INSERT INTO dws_fina_pit_daily (
  trade_date,
  ts_code,
  ann_date,
  end_date,
  roe,
  grossprofit_margin,
  debt_to_assets,
  netprofit_margin
)
SELECT
  cal.cal_date AS trade_date,
  f.ts_code,
  f.ann_date,
  f.end_date,
  f.roe,
  f.grossprofit_margin,
  f.debt_to_assets,
  f.netprofit_margin
FROM dim_trade_cal cal
JOIN dwd_fina_indicator f
  ON f.ann_date <= cal.cal_date
WHERE cal.is_open = 1
  AND cal.exchange = 'SSE'
  AND NOT EXISTS (
    SELECT 1
    FROM dwd_fina_indicator f2
    WHERE f2.ts_code = f.ts_code
      AND f2.ann_date <= cal.cal_date
      AND (f2.ann_date > f.ann_date OR (f2.ann_date = f.ann_date AND f2.end_date > f.end_date))
  )
ON DUPLICATE KEY UPDATE
  ann_date = VALUES(ann_date),
  end_date = VALUES(end_date),
  roe = VALUES(roe),
  grossprofit_margin = VALUES(grossprofit_margin),
  debt_to_assets = VALUES(debt_to_assets),
  netprofit_margin = VALUES(netprofit_margin),
  updated_at = CURRENT_TIMESTAMP;

-- ADS 特征快照（示例）
INSERT INTO ads_features_stock_daily (
  trade_date,
  ts_code,
  ret_5,
  ret_20,
  ret_60,
  vol_20,
  vol_60,
  amt_ma20,
  turnover_rate,
  pe_ttm,
  pb,
  total_mv,
  circ_mv,
  roe,
  grossprofit_margin,
  debt_to_assets,
  industry_code
)
SELECT
  d.trade_date,
  d.ts_code,
  p.qfq_ret_5,
  p.qfq_ret_20,
  p.qfq_ret_60,
  NULL AS vol_20,
  NULL AS vol_60,
  NULL AS amt_ma20,
  b.turnover_rate,
  b.pe_ttm,
  b.pb,
  b.total_mv,
  b.circ_mv,
  f.roe,
  f.grossprofit_margin,
  f.debt_to_assets,
  s.industry AS industry_code
FROM dwd_daily d
LEFT JOIN dws_price_adj_daily p
  ON p.trade_date = d.trade_date AND p.ts_code = d.ts_code
LEFT JOIN dwd_daily_basic b
  ON b.trade_date = d.trade_date AND b.ts_code = d.ts_code
LEFT JOIN dws_fina_pit_daily f
  ON f.trade_date = d.trade_date AND f.ts_code = d.ts_code
LEFT JOIN dim_stock s
  ON s.ts_code = d.ts_code
ON DUPLICATE KEY UPDATE
  ret_5 = VALUES(ret_5),
  ret_20 = VALUES(ret_20),
  ret_60 = VALUES(ret_60),
  vol_20 = VALUES(vol_20),
  vol_60 = VALUES(vol_60),
  amt_ma20 = VALUES(amt_ma20),
  turnover_rate = VALUES(turnover_rate),
  pe_ttm = VALUES(pe_ttm),
  pb = VALUES(pb),
  total_mv = VALUES(total_mv),
  circ_mv = VALUES(circ_mv),
  roe = VALUES(roe),
  grossprofit_margin = VALUES(grossprofit_margin),
  debt_to_assets = VALUES(debt_to_assets),
  industry_code = VALUES(industry_code),
  updated_at = CURRENT_TIMESTAMP;

-- ADS 股票池快照（示例）
INSERT INTO ads_universe_daily (
  trade_date,
  ts_code,
  is_tradable,
  is_listed,
  is_suspended,
  no_amount,
  filter_flags
)
SELECT
  d.trade_date,
  d.ts_code,
  CASE WHEN d.amount IS NOT NULL AND d.amount > 0 THEN 1 ELSE 0 END AS is_tradable,
  CASE WHEN s.list_date IS NOT NULL AND s.list_date <= d.trade_date AND (s.delist_date IS NULL OR s.delist_date > d.trade_date) THEN 1 ELSE 0 END AS is_listed,
  CASE WHEN d.amount IS NULL OR d.amount = 0 THEN 1 ELSE 0 END AS is_suspended,
  CASE WHEN d.amount IS NULL OR d.amount = 0 THEN 1 ELSE 0 END AS no_amount,
  NULL AS filter_flags
FROM dwd_daily d
LEFT JOIN dim_stock s
  ON s.ts_code = d.ts_code
ON DUPLICATE KEY UPDATE
  is_tradable = VALUES(is_tradable),
  is_listed = VALUES(is_listed),
  is_suspended = VALUES(is_suspended),
  no_amount = VALUES(no_amount),
  filter_flags = VALUES(filter_flags),
  updated_at = CURRENT_TIMESTAMP;
