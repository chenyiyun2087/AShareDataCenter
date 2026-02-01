-- 数据清洗与预处理策略（基于 ODS 的示例实现，MySQL 8.0）
USE tushare_stock;

-- 3.1 缺失值处理：价格数据前值填充（Forward Fill）
-- 说明：当日缺失时，取同股票上一个有值交易日的收盘价作为填充值。
CREATE OR REPLACE VIEW ods_daily_ffill AS
SELECT
  d.trade_date,
  d.ts_code,
  COALESCE(
    d.close,
    (
      SELECT d2.close
      FROM ods_daily d2
      WHERE d2.ts_code = d.ts_code
        AND d2.trade_date <= d.trade_date
        AND d2.close IS NOT NULL
      ORDER BY d2.trade_date DESC
      LIMIT 1
    )
  ) AS close_ffill,
  d.open,
  d.high,
  d.low,
  d.vol,
  d.amount
FROM ods_daily d;

-- 3.1 缺失值处理：财务数据行业中位数/零值填充
-- 说明：对 YoY 增速等财务字段，优先使用行业中位数，若为空则回退为 0。
CREATE OR REPLACE VIEW ods_fina_imputed AS
WITH base AS (
  SELECT
    f.*,
    s.industry AS industry_code
  FROM ods_fina_indicator f
  LEFT JOIN dim_stock s
    ON s.ts_code = f.ts_code
),
ranked AS (
  SELECT
    base.*,
    PERCENT_RANK() OVER (PARTITION BY industry_code, ann_date ORDER BY netprofit_yoy) AS netprofit_rank,
    PERCENT_RANK() OVER (PARTITION BY industry_code, ann_date ORDER BY or_yoy) AS revenue_rank
  FROM base
),
stats AS (
  SELECT
    ranked.*,
    MIN(CASE WHEN netprofit_rank >= 0.5 THEN netprofit_yoy END)
      OVER (PARTITION BY industry_code, ann_date) AS netprofit_median,
    MIN(CASE WHEN revenue_rank >= 0.5 THEN or_yoy END)
      OVER (PARTITION BY industry_code, ann_date) AS revenue_median
  FROM ranked
)
SELECT
  stats.*,
  COALESCE(stats.netprofit_yoy, stats.netprofit_median, 0) AS netprofit_yoy_imputed,
  COALESCE(stats.or_yoy, stats.revenue_median, 0) AS revenue_yoy_imputed
FROM stats;

-- 3.2 极值处理（Winsorization）：百分位裁剪（1% - 99%）
-- 说明：对估值与交易类因子进行分位截断，减少异常值对标准化的影响。
CREATE OR REPLACE VIEW ads_features_stock_daily_clean AS
WITH raw AS (
  SELECT
    d.trade_date,
    d.ts_code,
    b.turnover_rate,
    b.pe_ttm,
    b.pb,
    b.total_mv,
    b.circ_mv,
    f.roe,
    f.grossprofit_margin,
    f.debt_to_assets,
    s.industry AS industry_code
  FROM ods_daily_ffill d
  LEFT JOIN ods_daily_basic b
    ON b.trade_date = d.trade_date AND b.ts_code = d.ts_code
  LEFT JOIN ods_fina_imputed f
    ON f.ts_code = d.ts_code
  LEFT JOIN dim_stock s
    ON s.ts_code = d.ts_code
),
ranked AS (
  SELECT
    raw.*,
    PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY pe_ttm) AS pe_rank,
    PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY pb) AS pb_rank,
    PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY turnover_rate) AS turnover_rank
  FROM raw
),
thresholds AS (
  SELECT
    ranked.*,
    MIN(CASE WHEN pe_rank >= 0.01 THEN pe_ttm END)
      OVER (PARTITION BY trade_date) AS pe_p01,
    MAX(CASE WHEN pe_rank <= 0.99 THEN pe_ttm END)
      OVER (PARTITION BY trade_date) AS pe_p99,
    MIN(CASE WHEN pb_rank >= 0.01 THEN pb END)
      OVER (PARTITION BY trade_date) AS pb_p01,
    MAX(CASE WHEN pb_rank <= 0.99 THEN pb END)
      OVER (PARTITION BY trade_date) AS pb_p99,
    MIN(CASE WHEN turnover_rank >= 0.01 THEN turnover_rate END)
      OVER (PARTITION BY trade_date) AS turnover_p01,
    MAX(CASE WHEN turnover_rank <= 0.99 THEN turnover_rate END)
      OVER (PARTITION BY trade_date) AS turnover_p99
  FROM ranked
)
SELECT
  trade_date,
  ts_code,
  CASE
    WHEN pe_ttm IS NULL THEN NULL
    WHEN pe_ttm < pe_p01 THEN pe_p01
    WHEN pe_ttm > pe_p99 THEN pe_p99
    ELSE pe_ttm
  END AS pe_ttm_winsor,
  CASE
    WHEN pb IS NULL THEN NULL
    WHEN pb < pb_p01 THEN pb_p01
    WHEN pb > pb_p99 THEN pb_p99
    ELSE pb
  END AS pb_winsor,
  CASE
    WHEN turnover_rate IS NULL THEN NULL
    WHEN turnover_rate < turnover_p01 THEN turnover_p01
    WHEN turnover_rate > turnover_p99 THEN turnover_p99
    ELSE turnover_rate
  END AS turnover_rate_winsor,
  total_mv,
  circ_mv,
  roe,
  grossprofit_margin,
  debt_to_assets,
  industry_code
FROM thresholds;

-- 3.3 数据对齐与日历映射（最新可用财务数据）
-- 说明：对每个交易日取 ann_date <= trade_date 的最新财务记录。
CREATE OR REPLACE VIEW dws_fina_pit_aligned AS
SELECT
  cal.cal_date AS trade_date,
  f.ts_code,
  f.ann_date,
  f.end_date,
  f.roe,
  f.grossprofit_margin,
  f.debt_to_assets,
  f.netprofit_yoy_imputed,
  f.revenue_yoy_imputed
FROM dim_trade_cal cal
JOIN ods_fina_imputed f
  ON f.ann_date <= cal.cal_date
WHERE cal.is_open = 1
  AND cal.exchange = 'SSE'
  AND NOT EXISTS (
    SELECT 1
    FROM ods_fina_imputed f2
    WHERE f2.ts_code = f.ts_code
      AND f2.ann_date <= cal.cal_date
      AND (f2.ann_date > f.ann_date OR (f2.ann_date = f.ann_date AND f2.end_date > f.end_date))
  );
