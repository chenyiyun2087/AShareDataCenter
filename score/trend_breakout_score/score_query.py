"""
趋势突破评分查询模块

基于趋势突破因子权重在SQL侧计算单日评分。
"""

from typing import Dict, List

import pandas as pd
from sqlalchemy import create_engine, text


SCORE_SQL = """
WITH price_window AS (
    SELECT
        d.trade_date,
        d.ts_code,
        d.close,
        d.high,
        d.vol,
        d.amount,
        COALESCE(p.qfq_ret_1, d.pct_chg / 100) AS ret_1,
        p.qfq_ret_20 AS ret_20,
        MAX(d.high) OVER (
            PARTITION BY d.ts_code
            ORDER BY d.trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS high_20,
        AVG(d.close) OVER (
            PARTITION BY d.ts_code
            ORDER BY d.trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ma5,
        AVG(d.close) OVER (
            PARTITION BY d.ts_code
            ORDER BY d.trade_date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS ma10,
        AVG(d.close) OVER (
            PARTITION BY d.ts_code
            ORDER BY d.trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma20,
        AVG(d.vol) OVER (
            PARTITION BY d.ts_code
            ORDER BY d.trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vol_ma20,
        AVG(d.amount) OVER (
            PARTITION BY d.ts_code
            ORDER BY d.trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS amt_ma20_calc,
        STDDEV_POP(COALESCE(p.qfq_ret_1, d.pct_chg / 100)) OVER (
            PARTITION BY d.ts_code
            ORDER BY d.trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS vol_5,
        STDDEV_POP(COALESCE(p.qfq_ret_1, d.pct_chg / 100)) OVER (
            PARTITION BY d.ts_code
            ORDER BY d.trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vol_20
    FROM dwd_daily d
    LEFT JOIN dws_price_adj_daily p
        ON p.trade_date = d.trade_date AND p.ts_code = d.ts_code
    WHERE d.trade_date <= :trade_date
),
price_enriched AS (
    SELECT
        pw.*,
        pw.ma20 - LAG(pw.ma20, 5) OVER (
            PARTITION BY pw.ts_code
            ORDER BY pw.trade_date
        ) AS ma20_slope
    FROM price_window pw
),
factor_base AS (
    SELECT
        pw.trade_date,
        pw.ts_code,
        pw.close,
        pw.high_20,
        pw.ma5,
        pw.ma10,
        pw.ma20,
        pw.ma20_slope,
        pw.vol_ma20,
        pw.amt_ma20_calc,
        pw.ret_20,
        pw.vol_5,
        pw.vol_20,
        pw.vol,
        pw.amount,
        b.turnover_rate,
        f.amt_ma20 AS amt_ma20_ads,
        cs.chip_concentration,
        cs.cost_deviation
    FROM price_enriched pw
    LEFT JOIN dwd_daily_basic b
        ON b.trade_date = pw.trade_date AND b.ts_code = pw.ts_code
    LEFT JOIN ads_features_stock_daily f
        ON f.trade_date = pw.trade_date AND f.ts_code = pw.ts_code
    LEFT JOIN dwd_chip_stability cs
        ON cs.trade_date = pw.trade_date AND cs.ts_code = pw.ts_code
    WHERE pw.trade_date = :trade_date
),
factors AS (
    SELECT
        trade_date,
        ts_code,
        close,
        high_20,
        ma5,
        ma10,
        ma20,
        ma20_slope,
        CASE WHEN high_20 IS NULL OR high_20 = 0 THEN NULL
             ELSE LEAST(close / high_20, 1.2) END AS breakout_ratio,
        CASE WHEN vol_ma20 IS NULL OR vol_ma20 = 0 THEN NULL
             ELSE vol / vol_ma20 END AS vol_ratio,
        ret_20,
        COALESCE(amt_ma20_ads, amt_ma20_calc, amount) AS amt_ma20,
        CASE WHEN vol_20 IS NULL OR vol_20 = 0 THEN NULL
             ELSE vol_5 / vol_20 END AS contraction_ratio,
        CASE WHEN ma20 IS NULL OR ma20 = 0 THEN NULL
             ELSE (close / ma20) - 1 END AS bias,
        turnover_rate,
        chip_concentration,
        cost_deviation
    FROM factor_base
),
scored AS (
    SELECT
        trade_date,
        ts_code,
        breakout_ratio,
        vol_ratio,
        ret_20,
        amt_ma20,
        contraction_ratio,
        bias,
        ma5,
        ma10,
        ma20,
        chip_concentration,
        cost_deviation,
        (PERCENT_RANK() OVER (ORDER BY breakout_ratio)) * 100 AS s_breakout,
        (CASE
            WHEN close > ma20 AND ma10 > ma20 AND ma20_slope > 0 THEN 100
            WHEN close > ma20 AND ma10 > ma20 THEN 70
            WHEN close > ma20 THEN 40
            ELSE 0
        END) AS s_trend,
        (PERCENT_RANK() OVER (ORDER BY vol_ratio)) * 100 AS s_volume,
        (PERCENT_RANK() OVER (ORDER BY ret_20)) * 100 AS s_rs,
        (PERCENT_RANK() OVER (ORDER BY amt_ma20)) * 100 AS s_liquidity,
        (PERCENT_RANK() OVER (ORDER BY contraction_ratio DESC)) * 100 AS s_contraction,
        (CASE WHEN ma5 > ma10 AND ma10 > ma20 THEN 100 ELSE 0 END) AS s_bull_align,
        (CASE
            WHEN bias IS NULL THEN 50
            WHEN ABS(bias) <= 0.02 THEN 100
            WHEN ABS(bias) <= 0.05 THEN 70
            WHEN ABS(bias) <= 0.08 THEN 40
            ELSE 10
        END) AS s_bias,
        (CASE
            WHEN vol_ratio IS NULL THEN 50
            ELSE GREATEST(0, 100 - LEAST(ABS(vol_ratio - 1.5) / 1.5 * 100, 100))
        END) AS s_vol_mild,
        (
            COALESCE(PERCENT_RANK() OVER (ORDER BY chip_concentration), 0.5) * 60 +
            (1 - COALESCE(PERCENT_RANK() OVER (ORDER BY cost_deviation), 0.5)) * 40
        ) AS s_chip
    FROM factors
)
SELECT
    trade_date,
    ts_code,
    s_breakout,
    s_trend,
    s_volume,
    s_rs,
    s_liquidity,
    s_contraction,
    s_bull_align,
    s_bias,
    s_vol_mild,
    s_chip,
    (s_breakout * 0.22 +
     s_trend * 0.12 +
     s_volume * 0.12 +
     s_rs * 0.12 +
     s_liquidity * 0.10 +
     s_contraction * 0.10 +
     s_bull_align * 0.08 +
     s_bias * 0.07 +
     s_vol_mild * 0.04 +
     s_chip * 0.03
    ) AS total_score
FROM scored
"""


class TrendBreakoutScoreQuery:
    """趋势突破评分查询器."""

    def __init__(self, engine):
        self.engine = engine

    def get_scores(self, trade_date: int) -> pd.DataFrame:
        """计算指定日期趋势突破评分."""
        sql = f"""
        SELECT *
        FROM ({SCORE_SQL}) t
        ORDER BY total_score DESC
        """
        return pd.read_sql(text(sql), self.engine, params={"trade_date": trade_date})

    def get_top_stocks(self, trade_date: int, top_n: int = 50) -> pd.DataFrame:
        """获取评分最高的股票."""
        df = self.get_scores(trade_date)
        if df.empty:
            return df
        return df.head(top_n)

    def get_stocks_by_codes(self, trade_date: int, ts_codes: List[str]) -> pd.DataFrame:
        """查询指定股票代码的评分."""
        if not ts_codes:
            return pd.DataFrame()

        placeholders = ", ".join([f":code_{i}" for i in range(len(ts_codes))])
        params: Dict[str, str] = {"trade_date": trade_date}
        for i, code in enumerate(ts_codes):
            params[f"code_{i}"] = code

        sql = f"""
        SELECT *
        FROM ({SCORE_SQL}) t
        WHERE t.ts_code IN ({placeholders})
        """
        return pd.read_sql(text(sql), self.engine, params=params)


def get_engine(host="localhost", port=3306, user="root", password="", database="tushare_stock"):
    """创建数据库连接."""
    connection_string = (
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        f"?charset=utf8mb4"
    )
    return create_engine(connection_string, echo=False)
