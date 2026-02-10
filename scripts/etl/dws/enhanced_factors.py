"""
Enhanced factor calculations for Phase 1.
Includes: liquidity, momentum extended, quality extended, risk factors.
"""
from __future__ import annotations
import logging
from typing import Optional


def _get_lookback_date(cursor, target_date: int, days: int) -> int:
    """Get the trade date N trading days ago."""
    sql = (
        "SELECT cal_date FROM dim_trade_cal "
        "WHERE exchange='SSE' AND is_open=1 AND cal_date <= %s "
        "ORDER BY cal_date DESC LIMIT 1 OFFSET %s"
    )
    cursor.execute(sql, (target_date, days))
    row = cursor.fetchone()
    return int(row[0]) if row else 20100101


def run_liquidity_factor(cursor, trade_date: int) -> None:
    """Calculate liquidity factors using window functions."""
    sql = """
    INSERT INTO dws_liquidity_factor (
        trade_date, ts_code, turnover_vol_20, amihud_20, vol_concentration, bid_ask_spread
    )
    SELECT 
        base.trade_date,
        base.ts_code,
        -- 换手率波动 (20日标准差)
        liq.turnover_vol_20,
        -- Amihud非流动性 = AVG(|ret| / amount)
        liq.amihud_20,
        -- 成交量集中度 = MAX(5) / AVG(20)
        liq.vol_concentration,
        -- 买卖价差代理 = 2*(H-L)/(H+L)
        CASE WHEN (base.high + base.low) > 0 
             THEN 2.0 * (base.high - base.low) / (base.high + base.low) 
             ELSE NULL END AS bid_ask_spread
    FROM dwd_daily base
    LEFT JOIN (
        SELECT 
            d.trade_date,
            d.ts_code,
            -- 20日换手率标准差
            STDDEV_SAMP(db.turnover_rate_f) OVER w20 AS turnover_vol_20,
            -- Amihud: 价格冲击
            AVG(
                CASE WHEN d.amount > 0 
                     THEN ABS(d.pct_chg / 100.0) / d.amount 
                     ELSE NULL END
            ) OVER w20 AS amihud_20,
            -- 成交量集中度
            CASE WHEN AVG(d.vol) OVER w20 > 0
                 THEN MAX(d.vol) OVER w5 / AVG(d.vol) OVER w20
                 ELSE NULL END AS vol_concentration
        FROM dwd_daily d
        LEFT JOIN dwd_daily_basic db ON db.trade_date = d.trade_date AND db.ts_code = d.ts_code
        WHERE d.trade_date >= %s
        WINDOW 
            w5 AS (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
            w20 AS (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    ) liq ON liq.trade_date = base.trade_date AND liq.ts_code = base.ts_code
    WHERE base.trade_date = %s

    ON DUPLICATE KEY UPDATE
        turnover_vol_20 = VALUES(turnover_vol_20),
        amihud_20 = VALUES(amihud_20),
        vol_concentration = VALUES(vol_concentration),
        bid_ask_spread = VALUES(bid_ask_spread)
    """
    start_date = _get_lookback_date(cursor, trade_date, 60)
    cursor.execute(sql, (start_date, trade_date))


def run_momentum_extended(cursor, trade_date: int) -> None:

    """Calculate extended momentum factors."""
    sql = """
    INSERT INTO dws_momentum_extended (
        trade_date, ts_code, high_52w_dist, reversal_5, mom_12m_1m, vol_price_corr
    )
    SELECT 
        base.trade_date,
        base.ts_code,
        mom.high_52w_dist,
        mom.reversal_5,
        mom.mom_12m_1m,
        mom.vol_price_corr
    FROM dwd_daily base
    LEFT JOIN (
        SELECT 
            d.trade_date,
            d.ts_code,
            -- 52周高点距离
            CASE WHEN MAX(d.close) OVER w250 > 0 
                 THEN d.close / MAX(d.close) OVER w250 - 1 
                 ELSE NULL END AS high_52w_dist,
            -- 5日反转 (负的5日收益)
            -(adj.qfq_ret_5) AS reversal_5,
            -- 12m-1m动量
            CASE WHEN LAG(adj.qfq_close, 250) OVER w IS NOT NULL AND LAG(adj.qfq_close, 20) OVER w IS NOT NULL
                 THEN (adj.qfq_close / LAG(adj.qfq_close, 250) OVER w - 1) 
                      - (adj.qfq_close / LAG(adj.qfq_close, 20) OVER w - 1)
                 ELSE NULL END AS mom_12m_1m,
            -- 量价相关系数 (需要用子查询计算)
            NULL AS vol_price_corr
        FROM dwd_daily d
        LEFT JOIN dws_price_adj_daily adj ON adj.trade_date = d.trade_date AND adj.ts_code = d.ts_code
        WHERE d.trade_date >= %s
        WINDOW 
            w AS (PARTITION BY d.ts_code ORDER BY d.trade_date),
            w250 AS (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW)
    ) mom ON mom.trade_date = base.trade_date AND mom.ts_code = base.ts_code
    WHERE base.trade_date = %s

    ON DUPLICATE KEY UPDATE
        high_52w_dist = VALUES(high_52w_dist),
        reversal_5 = VALUES(reversal_5),
        mom_12m_1m = VALUES(mom_12m_1m),
        vol_price_corr = VALUES(vol_price_corr)
    """
    start_date = _get_lookback_date(cursor, trade_date, 300)
    cursor.execute(sql, (start_date, trade_date))


def run_quality_extended(cursor, trade_date: int) -> None:

    """Calculate extended quality factors (DuPont decomposition)."""
    sql = """
    INSERT INTO dws_quality_extended (
        trade_date, ts_code, dupont_margin, dupont_turnover, dupont_leverage, roe_trend
    )
    SELECT 
        base.trade_date,
        base.ts_code,
        base.dupont_margin,
        base.dupont_turnover,
        base.dupont_leverage,
        base.roe_trend
    FROM (
        SELECT 
            f.trade_date,
            f.ts_code,
            -- 杜邦-净利率
            fi.netprofit_margin AS dupont_margin,
            -- 杜邦-资产周转率 = 营收/总资产
            CASE WHEN fi.total_assets > 0 THEN fi.op_income / fi.total_assets ELSE NULL END AS dupont_turnover,
            -- 杜邦-财务杠杆 = 总资产/股东权益
            CASE WHEN fi.total_hldr_eqy > 0 THEN fi.total_assets / fi.total_hldr_eqy ELSE NULL END AS dupont_leverage,
            -- ROE趋势 (同比变化) - 简化版，用季度差
            f.roe - LAG(f.roe, 4) OVER (PARTITION BY f.ts_code ORDER BY f.trade_date) AS roe_trend
        FROM dws_fina_pit_daily f
        LEFT JOIN dwd_fina_indicator fi ON fi.ts_code = f.ts_code AND fi.end_date = f.end_date
        WHERE f.trade_date >= %s
    ) base
    WHERE base.trade_date = %s

    ON DUPLICATE KEY UPDATE
        dupont_margin = VALUES(dupont_margin),
        dupont_turnover = VALUES(dupont_turnover),
        dupont_leverage = VALUES(dupont_leverage),
        roe_trend = VALUES(roe_trend)
    """
    start_date = _get_lookback_date(cursor, trade_date, 400) # Need enough history for LAG(4)
    cursor.execute(sql, (start_date, trade_date))


def run_risk_factor(cursor, trade_date: int) -> None:

    """Calculate risk factors (downside vol, max drawdown, VaR)."""
    sql = """
    INSERT INTO dws_risk_factor (
        trade_date, ts_code, downside_vol_60, max_drawdown_60, var_5pct_60, beta_60, ivol_20
    )
    SELECT 
        base.trade_date,
        base.ts_code,
        risk.downside_vol_60,
        risk.max_drawdown_60,
        risk.var_5pct_60,
        NULL AS beta_60,  -- Phase 2: 需要指数数据
        NULL AS ivol_20   -- Phase 2: 需要指数数据
    FROM dwd_daily base
    LEFT JOIN (
        SELECT 
            d.trade_date,
            d.ts_code,
            -- 下行波动率 (只算负收益的标准差)
            STDDEV_SAMP(CASE WHEN d.pct_chg < 0 THEN d.pct_chg ELSE NULL END) OVER w60 AS downside_vol_60,
            -- 最大回撤 (60日)
            (MAX(adj.qfq_close) OVER w60 - adj.qfq_close) / NULLIF(MAX(adj.qfq_close) OVER w60, 0) AS max_drawdown_60,
            -- VaR 5%% (简化: MySQL不支持PERCENTILE_CONT)
            NULL AS var_5pct_60
        FROM dwd_daily d
        LEFT JOIN dws_price_adj_daily adj ON adj.trade_date = d.trade_date AND adj.ts_code = d.ts_code
        WHERE d.trade_date >= %s
        WINDOW w60 AS (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    ) risk ON risk.trade_date = base.trade_date AND risk.ts_code = base.ts_code
    WHERE base.trade_date = %s

    ON DUPLICATE KEY UPDATE
        downside_vol_60 = VALUES(downside_vol_60),
        max_drawdown_60 = VALUES(max_drawdown_60),
        var_5pct_60 = VALUES(var_5pct_60),
        beta_60 = VALUES(beta_60),
        ivol_20 = VALUES(ivol_20)
    """
    start_date = _get_lookback_date(cursor, trade_date, 100)
    cursor.execute(sql, (start_date, trade_date))

