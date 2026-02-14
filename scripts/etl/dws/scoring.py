"""DWS Scoring Functions - 6个维度的评分计算逻辑

基于 claude_score 包的打分逻辑，转换为纯 SQL 实现。
"""
from __future__ import annotations

import logging


def _run_momentum_score(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """计算动量评分（0-25分）.
    
    - ret_5_score: 5日收益评分 (0-3)
    - ret_20_score: 20日收益评分 (0-2) - 与MTM重叠，权重降低
    - ret_60_score: 60日收益评分 (0-3)
    - vol_ratio_score: 量比评分 (0-4)
    - turnover_score: 换手率评分 (0-4)
    - mtm_score: MTM动量指标评分 (0-5)
    - mtmma_score: MTM与MTMMA交叉信号 (0-4) - 趋势确认
    """
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "WHERE p.trade_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "WHERE p.trade_date = %s"
        params = [trade_date]
    
    sql = f"""
    INSERT INTO dws_momentum_score (
        trade_date, ts_code, 
        ret_5_score, ret_20_score, ret_60_score,
        vol_ratio_score, turnover_score, mtm_score, mtmma_score, momentum_score
    )
    SELECT
        p.trade_date,
        p.ts_code,
        -- 5日收益评分 (0-3)
        CASE 
            WHEN p.qfq_ret_5 > 0.10 THEN 3.0
            WHEN p.qfq_ret_5 > 0.05 THEN 2.0
            WHEN p.qfq_ret_5 > 0 THEN 1.0
            ELSE 0
        END AS ret_5_score,
        -- 20日收益评分 (0-2) - 与MTM重叠，权重降低
        CASE 
            WHEN p.qfq_ret_20 > 0.15 THEN 2.0
            WHEN p.qfq_ret_20 > 0.05 THEN 1.0
            ELSE 0
        END AS ret_20_score,
        -- 60日收益评分 (0-3)
        CASE 
            WHEN p.qfq_ret_60 > 0.30 THEN 3.0
            WHEN p.qfq_ret_60 > 0.10 THEN 2.0
            WHEN p.qfq_ret_60 > 0 THEN 1.0
            ELSE 0
        END AS ret_60_score,
        -- 量比评分 (0-4)
        CASE 
            WHEN b.volume_ratio > 1.5 THEN 4.0
            WHEN b.volume_ratio > 1.2 THEN 3.0
            WHEN b.volume_ratio > 1.0 THEN 2.0
            ELSE 1.0
        END AS vol_ratio_score,
        -- 换手率评分 (0-4)
        CASE 
            WHEN b.turnover_rate > 10 THEN 4.0
            WHEN b.turnover_rate > 5 THEN 3.0
            WHEN b.turnover_rate > 2 THEN 2.0
            ELSE 1.0
        END AS turnover_score,
        -- MTM动量指标评分 (0-5)
        CASE 
            WHEN f.mtm_qfq > 1.0 THEN 5.0
            WHEN f.mtm_qfq > 0.5 THEN 4.0
            WHEN f.mtm_qfq > 0.2 THEN 3.0
            WHEN f.mtm_qfq > 0 THEN 2.0
            WHEN f.mtm_qfq > -0.5 THEN 1.0
            ELSE 0
        END AS mtm_score,
        -- MTMMA交叉信号评分 (0-4)
        -- MTM > MTMMA 表示动量上穿均线，多头信号
        CASE 
            WHEN f.mtm_qfq > f.mtmma_qfq AND f.mtm_qfq > 0 THEN 4.0  -- 金叉+多头
            WHEN f.mtm_qfq > f.mtmma_qfq THEN 3.0                    -- 金叉
            WHEN f.mtm_qfq > 0 AND f.mtmma_qfq > 0 THEN 2.0          -- 双多头区
            WHEN f.mtm_qfq > -0.2 AND f.mtmma_qfq > -0.2 THEN 1.0    -- 接近零轴
            ELSE 0
        END AS mtmma_score,
        -- 动量总分 (0-25)
        (CASE WHEN p.qfq_ret_5 > 0.10 THEN 3.0 WHEN p.qfq_ret_5 > 0.05 THEN 2.0 WHEN p.qfq_ret_5 > 0 THEN 1.0 ELSE 0 END) +
        (CASE WHEN p.qfq_ret_20 > 0.15 THEN 2.0 WHEN p.qfq_ret_20 > 0.05 THEN 1.0 ELSE 0 END) +
        (CASE WHEN p.qfq_ret_60 > 0.30 THEN 3.0 WHEN p.qfq_ret_60 > 0.10 THEN 2.0 WHEN p.qfq_ret_60 > 0 THEN 1.0 ELSE 0 END) +
        (CASE WHEN b.volume_ratio > 1.5 THEN 4.0 WHEN b.volume_ratio > 1.2 THEN 3.0 WHEN b.volume_ratio > 1.0 THEN 2.0 ELSE 1.0 END) +
        (CASE WHEN b.turnover_rate > 10 THEN 4.0 WHEN b.turnover_rate > 5 THEN 3.0 WHEN b.turnover_rate > 2 THEN 2.0 ELSE 1.0 END) +
        (CASE WHEN f.mtm_qfq > 1.0 THEN 5.0 WHEN f.mtm_qfq > 0.5 THEN 4.0 WHEN f.mtm_qfq > 0.2 THEN 3.0 WHEN f.mtm_qfq > 0 THEN 2.0 WHEN f.mtm_qfq > -0.5 THEN 1.0 ELSE 0 END) +
        (CASE WHEN f.mtm_qfq > f.mtmma_qfq AND f.mtm_qfq > 0 THEN 4.0 WHEN f.mtm_qfq > f.mtmma_qfq THEN 3.0 WHEN f.mtm_qfq > 0 AND f.mtmma_qfq > 0 THEN 2.0 WHEN f.mtm_qfq > -0.2 AND f.mtmma_qfq > -0.2 THEN 1.0 ELSE 0 END)
        AS momentum_score
    FROM dws_price_adj_daily p
    JOIN dwd_daily_basic b ON p.trade_date = b.trade_date AND p.ts_code = b.ts_code
    LEFT JOIN ods_stk_factor f ON p.trade_date = f.trade_date AND p.ts_code = f.ts_code
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        ret_5_score = VALUES(ret_5_score),
        ret_20_score = VALUES(ret_20_score),
        ret_60_score = VALUES(ret_60_score),
        vol_ratio_score = VALUES(vol_ratio_score),
        turnover_score = VALUES(turnover_score),
        mtm_score = VALUES(mtm_score),
        mtmma_score = VALUES(mtmma_score),
        momentum_score = VALUES(momentum_score)
    """
    cursor.execute(sql, params if params else None)


def _run_value_score(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """计算价值评分（0-20分）.
    
    - pe_score: PE评分 (0-7)
    - pb_score: PB评分 (0-7)
    - ps_score: PS评分 (0-6)
    """
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "WHERE trade_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "WHERE trade_date = %s"
        params = [trade_date]
    
    sql = f"""
    INSERT INTO dws_value_score (
        trade_date, ts_code, pe_score, pb_score, ps_score, value_score
    )
    SELECT
        trade_date,
        ts_code,
        -- PE评分 (低估值高分)
        CASE 
            WHEN pe_ttm <= 0 THEN 0
            WHEN pe_ttm < 15 THEN 7.0
            WHEN pe_ttm < 25 THEN 5.0
            WHEN pe_ttm < 40 THEN 3.0
            WHEN pe_ttm < 60 THEN 1.0
            ELSE 0
        END AS pe_score,
        -- PB评分 (破净高分)
        CASE 
            WHEN pb IS NULL THEN 0
            WHEN pb < 1.0 THEN 7.0
            WHEN pb < 2.0 THEN 6.0
            WHEN pb < 3.0 THEN 4.0
            WHEN pb < 5.0 THEN 2.0
            ELSE 0
        END AS pb_score,
        -- PS评分
        CASE 
            WHEN ps_ttm IS NULL THEN 0
            WHEN ps_ttm < 1.0 THEN 6.0
            WHEN ps_ttm < 2.0 THEN 5.0
            WHEN ps_ttm < 3.0 THEN 3.0
            WHEN ps_ttm < 5.0 THEN 1.0
            ELSE 0
        END AS ps_score,
        -- 价值总分
        (CASE WHEN pe_ttm <= 0 THEN 0 WHEN pe_ttm < 15 THEN 7.0 WHEN pe_ttm < 25 THEN 5.0 WHEN pe_ttm < 40 THEN 3.0 WHEN pe_ttm < 60 THEN 1.0 ELSE 0 END) +
        (CASE WHEN pb IS NULL THEN 0 WHEN pb < 1.0 THEN 7.0 WHEN pb < 2.0 THEN 6.0 WHEN pb < 3.0 THEN 4.0 WHEN pb < 5.0 THEN 2.0 ELSE 0 END) +
        (CASE WHEN ps_ttm IS NULL THEN 0 WHEN ps_ttm < 1.0 THEN 6.0 WHEN ps_ttm < 2.0 THEN 5.0 WHEN ps_ttm < 3.0 THEN 3.0 WHEN ps_ttm < 5.0 THEN 1.0 ELSE 0 END)
        AS value_score
    FROM dwd_daily_basic
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        pe_score = VALUES(pe_score),
        pb_score = VALUES(pb_score),
        ps_score = VALUES(ps_score),
        value_score = VALUES(value_score)
    """
    cursor.execute(sql, params if params else None)


def _run_quality_score(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """计算质量评分（0-20分）.
    
    - roe_score: ROE评分 (0-8)
    - margin_score: 毛利率评分 (0-6)
    - leverage_score: 负债率评分 (0-6)
    """
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "WHERE trade_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "WHERE trade_date = %s"
        params = [trade_date]
    
    sql = f"""
    INSERT INTO dws_quality_score (
        trade_date, ts_code, roe_score, margin_score, leverage_score, quality_score
    )
    SELECT
        trade_date,
        ts_code,
        -- ROE评分
        CASE 
            WHEN roe_ttm IS NULL THEN 0
            WHEN roe_ttm > 0.20 THEN 8.0
            WHEN roe_ttm > 0.15 THEN 6.0
            WHEN roe_ttm > 0.10 THEN 4.0
            WHEN roe_ttm > 0.05 THEN 2.0
            ELSE 0
        END AS roe_score,
        -- 毛利率评分
        CASE 
            WHEN grossprofit_margin IS NULL THEN 0
            WHEN grossprofit_margin > 0.50 THEN 6.0
            WHEN grossprofit_margin > 0.30 THEN 5.0
            WHEN grossprofit_margin > 0.20 THEN 3.0
            WHEN grossprofit_margin > 0.10 THEN 1.0
            ELSE 0
        END AS margin_score,
        -- 负债率评分 (低负债高分)
        CASE 
            WHEN debt_to_assets IS NULL THEN 0
            WHEN debt_to_assets < 0.30 THEN 6.0
            WHEN debt_to_assets < 0.50 THEN 5.0
            WHEN debt_to_assets < 0.70 THEN 3.0
            ELSE 1.0
        END AS leverage_score,
        -- 质量总分
        (CASE WHEN roe_ttm IS NULL THEN 0 WHEN roe_ttm > 0.20 THEN 8.0 WHEN roe_ttm > 0.15 THEN 6.0 WHEN roe_ttm > 0.10 THEN 4.0 WHEN roe_ttm > 0.05 THEN 2.0 ELSE 0 END) +
        (CASE WHEN grossprofit_margin IS NULL THEN 0 WHEN grossprofit_margin > 0.50 THEN 6.0 WHEN grossprofit_margin > 0.30 THEN 5.0 WHEN grossprofit_margin > 0.20 THEN 3.0 WHEN grossprofit_margin > 0.10 THEN 1.0 ELSE 0 END) +
        (CASE WHEN debt_to_assets IS NULL THEN 0 WHEN debt_to_assets < 0.30 THEN 6.0 WHEN debt_to_assets < 0.50 THEN 5.0 WHEN debt_to_assets < 0.70 THEN 3.0 ELSE 1.0 END)
        AS quality_score
    FROM dwd_fina_snapshot
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        roe_score = VALUES(roe_score),
        margin_score = VALUES(margin_score),
        leverage_score = VALUES(leverage_score),
        quality_score = VALUES(quality_score)
    """
    cursor.execute(sql, params if params else None)


def _run_technical_score(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """计算技术评分（0-15分）.
    
    使用前复权版本技术指标 (_qfq 后缀):
    - macd_score: MACD评分 (0-4)
    - kdj_score: KDJ J值评分 (0-3)
    - rsi_score: RSI评分 (0-3)
    - cci_score: CCI顺势指标评分 (0-3) - 新增
    - bias_score: BIAS乖离率评分 (0-2) - 新增
    """
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "WHERE trade_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "WHERE trade_date = %s"
        params = [trade_date]
    
    sql = f"""
    INSERT INTO dws_technical_score (
        trade_date, ts_code, macd_score, kdj_score, rsi_score, cci_score, bias_score, technical_score
    )
    SELECT
        trade_date,
        ts_code,
        -- MACD评分 (0-4)
        CASE 
            WHEN macd_qfq > 0 AND macd_dif_qfq > macd_dea_qfq THEN 4.0
            WHEN macd_qfq > 0 THEN 2.0
            WHEN macd_dif_qfq > macd_dea_qfq THEN 1.0
            ELSE 0
        END AS macd_score,
        -- KDJ J值评分 (0-3)
        CASE 
            WHEN kdj_qfq IS NULL THEN 1.0
            WHEN kdj_qfq > 80 THEN 0
            WHEN kdj_qfq < 20 THEN 3.0
            WHEN kdj_qfq BETWEEN 40 AND 60 THEN 2.0
            ELSE 1.0
        END AS kdj_score,
        -- RSI评分 (0-3)
        CASE 
            WHEN rsi_qfq_6 IS NULL THEN 1.0
            WHEN rsi_qfq_6 > 70 THEN 0
            WHEN rsi_qfq_6 < 30 THEN 3.0
            WHEN rsi_qfq_6 BETWEEN 40 AND 60 THEN 2.0
            ELSE 1.0
        END AS rsi_score,
        -- CCI顺势指标评分 (0-3)
        -- CCI > 100 = 强势突破, CCI < -100 = 超卖机会
        CASE 
            WHEN cci_qfq IS NULL THEN 1.0
            WHEN cci_qfq > 100 THEN 3.0      -- 强势突破
            WHEN cci_qfq > 0 THEN 2.0        -- 多头区域
            WHEN cci_qfq < -100 THEN 2.0     -- 超卖反弹机会
            ELSE 1.0
        END AS cci_score,
        -- BIAS乖离率评分 (0-2)
        -- 极端乖离 = 均值回归机会
        CASE 
            WHEN bias1_qfq IS NULL THEN 1.0
            WHEN bias1_qfq < -3 THEN 2.0     -- 超卖，反弹机会
            WHEN bias1_qfq > 5 THEN 0        -- 过热，调整风险
            WHEN bias1_qfq BETWEEN -1 AND 1 THEN 1.0  -- 正常区间
            ELSE 1.0
        END AS bias_score,
        -- 技术总分 (0-15)
        (CASE WHEN macd_qfq > 0 AND macd_dif_qfq > macd_dea_qfq THEN 4.0 WHEN macd_qfq > 0 THEN 2.0 WHEN macd_dif_qfq > macd_dea_qfq THEN 1.0 ELSE 0 END) +
        (CASE WHEN kdj_qfq IS NULL THEN 1.0 WHEN kdj_qfq > 80 THEN 0 WHEN kdj_qfq < 20 THEN 3.0 WHEN kdj_qfq BETWEEN 40 AND 60 THEN 2.0 ELSE 1.0 END) +
        (CASE WHEN rsi_qfq_6 IS NULL THEN 1.0 WHEN rsi_qfq_6 > 70 THEN 0 WHEN rsi_qfq_6 < 30 THEN 3.0 WHEN rsi_qfq_6 BETWEEN 40 AND 60 THEN 2.0 ELSE 1.0 END) +
        (CASE WHEN cci_qfq IS NULL THEN 1.0 WHEN cci_qfq > 100 THEN 3.0 WHEN cci_qfq > 0 THEN 2.0 WHEN cci_qfq < -100 THEN 2.0 ELSE 1.0 END) +
        (CASE WHEN bias1_qfq IS NULL THEN 1.0 WHEN bias1_qfq < -3 THEN 2.0 WHEN bias1_qfq > 5 THEN 0 WHEN bias1_qfq BETWEEN -1 AND 1 THEN 1.0 ELSE 1.0 END)
        AS technical_score
    FROM ods_stk_factor
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        macd_score = VALUES(macd_score),
        kdj_score = VALUES(kdj_score),
        rsi_score = VALUES(rsi_score),
        cci_score = VALUES(cci_score),
        bias_score = VALUES(bias_score),
        technical_score = VALUES(technical_score)
    """
    cursor.execute(sql, params if params else None)


def _run_capital_score(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """计算资金评分（0-10分）.
    
    - elg_score: 特大单评分 (0-5)
    - lg_score: 大单评分 (0-3)
    - margin_score: 融资融券评分 (0-2) - 新增
      非融资标的给基准分1分
    """
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "WHERE mf.trade_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "WHERE mf.trade_date = %s"
        params = [trade_date]
    
    sql = f"""
    INSERT INTO dws_capital_score (
        trade_date, ts_code, elg_net, lg_net, elg_score, lg_score, margin_score, capital_score
    )
    SELECT
        mf.trade_date,
        mf.ts_code,
        (COALESCE(mf.buy_elg_amount, 0) - COALESCE(mf.sell_elg_amount, 0)) / 10000 AS elg_net,
        (COALESCE(mf.buy_lg_amount, 0) - COALESCE(mf.sell_lg_amount, 0)) / 10000 AS lg_net,
        -- 特大单评分 (0-5)
        CASE 
            WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 100000000 THEN 5.0
            WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 50000000 THEN 4.0
            WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 10000000 THEN 2.0
            WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 0 THEN 1.0
            ELSE 0
        END AS elg_score,
        -- 大单评分 (0-3)
        CASE 
            WHEN (mf.buy_lg_amount - mf.sell_lg_amount) > 50000000 THEN 3.0
            WHEN (mf.buy_lg_amount - mf.sell_lg_amount) > 20000000 THEN 2.0
            WHEN (mf.buy_lg_amount - mf.sell_lg_amount) > 0 THEN 1.0
            ELSE 0
        END AS lg_score,
        -- 融资融券评分 (0-2): 使用百分比消除大小市值偏差
        CASE 
            WHEN mg.ts_code IS NULL THEN 1.0  -- 非融资标的基准分
            WHEN mg.rzye = 0 THEN 1.0         -- 无融资余额
            WHEN (mg.rzmre - mg.rzche) / mg.rzye * 100 > 2.0 THEN 2.0   -- 净买入>2pct
            WHEN (mg.rzmre - mg.rzche) / mg.rzye * 100 > 0.5 THEN 1.5   -- 净买入>0.5pct
            WHEN (mg.rzmre - mg.rzche) / mg.rzye * 100 > 0 THEN 1.0     -- 净买入>0
            ELSE 0.5                                                     -- 净卖出
        END AS margin_score,
        -- 资金总分 (0-10)
        (CASE WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 100000000 THEN 5.0 WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 50000000 THEN 4.0 WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 10000000 THEN 2.0 WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 0 THEN 1.0 ELSE 0 END) +
        (CASE WHEN (mf.buy_lg_amount - mf.sell_lg_amount) > 50000000 THEN 3.0 WHEN (mf.buy_lg_amount - mf.sell_lg_amount) > 20000000 THEN 2.0 WHEN (mf.buy_lg_amount - mf.sell_lg_amount) > 0 THEN 1.0 ELSE 0 END) +
        (CASE WHEN mg.ts_code IS NULL THEN 1.0 WHEN mg.rzye = 0 THEN 1.0 WHEN (mg.rzmre - mg.rzche) / mg.rzye * 100 > 2.0 THEN 2.0 WHEN (mg.rzmre - mg.rzche) / mg.rzye * 100 > 0.5 THEN 1.5 WHEN (mg.rzmre - mg.rzche) / mg.rzye * 100 > 0 THEN 1.0 ELSE 0.5 END)
        AS capital_score
    FROM ods_moneyflow mf
    LEFT JOIN ods_margin_detail mg ON mf.trade_date = mg.trade_date AND mf.ts_code = mg.ts_code
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        elg_net = VALUES(elg_net),
        lg_net = VALUES(lg_net),
        elg_score = VALUES(elg_score),
        lg_score = VALUES(lg_score),
        margin_score = VALUES(margin_score),
        capital_score = VALUES(capital_score)
    """
    cursor.execute(sql, params if params else None)


def _run_chip_score(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """计算筹码评分（0-10分）.
    
    - winner_score: 获利比例评分 (0-6)
    - cost_score: 成本偏离评分 (0-4)
    """
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "WHERE c.trade_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "WHERE c.trade_date = %s"
        params = [trade_date]
    
    sql = f"""
    INSERT INTO dws_chip_score (
        trade_date, ts_code, winner_score, cost_score, chip_score
    )
    SELECT
        c.trade_date,
        c.ts_code,
        -- 获利比例评分 (深度套牢=反转机会)
        CASE 
            WHEN c.winner_rate IS NULL THEN 0
            WHEN c.winner_rate < 0.10 THEN 6.0
            WHEN c.winner_rate < 0.30 THEN 5.0
            WHEN c.winner_rate BETWEEN 0.40 AND 0.60 THEN 3.0
            WHEN c.winner_rate > 0.90 THEN 1.0
            ELSE 2.0
        END AS winner_score,
        -- 成本偏离评分 (突破成本高分)
        CASE 
            WHEN c.cost_50pct IS NULL OR c.cost_50pct = 0 THEN 0
            WHEN d.close / c.cost_50pct > 1.10 THEN 4.0
            WHEN d.close / c.cost_50pct > 1.05 THEN 3.0
            WHEN d.close / c.cost_50pct > 1.00 THEN 2.0
            ELSE 0
        END AS cost_score,
        -- 筹码总分
        (CASE WHEN c.winner_rate IS NULL THEN 0 WHEN c.winner_rate < 0.10 THEN 6.0 WHEN c.winner_rate < 0.30 THEN 5.0 WHEN c.winner_rate BETWEEN 0.40 AND 0.60 THEN 3.0 WHEN c.winner_rate > 0.90 THEN 1.0 ELSE 2.0 END) +
        (CASE WHEN c.cost_50pct IS NULL OR c.cost_50pct = 0 THEN 0 WHEN d.close / c.cost_50pct > 1.10 THEN 4.0 WHEN d.close / c.cost_50pct > 1.05 THEN 3.0 WHEN d.close / c.cost_50pct > 1.00 THEN 2.0 ELSE 0 END)
        AS chip_score
    FROM ods_cyq_perf c
    JOIN ods_daily d ON c.trade_date = d.trade_date AND c.ts_code = d.ts_code
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        winner_score = VALUES(winner_score),
        cost_score = VALUES(cost_score),
        chip_score = VALUES(chip_score)
    """
    cursor.execute(sql, params if params else None)
