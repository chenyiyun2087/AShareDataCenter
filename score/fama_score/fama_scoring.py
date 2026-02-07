"""
Fama-French 评分系统
基于 Fama-French 五因子模型的增强型评分

因子覆盖:
- SMB: 市值因子 (10分)
- MOM: 动量因子 (22分) 
- HML: 价值因子 (18分)
- RMW: 盈利因子 (质量评分, 17分)
- CMA: 投资因子 (质量评分, 5分)
- 技术: 精简版 (10分)
- 资金: 主力资金 (10分)
- 筹码: 筹码分布 (8分)
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def run_fama_scoring(cursor, trade_date: Optional[int] = None) -> None:
    """运行 Fama-French 评分计算"""
    logger.info(f"开始 Fama-French 评分计算: {trade_date or 'all'}")
    
    _run_size_score(cursor, trade_date)
    _run_fama_momentum_score(cursor, trade_date)
    _run_fama_value_score(cursor, trade_date)
    _run_fama_quality_score(cursor, trade_date)
    _run_fama_technical_score(cursor, trade_date)
    _run_fama_capital_score(cursor, trade_date)
    _run_fama_chip_score(cursor, trade_date)
    
    logger.info("Fama-French 评分计算完成")


def _run_size_score(cursor, trade_date: Optional[int] = None) -> None:
    """计算 SMB 市值因子评分 (0-10分)
    
    小盘股溢价效应:
    - 流通市值 < 30亿: 10分
    - 30-80亿: 8分
    - 80-200亿: 6分
    - 200-500亿: 4分
    - 500-1000亿: 2分
    - > 1000亿: 1分
    """
    filter_sql = ""
    params = []
    if trade_date is not None:
        filter_sql = "WHERE sf.trade_date = %s"
        params.append(trade_date)
    
    sql = f"""
    INSERT INTO dws_fama_size_score (trade_date, ts_code, circ_mv, size_rank, size_score)
    SELECT
        sf.trade_date,
        sf.ts_code,
        sf.circ_mv,
        -- 市值排名百分位 (越小越好)
        PERCENT_RANK() OVER (PARTITION BY sf.trade_date ORDER BY sf.circ_mv ASC) AS size_rank,
        -- 市值评分
        CASE 
            WHEN sf.circ_mv < 300000 THEN 10.0   -- < 30亿
            WHEN sf.circ_mv < 800000 THEN 8.0    -- 30-80亿
            WHEN sf.circ_mv < 2000000 THEN 6.0   -- 80-200亿
            WHEN sf.circ_mv < 5000000 THEN 4.0   -- 200-500亿
            WHEN sf.circ_mv < 10000000 THEN 2.0  -- 500-1000亿
            ELSE 1.0                              -- > 1000亿
        END AS size_score
    FROM ods_stk_factor sf
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        circ_mv = VALUES(circ_mv),
        size_rank = VALUES(size_rank),
        size_score = VALUES(size_score)
    """
    cursor.execute(sql, params if params else None)
    logger.info(f"SMB市值评分完成: {cursor.rowcount} rows")


def _run_fama_momentum_score(cursor, trade_date: Optional[int] = None) -> None:
    """计算动量因子评分 (0-22分)
    
    - 12月动量: 6分 (排名法)
    - 1月反转: 2分
    - 量比: 4分
    - 换手率: 4分
    - MTM: 4分
    - MTMMA交叉: 2分
    """
    filter_sql = ""
    params = []
    if trade_date is not None:
        filter_sql = "WHERE sf.trade_date = %s"
        params.append(trade_date)
    
    # 由于12月动量需要历史数据，这里简化处理，使用现有的MTM指标
    sql = f"""
    INSERT INTO dws_fama_momentum_score (
        trade_date, ts_code, volume_ratio, turnover_rate, mtm, mtmma, momentum_score
    )
    SELECT
        sf.trade_date,
        sf.ts_code,
        sf.volume_ratio,
        sf.turnover_rate,
        sf.mtm_qfq AS mtm,
        sf.mtmma_qfq AS mtmma,
        -- 动量评分 (简化版，待后续接入12月涨幅)
        (
            -- 量比 (4分)
            CASE 
                WHEN sf.volume_ratio > 1.5 THEN 4.0
                WHEN sf.volume_ratio > 1.2 THEN 3.0
                WHEN sf.volume_ratio > 1.0 THEN 2.0
                ELSE 1.0
            END +
            -- 换手率 (4分)
            CASE 
                WHEN sf.turnover_rate > 10 THEN 4.0
                WHEN sf.turnover_rate > 5 THEN 3.0
                WHEN sf.turnover_rate > 2 THEN 2.0
                ELSE 1.0
            END +
            -- MTM (6分, 权重调整)
            CASE 
                WHEN sf.mtm_qfq > 1.0 THEN 6.0
                WHEN sf.mtm_qfq > 0.5 THEN 5.0
                WHEN sf.mtm_qfq > 0.2 THEN 4.0
                WHEN sf.mtm_qfq > 0 THEN 3.0
                WHEN sf.mtm_qfq > -0.5 THEN 1.0
                ELSE 0
            END +
            -- MTMMA交叉 (4分)
            CASE 
                WHEN sf.mtm_qfq > sf.mtmma_qfq AND sf.mtm_qfq > 0 AND sf.mtmma_qfq > 0 THEN 4.0
                WHEN sf.mtm_qfq > sf.mtmma_qfq AND sf.mtm_qfq > 0 THEN 3.0
                WHEN sf.mtm_qfq > 0 AND sf.mtmma_qfq > 0 THEN 2.0
                WHEN ABS(sf.mtm_qfq) < 0.1 THEN 1.0
                ELSE 0
            END +
            -- 预留 12月动量/1月反转 (4分), 暂用固定值
            2.0
        ) AS momentum_score
    FROM ods_stk_factor sf
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        volume_ratio = VALUES(volume_ratio),
        turnover_rate = VALUES(turnover_rate),
        mtm = VALUES(mtm),
        mtmma = VALUES(mtmma),
        momentum_score = VALUES(momentum_score)
    """
    cursor.execute(sql, params if params else None)
    logger.info(f"MOM动量评分完成: {cursor.rowcount} rows")


def _run_fama_value_score(cursor, trade_date: Optional[int] = None) -> None:
    """计算 HML 价值因子评分 (0-18分)
    
    - PB倒数(B/M): 8分 (排名法)
    - PE: 5分
    - PS: 5分
    """
    filter_sql = ""
    params = []
    if trade_date is not None:
        filter_sql = "WHERE sf.trade_date = %s"
        params.append(trade_date)
    
    sql = f"""
    INSERT INTO dws_fama_value_score (
        trade_date, ts_code, pb, bm_ratio, bm_rank, pe_ttm, ps_ttm, value_score
    )
    SELECT
        sf.trade_date,
        sf.ts_code,
        sf.pb,
        CASE WHEN sf.pb > 0 THEN 1.0 / sf.pb ELSE NULL END AS bm_ratio,
        -- B/M 排名百分位 (越高越好)
        PERCENT_RANK() OVER (PARTITION BY sf.trade_date ORDER BY CASE WHEN sf.pb > 0 THEN 1.0/sf.pb ELSE 0 END DESC) AS bm_rank,
        sf.pe_ttm,
        sf.ps_ttm,
        -- 价值评分
        (
            -- B/M 排名评分 (8分) - 使用排名法
            CASE 
                WHEN sf.pb > 0 AND sf.pb < 1.0 THEN 8.0   -- 破净股
                WHEN sf.pb > 0 AND sf.pb < 2.0 THEN 6.0
                WHEN sf.pb > 0 AND sf.pb < 3.0 THEN 4.0
                WHEN sf.pb > 0 AND sf.pb < 5.0 THEN 2.0
                ELSE 0
            END +
            -- PE评分 (5分)
            CASE 
                WHEN sf.pe_ttm > 0 AND sf.pe_ttm < 15 THEN 5.0
                WHEN sf.pe_ttm > 0 AND sf.pe_ttm < 25 THEN 3.0
                WHEN sf.pe_ttm > 0 AND sf.pe_ttm < 40 THEN 2.0
                ELSE 0
            END +
            -- PS评分 (5分)
            CASE 
                WHEN sf.ps_ttm > 0 AND sf.ps_ttm < 1.5 THEN 5.0
                WHEN sf.ps_ttm > 0 AND sf.ps_ttm < 3.0 THEN 3.0
                WHEN sf.ps_ttm > 0 AND sf.ps_ttm < 5.0 THEN 1.0
                ELSE 0
            END
        ) AS value_score
    FROM ods_stk_factor sf
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        pb = VALUES(pb),
        bm_ratio = VALUES(bm_ratio),
        bm_rank = VALUES(bm_rank),
        pe_ttm = VALUES(pe_ttm),
        ps_ttm = VALUES(ps_ttm),
        value_score = VALUES(value_score)
    """
    cursor.execute(sql, params if params else None)
    logger.info(f"HML价值评分完成: {cursor.rowcount} rows")


def _run_fama_quality_score(cursor, trade_date: Optional[int] = None) -> None:
    """计算 RMW + CMA 质量因子评分 (0-22分)
    
    RMW 盈利因子 (17分):
    - ROE: 8分
    - 毛利率: 5分
    - 资产负债率: 4分
    
    CMA 投资因子 (5分):
    - 资产增长率: 5分 (待实现)
    """
    filter_sql = ""
    params = []
    if trade_date is not None:
        filter_sql = "WHERE fs.trade_date = %s AND fs.roe_ttm IS NOT NULL"
        params.append(trade_date)
    else:
        filter_sql = "WHERE fs.roe_ttm IS NOT NULL"
    
    sql = f"""
    INSERT INTO dws_fama_quality_score (
        trade_date, ts_code, roe_ttm, grossprofit_margin, debt_to_assets,
        asset_growth_rate, rwm_score, cma_score, quality_score
    )
    SELECT
        fs.trade_date,
        fs.ts_code,
        fs.roe_ttm,
        fs.grossprofit_margin,
        fs.debt_to_assets,
        NULL AS asset_growth_rate,  -- 待实现
        -- RMW盈利评分 (17分)
        (
            CASE 
                WHEN fs.roe_ttm > 20 THEN 8.0
                WHEN fs.roe_ttm > 15 THEN 6.0
                WHEN fs.roe_ttm > 10 THEN 4.0
                WHEN fs.roe_ttm > 5 THEN 2.0
                ELSE 0
            END +
            CASE 
                WHEN fs.grossprofit_margin > 50 THEN 5.0
                WHEN fs.grossprofit_margin > 30 THEN 4.0
                WHEN fs.grossprofit_margin > 20 THEN 2.0
                ELSE 0
            END +
            CASE 
                WHEN fs.debt_to_assets < 30 THEN 4.0
                WHEN fs.debt_to_assets < 50 THEN 3.0
                WHEN fs.debt_to_assets < 70 THEN 2.0
                ELSE 0
            END
        ) AS rwm_score,
        -- CMA投资评分 (5分) - 暂用中间值
        2.5 AS cma_score,
        -- 总质量评分
        (
            CASE 
                WHEN fs.roe_ttm > 20 THEN 8.0
                WHEN fs.roe_ttm > 15 THEN 6.0
                WHEN fs.roe_ttm > 10 THEN 4.0
                WHEN fs.roe_ttm > 5 THEN 2.0
                ELSE 0
            END +
            CASE 
                WHEN fs.grossprofit_margin > 50 THEN 5.0
                WHEN fs.grossprofit_margin > 30 THEN 4.0
                WHEN fs.grossprofit_margin > 20 THEN 2.0
                ELSE 0
            END +
            CASE 
                WHEN fs.debt_to_assets < 30 THEN 4.0
                WHEN fs.debt_to_assets < 50 THEN 3.0
                WHEN fs.debt_to_assets < 70 THEN 2.0
                ELSE 0
            END +
            2.5  -- CMA暂用固定值
        ) AS quality_score
    FROM dwd_fina_snapshot fs
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        roe_ttm = VALUES(roe_ttm),
        grossprofit_margin = VALUES(grossprofit_margin),
        debt_to_assets = VALUES(debt_to_assets),
        asset_growth_rate = VALUES(asset_growth_rate),
        rwm_score = VALUES(rwm_score),
        cma_score = VALUES(cma_score),
        quality_score = VALUES(quality_score)
    """
    cursor.execute(sql, params if params else None)
    logger.info(f"RMW+CMA质量评分完成: {cursor.rowcount} rows")


def _run_fama_technical_score(cursor, trade_date: Optional[int] = None) -> None:
    """计算技术因子评分 (0-10分) - 精简版
    
    - MACD: 4分
    - KDJ: 3分
    - RSI: 3分
    """
    filter_sql = ""
    params = []
    if trade_date is not None:
        filter_sql = "WHERE sf.trade_date = %s"
        params.append(trade_date)
    
    sql = f"""
    INSERT INTO dws_fama_technical_score (
        trade_date, ts_code, macd, macd_dif, macd_dea, kdj_j, rsi_6, technical_score
    )
    SELECT
        sf.trade_date,
        sf.ts_code,
        sf.macd_qfq AS macd,
        sf.macd_dif_qfq AS macd_dif,
        sf.macd_dea_qfq AS macd_dea,
        sf.kdj_qfq AS kdj_j,
        sf.rsi_qfq_6 AS rsi_6,
        (
            -- MACD评分 (4分)
            CASE 
                WHEN sf.macd_dif_qfq > sf.macd_dea_qfq AND sf.macd_qfq > 0 THEN 4.0
                WHEN sf.macd_qfq > 0 THEN 2.0
                WHEN sf.macd_dif_qfq > sf.macd_dea_qfq THEN 1.0
                ELSE 0
            END +
            -- KDJ评分 (3分)
            CASE 
                WHEN sf.kdj_qfq < 20 THEN 3.0
                WHEN sf.kdj_qfq BETWEEN 40 AND 60 THEN 2.0
                WHEN sf.kdj_qfq > 80 THEN 0
                ELSE 1.0
            END +
            -- RSI评分 (3分)
            CASE 
                WHEN sf.rsi_qfq_6 < 30 THEN 3.0
                WHEN sf.rsi_qfq_6 BETWEEN 40 AND 60 THEN 2.0
                WHEN sf.rsi_qfq_6 > 70 THEN 0
                ELSE 1.0
            END
        ) AS technical_score
    FROM ods_stk_factor sf
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        macd = VALUES(macd),
        macd_dif = VALUES(macd_dif),
        macd_dea = VALUES(macd_dea),
        kdj_j = VALUES(kdj_j),
        rsi_6 = VALUES(rsi_6),
        technical_score = VALUES(technical_score)
    """
    cursor.execute(sql, params if params else None)
    logger.info(f"技术评分完成: {cursor.rowcount} rows")


def _run_fama_capital_score(cursor, trade_date: Optional[int] = None) -> None:
    """计算资金因子评分 (0-10分)"""
    filter_sql = ""
    params = []
    if trade_date is not None:
        filter_sql = "WHERE mf.trade_date = %s"
        params.append(trade_date)
    
    sql = f"""
    INSERT INTO dws_fama_capital_score (trade_date, ts_code, elg_net, lg_net, margin_net_pct, capital_score)
    SELECT
        mf.trade_date,
        mf.ts_code,
        (COALESCE(mf.buy_elg_amount, 0) - COALESCE(mf.sell_elg_amount, 0)) / 10000 AS elg_net,
        (COALESCE(mf.buy_lg_amount, 0) - COALESCE(mf.sell_lg_amount, 0)) / 10000 AS lg_net,
        CASE WHEN mg.rzye > 0 THEN (mg.rzmre - mg.rzche) / mg.rzye * 100 ELSE NULL END AS margin_net_pct,
        (
            CASE 
                WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 100000000 THEN 5.0
                WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 50000000 THEN 4.0
                WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 10000000 THEN 2.0
                WHEN (mf.buy_elg_amount - mf.sell_elg_amount) > 0 THEN 1.0
                ELSE 0
            END +
            CASE 
                WHEN (mf.buy_lg_amount - mf.sell_lg_amount) > 50000000 THEN 3.0
                WHEN (mf.buy_lg_amount - mf.sell_lg_amount) > 20000000 THEN 2.0
                WHEN (mf.buy_lg_amount - mf.sell_lg_amount) > 0 THEN 1.0
                ELSE 0
            END +
            CASE 
                WHEN mg.ts_code IS NULL THEN 1.0
                WHEN mg.rzye = 0 THEN 1.0
                WHEN (mg.rzmre - mg.rzche) / mg.rzye * 100 > 2.0 THEN 2.0
                WHEN (mg.rzmre - mg.rzche) / mg.rzye * 100 > 0.5 THEN 1.5
                WHEN (mg.rzmre - mg.rzche) / mg.rzye * 100 > 0 THEN 1.0
                ELSE 0.5
            END
        ) AS capital_score
    FROM ods_moneyflow mf
    LEFT JOIN ods_margin_detail mg ON mf.trade_date = mg.trade_date AND mf.ts_code = mg.ts_code
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        elg_net = VALUES(elg_net),
        lg_net = VALUES(lg_net),
        margin_net_pct = VALUES(margin_net_pct),
        capital_score = VALUES(capital_score)
    """
    cursor.execute(sql, params if params else None)
    logger.info(f"资金评分完成: {cursor.rowcount} rows")


def _run_fama_chip_score(cursor, trade_date: Optional[int] = None) -> None:
    """计算筹码因子评分 (0-8分)"""
    filter_sql = ""
    params = []
    if trade_date is not None:
        filter_sql = "WHERE c.trade_date = %s"
        params.append(trade_date)
    
    sql = f"""
    INSERT INTO dws_fama_chip_score (trade_date, ts_code, winner_rate, cost_deviation, chip_score)
    SELECT
        c.trade_date,
        c.ts_code,
        c.winner_rate,
        d.close / NULLIF(c.weight_avg, 0) AS cost_deviation,
        (
            -- 获利比例评分 (5分)
            CASE 
                WHEN c.winner_rate < 10 THEN 5.0
                WHEN c.winner_rate < 30 THEN 4.0
                WHEN c.winner_rate BETWEEN 40 AND 60 THEN 2.0
                WHEN c.winner_rate > 90 THEN 0.5
                ELSE 1.0
            END +
            -- 成本偏离评分 (3分)
            CASE 
                WHEN d.close / NULLIF(c.weight_avg, 0) > 1.1 THEN 3.0
                WHEN d.close / NULLIF(c.weight_avg, 0) > 1.05 THEN 2.0
                WHEN d.close / NULLIF(c.weight_avg, 0) > 1.0 THEN 1.0
                ELSE 0
            END
        ) AS chip_score
    FROM ods_cyq_perf c
    JOIN ods_daily d ON c.trade_date = d.trade_date AND c.ts_code = d.ts_code
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        winner_rate = VALUES(winner_rate),
        cost_deviation = VALUES(cost_deviation),
        chip_score = VALUES(chip_score)
    """
    cursor.execute(sql, params if params else None)
    logger.info(f"筹码评分完成: {cursor.rowcount} rows")
