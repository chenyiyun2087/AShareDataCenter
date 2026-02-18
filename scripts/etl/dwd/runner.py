from __future__ import annotations

import logging
from typing import Optional

from ..base.runtime import (
    ensure_watermark,
    get_env_config,
    get_mysql_session,
    get_watermark,
    list_trade_dates,
    list_trade_dates_after,
    log_run_end,
    log_run_start,
    update_watermark,
)

def _create_tmp_base_adj(cursor) -> None:
    """Create a temporary table for base adjustment factors to optimize standardization."""
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_base_adj")
    cursor.execute("""
        CREATE TEMPORARY TABLE tmp_base_adj AS
        SELECT af.ts_code, af.adj_factor AS base_adj
        FROM dwd_adj_factor af
        JOIN (SELECT MAX(trade_date) AS base_date FROM dwd_daily) lt
            ON af.trade_date = lt.base_date
    """)
    cursor.execute("ALTER TABLE tmp_base_adj ADD PRIMARY KEY (ts_code)")


def load_dwd_daily(cursor, trade_date: int) -> None:
    sql = (
        "INSERT INTO dwd_daily (trade_date, ts_code, open, high, low, close, pre_close, "
        "change_amount, pct_chg, vol, amount) "
        "SELECT trade_date, ts_code, open, high, low, close, pre_close, "
        "`change` AS change_amount, pct_chg, vol, amount "
        "FROM ods_daily WHERE trade_date = %s "
        "ON DUPLICATE KEY UPDATE "
        "open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), "
        "pre_close=VALUES(pre_close), change_amount=VALUES(change_amount), "
        "pct_chg=VALUES(pct_chg), vol=VALUES(vol), amount=VALUES(amount)"
    )
    cursor.execute(sql, (trade_date,))


def load_dwd_daily_basic(cursor, trade_date: int) -> None:
    max_decimal = 999999.999999
    sql = (
        "INSERT INTO dwd_daily_basic (trade_date, ts_code, close, turnover_rate, turnover_rate_f, "
        "volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, "
        "float_share, free_share, total_mv, circ_mv) "
        "SELECT trade_date, ts_code, close, turnover_rate, turnover_rate_f, volume_ratio, "
        "CASE WHEN pe IS NULL OR pe BETWEEN -%s AND %s THEN pe ELSE NULL END AS pe, "
        "CASE WHEN pe_ttm IS NULL OR pe_ttm BETWEEN -%s AND %s THEN pe_ttm ELSE NULL END AS pe_ttm, "
        "CASE WHEN pb IS NULL OR pb BETWEEN -%s AND %s THEN pb ELSE NULL END AS pb, "
        "CASE WHEN ps IS NULL OR ps BETWEEN -%s AND %s THEN ps ELSE NULL END AS ps, "
        "CASE WHEN ps_ttm IS NULL OR ps_ttm BETWEEN -%s AND %s THEN ps_ttm ELSE NULL END AS ps_ttm, "
        "dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv "
        "FROM ods_daily_basic WHERE trade_date = %s "
        "ON DUPLICATE KEY UPDATE "
        "close=VALUES(close), turnover_rate=VALUES(turnover_rate), turnover_rate_f=VALUES(turnover_rate_f), "
        "volume_ratio=VALUES(volume_ratio), pe=VALUES(pe), pe_ttm=VALUES(pe_ttm), pb=VALUES(pb), "
        "ps=VALUES(ps), ps_ttm=VALUES(ps_ttm), dv_ratio=VALUES(dv_ratio), dv_ttm=VALUES(dv_ttm), "
        "total_share=VALUES(total_share), float_share=VALUES(float_share), free_share=VALUES(free_share), "
        "total_mv=VALUES(total_mv), circ_mv=VALUES(circ_mv)"
    )
    cursor.execute(
        sql,
        (
            max_decimal,
            max_decimal,
            max_decimal,
            max_decimal,
            max_decimal,
            max_decimal,
            max_decimal,
            max_decimal,
            max_decimal,
            max_decimal,
            trade_date,
        ),
    )


def load_dwd_adj_factor(cursor, trade_date: int) -> None:
    sql = (
        "INSERT INTO dwd_adj_factor (trade_date, ts_code, adj_factor) "
        "SELECT trade_date, ts_code, adj_factor FROM ods_adj_factor WHERE trade_date = %s "
        "ON DUPLICATE KEY UPDATE adj_factor=VALUES(adj_factor)"
    )
    cursor.execute(sql, (trade_date,))


def load_dwd_fina_indicator(cursor, start_date: int, end_date: int) -> None:
    sql = (
        "INSERT INTO dwd_fina_indicator (ts_code, ann_date, end_date, report_type, roe, "
        "grossprofit_margin, debt_to_assets, netprofit_margin, op_income, total_assets, total_hldr_eqy) "
        "SELECT ts_code, ann_date, end_date, report_type, roe, grossprofit_margin, debt_to_assets, "
        "netprofit_margin, op_income, total_assets, total_hldr_eqy "
        "FROM ods_fina_indicator WHERE ann_date BETWEEN %s AND %s "
        "ON DUPLICATE KEY UPDATE "
        "report_type=VALUES(report_type), roe=VALUES(roe), grossprofit_margin=VALUES(grossprofit_margin), "
        "debt_to_assets=VALUES(debt_to_assets), netprofit_margin=VALUES(netprofit_margin), "
        "op_income=VALUES(op_income), total_assets=VALUES(total_assets), total_hldr_eqy=VALUES(total_hldr_eqy)"
    )
    cursor.execute(sql, (start_date, end_date))


def load_dwd_stock_daily_standard(cursor, trade_date: int) -> None:
    """Load standardized price data with front-adjusted prices."""
    # Optimization: Use tmp_base_adj created in _create_tmp_base_adj
    sql = """
    INSERT INTO dwd_stock_daily_standard (
        trade_date, ts_code, adj_open, adj_high, adj_low, adj_close,
        turnover_rate_f, vol, amount
    )
    SELECT
        d.trade_date,
        d.ts_code,
        ROUND(d.open * b.base_adj / a.adj_factor, 4) AS adj_open,
        ROUND(d.high * b.base_adj / a.adj_factor, 4) AS adj_high,
        ROUND(d.low * b.base_adj / a.adj_factor, 4) AS adj_low,
        ROUND(d.close * b.base_adj / a.adj_factor, 4) AS adj_close,
        db.turnover_rate_f,
        d.vol,
        d.amount
    FROM dwd_daily d
    JOIN dwd_adj_factor a ON a.trade_date = d.trade_date AND a.ts_code = d.ts_code
    JOIN tmp_base_adj b ON b.ts_code = d.ts_code
    LEFT JOIN dwd_daily_basic db ON db.trade_date = d.trade_date AND db.ts_code = d.ts_code
    WHERE d.trade_date = %s
    ON DUPLICATE KEY UPDATE
        adj_open = VALUES(adj_open),
        adj_high = VALUES(adj_high),
        adj_low = VALUES(adj_low),
        adj_close = VALUES(adj_close),
        turnover_rate_f = VALUES(turnover_rate_f),
        vol = VALUES(vol),
        amount = VALUES(amount)
    """
    cursor.execute(sql, (trade_date,))


def load_dwd_fina_snapshot(cursor, trade_date: int) -> None:
    """Load daily financial snapshot from PIT data."""
    sql = """
    INSERT INTO dwd_fina_snapshot (
        trade_date, ts_code, roe_ttm, netprofit_margin, grossprofit_margin, debt_to_assets
    )
    SELECT
        %s AS trade_date,
        f.ts_code,
        f.roe AS roe_ttm,
        f.netprofit_margin,
        f.grossprofit_margin,
        f.debt_to_assets
    FROM dws_fina_pit_daily f
    WHERE f.trade_date = %s
    ON DUPLICATE KEY UPDATE
        roe_ttm = VALUES(roe_ttm),
        netprofit_margin = VALUES(netprofit_margin),
        grossprofit_margin = VALUES(grossprofit_margin),
        debt_to_assets = VALUES(debt_to_assets)
    """
    cursor.execute(sql, (trade_date, trade_date))


def load_dwd_margin_sentiment(cursor, trade_date: int, prev_trade_date: Optional[int]) -> None:
    """Load margin trading sentiment indicators."""
    if not prev_trade_date:
        return
    
    sql = """
    INSERT INTO dwd_margin_sentiment (
        trade_date, ts_code, rz_net_buy, rz_net_buy_ratio, rz_change_rate, rq_pressure
    )
    SELECT
        m.trade_date,
        m.ts_code,
        (m.rzmre - m.rzche) AS rz_net_buy,
        CASE WHEN d.amount > 0 THEN (m.rzmre - m.rzche) / (d.amount * 1000) ELSE NULL END AS rz_net_buy_ratio,
        CASE WHEN lag_m.rzye > 0 THEN (m.rzye - lag_m.rzye) / lag_m.rzye ELSE NULL END AS rz_change_rate,
        CASE WHEN d.vol > 0 THEN m.rqyl / (d.vol * 100) ELSE NULL END AS rq_pressure
    FROM ods_margin_detail m
    JOIN dwd_daily d ON d.trade_date = m.trade_date AND d.ts_code = m.ts_code
    LEFT JOIN ods_margin_detail lag_m ON lag_m.trade_date = %s AND lag_m.ts_code = m.ts_code
    WHERE m.trade_date = %s
    ON DUPLICATE KEY UPDATE
        rz_net_buy = VALUES(rz_net_buy),
        rz_net_buy_ratio = VALUES(rz_net_buy_ratio),
        rz_change_rate = VALUES(rz_change_rate),
        rq_pressure = VALUES(rq_pressure)
    """
    cursor.execute(sql, (prev_trade_date, trade_date))


def load_dwd_chip_stability(cursor, trade_date: int) -> None:
    """Load chip stability indicators from CYQ data."""
    sql = """
    INSERT INTO dwd_chip_stability (
        trade_date, ts_code, avg_cost, winner_rate, chip_concentration, cost_deviation
    )
    SELECT
        c.trade_date,
        c.ts_code,
        c.weight_avg AS avg_cost,
        c.winner_rate,
        CASE WHEN c.weight_avg > 0 THEN (c.cost_95pct - c.cost_5pct) / c.weight_avg ELSE NULL END AS chip_concentration,
        CASE WHEN c.weight_avg > 0 THEN (d.close - c.weight_avg) / c.weight_avg ELSE NULL END AS cost_deviation
    FROM ods_cyq_perf c
    JOIN dwd_daily d ON d.trade_date = c.trade_date AND d.ts_code = c.ts_code
    WHERE c.trade_date = %s
    ON DUPLICATE KEY UPDATE
        avg_cost = VALUES(avg_cost),
        winner_rate = VALUES(winner_rate),
        chip_concentration = VALUES(chip_concentration),
        cost_deviation = VALUES(cost_deviation)
    """
    cursor.execute(sql, (trade_date,))


def load_dwd_stock_label_daily(cursor, trade_date: int) -> None:
    """Load daily stock labels: ST, new stock, limit type, market."""
    sql = """
    INSERT INTO dwd_stock_label_daily (
        trade_date, ts_code, name, is_st, is_new, limit_type, market, industry
    )
    SELECT
        %s AS trade_date,
        s.ts_code,
        s.name,
        -- ST判断: 名称包含ST或*ST
        CASE WHEN s.name LIKE '%%ST%%' OR s.name LIKE '%%*ST%%' THEN 1 ELSE 0 END AS is_st,
        -- 次新股: 上市不足60个交易日
        CASE WHEN s.list_date IS NOT NULL AND DATEDIFF(
            STR_TO_DATE(CAST(%s AS CHAR), '%%Y%%m%%d'),
            STR_TO_DATE(CAST(s.list_date AS CHAR), '%%Y%%m%%d')
        ) < 60 THEN 1 ELSE 0 END AS is_new,
        -- 涨跌幅限制类型
        CASE 
            WHEN s.ts_code LIKE '68%%' THEN 20    -- 科创板 20%%
            WHEN s.ts_code LIKE '30%%' THEN 20    -- 创业板 20%%
            WHEN s.ts_code LIKE '8%%' OR s.ts_code LIKE '4%%' THEN 30  -- 北交所 30%%
            ELSE 10  -- 主板 10%%
        END AS limit_type,
        -- 市场类型
        CASE 
            WHEN s.ts_code LIKE '68%%' THEN '科创板'
            WHEN s.ts_code LIKE '30%%' THEN '创业板'
            WHEN s.ts_code LIKE '8%%' OR s.ts_code LIKE '4%%' THEN '北交所'
            WHEN s.ts_code LIKE '60%%' THEN '沪主板'
            WHEN s.ts_code LIKE '00%%' THEN '深主板'
            ELSE '其他'
        END AS market,
        s.industry
    FROM dim_stock s
    WHERE s.ts_code IN (SELECT DISTINCT ts_code FROM dwd_daily WHERE trade_date = %s)
    ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        is_st = VALUES(is_st),
        is_new = VALUES(is_new),
        limit_type = VALUES(limit_type),
        market = VALUES(market),
        industry = VALUES(industry)
    """
    cursor.execute(sql, (trade_date, trade_date, trade_date))


def run_full(start_date: int, end_date: Optional[int] = None) -> None:
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "dwd", "full")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                trade_dates = list_trade_dates(cursor, start_date, end_date)

            total_dates = len(trade_dates)
            logging.info(f"Processing {total_dates} trade dates from {start_date}")
            
            with conn.cursor() as cursor:
                _create_tmp_base_adj(cursor)
                conn.commit()

            for idx, trade_date in enumerate(trade_dates, 1):
                if idx == 1 or idx % 50 == 0 or idx == total_dates:
                    logging.info(f"[{idx}/{total_dates}] Processing trade_date={trade_date}")
                with conn.cursor() as cursor:
                    prev_trade_date = trade_dates[idx-2] if idx > 1 else None
                    load_dwd_daily(cursor, trade_date)
                    load_dwd_daily_basic(cursor, trade_date)
                    load_dwd_adj_factor(cursor, trade_date)
                    # New DWD tables
                    load_dwd_stock_daily_standard(cursor, trade_date)
                    load_dwd_fina_snapshot(cursor, trade_date)
                    load_dwd_margin_sentiment(cursor, trade_date, prev_trade_date)
                    load_dwd_chip_stability(cursor, trade_date)
                    load_dwd_stock_label_daily(cursor, trade_date)
                    conn.commit()
            
            logging.info("All DWD tables updated successfully")

            with conn.cursor() as cursor:
                ensure_watermark(cursor, "dwd_daily", start_date - 1)
                ensure_watermark(cursor, "dwd_daily_basic", start_date - 1)
                ensure_watermark(cursor, "dwd_adj_factor", start_date - 1)
                conn.commit()

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise


def run_incremental(start_date: Optional[int] = None, end_date: Optional[int] = None) -> None:
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "dwd", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                if start_date:
                    last_date = start_date - 1
                else:
                    last_date = get_watermark(cursor, "dwd_daily")

                if last_date is None:
                    cursor.execute("SELECT MAX(trade_date) FROM dwd_daily")
                    row = cursor.fetchone()
                    if row and row[0]:
                        last_date = int(row[0])
                    else:
                        cursor.execute("SELECT MIN(trade_date) FROM ods_daily")
                        row = cursor.fetchone()
                        if not row or row[0] is None:
                            raise RuntimeError("missing watermark for dwd_daily and ods_daily is empty")
                        min_trade_date = int(row[0])
                        cursor.execute(
                            "SELECT pretrade_date FROM dim_trade_cal WHERE exchange='SSE' AND cal_date=%s",
                            (min_trade_date,),
                        )
                        row = cursor.fetchone()
                        last_date = int(row[0]) if row and row[0] else min_trade_date - 1
                    ensure_watermark(cursor, "dwd_daily", last_date)
                    ensure_watermark(cursor, "dwd_daily_basic", last_date)
                    ensure_watermark(cursor, "dwd_adj_factor", last_date)
                    conn.commit()
                
                if start_date:
                    trade_dates = list_trade_dates(cursor, start_date, end_date)
                else:
                    trade_dates = list_trade_dates_after(cursor, last_date, end_date)
                
                # Cap dates at today to avoid processing future calendar dates
                from datetime import datetime
                today_int = int(datetime.now().strftime('%Y%m%d'))
                trade_dates = [d for d in trade_dates if d <= today_int]

            with conn.cursor() as cursor:
                _create_tmp_base_adj(cursor)
                conn.commit()

            for trade_date in trade_dates:
                logging.info(f"Processing trade_date={trade_date}")
                with conn.cursor() as cursor:
                    try:
                        # Get previous trade date
                        cursor.execute("SELECT MAX(trade_date) FROM dwd_daily WHERE trade_date < %s", (trade_date,))
                        res = cursor.fetchone()
                        prev_trade_date = res[0] if res else None
                        
                        load_dwd_daily(cursor, trade_date)
                        load_dwd_daily_basic(cursor, trade_date)
                        load_dwd_adj_factor(cursor, trade_date)
                        # New DWD tables
                        load_dwd_stock_daily_standard(cursor, trade_date)
                        load_dwd_fina_snapshot(cursor, trade_date)
                        load_dwd_margin_sentiment(cursor, trade_date, prev_trade_date)
                        load_dwd_chip_stability(cursor, trade_date)
                        load_dwd_stock_label_daily(cursor, trade_date)
                        update_watermark(cursor, "dwd_daily", trade_date, "SUCCESS")
                        update_watermark(cursor, "dwd_daily_basic", trade_date, "SUCCESS")
                        update_watermark(cursor, "dwd_adj_factor", trade_date, "SUCCESS")
                        conn.commit()
                        logging.info(f"  Completed trade_date={trade_date}")
                    except Exception as exc:
                        update_watermark(cursor, "dwd_daily", last_date, "FAILED", str(exc))
                        update_watermark(cursor, "dwd_daily_basic", last_date, "FAILED", str(exc))
                        update_watermark(cursor, "dwd_adj_factor", last_date, "FAILED", str(exc))
                        conn.rollback()
                        raise

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise


def run_fina_incremental(start_date: int, end_date: int) -> None:
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "dwd_fina_indicator", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                load_dwd_fina_indicator(cursor, start_date, end_date)
                update_watermark(cursor, "dwd_fina_indicator", end_date, "SUCCESS")
                conn.commit()

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise
