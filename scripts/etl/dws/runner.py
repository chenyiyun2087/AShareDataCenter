from __future__ import annotations

import logging
import os
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

from .scoring import (
    _run_momentum_score,
    _run_value_score,
    _run_quality_score,
    _run_technical_score,
    _run_capital_score,
    _run_chip_score,
)
from .enhanced_factors import (
    run_liquidity_factor,
    run_momentum_extended,
    run_momentum_extended_batch,
    run_quality_extended,
    run_risk_factor,
)


def _run_price_adj(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    filter_sql = ""
    update_filter = ""
    if trade_date is not None and end_date is not None:
        lookback_sql = "WHERE trade_date BETWEEN (SELECT cal_date FROM dim_trade_cal WHERE exchange='SSE' AND is_open=1 AND cal_date <= %s ORDER BY cal_date DESC LIMIT 1 OFFSET 70) AND %s"
        filter_sql = "WHERE d.trade_date BETWEEN %s AND %s"
        update_filter = "WHERE cur.trade_date BETWEEN %s AND %s"
        insert_params = [trade_date, end_date]
        update_params = [trade_date, end_date, trade_date, end_date]
    elif trade_date is not None:
        lookback_sql = "WHERE trade_date BETWEEN (SELECT cal_date FROM dim_trade_cal WHERE exchange='SSE' AND is_open=1 AND cal_date <= %s ORDER BY cal_date DESC LIMIT 1 OFFSET 70) AND %s"
        filter_sql = "WHERE d.trade_date = %s"
        update_filter = "WHERE cur.trade_date = %s"
        insert_params = [trade_date]
        update_params = [trade_date, trade_date, trade_date]
    else:
        lookback_sql = ""
        insert_params = []
        update_params = []

    insert_sql = f"""
    INSERT INTO dws_price_adj_daily (
      trade_date, ts_code, qfq_close, qfq_ret_1, qfq_ret_5, qfq_ret_20, qfq_ret_60
    )
    SELECT
      d.trade_date, d.ts_code,
      ROUND(d.close * b.base_adj / a.adj_factor, 4) AS qfq_close,
      NULL, NULL, NULL, NULL
    FROM dwd_daily d
    JOIN dwd_adj_factor a ON a.trade_date = d.trade_date AND a.ts_code = d.ts_code
    JOIN tmp_base_adj b ON b.ts_code = d.ts_code
    {filter_sql}
    ON DUPLICATE KEY UPDATE qfq_close = VALUES(qfq_close);
    """
    cursor.execute(insert_sql, insert_params)

    update_sql = f"""
    UPDATE dws_price_adj_daily cur
    JOIN (
      SELECT
        trade_date, ts_code, qfq_close,
        LAG(qfq_close, 1) OVER w AS prev_1,
        LAG(qfq_close, 5) OVER w AS prev_5,
        LAG(qfq_close, 20) OVER w AS prev_20,
        LAG(qfq_close, 60) OVER w AS prev_60
      FROM dws_price_adj_daily
      {lookback_sql}
      WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
    ) hist ON hist.trade_date = cur.trade_date AND hist.ts_code = cur.ts_code
    SET
      cur.qfq_ret_1 = (cur.qfq_close / hist.prev_1) - 1,
      cur.qfq_ret_5 = CASE WHEN hist.prev_5 IS NULL THEN NULL ELSE (cur.qfq_close / hist.prev_5) - 1 END,
      cur.qfq_ret_20 = CASE WHEN hist.prev_20 IS NULL THEN NULL ELSE (cur.qfq_close / hist.prev_20) - 1 END,
      cur.qfq_ret_60 = CASE WHEN hist.prev_60 IS NULL THEN NULL ELSE (cur.qfq_close / hist.prev_60) - 1 END,
      cur.updated_at = CURRENT_TIMESTAMP
    {update_filter};
    """
    cursor.execute(update_sql, update_params)


def _run_fina_pit(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "AND cal.cal_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "AND cal.cal_date = %s"
        params.append(trade_date)
    sql = f"""
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
      {filter_sql}
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
    """
    cursor.execute(sql, params if params else None)


def _run_tech_pattern(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """Calculate technical pattern indicators: HMA, RSI, Bollinger Bands."""
    if trade_date is not None and end_date is not None:
        lookback_date = "(SELECT cal_date FROM dim_trade_cal WHERE exchange='SSE' AND is_open=1 AND cal_date <= %s ORDER BY cal_date DESC LIMIT 1 OFFSET 30)"
        params = [trade_date, end_date, trade_date, end_date]
        filter_clause = "WHERE base.trade_date BETWEEN %s AND %s"
        inner_where = f"WHERE base.trade_date >= {lookback_date} AND base.trade_date <= %s"
    elif trade_date is not None:
        lookback_date = "(SELECT cal_date FROM dim_trade_cal WHERE exchange='SSE' AND is_open=1 AND cal_date <= %s ORDER BY cal_date DESC LIMIT 1 OFFSET 30)"
        params = [trade_date, trade_date, trade_date]
        filter_clause = "WHERE base.trade_date = %s"
        inner_where = f"WHERE base.trade_date >= {lookback_date} AND base.trade_date <= %s"
    else:
        params = []
        filter_clause = ""
        inner_where = ""
    
    sql = f"""
    INSERT INTO dws_tech_pattern (
        trade_date, ts_code, hma_5, hma_slope, rsi_14,
        boll_upper, boll_mid, boll_lower, boll_width
    )
    SELECT * FROM (
        SELECT
            base.trade_date,
            base.ts_code,
            base.adj_close AS hma_5,
            (base.adj_close - LAG(base.adj_close, 1) OVER w) / NULLIF(LAG(base.adj_close, 1) OVER w, 0) AS hma_slope,
            sf.rsi_bfq_12 AS rsi_14,
            sf.boll_upper_bfq AS boll_upper,
            sf.boll_mid_bfq AS boll_mid,
            sf.boll_lower_bfq AS boll_lower,
            CASE WHEN sf.boll_mid_bfq > 0 
                 THEN (sf.boll_upper_bfq - sf.boll_lower_bfq) / sf.boll_mid_bfq 
                 ELSE NULL END AS boll_width
        FROM dwd_stock_daily_standard base
        LEFT JOIN ods_stk_factor sf ON sf.trade_date = base.trade_date AND sf.ts_code = base.ts_code
        {inner_where}
        WINDOW w AS (PARTITION BY base.ts_code ORDER BY base.trade_date)
    ) base
    {filter_clause}
    ON DUPLICATE KEY UPDATE
        hma_5 = VALUES(hma_5), hma_slope = VALUES(hma_slope), rsi_14 = VALUES(rsi_14),
        boll_upper = VALUES(boll_upper), boll_mid = VALUES(boll_mid),
        boll_lower = VALUES(boll_lower), boll_width = VALUES(boll_width)
    """
    cursor.execute(sql, params)


def _run_capital_flow(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """Calculate capital flow indicators from moneyflow data."""
    if trade_date is not None and end_date is not None:
        lookback_date = "(SELECT cal_date FROM dim_trade_cal WHERE exchange='SSE' AND is_open=1 AND cal_date <= %s ORDER BY cal_date DESC LIMIT 1 OFFSET 10)"
        params = [trade_date, end_date, trade_date, end_date]
        filter_clause = "WHERE base.trade_date BETWEEN %s AND %s"
        inner_where = f"WHERE mf.trade_date >= {lookback_date} AND mf.trade_date <= %s"
    elif trade_date is not None:
        lookback_date = "(SELECT cal_date FROM dim_trade_cal WHERE exchange='SSE' AND is_open=1 AND cal_date <= %s ORDER BY cal_date DESC LIMIT 1 OFFSET 10)"
        params = [trade_date, trade_date, trade_date]
        filter_clause = "WHERE base.trade_date = %s"
        inner_where = f"WHERE mf.trade_date >= {lookback_date} AND mf.trade_date <= %s"
    else:
        params = []
        filter_clause = ""
        inner_where = ""

    sql = f"""
    INSERT INTO dws_capital_flow (
        trade_date, ts_code, main_net_inflow, main_net_ratio, main_net_ma5, vol_price_corr
    )
    SELECT * FROM (
        SELECT
            mf.trade_date,
            mf.ts_code,
            (mf.buy_lg_amount + mf.buy_elg_amount - mf.sell_lg_amount - mf.sell_elg_amount) AS main_net_inflow,
            CASE WHEN d.amount > 0 
                 THEN (mf.buy_lg_amount + mf.buy_elg_amount - mf.sell_lg_amount - mf.sell_elg_amount) / (d.amount / 10)
                 ELSE NULL END AS main_net_ratio,
            AVG(mf.buy_lg_amount + mf.buy_elg_amount - mf.sell_lg_amount - mf.sell_elg_amount) 
                OVER (PARTITION BY mf.ts_code ORDER BY mf.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS main_net_ma5,
            CASE WHEN d.pct_chg > 0 AND mf.net_mf_amount > 0 THEN 1
                 WHEN d.pct_chg < 0 AND mf.net_mf_amount < 0 THEN 1
                 ELSE -1 END AS vol_price_corr
        FROM ods_moneyflow mf
        JOIN dwd_daily d ON d.trade_date = mf.trade_date AND d.ts_code = mf.ts_code
        {inner_where}
    ) base
    {filter_clause}
    ON DUPLICATE KEY UPDATE
        main_net_inflow = VALUES(main_net_inflow), main_net_ratio = VALUES(main_net_ratio),
        main_net_ma5 = VALUES(main_net_ma5), vol_price_corr = VALUES(vol_price_corr)
    """
    cursor.execute(sql, params)

def _run_leverage_sentiment(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """Calculate leverage and sentiment indicators."""
    if trade_date is not None and end_date is not None:
        lookback_date = "(SELECT cal_date FROM dim_trade_cal WHERE exchange='SSE' AND is_open=1 AND cal_date <= %s ORDER BY cal_date DESC LIMIT 1 OFFSET 30)"
        params = [trade_date, end_date, trade_date, end_date]
        filter_clause = "WHERE base.trade_date BETWEEN %s AND %s"
        inner_where = f"WHERE ms.trade_date >= {lookback_date} AND ms.trade_date <= %s"
    elif trade_date is not None:
        lookback_date = "(SELECT cal_date FROM dim_trade_cal WHERE exchange='SSE' AND is_open=1 AND cal_date <= %s ORDER BY cal_date DESC LIMIT 1 OFFSET 30)"
        params = [trade_date, trade_date, trade_date]
        filter_clause = "WHERE base.trade_date = %s"
        inner_where = f"WHERE ms.trade_date >= {lookback_date} AND ms.trade_date <= %s"
    else:
        params = []
        filter_clause = ""
        inner_where = ""

    sql = f"""
    INSERT INTO dws_leverage_sentiment (
        trade_date, ts_code, rz_buy_intensity, rz_concentration, rq_pressure_factor, turnover_spike
    )
    SELECT * FROM (
        SELECT
            ms.trade_date,
            ms.ts_code,
            ms.rz_net_buy_ratio AS rz_buy_intensity,
            NULL AS rz_concentration,
            ms.rq_pressure AS rq_pressure_factor,
            CASE WHEN db.turnover_rate_f > 0 
                 THEN db.turnover_rate_f / NULLIF(
                     AVG(db.turnover_rate_f) OVER (PARTITION BY ms.ts_code ORDER BY ms.trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING), 0)
                 ELSE NULL END AS turnover_spike
        FROM dwd_margin_sentiment ms
        LEFT JOIN dwd_daily_basic db ON db.trade_date = ms.trade_date AND db.ts_code = ms.ts_code
        {inner_where}
    ) base
    {filter_clause}
    ON DUPLICATE KEY UPDATE
        rz_buy_intensity = VALUES(rz_buy_intensity), rz_concentration = VALUES(rz_concentration),
        rq_pressure_factor = VALUES(rq_pressure_factor), turnover_spike = VALUES(turnover_spike)
    """
    cursor.execute(sql, params)


def _run_chip_dynamics(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """Calculate chip distribution dynamics."""
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "WHERE cs.trade_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "WHERE cs.trade_date = %s"
        params.append(trade_date)
    
    sql = f"""
    INSERT INTO dws_chip_dynamics (
        trade_date, ts_code, profit_ratio, profit_pressure, support_strength, chip_peak_cross
    )
    SELECT
        cs.trade_date,
        cs.ts_code,
        cs.winner_rate AS profit_ratio,
        CASE WHEN cs.winner_rate > 0.9 THEN (cs.winner_rate - 0.9) * 10 
             ELSE 0 END AS profit_pressure,
        CASE WHEN cs.winner_rate < 0.1 THEN (0.1 - cs.winner_rate) * 10 
             ELSE 0 END AS support_strength,
        cs.chip_concentration AS chip_peak_cross
    FROM dwd_chip_stability cs
    {filter_sql}
    ON DUPLICATE KEY UPDATE
        profit_ratio = VALUES(profit_ratio),
        profit_pressure = VALUES(profit_pressure),
        support_strength = VALUES(support_strength),
        chip_peak_cross = VALUES(chip_peak_cross)
    """
    cursor.execute(sql, params if params else None)


def _run_dws_batch(cursor, start_date: int, end_date: int, use_batch_momentum: bool = True) -> None:
    """Execute all DWS steps in batch mode for a date range."""
    logging.info(f"  [1/16] Running dws_price_adj_daily batch...")
    _run_price_adj(cursor, start_date, end_date)
    logging.info(f"  [2/16] Running dws_fina_pit_daily batch...")
    _run_fina_pit(cursor, start_date, end_date)
    logging.info(f"  [3/16] Running dws_tech_pattern batch...")
    _run_tech_pattern(cursor, start_date, end_date)
    logging.info(f"  [4/16] Running dws_capital_flow batch...")
    _run_capital_flow(cursor, start_date, end_date)
    logging.info(f"  [5/16] Running dws_leverage_sentiment batch...")
    _run_leverage_sentiment(cursor, start_date, end_date)
    logging.info(f"  [6/16] Running dws_chip_dynamics batch...")
    _run_chip_dynamics(cursor, start_date, end_date)
    
    # Scoring tables
    logging.info(f"  [7/16] Running dws_momentum_score batch...")
    _run_momentum_score(cursor, start_date, end_date)
    logging.info(f"  [8/16] Running dws_value_score batch...")
    _run_value_score(cursor, start_date, end_date)
    logging.info(f"  [9/16] Running dws_quality_score batch...")
    _run_quality_score(cursor, start_date, end_date)
    logging.info(f"  [10/16] Running dws_technical_score batch...")
    _run_technical_score(cursor, start_date, end_date)
    logging.info(f"  [11/16] Running dws_capital_score batch...")
    _run_capital_score(cursor, start_date, end_date)
    logging.info(f"  [12/16] Running dws_chip_score batch...")
    _run_chip_score(cursor, start_date, end_date)
    
    # Enhanced factors
    logging.info(f"  [13/16] Running dws_liquidity_factor batch...")
    run_liquidity_factor(cursor, start_date, end_date)
    if use_batch_momentum:
        logging.info(f"  [14/16] Running dws_momentum_extended batch...")
        run_momentum_extended_batch(cursor, start_date, end_date)
    logging.info(f"  [15/16] Running dws_quality_extended batch...")
    run_quality_extended(cursor, start_date, end_date)
    logging.info(f"  [16/16] Running dws_risk_factor batch...")
    run_risk_factor(cursor, start_date, end_date)


def run_full(start_date: int, end_date: int | None = None) -> None:
    """Run full DWS ETL."""
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "dws", "full")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                trade_dates = list_trade_dates(cursor, start_date, end_date)
            
            if not trade_dates:
                logging.info("No trade dates to process.")
                return

            total_dates = len(trade_dates)
            logging.info(f"Processing {total_dates} trade dates from {start_date} in BATCH mode")
            
            with conn.cursor() as cursor:
                _create_tmp_base_adj(cursor)
                conn.commit()
            
            # For full refill, we treat the whole range as one batch
            with conn.cursor() as cursor:
                _run_dws_batch(cursor, trade_dates[0], trade_dates[-1])
                conn.commit()
            
            logging.info("All DWS tables updated successfully")

            with conn.cursor() as cursor:
                ensure_watermark(cursor, "dws", start_date - 1)
                conn.commit()

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise


def run_incremental(start_date: int | None = None, end_date: int | None = None) -> None:
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "dws", "incremental")
            conn.commit()
            
            # Switch to READ COMMITTED to avoid "Lock wait timeout" and "Lock table size" errors
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute("SET SESSION innodb_lock_wait_timeout = 300")
            cursor.execute("SET SESSION net_read_timeout = 300")
            cursor.execute("SET SESSION net_write_timeout = 300")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                if start_date:
                    last_date = start_date - 1
                else:
                    last_date = get_watermark(cursor, "dws")

                if last_date is None:
                    raise RuntimeError("missing watermark for dws")
                
                if start_date:
                    trade_dates = list_trade_dates(cursor, start_date, end_date)
                else:
                    trade_dates = list_trade_dates_after(cursor, last_date, end_date)
                
                # Cap dates at today to avoid processing future calendar dates
                from datetime import datetime
                today_int = int(datetime.now().strftime('%Y%m%d'))
                trade_dates = [d for d in trade_dates if d <= today_int]

            if not trade_dates:
                logging.info("No new trade dates to process.")
                return

            with conn.cursor() as cursor:
                _create_tmp_base_adj(cursor)
                conn.commit()

            # Batch Optimization: if more than 5 days, use batch mode
            if len(trade_dates) > 5:
                logging.info(f"Running batch DWS sync for {len(trade_dates)} days: {trade_dates[0]} to {trade_dates[-1]}")
                with conn.cursor() as cursor:
                    _run_dws_batch(cursor, trade_dates[0], trade_dates[-1])
                    if os.environ.get("DWS_DISABLE_WATERMARK", "0") == "0":
                        update_watermark(cursor, "dws", trade_dates[-1], "SUCCESS")
                    conn.commit()
            else:
                # Fallback to daily mode for small increments
                disable_watermark = os.environ.get("DWS_DISABLE_WATERMARK", "0") == "1"
                for trade_date in trade_dates:
                    logging.info(f"Processing trade_date={trade_date}")
                    with conn.cursor() as cursor:
                        try:
                            _run_dws_batch(cursor, trade_date, trade_date, use_batch_momentum=False)
                            run_momentum_extended(cursor, trade_date)
                            if not disable_watermark:
                                update_watermark(cursor, "dws", trade_date, "SUCCESS")
                            conn.commit()
                        except Exception as exc:
                            if not disable_watermark:
                                update_watermark(cursor, "dws", last_date, "FAILED", str(exc))
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
            run_id = log_run_start(cursor, "dws_fina_indicator", "incremental")
            conn.commit()
        try:
            from ..dwd.runner import load_dwd_fina_indicator
            with conn.cursor() as cursor:
                load_dwd_fina_indicator(cursor, start_date, end_date)
                update_watermark(cursor, "dws_fina_indicator", end_date, "SUCCESS")
                conn.commit()

            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "SUCCESS")
                conn.commit()
        except Exception as exc:
            with conn.cursor() as cursor:
                log_run_end(cursor, run_id, "FAILED", str(exc))
                conn.commit()
            raise
