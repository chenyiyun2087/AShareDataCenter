from __future__ import annotations

import logging
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


def _run_features(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "WHERE d.trade_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "WHERE d.trade_date = %s"
        params.append(trade_date)
    sql = f"""
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
    {filter_sql}
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
    """
    cursor.execute(sql, params if params else None)


def _run_universe(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "WHERE d.trade_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "WHERE d.trade_date = %s"
        params.append(trade_date)
    sql = f"""
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
    {filter_sql}
    ON DUPLICATE KEY UPDATE
      is_tradable = VALUES(is_tradable),
      is_listed = VALUES(is_listed),
      is_suspended = VALUES(is_suspended),
      no_amount = VALUES(no_amount),
      filter_flags = VALUES(filter_flags),
      updated_at = CURRENT_TIMESTAMP;
    """
    cursor.execute(sql, params if params else None)


def _run_stock_score(cursor, trade_date: int | None = None, end_date: int | None = None) -> None:
    """Calculate comprehensive stock scores with Z-score normalization and dynamic weights."""
    filter_sql = ""
    params = []
    if trade_date is not None and end_date is not None:
        filter_sql = "WHERE tp.trade_date BETWEEN %s AND %s"
        params = [trade_date, end_date]
    elif trade_date is not None:
        filter_sql = "WHERE tp.trade_date = %s"
        params.append(trade_date)
    
    # Use percentile rank for normalization (maps to 0-100 range)
    sql = f"""
    INSERT INTO ads_stock_score_daily (
        trade_date, ts_code, tech_score, capital_score, sentiment_score, chip_score, total_score, score_rank
    )
    WITH base_data AS (
        SELECT
            tp.trade_date,
            tp.ts_code,
            -- Technical indicators (higher is better for momentum)
            tp.hma_slope,
            tp.rsi_14,
            tp.boll_width,
            -- Capital flow (higher is better)
            cf.main_net_ratio,
            cf.vol_price_corr,
            -- Sentiment (complex - need to normalize)
            ls.rz_buy_intensity,
            ls.turnover_spike,
            -- Chip (lower concentration is better for retail)
            cd.profit_ratio,
            cd.chip_peak_cross
        FROM dws_tech_pattern tp
        LEFT JOIN dws_capital_flow cf ON cf.trade_date = tp.trade_date AND cf.ts_code = tp.ts_code
        LEFT JOIN dws_leverage_sentiment ls ON ls.trade_date = tp.trade_date AND ls.ts_code = tp.ts_code
        LEFT JOIN dws_chip_dynamics cd ON cd.trade_date = tp.trade_date AND cd.ts_code = tp.ts_code
        {filter_sql}
    ),
    ranked_data AS (
        SELECT
            trade_date,
            ts_code,
            -- Tech score: combine RSI (inverted for overbought), HMA slope, BOLL width
            (PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY hma_slope) * 50 +
             PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY CASE WHEN rsi_14 BETWEEN 30 AND 70 THEN 1 ELSE 0 END DESC, rsi_14 DESC) * 30 +
             PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY boll_width DESC) * 20
            ) AS tech_score,
            -- Capital score: combine main_net_ratio and vol_price_corr
            (PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY main_net_ratio) * 60 +
             PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY vol_price_corr) * 40
            ) AS capital_score,
            -- Sentiment score: moderate leverage is best
            (PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY rz_buy_intensity) * 50 +
             PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY CASE WHEN turnover_spike BETWEEN 0.8 AND 2 THEN 1 ELSE 0 END DESC) * 50
            ) AS sentiment_score,
            -- Chip score: profit_ratio in mid-range is best
            (PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY CASE WHEN profit_ratio BETWEEN 0.3 AND 0.7 THEN 1 ELSE 0 END DESC) * 60 +
             PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY chip_peak_cross DESC) * 40
            ) AS chip_score
        FROM base_data
    )
    SELECT
        trade_date,
        ts_code,
        ROUND(tech_score, 6) AS tech_score,
        ROUND(capital_score, 6) AS capital_score,
        ROUND(sentiment_score, 6) AS sentiment_score,
        ROUND(chip_score, 6) AS chip_score,
        -- Weighted total: tech 40, capital 25, sentiment 20, chip 15
        ROUND(tech_score * 0.4 + capital_score * 0.25 + sentiment_score * 0.2 + chip_score * 0.15, 6) AS total_score,
        RANK() OVER (PARTITION BY trade_date ORDER BY (tech_score * 0.4 + capital_score * 0.25 + sentiment_score * 0.2 + chip_score * 0.15) DESC) AS score_rank
    FROM ranked_data
    ON DUPLICATE KEY UPDATE
        tech_score = VALUES(tech_score),
        capital_score = VALUES(capital_score),
        sentiment_score = VALUES(sentiment_score),
        chip_score = VALUES(chip_score),
        total_score = VALUES(total_score),
        score_rank = VALUES(score_rank),
        updated_at = CURRENT_TIMESTAMP
    """
    cursor.execute(sql, params if params else None)


def _run_ads_batch(cursor, start_date: int, end_date: int) -> None:
    """Execute all ADS steps in batch mode for a date range."""
    logging.info(f"  [1/3] Running ads_features_stock_daily batch...")
    _run_features(cursor, start_date, end_date)
    logging.info(f"  [2/3] Running ads_universe_daily batch...")
    _run_universe(cursor, start_date, end_date)
    logging.info(f"  [3/3] Running ads_stock_score_daily batch...")
    _run_stock_score(cursor, start_date, end_date)


def run_full(start_date: int, end_date: int | None = None) -> None:
    """Run full ADS ETL."""
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ads", "full")
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
                _run_ads_batch(cursor, trade_dates[0], trade_dates[-1])
                conn.commit()
            
            logging.info("All ADS tables updated successfully")

            with conn.cursor() as cursor:
                ensure_watermark(cursor, "ads", start_date - 1)
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
            run_id = log_run_start(cursor, "ads", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                if start_date:
                    last_date = start_date - 1
                else:
                    last_date = get_watermark(cursor, "ads")

                if last_date is None:
                    raise RuntimeError("missing watermark for ads")
                
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

            # Batch Optimization: if more than 5 days, use batch mode
            if len(trade_dates) > 5:
                logging.info(f"Running batch ADS sync for {len(trade_dates)} days: {trade_dates[0]} to {trade_dates[-1]}")
                with conn.cursor() as cursor:
                    _run_ads_batch(cursor, trade_dates[0], trade_dates[-1])
                    update_watermark(cursor, "ads", trade_dates[-1], "SUCCESS")
                    conn.commit()
            else:
                for trade_date in trade_dates:
                    logging.info(f"Processing trade_date={trade_date}")
                    with conn.cursor() as cursor:
                        try:
                            _run_ads_batch(cursor, trade_date, trade_date)
                            update_watermark(cursor, "ads", trade_date, "SUCCESS")
                            conn.commit()
                        except Exception as exc:
                            update_watermark(cursor, "ads", last_date, "FAILED", str(exc))
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
