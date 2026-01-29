from __future__ import annotations

from ..base.runtime import (
    ensure_watermark,
    get_env_config,
    get_mysql_connection,
    get_watermark,
    list_trade_dates_after,
    log_run_end,
    log_run_start,
    update_watermark,
)


def _run_features(cursor, trade_date: int | None = None) -> None:
    filter_sql = ""
    params = []
    if trade_date is not None:
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
    cursor.execute(sql, params)


def _run_universe(cursor, trade_date: int | None = None) -> None:
    filter_sql = ""
    params = []
    if trade_date is not None:
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
    cursor.execute(sql, params)


def run_full(start_date: int) -> None:
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ads", "full")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                _run_features(cursor)
                _run_universe(cursor)
                conn.commit()

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


def run_incremental() -> None:
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "ads", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                last_date = get_watermark(cursor, "ads")
                if last_date is None:
                    raise RuntimeError("missing watermark for ads")
                trade_dates = list_trade_dates_after(cursor, last_date)

            for trade_date in trade_dates:
                with conn.cursor() as cursor:
                    try:
                        _run_features(cursor, trade_date)
                        _run_universe(cursor, trade_date)
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
