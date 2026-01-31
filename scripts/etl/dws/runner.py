from __future__ import annotations

from ..base.runtime import (
    ensure_watermark,
    get_env_config,
    get_mysql_connection,
    get_watermark,
    list_trade_dates,
    list_trade_dates_after,
    log_run_end,
    log_run_start,
    update_watermark,
)


def _run_price_adj(cursor, trade_date: int | None = None) -> None:
    filter_sql = ""
    update_filter = ""
    params = []
    if trade_date is not None:
        filter_sql = "WHERE d.trade_date = %s"
        update_filter = "WHERE cur.trade_date = %s"
        params.append(trade_date)
    insert_sql = f"""
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
    JOIN (
      SELECT af.ts_code, af.adj_factor AS base_adj
      FROM dwd_adj_factor af
      JOIN (SELECT MAX(trade_date) AS base_date FROM dwd_daily) lt
        ON af.trade_date = lt.base_date
    ) b
      ON b.ts_code = d.ts_code
    {filter_sql}
    ON DUPLICATE KEY UPDATE
      qfq_close = VALUES(qfq_close),
      qfq_ret_1 = VALUES(qfq_ret_1),
      qfq_ret_5 = VALUES(qfq_ret_5),
      qfq_ret_20 = VALUES(qfq_ret_20),
      qfq_ret_60 = VALUES(qfq_ret_60),
      updated_at = CURRENT_TIMESTAMP;
    """
    cursor.execute(insert_sql, params)

    update_sql = f"""
    UPDATE dws_price_adj_daily cur
    JOIN (
      SELECT
        trade_date,
        ts_code,
        qfq_close,
        LAG(qfq_close, 1) OVER w AS prev_1,
        LAG(qfq_close, 5) OVER w AS prev_5,
        LAG(qfq_close, 20) OVER w AS prev_20,
        LAG(qfq_close, 60) OVER w AS prev_60
      FROM dws_price_adj_daily
      WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
    ) hist
      ON hist.trade_date = cur.trade_date AND hist.ts_code = cur.ts_code
    SET
      cur.qfq_ret_1 = (cur.qfq_close / hist.prev_1) - 1,
      cur.qfq_ret_5 = CASE WHEN hist.prev_5 IS NULL THEN NULL ELSE (cur.qfq_close / hist.prev_5) - 1 END,
      cur.qfq_ret_20 = CASE WHEN hist.prev_20 IS NULL THEN NULL ELSE (cur.qfq_close / hist.prev_20) - 1 END,
      cur.qfq_ret_60 = CASE WHEN hist.prev_60 IS NULL THEN NULL ELSE (cur.qfq_close / hist.prev_60) - 1 END,
      cur.updated_at = CURRENT_TIMESTAMP
    {update_filter};
    """
    cursor.execute(update_sql, params)


def _run_fina_pit(cursor, trade_date: int | None = None) -> None:
    filter_sql = ""
    params = []
    if trade_date is not None:
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
    cursor.execute(sql, params)


def run_full(start_date: int) -> None:
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "dws", "full")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                _run_price_adj(cursor)
                _run_fina_pit(cursor)
                conn.commit()

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


def run_incremental() -> None:
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "dws", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                last_date = get_watermark(cursor, "dws")
                if last_date is None:
                    raise RuntimeError("missing watermark for dws")
                trade_dates = list_trade_dates_after(cursor, last_date)

            for trade_date in trade_dates:
                with conn.cursor() as cursor:
                    try:
                        _run_price_adj(cursor, trade_date)
                        _run_fina_pit(cursor, trade_date)
                        update_watermark(cursor, "dws", trade_date, "SUCCESS")
                        conn.commit()
                    except Exception as exc:
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
