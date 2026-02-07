from __future__ import annotations

from typing import Optional

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


def run_full(start_date: int) -> None:
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "dwd", "full")
            conn.commit()
        try:
            with conn.cursor() as cursor:
                trade_dates = list_trade_dates(cursor, start_date)

            for trade_date in trade_dates:
                with conn.cursor() as cursor:
                    load_dwd_daily(cursor, trade_date)
                    load_dwd_daily_basic(cursor, trade_date)
                    load_dwd_adj_factor(cursor, trade_date)
                    conn.commit()

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


def run_incremental() -> None:
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            run_id = log_run_start(cursor, "dwd", "incremental")
            conn.commit()
        try:
            with conn.cursor() as cursor:
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
                trade_dates = list_trade_dates_after(cursor, last_date)

            for trade_date in trade_dates:
                with conn.cursor() as cursor:
                    try:
                        load_dwd_daily(cursor, trade_date)
                        load_dwd_daily_basic(cursor, trade_date)
                        load_dwd_adj_factor(cursor, trade_date)
                        update_watermark(cursor, "dwd_daily", trade_date, "SUCCESS")
                        update_watermark(cursor, "dwd_daily_basic", trade_date, "SUCCESS")
                        update_watermark(cursor, "dwd_adj_factor", trade_date, "SUCCESS")
                        conn.commit()
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
    with get_mysql_connection(cfg) as conn:
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
