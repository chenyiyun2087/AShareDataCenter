#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pandas as pd
import tushare as ts

from etl.base.runtime import (
    RateLimiter,
    get_env_config,
    get_mysql_connection,
    get_tushare_token,
    get_tushare_limit,
    list_trade_dates,
    to_records,
    upsert_rows,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch TuShare feature datasets into ODS raw tables.")
    parser.add_argument("--start-date", type=int, required=True)
    parser.add_argument("--end-date", type=int, required=True)
    parser.add_argument("--token", default=None)
    parser.add_argument("--rate-limit", type=int, default=None)
    parser.add_argument(
        "--cyq-rate-limit",
        type=int,
        default=180,
        help="Requests per minute for cyq_chips (default: 180).",
    )
    parser.add_argument(
        "--cyq-chunk-days",
        type=int,
        default=5,
        help="Chunk size in trading days for cyq_chips batch requests (default: 5).",
    )
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument(
        "--apis",
        default="margin,margin_detail,margin_target,moneyflow,moneyflow_ths,cyq_chips,cyq_perf,stk_factor",
        help="Comma-separated API list.",
    )
    return parser.parse_args()


API_COLUMNS: Dict[str, List[str]] = {
    "margin": [
        "trade_date",
        "exchange_id",
        "rzye",
        "rzmre",
        "rzche",
        "rqye",
        "rqmcl",
        "rzrqye",
        "rqyl",
    ],
    "margin_detail": [
        "trade_date",
        "ts_code",
        "rzye",
        "rqye",
        "rzmre",
        "rqyl",
        "rzche",
        "rqchl",
        "rqmcl",
        "rzrqye",
    ],
    "margin_target": ["ts_code", "mg_type", "is_new", "in_date", "out_date", "ann_date"],
    "moneyflow": [
        "trade_date",
        "ts_code",
        "buy_sm_vol",
        "buy_sm_amount",
        "sell_sm_vol",
        "sell_sm_amount",
        "buy_md_vol",
        "buy_md_amount",
        "sell_md_vol",
        "sell_md_amount",
        "buy_lg_vol",
        "buy_lg_amount",
        "sell_lg_vol",
        "sell_lg_amount",
        "buy_elg_vol",
        "buy_elg_amount",
        "sell_elg_vol",
        "sell_elg_amount",
        "net_mf_vol",
        "net_mf_amount",
    ],
    "moneyflow_ths": [
        "trade_date",
        "ts_code",
        "net_mf_amount",
        "net_mf_vol",
        "buy_sm_amount",
        "sell_sm_amount",
        "buy_md_amount",
        "sell_md_amount",
        "buy_lg_amount",
        "sell_lg_amount",
        "buy_elg_amount",
        "sell_elg_amount",
    ],
    "cyq_chips": ["trade_date", "ts_code", "price", "percent"],
    "cyq_perf": [
        "trade_date",
        "ts_code",
        "his_low",
        "his_high",
        "cost_5pct",
        "cost_15pct",
        "cost_50pct",
        "cost_85pct",
        "cost_95pct",
        "weight_avg",
        "winner_rate",
    ],
    "stk_factor": [
        "trade_date", "ts_code",
        # Basic
        "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount",
        "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm",
        "dv_ratio", "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv", "adj_factor",
        # Adjusted
        "open_hfq", "open_qfq", "high_hfq", "high_qfq", "low_hfq", "low_qfq",
        "close_hfq", "close_qfq", "pre_close_hfq", "pre_close_qfq",
        # Indicators
        "macd_bfq", "macd_hfq", "macd_qfq",
        "macd_dea_bfq", "macd_dea_hfq", "macd_dea_qfq",
        "macd_dif_bfq", "macd_dif_hfq", "macd_dif_qfq",
        "kdj_bfq", "kdj_hfq", "kdj_qfq",
        "kdj_d_bfq", "kdj_d_hfq", "kdj_d_qfq", 
        "kdj_k_bfq", "kdj_k_hfq", "kdj_k_qfq",
        "rsi_bfq_6", "rsi_hfq_6", "rsi_qfq_6",
        "rsi_bfq_12", "rsi_hfq_12", "rsi_qfq_12",
        "rsi_bfq_24", "rsi_hfq_24", "rsi_qfq_24",
        "boll_upper_bfq", "boll_upper_hfq", "boll_upper_qfq",
        "boll_mid_bfq", "boll_mid_hfq", "boll_mid_qfq",
        "boll_lower_bfq", "boll_lower_hfq", "boll_lower_qfq",
        "cci_bfq", "cci_hfq", "cci_qfq",
        "asi_bfq", "asi_hfq", "asi_qfq",
        "asit_bfq", "asit_hfq", "asit_qfq",
        "atr_bfq", "atr_hfq", "atr_qfq",
        "bbi_bfq", "bbi_hfq", "bbi_qfq",
        "bias1_bfq", "bias1_hfq", "bias1_qfq",
        "bias2_bfq", "bias2_hfq", "bias2_qfq",
        "bias3_bfq", "bias3_hfq", "bias3_qfq",
        "brar_ar_bfq", "brar_ar_hfq", "brar_ar_qfq",
        "brar_br_bfq", "brar_br_hfq", "brar_br_qfq",
        "cr_bfq", "cr_hfq", "cr_qfq",
        "dfma_dif_bfq", "dfma_dif_hfq", "dfma_dif_qfq",
        "dfma_difma_bfq", "dfma_difma_hfq", "dfma_difma_qfq",
        "dmi_adx_bfq", "dmi_adx_hfq", "dmi_adx_qfq",
        "dmi_adxr_bfq", "dmi_adxr_hfq", "dmi_adxr_qfq",
        "dmi_mdi_bfq", "dmi_mdi_hfq", "dmi_mdi_qfq",
        "dmi_pdi_bfq", "dmi_pdi_hfq", "dmi_pdi_qfq",
        "dpo_bfq", "dpo_hfq", "dpo_qfq",
        "madpo_bfq", "madpo_hfq", "madpo_qfq",
        "emv_bfq", "emv_hfq", "emv_qfq",
        "maemv_bfq", "maemv_hfq", "maemv_qfq",
        "ktn_down_bfq", "ktn_down_hfq", "ktn_down_qfq",
        "ktn_mid_bfq", "ktn_mid_hfq", "ktn_mid_qfq",
        "ktn_upper_bfq", "ktn_upper_hfq", "ktn_upper_qfq",
        "mass_bfq", "mass_hfq", "mass_qfq",
        "ma_mass_bfq", "ma_mass_hfq", "ma_mass_qfq",
        "mfi_bfq", "mfi_hfq", "mfi_qfq",
        "mtm_bfq", "mtm_hfq", "mtm_qfq",
        "mtmma_bfq", "mtmma_hfq", "mtmma_qfq",
        "obv_bfq", "obv_hfq", "obv_qfq",
        "psy_bfq", "psy_hfq", "psy_qfq",
        "psyma_bfq", "psyma_hfq", "psyma_qfq",
        "roc_bfq", "roc_hfq", "roc_qfq",
        "maroc_bfq", "maroc_hfq", "maroc_qfq",
        "taq_down_bfq", "taq_down_hfq", "taq_down_qfq",
        "taq_mid_bfq", "taq_mid_hfq", "taq_mid_qfq",
        "taq_up_bfq", "taq_up_hfq", "taq_up_qfq",
        "trix_bfq", "trix_hfq", "trix_qfq",
        "trma_bfq", "trma_hfq", "trma_qfq",
        "vr_bfq", "vr_hfq", "vr_qfq",
        "wr_bfq", "wr_hfq", "wr_qfq",
        "wr1_bfq", "wr1_hfq", "wr1_qfq",
        "xsii_td1_bfq", "xsii_td1_hfq", "xsii_td1_qfq",
        "xsii_td2_bfq", "xsii_td2_hfq", "xsii_td2_qfq",
        "xsii_td3_bfq", "xsii_td3_hfq", "xsii_td3_qfq",
        "xsii_td4_bfq", "xsii_td4_hfq", "xsii_td4_qfq",
         "ma_bfq_5", "ma_bfq_10", "ma_bfq_20", "ma_bfq_60", "ma_bfq_250",
         "ma_qfq_5", "ma_qfq_10", "ma_qfq_20", "ma_qfq_60", "ma_qfq_250",
         "ma_hfq_5", "ma_hfq_10", "ma_hfq_20", "ma_hfq_60", "ma_hfq_250",
         "updays", "downdays", "lowdays", "topdays"
    ],
}


def _fetch_margin(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.margin(trade_date=str(trade_date))


def _fetch_margin_detail(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.margin_detail(trade_date=str(trade_date))


def _fetch_margin_target(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.margin_target(trade_date=str(trade_date))


def _fetch_moneyflow(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.moneyflow(trade_date=str(trade_date))


def _fetch_moneyflow_ths(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.moneyflow_ths(trade_date=str(trade_date))


def _fetch_cyq_chips(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    if not ts_code:
        raise RuntimeError("cyq_chips requires ts_code")
    return pro.cyq_chips(trade_date=str(trade_date), ts_code=ts_code)


def _fetch_cyq_perf(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.cyq_perf(trade_date=str(trade_date))


def _fetch_stk_factor(
    pro: ts.pro_api, limiter: RateLimiter, trade_date: int, ts_code: Optional[str] = None
) -> pd.DataFrame:
    limiter.wait()
    return pro.stk_factor_pro(trade_date=str(trade_date))


def _table_for_api(api_name: str) -> str:
    table_map = {
        "margin": "ods_margin",
        "margin_detail": "ods_margin_detail",
        "margin_target": "ods_margin_target",
        "moneyflow": "ods_moneyflow",
        "moneyflow_ths": "ods_moneyflow_ths",
        "cyq_chips": "ods_cyq_chips",
        "cyq_perf": "ods_cyq_perf",
        "stk_factor": "ods_stk_factor",
    }
    return table_map[api_name]


def _prepare_rows(columns: List[str], df: pd.DataFrame, trade_date: int):
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]
    if "trade_date" in columns and "trade_date" not in df.columns:
        df["trade_date"] = trade_date
    if "ts_code" in columns and "ts_code" not in df.columns:
        df["ts_code"] = ""
    if "exchange_id" in columns and "exchange_id" not in df.columns:
        df["exchange_id"] = ""
    df = df.reindex(columns=columns)
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None, float("nan"): None, "nan": None, "NaN": None})
    return to_records(df, columns), columns


def _list_ts_codes(cursor) -> List[str]:
    cursor.execute("SELECT ts_code FROM dim_stock ORDER BY ts_code")
    return [row[0] for row in cursor.fetchall()]


def _existing_tables(cursor, schema: str, tables: List[str]) -> set[str]:
    if not tables:
        return set()
    placeholders = ",".join(["%s"] * len(tables))
    sql = (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema=%s AND table_name IN ({placeholders})"
    )
    cursor.execute(sql, [schema, *tables])
    return {row[0] for row in cursor.fetchall()}


def _table_columns(cursor, schema: str, table: str) -> List[str]:
    cursor.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
        [schema, table],
    )
    return [row[0] for row in cursor.fetchall()]


def _chunk_dates(dates: List[int], chunk_size: int) -> List[List[int]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [dates[i : i + chunk_size] for i in range(0, len(dates), chunk_size)]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()
    logging.info(f"Starting ODS Features ETL with args: {args}")
    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            config_path = (Path.cwd() / config_path).resolve()
        if not config_path.exists():
            raise RuntimeError(f"config file not found: {config_path}")
        os.environ["ETL_CONFIG_PATH"] = str(config_path)
    token = args.token or get_tushare_token()
    if not token:
        raise RuntimeError("missing TuShare token: use --token or TUSHARE_TOKEN")
    apis = [api.strip() for api in args.apis.split(",") if api.strip()]
    api_map: dict[str, Callable[[ts.pro_api, RateLimiter, int, Optional[str]], pd.DataFrame]] = {
        "margin": _fetch_margin,
        "margin_detail": _fetch_margin_detail,
        "margin_target": _fetch_margin_target,
        "moneyflow": _fetch_moneyflow,
        "moneyflow_ths": _fetch_moneyflow_ths,
        "cyq_chips": _fetch_cyq_chips,
        "cyq_perf": _fetch_cyq_perf,
        "stk_factor": _fetch_stk_factor,
    }
    apis_require_ts_code = {"cyq_chips"}
    cfg = get_env_config()
    rate_limit = args.rate_limit or get_tushare_limit()
    limiter = RateLimiter(rate_limit)
    limiter_map = {
        "cyq_chips": RateLimiter(args.cyq_rate_limit),
        "stk_factor": RateLimiter(args.stk_factor_rate_limit),
    }
    pro = ts.pro_api(token)

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            table_map = {api: _table_for_api(api) for api in apis if api in api_map}
            existing = _existing_tables(cursor, cfg.database, list(table_map.values()))
            missing_tables = {table for table in table_map.values() if table not in existing}
            if missing_tables:
                print(
                    "Missing tables detected: "
                    f"{', '.join(sorted(missing_tables))}. "
                    "Run sql/ddl.sql to create them."
                )
            apis = [api for api in apis if table_map.get(api) in existing]
            table_columns = {
                table: _table_columns(cursor, cfg.database, table) for table in existing
            }
            trade_dates = list_trade_dates(cursor, args.start_date)
            ts_codes = _list_ts_codes(cursor) if apis_require_ts_code.intersection(apis) else []
        trade_dates = [d for d in trade_dates if d <= args.end_date]
        if apis_require_ts_code.intersection(apis) and not ts_codes:
            print("No ts_code found in dim_stock; skipping APIs that require ts_code.")

        total_dates = len(trade_dates)
        total_apis = len(apis)
        total_ts_codes = len(ts_codes) if apis_require_ts_code.intersection(apis) else 0
        cyq_chunks = _chunk_dates(trade_dates, args.cyq_chunk_days)
        for api_index, api_name in enumerate(apis, start=1):
            fetcher = api_map.get(api_name)
            if not fetcher:
                continue
            if api_name == "cyq_chips":
                print(f"Progress: api {api_index}/{total_apis} ({api_name})")
                table = _table_for_api(api_name)
                api_columns = [
                    col for col in API_COLUMNS[api_name] if col in table_columns.get(table, [])
                ]
                if not api_columns:
                    print(f"Warning: no matching columns for {api_name} ({table}); skipping.")
                    continue
                missing_columns = [col for col in API_COLUMNS[api_name] if col not in api_columns]
                if missing_columns:
                    print(
                        "Warning: table columns missing for "
                        f"{api_name} ({table}): {', '.join(missing_columns)}"
                    )
                for ts_index, ts_code in enumerate(ts_codes, start=1):
                    if total_ts_codes:
                        print(
                            "Progress: ts_code "
                            f"{ts_index}/{total_ts_codes} ({ts_code}) for {api_name}"
                        )
                    for chunk_index, chunk in enumerate(cyq_chunks, start=1):
                        start_date = min(chunk)
                        end_date = max(chunk)
                        print(
                            "Progress: cyq_chips chunk "
                            f"{chunk_index}/{len(cyq_chunks)} {start_date}-{end_date}"
                        )
                        try:
                            api_limiter = limiter_map.get(api_name, limiter)
                            api_limiter.wait()
                            df = pro.cyq_chips(
                                ts_code=ts_code,
                                start_date=str(start_date),
                                end_date=str(end_date),
                            )
                        except Exception as exc:
                            print(
                                "Fetch failed for cyq_chips "
                                f"{ts_code} {start_date}-{end_date}: {exc}"
                            )
                            continue
                        if df.empty:
                            continue
                        rows, columns = _prepare_rows(api_columns, df, end_date)
                        if not rows:
                            continue
                        try:
                            with conn.cursor() as cursor:
                                upsert_rows(cursor, table, columns, rows)
                                conn.commit()
                        except Exception as exc:
                            conn.rollback()
                            print(
                                "Insert failed for cyq_chips "
                                f"{ts_code} {start_date}-{end_date}: {exc}"
                            )
                            continue
                print("Completed cyq_chips")
                continue
            for date_index, trade_date in enumerate(trade_dates, start=1):
                print(f"Progress: trade_date {date_index}/{total_dates} ({trade_date})")
                print(f"Progress: api {api_index}/{total_apis} ({api_name}) for {trade_date}")
                try:
                    df = fetcher(pro, limiter, trade_date)
                except Exception as exc:
                    print(f"Fetch failed for {api_name} on {trade_date}: {exc}")
                    continue
                table = _table_for_api(api_name)
                api_columns = [
                    col for col in API_COLUMNS[api_name] if col in table_columns.get(table, [])
                ]
                if not api_columns:
                    print(f"Warning: no matching columns for {api_name} ({table}); skipping.")
                    continue
                missing_columns = [col for col in API_COLUMNS[api_name] if col not in api_columns]
                if missing_columns:
                    print(
                        "Warning: table columns missing for "
                        f"{api_name} ({table}): {', '.join(missing_columns)}"
                    )
                rows, columns = _prepare_rows(api_columns, df, trade_date)
                if not rows:
                    continue
                try:
                    with conn.cursor() as cursor:
                        upsert_rows(cursor, table, columns, rows)
                        conn.commit()
                except Exception as exc:
                    conn.rollback()
                    print(f"Insert failed for {api_name} on {trade_date}: {exc}")
                    continue
            print(f"Completed feature APIs for api={api_name}")
    
    logging.info("ODS Features ETL completed successfully")


if __name__ == "__main__":
    main()
