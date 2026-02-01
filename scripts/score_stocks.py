#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from etl.base.runtime import get_env_config, get_mysql_connection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score stocks based on ADS features.")
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument("--trade-date", type=int, default=None)
    parser.add_argument("--ts-code", action="append", default=[], help="Single stock code.")
    parser.add_argument("--input-file", default=None, help="File with ts_code list (one per line).")
    parser.add_argument(
        "--fallback-latest",
        action="store_true",
        help="If no rows for trade-date, retry with latest available date from ADS/DWD/ODS.",
    )
    return parser.parse_args()


def _apply_config_args(args: argparse.Namespace) -> None:
    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            config_path = (Path.cwd() / config_path).resolve()
        if not config_path.exists():
            raise RuntimeError(f"config file not found: {config_path}")
        os.environ["ETL_CONFIG_PATH"] = str(config_path)
    if args.host:
        os.environ["MYSQL_HOST"] = args.host
    if args.port is not None:
        os.environ["MYSQL_PORT"] = str(args.port)
    if args.user:
        os.environ["MYSQL_USER"] = args.user
    if args.password:
        os.environ["MYSQL_PASSWORD"] = args.password
    if args.database:
        os.environ["MYSQL_DB"] = args.database


def _load_ts_codes(args: argparse.Namespace) -> List[str]:
    codes = list(args.ts_code or [])
    if args.input_file:
        file_path = Path(args.input_file).expanduser()
        if not file_path.is_absolute():
            file_path = (Path.cwd() / file_path).resolve()
        if not file_path.exists():
            raise RuntimeError(f"input file not found: {file_path}")
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                value = line.strip()
                if value:
                    codes.append(value)
    return sorted(set(codes))


def _fetch_latest_trade_date(cursor) -> int:
    cursor.execute("SELECT MAX(trade_date) FROM ads_features_stock_daily")
    row = cursor.fetchone()
    if not row or row[0] is None:
        cursor.execute("SELECT MAX(trade_date) FROM dwd_daily")
        row = cursor.fetchone()
    if not row or row[0] is None:
        raise RuntimeError("no trade_date available; ads_features_stock_daily and dwd_daily are empty.")
    return int(row[0])


def _fetch_features(cursor, trade_date: int, ts_codes: Iterable[str]) -> pd.DataFrame:
    ts_code_list = list(ts_codes)
    ads_filter = ""
    dwd_filter = ""
    params: List[object] = [trade_date]
    if ts_codes:
        placeholders = ",".join(["%s"] * len(ts_code_list))
        ads_filter = f" AND ts_code IN ({placeholders})"
        dwd_filter = f" AND d.ts_code IN ({placeholders})"
        params.extend(ts_code_list)
    ads_sql = (
        "SELECT trade_date, ts_code, ret_20, ret_60, turnover_rate, pe_ttm, pb, "
        "roe, grossprofit_margin, debt_to_assets "
        "FROM ads_features_stock_daily "
        f"WHERE trade_date = %s{ads_filter}"
    )
    cursor.execute(ads_sql, tuple(params))
    rows = cursor.fetchall()
    if rows:
        return pd.DataFrame(
            rows,
            columns=[
                "trade_date",
                "ts_code",
                "ret_20",
                "ret_60",
                "turnover_rate",
                "pe_ttm",
                "pb",
                "roe",
                "grossprofit_margin",
                "debt_to_assets",
            ],
        )
    fallback_sql = (
        "SELECT d.trade_date, d.ts_code, "
        "p.qfq_ret_20 AS ret_20, p.qfq_ret_60 AS ret_60, "
        "b.turnover_rate, b.pe_ttm, b.pb, "
        "f.roe, f.grossprofit_margin, f.debt_to_assets "
        "FROM dwd_daily d "
        "LEFT JOIN dws_price_adj_daily p "
        "  ON p.trade_date = d.trade_date AND p.ts_code = d.ts_code "
        "LEFT JOIN dwd_daily_basic b "
        "  ON b.trade_date = d.trade_date AND b.ts_code = d.ts_code "
        "LEFT JOIN dws_fina_pit_daily f "
        "  ON f.trade_date = d.trade_date AND f.ts_code = d.ts_code "
        f"WHERE d.trade_date = %s{dwd_filter}"
    )
    cursor.execute(fallback_sql, tuple(params))
    rows = cursor.fetchall()
    if not rows:
        cursor.execute("SELECT MAX(trade_date) FROM ads_features_stock_daily")
        ads_date = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(trade_date) FROM dwd_daily")
        dwd_date = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(trade_date) FROM ods_daily")
        ods_date = cursor.fetchone()[0]
        raise RuntimeError(
            "no feature rows available for trade_date="
            f"{trade_date}. latest ads={ads_date}, dwd={dwd_date}, ods={ods_date}"
        )
    return pd.DataFrame(
        rows,
        columns=[
            "trade_date",
            "ts_code",
            "ret_20",
            "ret_60",
            "turnover_rate",
            "pe_ttm",
            "pb",
            "roe",
            "grossprofit_margin",
            "debt_to_assets",
        ],
    )


def _zscore(series: pd.Series) -> pd.Series:
    mean = series.mean()
    std = series.std(ddof=0)
    if std == 0 or pd.isna(std):
        return pd.Series([0.0] * len(series), index=series.index)
    return (series - mean) / std


def _compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    numeric = df[
        [
            "ret_20",
            "ret_60",
            "turnover_rate",
            "pe_ttm",
            "pb",
            "roe",
            "grossprofit_margin",
            "debt_to_assets",
        ]
    ].apply(pd.to_numeric, errors="coerce")
    df.update(numeric)
    df["earnings_yield"] = df["pe_ttm"].apply(
        lambda value: (1 / value) if value is not None and value > 0 else None
    )
    df["value_score"] = _zscore(df["earnings_yield"])
    df["value_score"] += _zscore(df["pb"] * -1)

    df["growth_score"] = _zscore(df["roe"])
    df["growth_score"] += _zscore(df["grossprofit_margin"])

    df["momentum_score"] = _zscore(df["ret_20"])
    df["momentum_score"] += _zscore(df["ret_60"])

    df["quality_score"] = _zscore(df["debt_to_assets"] * -1)
    df["quality_score"] += _zscore(df["turnover_rate"] * -0.2)

    weights = {
        "value_score": 0.25,
        "growth_score": 0.30,
        "momentum_score": 0.20,
        "quality_score": 0.15,
    }
    df["raw_score"] = 0.0
    for key, weight in weights.items():
        df["raw_score"] += df[key].fillna(0) * weight

    df["score"] = df["raw_score"].rank(pct=True) * 100
    return df


def _render_results(df: pd.DataFrame, ts_codes: Iterable[str]) -> pd.DataFrame:
    if ts_codes:
        df = df[df["ts_code"].isin(ts_codes)]
    if df.empty:
        raise RuntimeError("no rows matched the provided ts_code list.")
    return df[
        [
            "trade_date",
            "ts_code",
            "score",
            "raw_score",
            "value_score",
            "growth_score",
            "momentum_score",
            "quality_score",
        ]
    ].sort_values(by="score", ascending=False)


def main() -> None:
    args = parse_args()
    _apply_config_args(args)
    ts_codes = _load_ts_codes(args)
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            trade_date = args.trade_date or _fetch_latest_trade_date(cursor)
            try:
                df = _fetch_features(cursor, trade_date, ts_codes)
            except RuntimeError as exc:
                if not args.fallback_latest:
                    raise
                trade_date = _fetch_latest_trade_date(cursor)
                if args.trade_date and trade_date == args.trade_date:
                    raise
                df = _fetch_features(cursor, trade_date, ts_codes)
    scored = _compute_scores(df)
    result = _render_results(scored, ts_codes)
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
