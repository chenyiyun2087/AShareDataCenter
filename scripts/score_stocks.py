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
        raise RuntimeError("ads_features_stock_daily is empty; cannot infer trade_date.")
    return int(row[0])


def _fetch_features(cursor, trade_date: int) -> pd.DataFrame:
    sql = (
        "SELECT trade_date, ts_code, ret_20, ret_60, turnover_rate, pe_ttm, pb, "
        "roe, grossprofit_margin, debt_to_assets "
        "FROM ads_features_stock_daily "
        "WHERE trade_date = %s"
    )
    cursor.execute(sql, (trade_date,))
    rows = cursor.fetchall()
    if not rows:
        raise RuntimeError(f"no ads_features_stock_daily rows for trade_date={trade_date}")
    df = pd.DataFrame(
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
    return df


def _zscore(series: pd.Series) -> pd.Series:
    mean = series.mean()
    std = series.std(ddof=0)
    if std == 0 or pd.isna(std):
        return pd.Series([0.0] * len(series), index=series.index)
    return (series - mean) / std


def _compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["earnings_yield"] = df["pe_ttm"].apply(
        lambda value: (1 / value) if value is not None and value > 0 else None
    )
    df["value_score"] = _zscore(df["earnings_yield"].astype(float))
    df["value_score"] += _zscore(df["pb"].astype(float) * -1)

    df["growth_score"] = _zscore(df["roe"].astype(float))
    df["growth_score"] += _zscore(df["grossprofit_margin"].astype(float))

    df["momentum_score"] = _zscore(df["ret_20"].astype(float))
    df["momentum_score"] += _zscore(df["ret_60"].astype(float))

    df["quality_score"] = _zscore(df["debt_to_assets"].astype(float) * -1)
    df["quality_score"] += _zscore(df["turnover_rate"].astype(float) * -0.2)

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
            df = _fetch_features(cursor, trade_date)
    scored = _compute_scores(df)
    result = _render_results(scored, ts_codes)
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
