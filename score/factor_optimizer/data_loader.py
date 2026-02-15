"""Load factor scores and market data from MySQL for backtesting."""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

from .config import OptimizerConfig, CATEGORY_NAMES

logger = logging.getLogger(__name__)


def _get_engine():
    """Create SQLAlchemy engine using etl.ini config."""
    # Reuse the same config resolution as the ETL scripts
    from configparser import ConfigParser

    config_candidates = [
        os.environ.get("ETL_CONFIG_PATH", ""),
        str(Path.cwd() / "config" / "etl.ini"),
        str(Path(__file__).resolve().parents[2] / "config" / "etl.ini"),
    ]

    parser = ConfigParser()
    for p in config_candidates:
        if p and Path(p).exists():
            parser.read(p)
            break

    host = os.environ.get("MYSQL_HOST") or parser.get("mysql", "host", fallback="127.0.0.1")
    port = os.environ.get("MYSQL_PORT") or parser.get("mysql", "port", fallback="3306")
    user = os.environ.get("MYSQL_USER") or parser.get("mysql", "user", fallback="root")
    password = os.environ.get("MYSQL_PASSWORD") or parser.get("mysql", "password", fallback="")
    database = os.environ.get("MYSQL_DB") or parser.get("mysql", "database", fallback="tushare_stock")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    return create_engine(url)


def load_category_scores(
    config: OptimizerConfig,
    engine=None,
) -> pd.DataFrame:
    """Load the 7 category-level scores for all stocks across the backtest period.

    Returns DataFrame with columns:
        trade_date, ts_code, momentum, value, quality, technical, capital, chip, size
    """
    if engine is None:
        engine = _get_engine()

    logger.info(f"Loading category scores: {config.backtest_start} -> {config.backtest_end}")

    # Size score computed inline from dwd_daily_basic.circ_mv
    # (dws_fama_size_score table may not exist)
    sql = text("""
    SELECT
        m.trade_date,
        m.ts_code,
        COALESCE(m.momentum_score, 0)   AS momentum,
        COALESCE(v.value_score, 0)      AS value,
        COALESCE(q.quality_score, 0)    AS quality,
        COALESCE(t.technical_score, 0)  AS technical,
        COALESCE(c.capital_score, 0)    AS capital,
        COALESCE(ch.chip_score, 0)      AS chip,
        CASE
            WHEN b.circ_mv < 300000 THEN 10.0
            WHEN b.circ_mv < 800000 THEN 8.0
            WHEN b.circ_mv < 2000000 THEN 6.0
            WHEN b.circ_mv < 5000000 THEN 4.0
            WHEN b.circ_mv < 10000000 THEN 2.0
            ELSE 1.0
        END AS size
    FROM dws_momentum_score m
    LEFT JOIN dws_value_score v
        ON m.trade_date = v.trade_date AND m.ts_code = v.ts_code
    LEFT JOIN dws_quality_score q
        ON m.trade_date = q.trade_date AND m.ts_code = q.ts_code
    LEFT JOIN dws_technical_score t
        ON m.trade_date = t.trade_date AND m.ts_code = t.ts_code
    LEFT JOIN dws_capital_score c
        ON m.trade_date = c.trade_date AND m.ts_code = c.ts_code
    LEFT JOIN dws_chip_score ch
        ON m.trade_date = ch.trade_date AND m.ts_code = ch.ts_code
    LEFT JOIN dwd_daily_basic b
        ON m.trade_date = b.trade_date AND m.ts_code = b.ts_code
    WHERE m.trade_date BETWEEN :start AND :end
    """)

    df = pd.read_sql(sql, engine, params={"start": config.backtest_start, "end": config.backtest_end})
    logger.info(f"Loaded {len(df)} score rows, {df['ts_code'].nunique()} stocks")
    return df


def load_daily_returns(
    config: OptimizerConfig,
    engine=None,
) -> pd.DataFrame:
    """Load daily returns and limit-up/suspension flags.

    Returns DataFrame with columns:
        trade_date, ts_code, pct_chg, close, vol, is_limit_up, is_suspended
    """
    if engine is None:
        engine = _get_engine()

    logger.info(f"Loading daily returns: {config.backtest_start} -> {config.backtest_end}")

    sql = text("""
    SELECT
        d.trade_date,
        d.ts_code,
        d.pct_chg,
        d.close,
        d.vol,
        CASE
            WHEN (d.ts_code LIKE '688%%' OR d.ts_code LIKE '3%%') AND d.pct_chg >= 19.9 THEN 1
            WHEN d.pct_chg >= 9.9 THEN 1
            ELSE 0
        END AS is_limit_up,
        CASE WHEN d.vol IS NULL OR d.vol = 0 THEN 1 ELSE 0 END AS is_suspended
    FROM ods_daily d
    WHERE d.trade_date BETWEEN :start AND :end
    """)

    df = pd.read_sql(sql, engine, params={"start": config.backtest_start, "end": config.backtest_end})
    logger.info(f"Loaded {len(df)} return rows")
    return df


def load_benchmark(
    config: OptimizerConfig,
    engine=None,
) -> pd.DataFrame:
    """Load benchmark (CSI 500) daily data.

    Returns DataFrame with columns: trade_date, close, pct_chg
    """
    if engine is None:
        engine = _get_engine()

    logger.info(f"Loading benchmark {config.benchmark_code}")

    sql = text("""
    SELECT trade_date, close, pct_chg
    FROM ods_index_daily
    WHERE ts_code = :code
      AND trade_date BETWEEN :start AND :end
    ORDER BY trade_date
    """)

    df = pd.read_sql(sql, engine, params={
        "code": config.benchmark_code,
        "start": config.backtest_start,
        "end": config.backtest_end,
    })

    if df.empty:
        logger.warning(f"No benchmark data for {config.benchmark_code}, trying ods_daily")
        # Fallback: try ods_daily for the index
        sql2 = text("""
        SELECT trade_date, close, pct_chg
        FROM ods_daily
        WHERE ts_code = :code
          AND trade_date BETWEEN :start AND :end
        ORDER BY trade_date
        """)
        df = pd.read_sql(sql2, engine, params={
            "code": config.benchmark_code,
            "start": config.backtest_start,
            "end": config.backtest_end,
        })

    logger.info(f"Loaded {len(df)} benchmark rows")
    return df


def load_all_data(
    config: OptimizerConfig,
    engine=None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load all data needed for optimization.

    Returns:
        (scores_df, returns_df, benchmark_df)
    """
    if engine is None:
        engine = _get_engine()

    scores = load_category_scores(config, engine)
    returns = load_daily_returns(config, engine)
    benchmark = load_benchmark(config, engine)

    return scores, returns, benchmark
