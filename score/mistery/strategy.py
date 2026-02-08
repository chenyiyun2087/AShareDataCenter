"""
Mistery 520 战法信号生成

核心逻辑：
- 5 日均线 (短期攻击线) 与 20 日均线 (中期生命线) 的交互
- 20 日均线方向具有一票否决权：仅在 20 日均线向上时才考虑金叉与回踩信号
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


@dataclass
class SignalConfig:
    close_col: str = "close"
    volume_col: Optional[str] = "vol"
    pullback_tolerance: float = 0.01
    require_volume_confirm: bool = True
    volume_window: int = 5


def get_engine(host="localhost", port=3306, user="root", password="", database="tushare_stock"):
    """创建数据库连接"""
    connection_string = (
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    )
    return create_engine(connection_string, echo=False)


def _cross_up(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    return (series_a > series_b) & (series_a.shift(1) <= series_b.shift(1))


def _cross_down(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    return (series_a < series_b) & (series_a.shift(1) >= series_b.shift(1))


def compute_520_signals(df: pd.DataFrame, config: Optional[SignalConfig] = None) -> pd.DataFrame:
    """
    计算 520 战法信号。

    必备列：
    - close (或自定义 close_col)
    - trade_date (用于排序)
    可选列：
    - vol (用于量能确认)
    """
    if config is None:
        config = SignalConfig()

    df = df.copy()
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date")

    close = df[config.close_col]
    df["ma5"] = close.rolling(window=5, min_periods=5).mean()
    df["ma20"] = close.rolling(window=20, min_periods=20).mean()

    df["ma20_up"] = df["ma20"] > df["ma20"].shift(1)

    if config.volume_col and config.volume_col in df.columns:
        df["vol_ma"] = df[config.volume_col].rolling(
            window=config.volume_window, min_periods=config.volume_window
        ).mean()
        df["volume_confirm"] = df[config.volume_col] > df["vol_ma"]
    else:
        df["volume_confirm"] = True

    golden_cross = _cross_up(df["ma5"], df["ma20"])
    dead_cross = _cross_down(df["ma5"], df["ma20"])

    ma5_turn_up = df["ma5"] > df["ma5"].shift(1)
    pullback_range = close >= df["ma20"]
    pullback_near = close <= df["ma20"] * (1 + config.pullback_tolerance)
    pullback_buy = pullback_range & pullback_near & ma5_turn_up

    reduce_position = close < df["ma5"]

    df["signal_buy_golden"] = golden_cross & df["ma20_up"]
    if config.require_volume_confirm:
        df["signal_buy_golden"] &= df["volume_confirm"]

    df["signal_buy_pullback"] = pullback_buy & df["ma20_up"]
    df["signal_reduce"] = reduce_position
    df["signal_exit"] = dead_cross

    df["signal"] = np.select(
        [
            df["signal_exit"],
            df["signal_reduce"],
            df["signal_buy_pullback"],
            df["signal_buy_golden"],
        ],
        ["exit", "reduce", "buy_pullback", "buy_golden"],
        default="hold",
    )

    return df


class Mistery520Strategy:
    """Mistery 520 战法信号生成器"""

    def __init__(self, engine=None, config: Optional[SignalConfig] = None):
        self.engine = engine
        self.config = config or SignalConfig()

    def load_daily_data(
        self,
        ts_code: str,
        start_date: int,
        end_date: int,
        table: str = "ods_daily",
        close_col: str = "close",
        volume_col: Optional[str] = "vol",
    ) -> pd.DataFrame:
        if self.engine is None:
            raise ValueError("请先传入数据库引擎")

        columns = ["trade_date", "ts_code", close_col]
        if volume_col:
            columns.append(volume_col)
        column_sql = ", ".join(columns)
        sql = f"""
        SELECT {column_sql}
        FROM {table}
        WHERE ts_code = :ts_code
          AND trade_date BETWEEN :start_date AND :end_date
        ORDER BY trade_date
        """
        return pd.read_sql(
            text(sql),
            self.engine,
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        return compute_520_signals(df, config=self.config)
