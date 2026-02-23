from __future__ import annotations

import numpy as np
import pandas as pd

from score.factor_optimizer.backtest import run_backtest
from score.factor_optimizer.config import OptimizerConfig


def _mock_scores() -> pd.DataFrame:
    rows = []
    for trade_date in [20240101, 20240102, 20240103]:
        for i, ts_code in enumerate(["000001.SZ", "000002.SZ", "000003.SZ"]):
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": ts_code,
                    "momentum": 10 - i,
                    "value": 9 - i,
                    "quality": 8 - i,
                    "technical": 7 - i,
                    "capital": 6 - i,
                    "chip": 5 - i,
                    "size": 4 - i,
                }
            )
    return pd.DataFrame(rows)


def _mock_returns() -> pd.DataFrame:
    rows = []
    for trade_date in [20240101, 20240102, 20240103]:
        for ts_code, pct in [("000001.SZ", 1.0), ("000002.SZ", 0.5), ("000003.SZ", -0.2)]:
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": ts_code,
                    "pct_chg": pct,
                    "is_limit_up": 0,
                    "is_suspended": 0,
                }
            )
    return pd.DataFrame(rows)


def test_backtest_uses_initial_capital_and_num_stocks_limit() -> None:
    config = OptimizerConfig(
        backtest_start=20240101,
        backtest_end=20240103,
        num_stocks=2,
        holding_days=1,
        initial_capital=2_000_000,
    )
    weights = np.ones(7) / 7
    nav, summary = run_backtest(_mock_scores(), _mock_returns(), weights, config)

    assert not nav.empty
    assert nav.iloc[0]["nav"] == 2_000_000
    assert nav["holdings_count"].max() <= 2
    assert "total_return" in summary


def test_config_top_n_alias_maps_to_num_stocks() -> None:
    config = OptimizerConfig(top_n=7)
    assert config.num_stocks == 7
    assert config.top_n == 7
