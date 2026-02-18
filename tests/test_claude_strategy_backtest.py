from __future__ import annotations

import pytest
pytest.importorskip("sqlalchemy")

import pandas as pd

from score.claude_score.advanced_analysis import AdvancedAnalyzer

def _mock_scores() -> pd.DataFrame:
    rows = []
    for trade_date in [20240101, 20240102, 20240103]:
        for i, ts_code in enumerate(["000001.SZ", "000002.SZ", "000003.SZ"]):
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": ts_code,
                    "total_score": 100 - i,
                    "momentum_score": 10 - i,
                    "value_score": 10 - i,
                    "quality_score": 10 - i,
                    "technical_score": 10 - i,
                    "capital_score": 10 - i,
                    "chip_score": 10 - i,
                }
            )
    return pd.DataFrame(rows)


def _mock_prices() -> pd.DataFrame:
    rows = []
    for trade_date in [20240101, 20240102, 20240103]:
        for ts_code, pct in [("000001.SZ", 1.0), ("000002.SZ", 0.5), ("000003.SZ", -0.2)]:
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": ts_code,
                    "open": 10.0,
                    "close": 10.0,
                    "pct_chg": pct,
                    "vol": 1000,
                    "is_limit_up": 0,
                    "is_limit_down": 0,
                    "is_suspended": 0,
                }
            )
    return pd.DataFrame(rows)


def test_backtest_supports_initial_capital_and_num_stocks(monkeypatch) -> None:
    analyzer = AdvancedAnalyzer(engine=None)
    monkeypatch.setattr(analyzer, "_get_score_data", lambda s, e: _mock_scores())
    monkeypatch.setattr(pd, "read_sql", lambda *args, **kwargs: _mock_prices())

    result = analyzer.backtest_score_strategy(
        start_date=20240101,
        end_date=20240103,
        num_stocks=2,
        holding_days=1,
        initial_capital=2_000_000,
    )

    assert not result.empty
    assert result.iloc[0]["nav"] == 2_000_000
    assert result["holdings_count"].max() <= 2
    # day-1后每日执行再平衡流程
    if len(result) > 1:
        assert result.iloc[1:]["is_rebalance"].all()


def test_backtest_top_n_alias_overrides_num_stocks(monkeypatch) -> None:
    analyzer = AdvancedAnalyzer(engine=None)
    monkeypatch.setattr(analyzer, "_get_score_data", lambda s, e: _mock_scores())
    monkeypatch.setattr(pd, "read_sql", lambda *args, **kwargs: _mock_prices())

    result = analyzer.backtest_score_strategy(
        start_date=20240101,
        end_date=20240103,
        num_stocks=3,
        top_n=1,
        holding_days=1,
        initial_capital=1_000_000,
    )

    assert not result.empty
    assert result["holdings_count"].max() <= 1


def test_backtest_holding_days_control(monkeypatch) -> None:
    analyzer = AdvancedAnalyzer(engine=None)
    monkeypatch.setattr(analyzer, "_get_score_data", lambda s, e: _mock_scores())
    monkeypatch.setattr(pd, "read_sql", lambda *args, **kwargs: _mock_prices())

    result = analyzer.backtest_score_strategy(
        start_date=20240101,
        end_date=20240103,
        num_stocks=2,
        holding_days=20,
        initial_capital=1_000_000,
    )

    assert not result.empty
    assert "avg_holding_days" in result.columns
    assert result["avg_holding_days"].max() <= 20
