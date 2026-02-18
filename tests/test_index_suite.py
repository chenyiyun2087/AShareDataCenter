from __future__ import annotations

import pandas as pd

from scripts.etl.ods.index_suite import TARGET_INDEXES, _ensure_columns, build_default_options


def test_target_indexes_contains_required_indices() -> None:
    assert len(TARGET_INDEXES) == 10
    assert TARGET_INDEXES["000001.SH"] == "Shanghai Composite Index"
    assert TARGET_INDEXES["399001.SZ"] == "SZSE COMPONENT INDEX"
    assert TARGET_INDEXES["399006.SZ"] == "ChiNext Index"


def test_ensure_columns_fills_missing_values() -> None:
    df = pd.DataFrame([{"trade_date": 20240101, "ts_code": "000300.SH"}])
    normalized = _ensure_columns(df, ["trade_date", "ts_code", "close"])
    assert list(normalized.columns) == ["trade_date", "ts_code", "close"]
    assert normalized.iloc[0]["close"] is None


def test_build_default_options_uses_custom_codes() -> None:
    options = build_default_options(20240101, 20240131, ["000300.SH"])
    assert options.start_date == 20240101
    assert options.end_date == 20240131
    assert options.index_codes == ["000300.SH"]
