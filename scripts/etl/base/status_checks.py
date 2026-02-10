"""Data status check functions for ODS/DWD/DWS/ADS layers.

Provides functions to verify:
1. Data status is normal
2. Most recent trading day data is correctly recorded
3. Next day is ready for data ingestion
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .runtime import get_env_config, get_mysql_session


@dataclass
class TableStatus:
    """Status of a single table."""
    table_name: str
    max_date: Optional[int]
    row_count: int
    expected_date: Optional[int]
    status: str  # OK, STALE, EMPTY, UNKNOWN
    message: str


@dataclass
class LayerStatus:
    """Status of an entire layer."""
    layer: str
    is_healthy: bool
    is_ready_for_next: bool
    latest_trade_date: Optional[int]
    expected_trade_date: Optional[int]
    table_statuses: List[TableStatus]
    watermark: Optional[int]
    message: str


def _get_latest_trade_date(cursor) -> Optional[int]:
    """Get the latest trade date from calendar (excluding future dates)."""
    cursor.execute(
        "SELECT MAX(cal_date) FROM dim_trade_cal "
        "WHERE exchange='SSE' AND is_open=1 "
        "AND cal_date <= DATE_FORMAT(CURDATE(), '%Y%m%d')"
    )
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] else None


def _get_next_trade_date(cursor, current_date: int) -> Optional[int]:
    """Get the next trade date after the given date."""
    cursor.execute(
        "SELECT MIN(cal_date) FROM dim_trade_cal "
        "WHERE exchange='SSE' AND is_open=1 AND cal_date > %s",
        (current_date,)
    )
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] else None


def _get_watermark(cursor, api_name: str) -> Optional[int]:
    """Get watermark for a given api/layer."""
    cursor.execute(
        "SELECT water_mark FROM meta_etl_watermark WHERE api_name = %s",
        (api_name,)
    )
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] else None


def _check_table(cursor, table_name: str, date_column: str, expected_date: Optional[int]) -> TableStatus:
    """Check a single table's status."""
    try:
        cursor.execute(f"SELECT MAX({date_column}), COUNT(*) FROM {table_name}")
        row = cursor.fetchone()
        max_date = int(row[0]) if row and row[0] else None
        row_count = int(row[1]) if row and row[1] else 0
    except Exception as e:
        return TableStatus(
            table_name=table_name,
            max_date=None,
            row_count=0,
            expected_date=expected_date,
            status="ERROR",
            message=str(e)
        )
    
    if row_count == 0:
        status = "EMPTY"
        message = "Table is empty"
    elif expected_date is None:
        status = "UNKNOWN"
        message = "No expected date to compare"
    elif max_date >= expected_date:
        status = "OK"
        message = f"Data up to date ({max_date})"
    else:
        status = "STALE"
        message = f"Data stale: {max_date} < expected {expected_date}"
    
    return TableStatus(
        table_name=table_name,
        max_date=max_date,
        row_count=row_count,
        expected_date=expected_date,
        status=status,
        message=message
    )


def check_ods_status(cursor, expected_date: Optional[int] = None) -> LayerStatus:
    """Check ODS layer data status.
    
    Verifies:
    - ods_daily, ods_daily_basic, ods_adj_factor have data for expected date
    - Watermarks are correctly set
    """
    if expected_date is None:
        expected_date = _get_latest_trade_date(cursor)
    
    tables = [
        ("ods_daily", "trade_date"),
        ("ods_daily_basic", "trade_date"),
        ("ods_adj_factor", "trade_date"),
        ("ods_margin_detail", "trade_date"),
        ("ods_moneyflow", "trade_date"),
        ("ods_cyq_perf", "trade_date"),
        ("ods_stk_factor", "trade_date"),
    ]
    
    statuses = [_check_table(cursor, t, c, expected_date) for t, c in tables]
    watermark = _get_watermark(cursor, "ods_daily")
    
    # Core tables must all be OK
    core_ok = all(s.status == "OK" for s in statuses[:3])
    any_stale = any(s.status in ("STALE", "EMPTY") for s in statuses)
    
    is_healthy = core_ok
    is_ready = watermark is not None and watermark >= (expected_date or 0)
    
    if is_healthy:
        message = f"ODS layer healthy, data up to {expected_date}"
    else:
        failed = [s.table_name for s in statuses if s.status != "OK"]
        message = f"ODS layer issues: {', '.join(failed)}"
    
    return LayerStatus(
        layer="ODS",
        is_healthy=is_healthy,
        is_ready_for_next=is_ready,
        latest_trade_date=max((s.max_date for s in statuses if s.max_date), default=None),
        expected_trade_date=expected_date,
        table_statuses=statuses,
        watermark=watermark,
        message=message
    )


def check_dwd_status(cursor, expected_date: Optional[int] = None) -> LayerStatus:
    """Check DWD layer data status."""
    if expected_date is None:
        expected_date = _get_latest_trade_date(cursor)
    
    tables = [
        ("dwd_daily", "trade_date"),
        ("dwd_daily_basic", "trade_date"),
        ("dwd_adj_factor", "trade_date"),
        ("dwd_stock_daily_standard", "trade_date"),
        ("dwd_fina_snapshot", "trade_date"),
        ("dwd_margin_sentiment", "trade_date"),
        ("dwd_chip_stability", "trade_date"),
    ]
    
    statuses = [_check_table(cursor, t, c, expected_date) for t, c in tables]
    watermark = _get_watermark(cursor, "dwd_daily")
    
    core_ok = all(s.status == "OK" for s in statuses[:3])
    is_healthy = core_ok
    is_ready = watermark is not None and watermark >= (expected_date or 0)
    
    if is_healthy:
        message = f"DWD layer healthy, data up to {expected_date}"
    else:
        failed = [s.table_name for s in statuses if s.status != "OK"]
        message = f"DWD layer issues: {', '.join(failed)}"
    
    return LayerStatus(
        layer="DWD",
        is_healthy=is_healthy,
        is_ready_for_next=is_ready,
        latest_trade_date=max((s.max_date for s in statuses if s.max_date), default=None),
        expected_trade_date=expected_date,
        table_statuses=statuses,
        watermark=watermark,
        message=message
    )


def check_dws_status(cursor, expected_date: Optional[int] = None) -> LayerStatus:
    """Check DWS layer data status."""
    if expected_date is None:
        expected_date = _get_latest_trade_date(cursor)
    
    tables = [
        ("dws_price_adj_daily", "trade_date"),
        ("dws_fina_pit_daily", "trade_date"),
        ("dws_tech_pattern", "trade_date"),
        ("dws_capital_flow", "trade_date"),
        ("dws_leverage_sentiment", "trade_date"),
        ("dws_chip_dynamics", "trade_date"),
    ]
    
    statuses = [_check_table(cursor, t, c, expected_date) for t, c in tables]
    watermark = _get_watermark(cursor, "dws")
    
    core_ok = statuses[0].status == "OK" if statuses else False
    is_healthy = core_ok
    is_ready = watermark is not None and watermark >= (expected_date or 0)
    
    if is_healthy:
        message = f"DWS layer healthy, data up to {expected_date}"
    else:
        failed = [s.table_name for s in statuses if s.status != "OK"]
        message = f"DWS layer issues: {', '.join(failed)}"
    
    return LayerStatus(
        layer="DWS",
        is_healthy=is_healthy,
        is_ready_for_next=is_ready,
        latest_trade_date=max((s.max_date for s in statuses if s.max_date), default=None),
        expected_trade_date=expected_date,
        table_statuses=statuses,
        watermark=watermark,
        message=message
    )


def check_ads_status(cursor, expected_date: Optional[int] = None) -> LayerStatus:
    """Check ADS layer data status."""
    if expected_date is None:
        expected_date = _get_latest_trade_date(cursor)
    
    tables = [
        ("ads_features_stock_daily", "trade_date"),
        ("ads_universe_daily", "trade_date"),
        ("ads_stock_score_daily", "trade_date"),
    ]
    
    statuses = [_check_table(cursor, t, c, expected_date) for t, c in tables]
    watermark = _get_watermark(cursor, "ads")
    
    core_ok = all(s.status == "OK" for s in statuses[:2])
    is_healthy = core_ok
    is_ready = watermark is not None and watermark >= (expected_date or 0)
    
    if is_healthy:
        message = f"ADS layer healthy, data up to {expected_date}"
    else:
        failed = [s.table_name for s in statuses if s.status != "OK"]
        message = f"ADS layer issues: {', '.join(failed)}"
    
    return LayerStatus(
        layer="ADS",
        is_healthy=is_healthy,
        is_ready_for_next=is_ready,
        latest_trade_date=max((s.max_date for s in statuses if s.max_date), default=None),
        expected_trade_date=expected_date,
        table_statuses=statuses,
        watermark=watermark,
        message=message
    )


@dataclass
class DataPipelineStatus:
    """Overall pipeline status."""
    is_healthy: bool
    is_ready_for_next_day: bool
    expected_trade_date: Optional[int]
    next_trade_date: Optional[int]
    ods_status: LayerStatus
    dwd_status: LayerStatus
    dws_status: LayerStatus
    ads_status: LayerStatus
    summary: str


def check_data_status(expected_date: Optional[int] = None) -> DataPipelineStatus:
    """Check overall data pipeline status.
    
    Returns comprehensive status covering:
    1. Data status is normal
    2. Most recent trading day data is correctly recorded
    3. Next day is ready for data ingestion
    """
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            if expected_date is None:
                expected_date = _get_latest_trade_date(cursor)
            
            next_date = _get_next_trade_date(cursor, expected_date) if expected_date else None
            
            ods = check_ods_status(cursor, expected_date)
            dwd = check_dwd_status(cursor, expected_date)
            dws = check_dws_status(cursor, expected_date)
            ads = check_ads_status(cursor, expected_date)
    
    all_healthy = ods.is_healthy and dwd.is_healthy and dws.is_healthy and ads.is_healthy
    all_ready = ods.is_ready_for_next and dwd.is_ready_for_next and dws.is_ready_for_next and ads.is_ready_for_next
    
    if all_healthy and all_ready:
        summary = f"✅ Pipeline healthy, ready for {next_date or 'next day'}"
    elif all_healthy:
        summary = f"⚠️ Pipeline healthy but watermarks may need update"
    else:
        issues = []
        if not ods.is_healthy:
            issues.append("ODS")
        if not dwd.is_healthy:
            issues.append("DWD")
        if not dws.is_healthy:
            issues.append("DWS")
        if not ads.is_healthy:
            issues.append("ADS")
        summary = f"❌ Issues in: {', '.join(issues)}"
    
    return DataPipelineStatus(
        is_healthy=all_healthy,
        is_ready_for_next_day=all_ready,
        expected_trade_date=expected_date,
        next_trade_date=next_date,
        ods_status=ods,
        dwd_status=dwd,
        dws_status=dws,
        ads_status=ads,
        summary=summary
    )


def print_status_report(status: DataPipelineStatus) -> None:
    """Print a formatted status report."""
    print("=" * 60)
    print("Data Pipeline Status Report")
    print("=" * 60)
    print(f"Expected Trade Date: {status.expected_trade_date}")
    print(f"Next Trade Date: {status.next_trade_date}")
    print(f"Overall Status: {status.summary}")
    print()
    
    for layer_status in [status.ods_status, status.dwd_status, status.dws_status, status.ads_status]:
        print(f"\n{layer_status.layer} Layer")
        print("-" * 40)
        print(f"  Healthy: {'✅' if layer_status.is_healthy else '❌'}")
        print(f"  Ready for Next: {'✅' if layer_status.is_ready_for_next else '❌'}")
        print(f"  Watermark: {layer_status.watermark}")
        print(f"  Message: {layer_status.message}")
        print(f"  Tables:")
        for ts in layer_status.table_statuses:
            icon = "✅" if ts.status == "OK" else ("⚠️" if ts.status == "STALE" else "❌")
            print(f"    {icon} {ts.table_name}: {ts.max_date} ({ts.row_count:,} rows) - {ts.status}")
