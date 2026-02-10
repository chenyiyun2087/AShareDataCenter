#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

# Add project scripts directory to sys.path to allow importing 'etl' package
scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.base.runtime import get_env_config, get_mysql_connection


@dataclass(frozen=True)
class TableCheck:
    name: str
    date_column: str
    category: str
    optional: bool = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check ODS/financial/feature data freshness.")
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument(
        "--expected-trade-date",
        type=int,
        default=None,
        help="Override expected latest trade_date (YYYYMMDD).",
    )
    parser.add_argument(
        "--min-date",
        type=int,
        default=None,
        help="Fallback minimum date for trade_date checks (YYYYMMDD).",
    )
    parser.add_argument(
        "--min-ods-date",
        type=int,
        default=None,
        help="Minimum trade_date for ODS daily tables.",
    )
    parser.add_argument(
        "--min-feature-date",
        type=int,
        default=None,
        help="Minimum trade_date for feature ODS tables.",
    )
    parser.add_argument(
        "--min-fina-ann-date",
        type=int,
        default=None,
        help="Minimum ann_date for financial tables.",
    )
    parser.add_argument(
        "--min-fina-trade-date",
        type=int,
        default=None,
        help="Minimum trade_date for financial PIT tables.",
    )
    parser.add_argument(
        "--categories",
        default=None,
        help="Comma-separated categories to check (e.g. ods,features,financial,dwd,dws,ads).",
    )
    parser.add_argument(
        "--fail-on-stale",
        action="store_true",
        help="Exit with non-zero status when any selected table is stale or empty.",
    )
    parser.add_argument(
        "--ignore-today",
        action="store_true",
        help="Do not fail if today's data is missing (handles TuShare data lag).",
    )
    return parser.parse_args()


def _apply_config_args(args: argparse.Namespace) -> None:
    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            # Try relative to CWD first
            cwd_path = (Path.cwd() / config_path).resolve()
            if cwd_path.exists():
                config_path = cwd_path
            else:
                # Fallback to project root
                root_path = (scripts_dir.parent / config_path).resolve()
                if root_path.exists():
                    config_path = root_path
                else:
                    config_path = cwd_path

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


def _latest_trade_date(cursor) -> Optional[int]:
    cursor.execute(
        "SELECT MAX(cal_date) FROM dim_trade_cal "
        "WHERE exchange='SSE' AND is_open=1 "
        "AND cal_date <= DATE_FORMAT(CURDATE(), '%Y%m%d')"
    )
    row = cursor.fetchone()
    if row and row[0]:
        return int(row[0])
    cursor.execute("SELECT MAX(trade_date) FROM ods_daily")
    row = cursor.fetchone()
    if row and row[0]:
        return int(row[0])
    return None


def _fetch_table_stats(cursor, table: TableCheck) -> tuple[Optional[int], int, Optional[str]]:
    sql = (
        f"SELECT MAX({table.date_column}), COUNT(*), MAX(updated_at) "
        f"FROM {table.name}"
    )
    cursor.execute(sql)
    max_date, total_rows, updated_at = cursor.fetchone()
    max_date_value = int(max_date) if max_date is not None else None
    updated_at_value = updated_at.isoformat(sep=" ") if updated_at else None
    return max_date_value, int(total_rows), updated_at_value


def _status_for_date(max_date: Optional[int], threshold: Optional[int]) -> str:
    if max_date is None:
        return "EMPTY"
    if threshold is None:
        return "UNKNOWN"
    return "OK" if max_date >= threshold else "STALE"


def _resolve_threshold(
    *,
    table: TableCheck,
    expected_trade_date: Optional[int],
    args: argparse.Namespace,
) -> Optional[int]:
    threshold = args.min_date or expected_trade_date
    
    if args.ignore_today and threshold is not None:
        from datetime import datetime
        today_int = int(datetime.now().strftime("%Y%m%d"))
        if threshold == today_int:
            # If threshold is today and we ignore today, we don't have a strict threshold for 'OK'
            # We can return None or a 'soft' threshold. Let's return None to skip STALE check.
            return None
            
    return threshold


def _print_group_header(title: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))


def _print_table_rows(
    *,
    cursor,
    tables: Iterable[TableCheck],
    expected_trade_date: Optional[int],
    args: argparse.Namespace,
) -> List[tuple[str, str, bool]]:
    """Returns a list of (table_name, status, is_failure)."""
    header = f"{'table':<28} {'max_date':<10} {'rows':<10} {'updated_at':<20} {'status':<8} {'threshold':<10}"
    print(header)
    print("-" * len(header))
    results: List[tuple[str, str, bool]] = []
    
    for table in tables:
        max_date, total_rows, updated_at = _fetch_table_stats(cursor, table)
        threshold = _resolve_threshold(
            table=table,
            expected_trade_date=expected_trade_date,
            args=args,
        )
        status = _status_for_date(max_date, threshold)
        
        is_failure = False
        if status in {"STALE", "EMPTY"}:
            if table.optional:
                status += "*"  # Mark as optional failure
            else:
                is_failure = True

        results.append((table.name, status, is_failure))
        
        print(
            f"{table.name:<28} {str(max_date):<10} {total_rows:<10} "
            f"{str(updated_at):<20} {status:<8} {str(threshold):<10}"
        )
    return results


def _print_watermarks(cursor, api_names: List[str]) -> None:
    if not api_names:
        return
    placeholders = ",".join(["%s"] * len(api_names))
    sql = (
        "SELECT api_name, water_mark, status, last_run_at, last_err "
        f"FROM meta_etl_watermark WHERE api_name IN ({placeholders}) "
        "ORDER BY api_name"
    )
    cursor.execute(sql, tuple(api_names))
    rows = cursor.fetchall()
    print("\nWatermarks")
    print("----------")
    for row in rows:
        print(row)


def main() -> None:
    args = parse_args()
    _apply_config_args(args)
    cfg = get_env_config()

    ods_tables = [
        TableCheck("ods_daily", "trade_date", "ods"),
        TableCheck("ods_daily_basic", "trade_date", "ods"),
        TableCheck("ods_adj_factor", "trade_date", "ods"),
    ]
    financial_tables = [
        TableCheck("ods_fina_indicator", "ann_date", "financial"),
        TableCheck("dwd_fina_indicator", "ann_date", "financial"),
        TableCheck("dws_fina_pit_daily", "trade_date", "financial"),
    ]
    feature_tables = [
        TableCheck("ods_margin", "trade_date", "features"),
        TableCheck("ods_margin_detail", "trade_date", "features"),
        TableCheck("ods_margin_target", "ann_date", "features"),
        TableCheck("ods_moneyflow", "trade_date", "features"),
        TableCheck("ods_moneyflow_ths", "trade_date", "features"),
        TableCheck("ods_cyq_chips", "trade_date", "features"),
        TableCheck("ods_stk_factor", "trade_date", "features"),
        TableCheck("ads_features_stock_daily", "trade_date", "features"),
    ]
    dwd_tables = [
        TableCheck("dwd_daily", "trade_date", "dwd"),
        TableCheck("dwd_daily_basic", "trade_date", "dwd"),
        TableCheck("dwd_adj_factor", "trade_date", "dwd"),
        # New DWD tables
        TableCheck("dwd_stock_daily_standard", "trade_date", "dwd"),
        TableCheck("dwd_fina_snapshot", "trade_date", "dwd"),
        TableCheck("dwd_margin_sentiment", "trade_date", "dwd", optional=True),  # T+1
        TableCheck("dwd_chip_stability", "trade_date", "dwd"),
        TableCheck("dwd_stock_label_daily", "trade_date", "dwd"),
    ]
    dws_tables = [
        TableCheck("dws_price_adj_daily", "trade_date", "dws"),
        # New DWS tables
        TableCheck("dws_tech_pattern", "trade_date", "dws"),
        TableCheck("dws_capital_flow", "trade_date", "dws"),
        TableCheck("dws_leverage_sentiment", "trade_date", "dws", optional=True),  # T+1
        TableCheck("dws_chip_dynamics", "trade_date", "dws"),
        # Enhanced factors
        TableCheck("dws_liquidity_factor", "trade_date", "dws"),
        TableCheck("dws_momentum_extended", "trade_date", "dws"),
        TableCheck("dws_quality_extended", "trade_date", "dws"),
        TableCheck("dws_risk_factor", "trade_date", "dws"),
    ]
    ads_tables = [
        TableCheck("ads_features_stock_daily", "trade_date", "ads"),
        TableCheck("ads_universe_daily", "trade_date", "ads"),
        # New ADS table
        TableCheck("ads_stock_score_daily", "trade_date", "ads"),
    ]

    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            expected_trade_date = args.expected_trade_date or _latest_trade_date(cursor)
            print(f"Expected latest trade_date: {expected_trade_date}")
            _print_watermarks(
                cursor,
                [
                    "base_trade_cal",
                    "base_stock",
                    "ods_daily",
                    "ods_daily_basic",
                    "ods_adj_factor",
                    "ods_fina_indicator",
                    "dwd_fina_indicator",
                    "dws",
                    "ads",
                ],
            )

            selected = None
            if args.categories:
                selected = {c.strip() for c in args.categories.split(",") if c.strip()}

            failures: List[str] = []

            if selected is None or "ods" in selected:
                _print_group_header("ODS Daily Tables")
                statuses = _print_table_rows(
                    cursor=cursor,
                    tables=ods_tables,
                    expected_trade_date=expected_trade_date,
                    args=args,
                )
                failures.extend([name for name, _, is_fail in statuses if is_fail])

            if selected is None or "financial" in selected:
                _print_group_header("Financial Tables")
                statuses = _print_table_rows(
                    cursor=cursor,
                    tables=financial_tables,
                    expected_trade_date=expected_trade_date,
                    args=args,
                )
                failures.extend([name for name, _, is_fail in statuses if is_fail])

            if selected is None or "features" in selected:
                _print_group_header("Feature Tables")
                statuses = _print_table_rows(
                    cursor=cursor,
                    tables=feature_tables,
                    expected_trade_date=expected_trade_date,
                    args=args,
                )
                failures.extend([name for name, _, is_fail in statuses if is_fail])

            if selected is None or "dwd" in selected:
                _print_group_header("DWD Tables")
                statuses = _print_table_rows(
                    cursor=cursor,
                    tables=dwd_tables,
                    expected_trade_date=expected_trade_date,
                    args=args,
                )
                failures.extend([name for name, _, is_fail in statuses if is_fail])

            if selected is None or "dws" in selected:
                _print_group_header("DWS Tables")
                statuses = _print_table_rows(
                    cursor=cursor,
                    tables=dws_tables,
                    expected_trade_date=expected_trade_date,
                    args=args,
                )
                failures.extend([name for name, _, is_fail in statuses if is_fail])

            if selected is None or "ads" in selected:
                _print_group_header("ADS Tables")
                statuses = _print_table_rows(
                    cursor=cursor,
                    tables=ads_tables,
                    expected_trade_date=expected_trade_date,
                    args=args,
                )
                failures.extend([name for name, _, is_fail in statuses if is_fail])

    if failures and args.fail_on_stale:
        raise SystemExit(f"Stale/empty tables detected: {', '.join(sorted(set(failures)))}")


if __name__ == "__main__":
    main()
