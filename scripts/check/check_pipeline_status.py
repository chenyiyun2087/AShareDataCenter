#!/usr/bin/env python3
"""CLI tool to check data pipeline status across all layers.

Usage:
    python scripts/check_pipeline_status.py --config config/etl.ini
    python scripts/check_pipeline_status.py --layer ods
    python scripts/check_pipeline_status.py --expected-date 20260206

Checks:
1. Data status is normal
2. Most recent trading day data is correctly recorded  
3. Next day is ready for data ingestion
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Add project scripts directory to sys.path to allow importing 'etl' package
scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

sys.path.insert(0, str(Path(__file__).resolve().parent))

from etl.base.runtime import get_env_config, get_mysql_connection
from etl.base.status_checks import (
    check_ods_status,
    check_dwd_status,
    check_dws_status,
    check_ads_status,
    check_data_status,
    print_status_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check data pipeline status")
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    parser.add_argument(
        "--layer",
        choices=["ods", "dwd", "dws", "ads", "all"],
        default="all",
        help="Layer to check (default: all)",
    )
    parser.add_argument(
        "--expected-date",
        type=int,
        default=None,
        help="Expected trade date (YYYYMMDD)",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit with non-zero status if issues found",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format",
    )
    return parser.parse_args()


def main() -> None:
    '''
    # 检查全部层
    python scripts/check_pipeline_status.py --config config/etl.ini
    # 单独检查某层
    python scripts/check_pipeline_status.py --config config/etl.ini --layer dwd
    # JSON 输出（适合程序调用）
    python scripts/check_pipeline_status.py --config config/etl.ini --json
    # 有问题时返回非零退出码
    python scripts/check_pipeline_status.py --config config/etl.ini --fail-on-issues
    :return:
    '''
    args = parse_args()
    
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
    
    if args.layer == "all":
        status = check_data_status(args.expected_date)
        
        if args.json:
            import json
            print(json.dumps({
                "is_healthy": status.is_healthy,
                "is_ready_for_next_day": status.is_ready_for_next_day,
                "expected_trade_date": status.expected_trade_date,
                "next_trade_date": status.next_trade_date,
                "summary": status.summary,
                "layers": {
                    layer.layer: {
                        "is_healthy": layer.is_healthy,
                        "is_ready_for_next": layer.is_ready_for_next,
                        "watermark": layer.watermark,
                        "tables": [
                            {"name": t.table_name, "max_date": t.max_date, "rows": t.row_count, "status": t.status}
                            for t in layer.table_statuses
                        ]
                    }
                    for layer in [status.ods_status, status.dwd_status, status.dws_status, status.ads_status]
                }
            }, indent=2))
        else:
            print_status_report(status)
        
        if args.fail_on_issues and not status.is_healthy:
            sys.exit(1)
    else:
        cfg = get_env_config()
        with get_mysql_connection(cfg) as conn:
            with conn.cursor() as cursor:
                if args.layer == "ods":
                    layer_status = check_ods_status(cursor, args.expected_date)
                elif args.layer == "dwd":
                    layer_status = check_dwd_status(cursor, args.expected_date)
                elif args.layer == "dws":
                    layer_status = check_dws_status(cursor, args.expected_date)
                elif args.layer == "ads":
                    layer_status = check_ads_status(cursor, args.expected_date)
        
        print(f"\n{layer_status.layer} Layer Status")
        print("=" * 40)
        print(f"Healthy: {'✅' if layer_status.is_healthy else '❌'}")
        print(f"Ready for Next: {'✅' if layer_status.is_ready_for_next else '❌'}")
        print(f"Watermark: {layer_status.watermark}")
        print(f"Message: {layer_status.message}")
        print(f"\nTables:")
        for ts in layer_status.table_statuses:
            icon = "✅" if ts.status == "OK" else ("⚠️" if ts.status == "STALE" else "❌")
            print(f"  {icon} {ts.table_name}: {ts.max_date} ({ts.row_count:,} rows) - {ts.status}")
        
        if args.fail_on_issues and not layer_status.is_healthy:
            sys.exit(1)


if __name__ == "__main__":
    main()
