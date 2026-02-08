#!/usr/bin/env python3
"""
Fama-French 评分系统运行脚本

用法:
    # 增量模式 (计算最新交易日)
    python scripts/sync/run_fama_score.py --mode incremental
    
    # 全量模式 (从指定日期重算)
    python scripts/sync/run_fama_score.py --mode full --start-date 20200101
    
    # 指定单日
    python scripts/sync/run_fama_score.py --trade-date 20260206
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from etl.base.runtime import get_env_config, get_mysql_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Fama-French scoring")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--start-date", type=int, help="Start date for full mode")
    parser.add_argument("--trade-date", type=int, help="Single trade date")
    parser.add_argument("--config", help="Path to etl.ini")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.info(f"Fama-French 评分启动: {args}")
    
    # 延迟导入，确保路径设置完成
    from score.fama_score.fama_scoring import run_fama_scoring
    
    cfg = get_env_config(args.config)
    
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            if args.trade_date:
                # 单日模式
                run_fama_scoring(cursor, args.trade_date)
                conn.commit()
            elif args.mode == "incremental":
                # 增量模式: 获取最新交易日
                cursor.execute("SELECT MAX(trade_date) FROM ods_daily")
                row = cursor.fetchone()
                if row and row[0]:
                    trade_date = row[0]
                    logger.info(f"增量模式: 计算 {trade_date}")
                    run_fama_scoring(cursor, trade_date)
                    conn.commit()
                else:
                    logger.warning("无可用交易日")
            else:
                # 全量模式
                if not args.start_date:
                    raise ValueError("全量模式需要指定 --start-date")
                cursor.execute(
                    "SELECT DISTINCT trade_date FROM ods_daily WHERE trade_date >= %s ORDER BY trade_date",
                    (args.start_date,)
                )
                dates = [row[0] for row in cursor.fetchall()]
                total = len(dates)
                for i, trade_date in enumerate(dates, 1):
                    logger.info(f"进度: {i}/{total} - {trade_date}")
                    run_fama_scoring(cursor, trade_date)
                    conn.commit()
    
    logger.info("Fama-French 评分完成")


if __name__ == "__main__":
    main()
