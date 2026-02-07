#!/usr/bin/env python3
"""
历史数据回补脚本
用于回补6年历史数据支持回测

用法:
    # 回补全部数据 (2020-2024)
    python run_backfill.py --start-year 2020 --end-year 2024

    # 只回补ODS特征数据
    python run_backfill.py --start-year 2020 --end-year 2024 --step ods_features

    # 分年回补 (避免接口限流)
    python run_backfill.py --start-year 2020 --end-year 2020 --step ods_features
    python run_backfill.py --start-year 2021 --end-year 2021 --step ods_features
    ...

步骤说明:
    1. ods_features: 回补 stk_factor, cyq_perf, moneyflow, margin 等
    2. dwd: 重建 DWD 层 (含 dwd_fina_snapshot)
    3. dws: 重建 DWS 评分表
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# 设置项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent  # 从 backfill/ 向上两级
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SYNC_DIR = SCRIPTS_DIR / "sync"
PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="历史数据回补脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--start-year", type=int, required=True, help="起始年份 (如 2020)")
    parser.add_argument("--end-year", type=int, required=True, help="结束年份 (如 2024)")
    parser.add_argument(
        "--step",
        choices=["all", "ods_features", "dwd", "dws"],
        default="all",
        help="执行步骤: all=全部, ods_features=ODS特征, dwd=DWD层, dws=DWS评分 (默认: all)"
    )
    parser.add_argument(
        "--apis",
        default="margin,margin_detail,margin_target,moneyflow,cyq_perf,stk_factor",
        help="ODS特征API列表 (默认: margin,margin_detail,margin_target,moneyflow,cyq_perf,stk_factor)"
    )
    parser.add_argument("--cyq-rate-limit", type=int, default=150, help="筹码接口限流 (默认: 150/分钟)")
    parser.add_argument("--dry-run", action="store_true", help="仅显示命令，不执行")
    return parser.parse_args()


def run_command(cmd: list, dry_run: bool = False) -> bool:
    """执行命令"""
    cmd_str = " ".join(str(c) for c in cmd)
    logger.info(f"执行命令: {cmd_str}")
    
    if dry_run:
        logger.info("[DRY RUN] 跳过执行")
        return True
    
    # 设置PYTHONPATH确保子模块可以正确导入
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SCRIPTS_DIR)
    
    try:
        result = subprocess.run(cmd, check=True, cwd=PROJECT_ROOT, env=env)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        logger.error(f"命令执行失败: {e}")
        return False


def step_ods_features(start_date: int, end_date: int, apis: str, cyq_rate_limit: int, dry_run: bool) -> bool:
    """步骤1: 回补ODS特征数据"""
    logger.info(f"\n{'='*60}")
    logger.info(f"步骤1: 回补ODS特征数据 ({start_date} ~ {end_date})")
    logger.info(f"APIs: {apis}")
    logger.info(f"{'='*60}")
    
    cmd = [
        PYTHON, SYNC_DIR / "run_ods_features.py",
        "--start-date", str(start_date),
        "--end-date", str(end_date),
        "--apis", apis,
        "--cyq-rate-limit", str(cyq_rate_limit),
    ]
    return run_command(cmd, dry_run)


def step_dwd(start_date: int, dry_run: bool) -> bool:
    """步骤2: 重建DWD层"""
    logger.info(f"\n{'='*60}")
    logger.info(f"步骤2: 重建DWD层 (从 {start_date} 开始)")
    logger.info(f"{'='*60}")
    
    cmd = [
        PYTHON, SYNC_DIR / "run_dwd.py",
        "--mode", "full",
        "--start-date", str(start_date),
    ]
    return run_command(cmd, dry_run)


def step_dws(start_date: int, dry_run: bool) -> bool:
    """步骤3: 重建DWS评分表"""
    logger.info(f"\n{'='*60}")
    logger.info(f"步骤3: 重建DWS评分表 (从 {start_date} 开始)")
    logger.info(f"{'='*60}")
    
    cmd = [
        PYTHON, SYNC_DIR / "run_dws.py",
        "--mode", "full",
        "--start-date", str(start_date),
        "--init-watermark",
    ]
    return run_command(cmd, dry_run)


def main() -> None:
    args = parse_args()
    
    if args.end_year < args.start_year:
        raise SystemExit("错误: end-year 必须 >= start-year")
    
    start_date = int(f"{args.start_year}0101")
    end_date = int(f"{args.end_year}1231")
    
    logger.info(f"\n{'#'*60}")
    logger.info(f"# 历史数据回补")
    logger.info(f"# 时间范围: {args.start_year} ~ {args.end_year}")
    logger.info(f"# 执行步骤: {args.step}")
    logger.info(f"{'#'*60}")
    
    if args.dry_run:
        logger.info(">>> DRY RUN 模式: 仅显示命令 <<<")
    
    steps_to_run = []
    if args.step in ("all", "ods_features"):
        steps_to_run.append(("ods_features", lambda: step_ods_features(
            start_date, end_date, args.apis, args.cyq_rate_limit, args.dry_run
        )))
    if args.step in ("all", "dwd"):
        steps_to_run.append(("dwd", lambda: step_dwd(start_date, args.dry_run)))
    if args.step in ("all", "dws"):
        steps_to_run.append(("dws", lambda: step_dws(start_date, args.dry_run)))
    
    start_time = datetime.now()
    results = {}
    
    for step_name, step_func in steps_to_run:
        logger.info(f"\n开始执行: {step_name}")
        results[step_name] = step_func()
        if not results[step_name] and not args.dry_run:
            logger.error(f"步骤 {step_name} 失败，停止执行")
            break
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # 汇总结果
    logger.info(f"\n{'='*60}")
    logger.info("回补完成汇总:")
    for step_name, success in results.items():
        status = "✅ 成功" if success else "❌ 失败"
        logger.info(f"  {step_name}: {status}")
    logger.info(f"总耗时: {duration}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
