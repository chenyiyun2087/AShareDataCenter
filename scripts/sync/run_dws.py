#!/usr/bin/env python3
import argparse
import logging
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Add project scripts directory to sys.path to allow importing 'etl' package
scripts_dir = Path(__file__).resolve().parents[1]
if str(scripts_dir) not in sys.path:
    sys.path.insert(0, str(scripts_dir))

from etl.dws import run_full, run_incremental, run_leverage_sentiment_incremental
from etl.base.runtime import (
    ensure_watermark,
    get_env_config,
    get_mysql_session,
    get_watermark,
    list_trade_dates,
    update_watermark,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DWS layer ETL")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", type=int, default=20100101)
    parser.add_argument("--end-date", type=int)
    parser.add_argument("--config", default=None, help="Path to etl.ini")

    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument(
        "--init-watermark",
        action="store_true",
        help="Initialize dws watermark if missing (uses start-date - 1).",
    )
    parser.add_argument(
        "--chunk-by",
        choices=["none", "year", "month"],
        default="none",
        help="Split incremental backfill into chunks for faster processing.",
    )
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers for chunked backfill.")
    parser.add_argument(
        "--estimate-minutes-per-day",
        type=float,
        default=20.0,
        help="Estimated runtime (minutes) per trade day for total DWS in your environment.",
    )
    parser.add_argument("--estimate-only", action="store_true", help="Only print runtime estimate and exit.")
    parser.add_argument(
        "--disable-watermark",
        action="store_true",
        help="Disable watermark updates (used by internal chunk workers).",
    )
    parser.add_argument(
        "--only-leverage-sentiment",
        action="store_true",
        help="Only refresh dws_leverage_sentiment for the specified date range.",
    )
    return parser.parse_args()


def _apply_env_overrides(args: argparse.Namespace) -> None:
    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            cwd_path = (Path.cwd() / config_path).resolve()
            if cwd_path.exists():
                config_path = cwd_path
            else:
                root_path = (scripts_dir.parent / config_path).resolve()
                config_path = root_path if root_path.exists() else cwd_path
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
    if args.disable_watermark:
        os.environ["DWS_DISABLE_WATERMARK"] = "1"


def _iter_chunks(start_date: int, end_date: int, chunk_by: str) -> list[tuple[int, int]]:
    if chunk_by == "none":
        return [(start_date, end_date)]
    chunks: list[tuple[int, int]] = []
    y, m = divmod(start_date // 100, 100)
    cur = y * 100 + m
    end_ym = end_date // 100
    while cur <= end_ym:
        yy = cur // 100
        mm = cur % 100
        chunk_start = max(start_date, yy * 10000 + mm * 100 + 1)
        if chunk_by == "year":
            chunk_end = min(end_date, yy * 10000 + 1231)
            cur = (yy + 1) * 100 + 1
        else:
            if mm in (1, 3, 5, 7, 8, 10, 12):
                month_end = 31
            elif mm == 2:
                month_end = 29
            else:
                month_end = 30
            chunk_end = min(end_date, yy * 10000 + mm * 100 + month_end)
            cur = (yy + 1) * 100 + 1 if mm == 12 else yy * 100 + (mm + 1)
        chunks.append((chunk_start, chunk_end))
    return chunks


def _estimate_runtime(args: argparse.Namespace) -> None:
    if args.mode != "incremental" or not args.end_date:
        return
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            trade_dates = list_trade_dates(cursor, args.start_date, args.end_date)
    total_trade_days = len(trade_dates)
    workers = max(1, args.workers)
    est_minutes = total_trade_days * args.estimate_minutes_per_day / workers
    est_hours = est_minutes / 60.0
    logging.info(
        "Estimated runtime: trade_days=%s, per_day=%.1f min, workers=%s => total≈%.1f h (%.1f days)",
        total_trade_days,
        args.estimate_minutes_per_day,
        workers,
        est_hours,
        est_hours / 24.0,
    )


def _build_child_cmd(args: argparse.Namespace, chunk_start: int, chunk_end: int) -> list[str]:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--mode",
        "incremental",
        "--start-date",
        str(chunk_start),
        "--end-date",
        str(chunk_end),
        "--chunk-by",
        "none",
        "--workers",
        "1",
        "--disable-watermark",
    ]
    if args.config:
        cmd += ["--config", args.config]
    if args.host:
        cmd += ["--host", args.host]
    if args.port is not None:
        cmd += ["--port", str(args.port)]
    if args.user:
        cmd += ["--user", args.user]
    if args.password:
        cmd += ["--password", args.password]
    if args.database:
        cmd += ["--database", args.database]
    if args.only_leverage_sentiment:
        cmd.append("--only-leverage-sentiment")
    return cmd


def _run_chunked_backfill(args: argparse.Namespace) -> None:
    assert args.end_date is not None
    chunks = _iter_chunks(args.start_date, args.end_date, args.chunk_by)
    logging.info("Running chunked incremental backfill: chunk_by=%s, workers=%s, chunks=%s", args.chunk_by, args.workers, len(chunks))

    if args.workers <= 1:
        for st, ed in chunks:
            logging.info("Chunk %s-%s start", st, ed)
            run_incremental(st, ed)
            logging.info("Chunk %s-%s done", st, ed)
    else:
        futures = {}
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for st, ed in chunks:
                cmd = _build_child_cmd(args, st, ed)
                futures[pool.submit(subprocess.run, cmd, check=True)] = (st, ed)
            for fut in as_completed(futures):
                st, ed = futures[fut]
                fut.result()
                logging.info("Chunk %s-%s done", st, ed)

    if not args.disable_watermark:
        cfg = get_env_config()
        with get_mysql_session(cfg) as conn:
            with conn.cursor() as cursor:
                update_watermark(cursor, "dws", args.end_date, "SUCCESS")
                conn.commit()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()
    logging.info(f"Starting DWS ETL with args: {args}")
    _apply_env_overrides(args)

    if args.init_watermark and not args.disable_watermark:
        cfg = get_env_config()
        with get_mysql_session(cfg) as conn:
            with conn.cursor() as cursor:
                last_date = get_watermark(cursor, "dws")
                if last_date is None:
                    ensure_watermark(cursor, "dws", args.start_date - 1)
                    conn.commit()

    _estimate_runtime(args)
    if args.estimate_only:
        logging.info("Estimate-only mode, exiting.")
        return

    if args.only_leverage_sentiment:
        if args.mode != "incremental":
            raise RuntimeError("--only-leverage-sentiment requires --mode incremental")
        if not args.end_date:
            raise RuntimeError("--only-leverage-sentiment requires --end-date")
        run_leverage_sentiment_incremental(args.start_date, args.end_date)
        logging.info("DWS leverage sentiment refresh completed successfully")
        return

    if args.mode == "full":
        run_full(args.start_date, args.end_date)
    elif args.chunk_by != "none" and args.end_date is not None:
        _run_chunked_backfill(args)
    else:
        run_incremental(args.start_date if args.start_date != 20100101 else None, args.end_date)

    logging.info("DWS ETL completed successfully")


if __name__ == "__main__":
    main()
