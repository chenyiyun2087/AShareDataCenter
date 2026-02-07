#!/usr/bin/env python3
import time
import argparse
import logging
import os
from pathlib import Path
import tushare as ts
from requests import exceptions as requests_exceptions
from etl.base.runtime import get_tushare_token

def test_rate(token: str, rate_limit: int, duration_sec: int = 10) -> bool:
    """
    Test a specific rate limit (requests per minute).
    Returns True if successful (no errors), False otherwise.
    """
    pro = ts.pro_api(token)
    interval = 60.0 / rate_limit
    print(f"Testing rate: {rate_limit} req/min (interval: {interval:.4f}s) for {duration_sec} seconds...")
    
    start_time = time.time()
    count = 0
    try:
        while time.time() - start_time < duration_sec:
            # Simple fetch: trade_cal is usually lightweight and free
            pro.trade_cal(exchange='SSE', start_date='20200101', end_date='20200105')
            count += 1
            time.sleep(interval)
            
        print(f"  -> Success: Made {count} requests in {duration_sec} seconds.")
        return True
    except Exception as e:
        print(f"  -> Failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Test TuShare API Rate Limit")
    parser.add_argument("--token", default=None)
    parser.add_argument("--start-rate", type=int, default=60, help="Start requests per minute")
    parser.add_argument("--step", type=int, default=60, help="Increase step")
    parser.add_argument("--max-rate", type=int, default=600, help="Max requests per minute")
    parser.add_argument("--duration", type=int, default=5, help="Test duration per step (seconds)")
    parser.add_argument("--config", default=None, help="Path to etl.ini")
    args = parser.parse_args()

    if args.config:
        config_path = Path(args.config).expanduser()
        if not config_path.is_absolute():
            config_path = (Path.cwd() / config_path).resolve()
        if not config_path.exists():
            print(f"Error: config file not found: {config_path}")
            return
        os.environ["ETL_CONFIG_PATH"] = str(config_path)

    token = args.token or get_tushare_token()
    if not token:
        print("Error: Missing TuShare token.")
        return

    current_rate = args.start_rate
    max_safe_rate = 0

    while current_rate <= args.max_rate:
        if test_rate(token, current_rate, args.duration):
            max_safe_rate = current_rate
            current_rate += args.step
        else:
            print(f"\nLimit reached! Max stable rate is approx: {max_safe_rate} req/min")
            break
            
    if current_rate > args.max_rate:
        print(f"\nReached max test rate ({args.max_rate} req/min) without error.")

if __name__ == "__main__":
    main()
