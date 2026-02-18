import logging
import sys
import os
import pymysql

# Add project root to path
sys.path.append(os.getcwd())

from scripts.etl.base.runtime import get_env_config, list_trade_dates
from scripts.etl.dwd.runner import (
    load_dwd_fina_snapshot,
    load_dwd_chip_stability
)
from scripts.etl.dws.runner import (
    _run_fina_pit,
    _run_tech_pattern,
    _run_chip_dynamics
)
from scripts.etl.dws.scoring import (
    _run_quality_score,
    _run_technical_score,
    _run_chip_score
)
from scripts.etl.ads.runner import _run_ads_batch

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def refill_quality_gap(cursor, trade_dates):
    if not trade_dates:
        return
    
    # Chunk by year
    chunks = []
    current_chunk = []
    current_year = trade_dates[0] // 10000
    for date in trade_dates:
        year = date // 10000
        if year != current_year:
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = [date]
            current_year = year
        else:
            current_chunk.append(date)
    if current_chunk:
        chunks.append(current_chunk)
        
    for i, chunk in enumerate(chunks, 1):
        start = chunk[0]
        end = chunk[-1]
        logging.info(f"Processing Quality Chunk {i}/{len(chunks)}: {start}-{end}")
        
        logging.info(f"  Updating PIT data (BATCH)...")
        _run_fina_pit(cursor, start, end)
        
        logging.info(f"  Loading DWD Snapshot for {len(chunk)} days...")
        for date in chunk:
            load_dwd_fina_snapshot(cursor, date)
            
        logging.info(f"  Refilling Quality Score (BATCH)...")
        _run_quality_score(cursor, start, end)

def refill_technical_gap(cursor, trade_dates):
    if not trade_dates:
        return
        
    # Chunk by year
    chunks = []
    current_chunk = []
    current_year = trade_dates[0] // 10000
    for date in trade_dates:
        year = date // 10000
        if year != current_year:
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = [date]
            current_year = year
        else:
            current_chunk.append(date)
    if current_chunk:
        chunks.append(current_chunk)
        
    for i, chunk in enumerate(chunks, 1):
        start = chunk[0]
        end = chunk[-1]
        logging.info(f"Refilling Technical Score Chunk {i}/{len(chunks)}: {start}-{end}")
        _run_tech_pattern(cursor, start, end)
        _run_technical_score(cursor, start, end)

def refill_chip_gap(cursor, trade_dates):
    if not trade_dates:
        return
    start = trade_dates[0]
    end = trade_dates[-1]
    
    logging.info(f"Loading DWD Chip Stability for {len(trade_dates)} days...")
    for date in trade_dates:
        load_dwd_chip_stability(cursor, date)
    
    logging.info(f"Refilling Chip Dynamics/Score (BATCH): {start}-{end}")
    _run_chip_dynamics(cursor, start, end)
    _run_chip_score(cursor, start, end)

def refill_ads_gap(cursor, start_date, end_date):
    logging.info(f"Refilling ADS Layer (BATCH) from {start_date} to {end_date}...")
    
    # Chunk by month for safety
    all_dates = list_trade_dates(cursor, start_date, end_date)
    if not all_dates:
        return

    chunks = []
    current_chunk = []
    current_month = all_dates[0] // 100
    
    for date in all_dates:
        month = date // 100
        if month != current_month:
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = [date]
            current_month = month
        else:
            current_chunk.append(date)
    if current_chunk:
        chunks.append(current_chunk)
        
    for i, chunk in enumerate(chunks, 1):
        chunk_start = chunk[0]
        chunk_end = chunk[-1]
        logging.info(f"  Processing ADS Chunk {i}/{len(chunks)}: {chunk_start}-{chunk_end}")
        _run_ads_batch(cursor, chunk_start, chunk_end)

def main():
    cfg = get_env_config()
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        autocommit=True
    )
    
    # Define gaps
    target_tasks = {
        "quality": (20200101, 20200105), # Fix 20200102
        "technical": (20210101, 20260213), # Fix 2021-2026 gaps
        "chip": (0, 0) # Skip
    }
    
    # ADS covers everything
    ads_range = (20200101, 20260213)

    try:
        with conn.cursor() as cursor:
            # 1. Quality Gap (Chunked)
            start, end = target_tasks["quality"]
            dates = list_trade_dates(cursor, start, end)
            refill_quality_gap(cursor, dates)
            
            # 2. Technical Gap (Batch)
            start, end = target_tasks["technical"]
            dates = list_trade_dates(cursor, start, end)
            refill_technical_gap(cursor, dates)
            
            # 3. Chip Gap (Batch)
            start, end = target_tasks["chip"]
            dates = list_trade_dates(cursor, start, end)
            refill_chip_gap(cursor, dates)
            
            # 4. ADS Refill (Chunked)
            start, end = ads_range
            refill_ads_gap(cursor, start, end)
            
        logging.info("All gaps refilled successfully.")
    except Exception as e:
        logging.error(f"Failed to refill gaps: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
