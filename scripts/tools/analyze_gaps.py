from etl.base.runtime import get_env_config, get_mysql_session, list_trade_dates
import logging
import argparse
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_missing_ranges(missing_dates):
    if not missing_dates:
        return []
    
    ranges = []
    start = missing_dates[0]
    prev = start
    
    for date in missing_dates[1:]:
        # Check if consecutive (simplified check, doesn't account for weekends/holidays strictly but good enough for grouping)
        # Actually, since we have the list of trade_dates, we can find indices.
        # But for quick reporting, just seeing the blocks of dates is enough.
        # Let's just group by year.
        pass
        
    # Better approach: Iterate trade_dates and check existence
    return []

def analyze_missing(cursor, table_name, trade_dates):
    start_date = trade_dates[0]
    end_date = trade_dates[-1]
    
    sql = f"""
    SELECT trade_date
    FROM {table_name} 
    WHERE trade_date BETWEEN %s AND %s 
    GROUP BY trade_date
    """
    cursor.execute(sql, (start_date, end_date))
    existing_dates = {row[0] for row in cursor.fetchall()}
    
    missing = sorted(list(set(trade_dates) - existing_dates))
    
    if not missing:
        return
        
    print(f"Table: {table_name} - Missing {len(missing)} dates")
    
    # Simple range finder
    ranges = []
    if not missing:
        return

    range_start = missing[0]
    prev = missing[0]
    
    # We need to know if they are consecutive in trade_dates
    trade_date_set = set(trade_dates)
    trade_date_list = sorted(list(trade_date_set))
    trade_date_idx = {d: i for i, d in enumerate(trade_date_list)}
    
    current_range_start = missing[0]
    prev_idx = trade_date_idx[missing[0]]
    
    for date in missing[1:]:
        curr_idx = trade_date_idx[date]
        if curr_idx != prev_idx + 1:
            # End of range
            print(f"  Gap: {current_range_start} -> {trade_date_list[prev_idx]}")
            current_range_start = date
        prev_idx = curr_idx
    
    # Last range
    print(f"  Gap: {current_range_start} -> {trade_date_list[prev_idx]}")

def main():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            # Get all trade dates 2019-now
            trade_dates = list_trade_dates(cursor, 20190101, 20260214)
            current_date_int = int(datetime.now().strftime('%Y%m%d'))
            trade_dates = [d for d in trade_dates if d <= current_date_int]
            
            check_tables = ['dws_price_adj_daily', 'dws_momentum_extended']
            
            for table in check_tables:
                analyze_missing(cursor, table, trade_dates)

if __name__ == "__main__":
    main()
