from etl.base.runtime import get_env_config, get_mysql_session, list_trade_dates
import logging
import argparse
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_table_integrity(cursor, table_name, trade_dates):
    logging.info(f"Checking {table_name}...")
    
    # Get counts for all dates in one query if possible, or batched
    # For 7 years (~1700 dates), we can just group by trade_date
    start_date = trade_dates[0]
    end_date = trade_dates[-1]
    
    sql = f"""
    SELECT trade_date, COUNT(*) 
    FROM {table_name} 
    WHERE trade_date BETWEEN %s AND %s 
    GROUP BY trade_date
    """
    cursor.execute(sql, (start_date, end_date))
    result = {row[0]: row[1] for row in cursor.fetchall()}
    
    missing_dates = []
    low_count_dates = [] # Threshold depends on stock count, but let's say < 1000 is suspicious for recent years
    
    for date in trade_dates:
        count = result.get(str(date)) or result.get(date) # Handle str/int if needed
        if not count:
            missing_dates.append(date)
        elif count < 1000: # Heuristic: A-share market should have > 1000 stocks
            low_count_dates.append((date, count))
            
    return missing_dates, low_count_dates

def main():
    parser = argparse.ArgumentParser(description="Check data integrity")
    parser.add_argument("--start-date", type=int, default=20190101, help="Start date YYYYMMDD")
    parser.add_argument("--end-date", type=int, default=int(datetime.now().strftime('%Y%m%d')), help="End date YYYYMMDD")
    args = parser.parse_args()
    
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            logging.info(f"Fetching trade dates from {args.start_date} to {args.end_date}")
            trade_dates = list_trade_dates(cursor, args.start_date, args.end_date)
            # Filter out future dates just in case
            current_date_int = int(datetime.now().strftime('%Y%m%d'))
            trade_dates = [d for d in trade_dates if d <= current_date_int]
            
            logging.info(f"Found {len(trade_dates)} trade dates.")
            
            tables_to_check = [
                'dwd_daily',
                'dws_price_adj_daily',
                'dws_momentum_score',
                'dws_momentum_extended',
                'dws_value_score',
                'dws_risk_factor'
            ]
            
            report = {}
            
            for table in tables_to_check:
                missing, low = check_table_integrity(cursor, table, trade_dates)
                report[table] = {
                    'missing_count': len(missing),
                    'low_count': len(low),
                    'missing_samples': missing[:5],
                    'low_samples': low[:5]
                }
                
            print("\n" + "="*50)
            print(f"DATA INTEGRITY REPORT ({args.start_date}-{args.end_date})")
            print("="*50)
            for table, data in report.items():
                print(f"Table: {table}")
                print(f"  Missing Dates: {data['missing_count']}")
                if data['missing_count'] > 0:
                    print(f"    Samples: {data['missing_samples']}...")
                print(f"  Low Count Dates (<1000): {data['low_count']}")
                if data['low_count'] > 0:
                    print(f"    Samples: {data['low_samples']}...")
                print("-" * 30)

if __name__ == "__main__":
    main()
