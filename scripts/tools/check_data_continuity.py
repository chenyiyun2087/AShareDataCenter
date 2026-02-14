from etl.base.runtime import get_env_config, get_mysql_session, list_trade_dates
import logging
import argparse
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(message)s')

def check_continuity_backwards(cursor, table_name, start_date_desc, date_col='trade_date'):
    logging.info(f"Checking continuity for {table_name} starting backwards from {start_date_desc}...")
    
    # Financial tables are quarterly, not daily. Just report max date.
    if 'fina' in table_name:
        sql = f"SELECT MAX({date_col}) FROM {table_name}"
        cursor.execute(sql)
        max_date = cursor.fetchone()[0]
        print(f"  Financial Table (Quarterly): Latest {date_col} is {max_date}")
        return

    # Get all trade dates up to the start_date
    # Extend back to 1990 to cover all possible A-Share history
    all_dates = list_trade_dates(cursor, 19901219, start_date_desc)
    all_dates.sort(reverse=True)
    
    # Get existing dates for the table
    try:
        sql = f"SELECT DISTINCT {date_col} FROM {table_name} WHERE {date_col} <= %s ORDER BY {date_col} DESC"
        cursor.execute(sql, (start_date_desc,))
        existing_dates = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"  Error querying {table_name}: {e}")
        return
    
    if not existing_dates:
        print(f"  Result: No data found <= {start_date_desc}")
        return

    # Compare
    valid_window_start = None
    valid_window_end = existing_dates[0]
    
    # Find the pointer in all_dates matching the latest existing date
    try:
        current_idx = all_dates.index(valid_window_end)
    except ValueError:
        print(f"  Warning: Latest data date {valid_window_end} not found in dim_trade_cal.")
        # Fallback: try to find the nearest date in all_dates that is <= valid_window_end
        # Since both are sorted DESC, we iterate all_dates until we find one <= valid_window_end
        found = False
        for idx, d in enumerate(all_dates):
            if d <= valid_window_end:
                current_idx = idx
                found = True
                break
        if not found:
             print(f"  Error: Could not align dates.")
             return

    # Check continuity
    consecutive_count = 0
    # We only check up to the length of existing_dates or all_dates remaining
    limit = min(len(existing_dates), len(all_dates) - current_idx)
    
    for i in range(limit):
        existing_date = existing_dates[i]
        expected_date = all_dates[current_idx + i]
        
        if existing_date != expected_date:
            # Gap found
            print(f"  Gap found! Expected {expected_date}, but data has {existing_date}")
            valid_window_start = existing_dates[i-1] if i > 0 else valid_window_end
            break
        
        consecutive_count += 1
        valid_window_start = existing_date
        
    print(f"  Continuous Window: {valid_window_start} -> {valid_window_end} ({consecutive_count} trade days)")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=int, default=20260213, help="Start checking backwards from this date")
    args = parser.parse_args()
    
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            tables_to_check = {
                'ODS': [
                    ('ods_daily', 'trade_date'), ('ods_moneyflow', 'trade_date'), 
                    ('ods_margin', 'trade_date'), ('ods_margin_detail', 'trade_date'), 
                    ('ods_fina_indicator', 'ann_date'), ('ods_adj_factor', 'trade_date'), 
                    ('ods_daily_basic', 'trade_date')
                ],
                'DWD': [
                    ('dwd_daily', 'trade_date'), ('dwd_daily_basic', 'trade_date'), 
                    ('dwd_adj_factor', 'trade_date'), ('dwd_fina_indicator', 'ann_date'), 
                    ('dwd_margin_sentiment', 'trade_date'), ('dwd_chip_stability', 'trade_date')
                ],
                'DWS': [
                    ('dws_price_adj_daily', 'trade_date'), ('dws_fina_pit_daily', 'trade_date'),
                    ('dws_momentum_score', 'trade_date'), ('dws_value_score', 'trade_date'), 
                    ('dws_quality_score', 'trade_date'), ('dws_technical_score', 'trade_date'), 
                    ('dws_capital_score', 'trade_date'), ('dws_chip_score', 'trade_date'),
                    ('dws_liquidity_factor', 'trade_date'), ('dws_momentum_extended', 'trade_date'), 
                    ('dws_quality_extended', 'trade_date'), ('dws_risk_factor', 'trade_date'), 
                    ('dws_capital_flow', 'trade_date'), ('dws_leverage_sentiment', 'trade_date'), 
                    ('dws_chip_dynamics', 'trade_date'), ('dws_tech_pattern', 'trade_date')
                ],
                'ADS': [
                    ('ads_features_stock_daily', 'trade_date'), ('ads_stock_score_daily', 'trade_date')
                ]
            }
            
            for layer, tables in tables_to_check.items():
                print(f"\n=== {layer} LAYER ===")
                for t, col in tables:
                    check_continuity_backwards(cursor, t, args.date, col)

if __name__ == "__main__":
    main()
