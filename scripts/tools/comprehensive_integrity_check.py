from etl.base.runtime import get_env_config, get_mysql_session, list_trade_dates
import sys
import os
import logging
from datetime import datetime

# Add project scripts directory to sys.path
scripts_dir = os.getcwd() + "/scripts"
if scripts_dir not in sys.path:
    sys.path.insert(0, scripts_dir)

logging.basicConfig(level=logging.INFO, format='%(message)s')

def check_table(cursor, table_name, trade_dates, date_col='trade_date', threshold=4000):
    """Check table for missing dates and low record counts."""
    start_date = 20200101
    end_date = 20260217
    
    # Financial tables might use ann_date or report_type range
    if date_col == 'ann_date':
        start_date = 20200101 # Financial data can be sparse by day
        threshold = 50 # Heuristic for daily announcements
    
    sql = f"""
    SELECT {date_col}, COUNT(*) 
    FROM {table_name} 
    WHERE {date_col} BETWEEN %s AND %s 
    GROUP BY {date_col}
    """
    try:
        cursor.execute(sql, (start_date, end_date))
        result = {row[0]: row[1] for row in cursor.fetchall()}
    except Exception as e:
        return f"ERROR: {str(e)}", []

    missing = []
    low = []
    
    # Check against trade_dates for daily tables
    dates_to_check = trade_dates if date_col == 'trade_date' else sorted(result.keys())
    
    if not result:
        return "EMPTY", []

    for d in trade_dates if date_col == 'trade_date' else []:
        cnt = result.get(str(d)) or result.get(int(d))
        if not cnt:
            missing.append(d)
        elif cnt < threshold:
            low.append((d, cnt))
            
    return missing, low

def main():
    cfg = get_env_config()
    with get_mysql_session(cfg) as conn:
        with conn.cursor() as cursor:
            trade_dates = list_trade_dates(cursor, 20200101, 20260217)
            trade_dates = [d for d in trade_dates if d <= 20260217]
            
            # Grouped tables
            categories = {
                "ODS Price/Basic": ['ods_daily', 'ods_daily_basic', 'ods_adj_factor'],
                "ODS Features": ['ods_margin', 'ods_margin_detail', 'ods_moneyflow', 'ods_moneyflow_ths', 'ods_cyq_perf', 'ods_stk_factor'],
                "DWD Layer": ['dwd_daily', 'dwd_daily_basic', 'dwd_adj_factor', 'dwd_stock_daily_standard', 'dwd_stock_label_daily'],
                "DWS Factors": ['dws_liquidity_factor', 'dws_momentum_extended', 'dws_quality_extended', 'dws_risk_factor'],
                "DWS Scores": ['dws_capital_score', 'dws_chip_score', 'dws_momentum_score', 'dws_quality_score', 'dws_technical_score', 'dws_value_score'],
                "ADS Layer": ['ads_features_stock_daily', 'ads_stock_score_daily', 'ads_universe_daily']
            }
            
            fina_tables = ['ods_fina_indicator', 'dwd_fina_indicator', 'dwd_fina_snapshot']
            
            print("="*60)
            print(f"COMPREHENSIVE DATA INTEGRITY REPORT (20200101 - 20260217)")
            print("="*60)
            
            for cat, tables in categories.items():
                print(f"\n>>> {cat}")
                for table in tables:
                    missing, low = check_table(cursor, table, trade_dates)
                    if isinstance(missing, str):
                        print(f"  [{table: <25}] {missing}")
                        continue
                    
                    status = "OK" if not missing and not low else "!!!"
                    m_count = len(missing)
                    l_count = len(low)
                    
                    print(f"  [{table: <25}] {status} | Missing: {m_count: >4} | Low: {l_count: >4}")
                    if m_count > 0:
                        print(f"      Missing Samples: {missing[:5]}")
                    if l_count > 0:
                        print(f"      Low Count Samples (first 3): {low[:3]}")

            print("\n>>> Financial Layer")
            for table in fina_tables:
                # Financial tables use ann_date and have different density, 
                # except dwd_fina_snapshot which uses trade_date
                date_col = 'ann_date' if 'snapshot' not in table else 'trade_date'
                missing, low = check_table(cursor, table, trade_dates, date_col=date_col, threshold=10)
                if isinstance(missing, str):
                    print(f"  [{table: <25}] {missing}")
                    continue
                
                # For financial, we just care if it's empty or has extreme gaps
                curr_date_col = date_col
                cursor.execute(f"SELECT MIN({curr_date_col}), MAX({curr_date_col}), COUNT(*) FROM {table} WHERE {curr_date_col} BETWEEN 20200101 AND 20260217")
                amin, amax, total = cursor.fetchone()
                print(f"  [{table: <25}] Total: {total: >7} | Range: {amin} - {amax}")

if __name__ == "__main__":
    main()
