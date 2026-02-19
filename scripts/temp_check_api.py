import tushare as ts
import pandas as pd
import os

token = "b7db1b7f209331ef7d15828d7f168f466f984677380b7fa2465fbdc9"
pro = ts.pro_api(token)

indices = ["000001.SH", "399001.SZ", "000300.SH", "000905.SH", "000016.SH", "000688.SH", "000133.SH", "000029.SH", "399330.SZ", "399006.SZ"]

print("--- Testing index_member ---")
for code in indices[:3]:
    df = pro.index_member(index_code=code)
    print(f"{code}: {len(df) if df is not None else 0} rows")

print("\n--- Testing index_dailybasic on missing indices ---")
missing_tech = ["000688.SH", "000133.SH", "000029.SH", "399330.SZ"]
for code in missing_tech:
    df = pro.index_dailybasic(ts_code=code, start_date="20240101", end_date="20240110")
    print(f"{code}: {len(df) if df is not None else 0} rows")
