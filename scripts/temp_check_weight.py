import tushare as ts
import pandas as pd

token = "b7db1b7f209331ef7d15828d7f168f466f984677380b7fa2465fbdc9"
pro = ts.pro_api(token)

print("--- Testing historical index_weight for CSI 500 (2020-2021) ---")
df = pro.index_weight(index_code="000905.SH", start_date="20200101", end_date="20211231")
if df is not None and not df.empty:
    print(f"Rows: {len(df)}")
    print(f"Unique dates: {df['trade_date'].nunique()}")
    print(f"Min date: {df['trade_date'].min()}")
    print(f"Max date: {df['trade_date'].max()}")
else:
    print("No data found for the range 2020-2021")
