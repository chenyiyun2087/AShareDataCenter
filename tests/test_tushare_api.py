import os
import unittest

import tushare as ts


class TuShareApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        token = os.environ.get("TUSHARE_TOKEN")
        if not token:
            raise unittest.SkipTest("TUSHARE_TOKEN is not set")
        cls.pro = ts.pro_api(token)
        cls.ts_code = "301251.SZ"
        cls.start_date = "20240101"
        cls.end_date = "20240131"

    def test_trade_cal_available(self):
        df = self.pro.trade_cal(start_date=self.start_date, end_date=self.end_date)
        self.assertFalse(df.empty)
        self.assertIn("cal_date", df.columns)
        print("trade_cal:", df.head(3).to_dict(orient="records"))

    def test_stock_basic_available(self):
        df = self.pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,is_hs",
        )
        self.assertFalse(df.empty)
        self.assertIn("ts_code", df.columns)
        print("stock_basic:", df.head(3).to_dict(orient="records"))

    def test_stock_info(self):
        df = self.pro.stock_basic(
            ts_code=self.ts_code,
            fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,is_hs",
        )
        self.assertFalse(df.empty)
        print("stock_info:", df.to_dict(orient="records"))

    def test_daily_line(self):
        df = self.pro.daily(ts_code=self.ts_code, start_date=self.start_date, end_date=self.end_date)
        self.assertFalse(df.empty)
        print("daily_line:", df.head(1).to_dict(orient="records"))

    def test_daily_basic(self):
        df = self.pro.daily_basic(
            ts_code=self.ts_code,
            start_date=self.start_date,
            end_date=self.end_date,
            fields="ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv,circ_mv",
        )
        self.assertFalse(df.empty)
        print("daily_basic:", df.head(3).to_dict(orient="records"))

    def test_adj_factor(self):
        df = self.pro.adj_factor(ts_code=self.ts_code, start_date=self.start_date, end_date=self.end_date)
        self.assertFalse(df.empty)
        print("adj_factor:", df.head(3).to_dict(orient="records"))

    def test_fina_indicator(self):
        df = self.pro.fina_indicator(
            ts_code=self.ts_code,
            start_date="20230101",
            end_date="20241231",
            fields="ts_code,ann_date,end_date,report_type,roe,grossprofit_margin,debt_to_assets,netprofit_margin,op_income,total_assets,total_hldr_eqy",
        )
        self.assertFalse(df.empty)
        print("fina_indicator:", df.head(3).to_dict(orient="records"))


if __name__ == "__main__":
    unittest.main()
