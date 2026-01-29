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

    def test_trade_cal_available(self):
        df = self.pro.trade_cal(start_date="20240101", end_date="20240131")
        self.assertFalse(df.empty)
        self.assertIn("cal_date", df.columns)

    def test_stock_basic_available(self):
        df = self.pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,is_hs",
        )
        self.assertFalse(df.empty)
        self.assertIn("ts_code", df.columns)


if __name__ == "__main__":
    unittest.main()
