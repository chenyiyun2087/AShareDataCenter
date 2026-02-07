"""
股票评分查询模块

从DWS评分表读取预计算的评分数据，提供查询和分析功能。
评分计算已移至 scripts/etl/dws/scoring.py (纯SQL实现)
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class ScoreQuery:
    """股票评分查询器 - 从DWS表读取评分"""
    
    def __init__(self, engine):
        """
        初始化查询器
        
        Parameters:
        -----------
        engine : sqlalchemy.engine.Engine
            数据库连接引擎
        """
        self.engine = engine
    
    def get_all_scores(self, trade_date: int) -> pd.DataFrame:
        """获取指定日期所有股票的各维度评分"""
        sql = """
        SELECT 
            m.trade_date, m.ts_code,
            m.momentum_score,
            v.value_score,
            q.quality_score,
            t.technical_score,
            c.capital_score,
            ch.chip_score,
            (COALESCE(m.momentum_score, 0) + COALESCE(v.value_score, 0) + 
             COALESCE(q.quality_score, 0) + COALESCE(t.technical_score, 0) + 
             COALESCE(c.capital_score, 0) + COALESCE(ch.chip_score, 0)) AS total_score
        FROM dws_momentum_score m
        LEFT JOIN dws_value_score v ON m.trade_date = v.trade_date AND m.ts_code = v.ts_code
        LEFT JOIN dws_quality_score q ON m.trade_date = q.trade_date AND m.ts_code = q.ts_code
        LEFT JOIN dws_technical_score t ON m.trade_date = t.trade_date AND m.ts_code = t.ts_code
        LEFT JOIN dws_capital_score c ON m.trade_date = c.trade_date AND m.ts_code = c.ts_code
        LEFT JOIN dws_chip_score ch ON m.trade_date = ch.trade_date AND m.ts_code = ch.ts_code
        WHERE m.trade_date = :trade_date
        """
        return pd.read_sql(text(sql), self.engine, params={"trade_date": trade_date})
    
    def get_top_stocks(self, trade_date: int, top_n: int = 50, 
                       min_score: float = 0) -> pd.DataFrame:
        """获取评分最高的股票"""
        df = self.get_all_scores(trade_date)
        if df.empty:
            return df
        
        df = df[df['total_score'] >= min_score]
        df = df.sort_values('total_score', ascending=False).head(top_n)
        df['rank'] = range(1, len(df) + 1)
        return df
    
    def get_stock_scores(self, ts_code: str, start_date: int, 
                         end_date: int) -> pd.DataFrame:
        """获取单只股票的评分趋势"""
        sql = """
        SELECT 
            m.trade_date, m.ts_code,
            m.momentum_score,
            v.value_score,
            q.quality_score,
            t.technical_score,
            c.capital_score,
            ch.chip_score,
            (COALESCE(m.momentum_score, 0) + COALESCE(v.value_score, 0) + 
             COALESCE(q.quality_score, 0) + COALESCE(t.technical_score, 0) + 
             COALESCE(c.capital_score, 0) + COALESCE(ch.chip_score, 0)) AS total_score
        FROM dws_momentum_score m
        LEFT JOIN dws_value_score v ON m.trade_date = v.trade_date AND m.ts_code = v.ts_code
        LEFT JOIN dws_quality_score q ON m.trade_date = q.trade_date AND m.ts_code = q.ts_code
        LEFT JOIN dws_technical_score t ON m.trade_date = t.trade_date AND m.ts_code = t.ts_code
        LEFT JOIN dws_capital_score c ON m.trade_date = c.trade_date AND m.ts_code = c.ts_code
        LEFT JOIN dws_chip_score ch ON m.trade_date = ch.trade_date AND m.ts_code = ch.ts_code
        WHERE m.ts_code = :ts_code
          AND m.trade_date BETWEEN :start_date AND :end_date
        ORDER BY m.trade_date
        """
        return pd.read_sql(text(sql), self.engine, 
                          params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date})
    
    def get_score_stats(self, trade_date: int) -> Dict:
        """获取评分统计"""
        df = self.get_all_scores(trade_date)
        if df.empty:
            return {}
        
        return {
            'count': len(df),
            'avg_score': df['total_score'].mean(),
            'max_score': df['total_score'].max(),
            'min_score': df['total_score'].min(),
            'std_score': df['total_score'].std(),
        }
    
    def screen_stocks(self, trade_date: int, criteria: Dict) -> pd.DataFrame:
        """
        自定义筛选股票
        
        criteria 格式: {'column': (min, max)}
        例如: {'total_score': (60, 100), 'momentum_score': (15, None)}
        """
        df = self.get_all_scores(trade_date)
        if df.empty:
            return df
        
        for col, (min_val, max_val) in criteria.items():
            if col in df.columns:
                if min_val is not None:
                    df = df[df[col] >= min_val]
                if max_val is not None:
                    df = df[df[col] <= max_val]
        
        return df.sort_values('total_score', ascending=False)
    
    def get_stocks_by_codes(self, trade_date: int, ts_codes: List[str]) -> pd.DataFrame:
        """
        查询指定股票代码的评分
        
        Parameters:
        -----------
        trade_date : int
            交易日期
        ts_codes : list
            股票代码列表，如 ['000001.SZ', '600519.SH']
        """
        if not ts_codes:
            return pd.DataFrame()
        
        # 构建IN查询
        placeholders = ', '.join([f':code_{i}' for i in range(len(ts_codes))])
        params = {"trade_date": trade_date}
        for i, code in enumerate(ts_codes):
            params[f'code_{i}'] = code
        
        sql = f"""
        SELECT 
            m.trade_date, m.ts_code,
            m.ret_5_score, m.ret_20_score, m.ret_60_score, 
            m.vol_ratio_score, m.turnover_score, m.momentum_score,
            v.pe_score, v.pb_score, v.ps_score, v.value_score,
            q.roe_score, q.margin_score, q.leverage_score, q.quality_score,
            t.macd_score, t.kdj_score, t.rsi_score, t.technical_score,
            c.elg_score, c.lg_score, c.capital_score,
            ch.winner_score, ch.cost_score, ch.chip_score,
            (COALESCE(m.momentum_score, 0) + COALESCE(v.value_score, 0) + 
             COALESCE(q.quality_score, 0) + COALESCE(t.technical_score, 0) + 
             COALESCE(c.capital_score, 0) + COALESCE(ch.chip_score, 0)) AS total_score
        FROM dws_momentum_score m
        LEFT JOIN dws_value_score v ON m.trade_date = v.trade_date AND m.ts_code = v.ts_code
        LEFT JOIN dws_quality_score q ON m.trade_date = q.trade_date AND m.ts_code = q.ts_code
        LEFT JOIN dws_technical_score t ON m.trade_date = t.trade_date AND m.ts_code = t.ts_code
        LEFT JOIN dws_capital_score c ON m.trade_date = c.trade_date AND m.ts_code = c.ts_code
        LEFT JOIN dws_chip_score ch ON m.trade_date = ch.trade_date AND m.ts_code = ch.ts_code
        WHERE m.trade_date = :trade_date
          AND m.ts_code IN ({placeholders})
        ORDER BY total_score DESC
        """
        return pd.read_sql(text(sql), self.engine, params=params)
    
    def print_stock_detail(self, trade_date: int, ts_codes: List[str]):
        """打印指定股票的详细评分 (表格形式)"""
        df = self.get_stocks_by_codes(trade_date, ts_codes)
        if df.empty:
            print(f"未找到股票数据: {ts_codes} (日期: {trade_date})")
            return
        
        print(f"\n=== 股票评分详情 (数据日期: {trade_date}) ===")
        print("维度满分: 动量25 | 价值20 | 质量20 | 技术15 | 资金10 | 筹码10 | 总分100")
        print("-" * 95)
        
        display_cols = ['ts_code', 'total_score', 
                       'momentum_score', 'value_score', 'quality_score',
                       'technical_score', 'capital_score', 'chip_score']
        
        # 重命名列以便显示
        df_display = df[display_cols].copy()
        df_display.columns = ['代码', '总分', '动量', '价值', '质量', '技术', '资金', '筹码']
        print(df_display.to_string(index=False))


def get_engine(host='localhost', port=3306, user='root', 
               password='', database='tushare_stock'):
    """创建数据库连接"""
    connection_string = (
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        f"?charset=utf8mb4"
    )
    return create_engine(connection_string, echo=False)


# 便捷函数
def query_top_stocks(trade_date: int, top_n: int = 50, **db_config):
    """快速查询Top股票"""
    engine = get_engine(**db_config)
    q = ScoreQuery(engine)
    return q.get_top_stocks(trade_date, top_n)


if __name__ == '__main__':
    import sys
    
    engine = get_engine(password='19871019')
    q = ScoreQuery(engine)
    trade_date = 20260206
    
    # 如果命令行指定了股票代码，查询这些股票
    if len(sys.argv) > 1:
        ts_codes = sys.argv[1:]
        print(f"查询股票: {ts_codes}")
        q.print_stock_detail(trade_date, ts_codes)
    else:
        # 默认展示Top 10
        print(f"=== {trade_date} 评分统计 ===")
        stats = q.get_score_stats(trade_date)
        print(f"股票数: {stats.get('count', 0)}")
        print(f"平均分: {stats.get('avg_score', 0):.2f}")
        print(f"最高分: {stats.get('max_score', 0):.2f}")
        
        print(f"\n=== Top 10 高分股票 (各维度详细分数) ===")
        print("维度满分: 动量25 | 价值20 | 质量20 | 技术15 | 资金10 | 筹码10 | 总分100")
        print("-" * 95)
        top = q.get_top_stocks(trade_date, top_n=10)
        display_cols = ['rank', 'ts_code', 'total_score', 
                       'momentum_score', 'value_score', 'quality_score',
                       'technical_score', 'capital_score', 'chip_score']
        top_display = top[display_cols].copy()
        top_display.columns = ['排名', '代码', '总分', '动量', '价值', '质量', '技术', '资金', '筹码']
        print(top_display.to_string(index=False))
        
        print("\n提示: 可指定股票代码查询详情，如:")
        print("  python score_query.py 000001.SZ 600519.SH")

