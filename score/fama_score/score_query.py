"""
Fama-French 评分查询模块
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def get_engine(host: str = 'localhost', port: int = 3306, user: str = 'root',
               password: str = '', database: str = 'ashare') -> Engine:
    """创建数据库引擎"""
    from sqlalchemy import create_engine
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


class FamaScoreQuery:
    """Fama-French 评分查询类"""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def get_all_scores(self, trade_date: int) -> pd.DataFrame:
        """获取指定日期所有股票的 Fama 评分"""
        sql = """
        SELECT 
            s.trade_date, s.ts_code,
            ds.name,
            od.pct_chg,
            CASE WHEN od.pct_chg >= 9.9 THEN '涨停' 
                 WHEN od.pct_chg <= -9.9 THEN '跌停'
                 ELSE '' END AS limit_flag,
            s.size_score,
            m.momentum_score,
            v.value_score,
            q.quality_score,
            t.technical_score,
            c.capital_score,
            ch.chip_score,
            (COALESCE(s.size_score, 0) + COALESCE(m.momentum_score, 0) + 
             COALESCE(v.value_score, 0) + COALESCE(q.quality_score, 0) + 
             COALESCE(t.technical_score, 0) + COALESCE(c.capital_score, 0) + 
             COALESCE(ch.chip_score, 0)) AS total_score
        FROM dws_fama_size_score s
        LEFT JOIN dws_fama_momentum_score m ON s.trade_date = m.trade_date AND s.ts_code = m.ts_code
        LEFT JOIN dws_fama_value_score v ON s.trade_date = v.trade_date AND s.ts_code = v.ts_code
        LEFT JOIN dws_fama_quality_score q ON s.trade_date = q.trade_date AND s.ts_code = q.ts_code
        LEFT JOIN dws_fama_technical_score t ON s.trade_date = t.trade_date AND s.ts_code = t.ts_code
        LEFT JOIN dws_fama_capital_score c ON s.trade_date = c.trade_date AND s.ts_code = c.ts_code
        LEFT JOIN dws_fama_chip_score ch ON s.trade_date = ch.trade_date AND s.ts_code = ch.ts_code
        LEFT JOIN ods_daily od ON s.trade_date = od.trade_date AND s.ts_code = od.ts_code
        LEFT JOIN dim_stock ds ON s.ts_code = ds.ts_code
        WHERE s.trade_date = :trade_date
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
    
    def compare_with_claude(self, trade_date: int, top_n: int = 50) -> pd.DataFrame:
        """对比 Fama 评分与 Claude 评分的 Top 股票"""
        fama_sql = """
        SELECT 
            s.ts_code,
            ds.name,
            (COALESCE(s.size_score, 0) + COALESCE(m.momentum_score, 0) + 
             COALESCE(v.value_score, 0) + COALESCE(q.quality_score, 0) + 
             COALESCE(t.technical_score, 0) + COALESCE(c.capital_score, 0) + 
             COALESCE(ch.chip_score, 0)) AS fama_score
        FROM dws_fama_size_score s
        LEFT JOIN dws_fama_momentum_score m ON s.trade_date = m.trade_date AND s.ts_code = m.ts_code
        LEFT JOIN dws_fama_value_score v ON s.trade_date = v.trade_date AND s.ts_code = v.ts_code
        LEFT JOIN dws_fama_quality_score q ON s.trade_date = q.trade_date AND s.ts_code = q.ts_code
        LEFT JOIN dws_fama_technical_score t ON s.trade_date = t.trade_date AND s.ts_code = t.ts_code
        LEFT JOIN dws_fama_capital_score c ON s.trade_date = c.trade_date AND s.ts_code = c.ts_code
        LEFT JOIN dws_fama_chip_score ch ON s.trade_date = ch.trade_date AND s.ts_code = ch.ts_code
        LEFT JOIN dim_stock ds ON s.ts_code = ds.ts_code
        WHERE s.trade_date = :trade_date
        """
        
        claude_sql = """
        SELECT 
            m.ts_code,
            (COALESCE(m.momentum_score, 0) + COALESCE(v.value_score, 0) + 
             COALESCE(q.quality_score, 0) + COALESCE(t.technical_score, 0) + 
             COALESCE(c.capital_score, 0) + COALESCE(ch.chip_score, 0)) AS claude_score
        FROM dws_momentum_score m
        LEFT JOIN dws_value_score v ON m.trade_date = v.trade_date AND m.ts_code = v.ts_code
        LEFT JOIN dws_quality_score q ON m.trade_date = q.trade_date AND m.ts_code = q.ts_code
        LEFT JOIN dws_technical_score t ON m.trade_date = t.trade_date AND m.ts_code = t.ts_code
        LEFT JOIN dws_capital_score c ON m.trade_date = c.trade_date AND m.ts_code = c.ts_code
        LEFT JOIN dws_chip_score ch ON m.trade_date = ch.trade_date AND m.ts_code = ch.ts_code
        WHERE m.trade_date = :trade_date
        """
        
        fama_df = pd.read_sql(text(fama_sql), self.engine, params={"trade_date": trade_date})
        claude_df = pd.read_sql(text(claude_sql), self.engine, params={"trade_date": trade_date})
        
        merged = fama_df.merge(claude_df, on='ts_code', how='outer')
        merged['score_diff'] = merged['fama_score'] - merged['claude_score']
        merged = merged.sort_values('fama_score', ascending=False).head(top_n)
        merged['fama_rank'] = range(1, len(merged) + 1)
        
        # 计算claude排名
        claude_sorted = merged.sort_values('claude_score', ascending=False).reset_index(drop=True)
        claude_sorted['claude_rank'] = range(1, len(claude_sorted) + 1)
        merged = merged.merge(claude_sorted[['ts_code', 'claude_rank']], on='ts_code')
        
        return merged
