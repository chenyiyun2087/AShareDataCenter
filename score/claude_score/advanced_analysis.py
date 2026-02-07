"""
高级分析工具
提供因子回测、行业对比等功能
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from typing import List, Dict, Optional
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


class AdvancedAnalyzer:
    """高级分析工具类 - 基于DWS评分表"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def _get_score_data(self, start_date: int, end_date: int) -> pd.DataFrame:
        """获取指定时间段的评分数据"""
        sql = """
        SELECT 
            m.trade_date, m.ts_code,
            (COALESCE(m.momentum_score, 0) + COALESCE(v.value_score, 0) + 
             COALESCE(q.quality_score, 0) + COALESCE(t.technical_score, 0) + 
             COALESCE(c.capital_score, 0) + COALESCE(ch.chip_score, 0)) AS total_score,
            m.momentum_score, v.value_score, q.quality_score,
            t.technical_score, c.capital_score, ch.chip_score
        FROM dws_momentum_score m
        LEFT JOIN dws_value_score v ON m.trade_date = v.trade_date AND m.ts_code = v.ts_code
        LEFT JOIN dws_quality_score q ON m.trade_date = q.trade_date AND m.ts_code = q.ts_code
        LEFT JOIN dws_technical_score t ON m.trade_date = t.trade_date AND m.ts_code = t.ts_code
        LEFT JOIN dws_capital_score c ON m.trade_date = c.trade_date AND m.ts_code = c.ts_code
        LEFT JOIN dws_chip_score ch ON m.trade_date = ch.trade_date AND m.ts_code = ch.ts_code
        WHERE m.trade_date BETWEEN :start_date AND :end_date
        """
        return pd.read_sql(text(sql), self.engine, params={"start_date": start_date, "end_date": end_date})

    def backtest_score_strategy(self, start_date: int, end_date: int, 
                                top_n: int = 50, holding_days: int = 20) -> pd.DataFrame:
        """
        回测评分选股策略
        
        策略逻辑：每次调仓选择评分最高的top_n只股票，持有holding_days天
        """
        logger.info(f"开始回测评分策略: {start_date} - {end_date}")
        logger.info(f"参数: Top {top_n} 股票, 持有 {holding_days} 天")
        
        # 1. 获取评分数据
        df_scores = self._get_score_data(start_date, end_date)
        if df_scores.empty:
            logger.warning("未找到评分数据")
            return pd.DataFrame()
        
        # 2. 获取行情数据 (ods_daily)
        price_sql = """
        SELECT trade_date, ts_code, close, pct_chg
        FROM ods_daily
        WHERE trade_date BETWEEN :start_date AND :end_date
        """
        df_price = pd.read_sql(text(price_sql), self.engine, params={"start_date": start_date, "end_date": end_date})
        if df_price.empty:
            logger.warning("未找到行情数据")
            return pd.DataFrame() # Return empty if no price data
            
        # 合并数据
        df = pd.merge(df_scores, df_price, on=['trade_date', 'ts_code'], how='inner')
        
        # 获取所有交易日
        trade_dates = sorted(df['trade_date'].unique())
        
        results = []
        current_portfolio = []
        buy_date = None
        next_rebalance_idx = 0
        
        # 初始资金1.0
        portfolio_value = 1.0
        benchmark_value = 1.0
        
        # 记录每日净值
        daily_nav = []
        
        for i, date in enumerate(trade_dates):
            # 每日计算：当前持仓的收益
            day_return = 0.0
            
            # --- 调仓逻辑 ---
            # 如果是第一天 或者 到了调仓日
            if i == next_rebalance_idx:
                # 选股：当日评分最高的Top N
                daily_data = df[df['trade_date'] == date]
                if not daily_data.empty:
                    top_stocks = daily_data.nlargest(top_n, 'total_score')['ts_code'].tolist()
                    current_portfolio = top_stocks
                    buy_date = date
                    next_rebalance_idx = min(i + holding_days, len(trade_dates) - 1)
                    if i + holding_days >= len(trade_dates): # 防止越界，但这其实意味着最后一次调仓一直持有到结束
                         next_rebalance_idx = len(trade_dates) # Ensure logic works
                    
                    logger.info(f"{date}: 调仓，买入 {len(current_portfolio)} 只股票")
            
            # --- 收益计算 ---
            if current_portfolio:
                # 获取持仓股票当日涨跌幅
                portfolio_data = df[(df['trade_date'] == date) & (df['ts_code'].isin(current_portfolio))]
                if not portfolio_data.empty:
                    # 等权重平均涨跌幅
                    # 注意：这里简化处理，假设每日收盘都能按pct_chg成交，未考虑手续费和滑点
                    avg_chg = portfolio_data['pct_chg'].mean()
                    day_return = avg_chg / 100.0
            
            # 更新净值
            portfolio_value *= (1 + day_return)
            
            # 记录
            daily_nav.append({
                'trade_date': date,
                'nav': portfolio_value,
                'daily_return': day_return,
                'holdings_count': len(current_portfolio)
            })
            
        df_results = pd.DataFrame(daily_nav)
        
        if not df_results.empty:
            # 计算回撤
            df_results['max_nav'] = df_results['nav'].cummax()
            df_results['drawdown'] = (df_results['nav'] - df_results['max_nav']) / df_results['max_nav']
            
            total_ret = df_results['nav'].iloc[-1] - 1
            max_dd = df_results['drawdown'].min()
            
            # 年化收益 (简单估算)
            days = len(trade_dates)
            annual_ret = (1 + total_ret) ** (252 / days) - 1 if days > 0 else 0
            
            logger.info(f"\n回测结果:")
            logger.info(f"总收益率: {total_ret*100:.2f}%")
            logger.info(f"年化收益: {annual_ret*100:.2f}%")
            logger.info(f"最大回撤: {max_dd*100:.2f}%")
            
        return df_results
    
    def compare_by_industry(self, trade_date: int) -> pd.DataFrame:
        """
        按行业对比评分
        使用 dim_stock 表的 industry 字段
        """
        # 1. 获取评分
        df_scores = self._get_score_data(trade_date, trade_date)
        if df_scores.empty:
            logger.warning(f"{trade_date} 无评分数据")
            return pd.DataFrame()
            
        # 2. 获取行业信息
        ind_sql = "SELECT ts_code, industry FROM dim_stock"
        df_ind = pd.read_sql(text(ind_sql), self.engine)
        
        # 合并
        df = pd.merge(df_scores, df_ind, on='ts_code', how='inner')
        
        # 聚合统计
        stats = df.groupby('industry').agg({
            'total_score': ['count', 'mean', 'median', 'max'],
            'momentum_score': 'mean',
            'value_score': 'mean',
            'quality_score': 'mean',
            'technical_score': 'mean',
            'capital_score': 'mean',
            'chip_score': 'mean'
        }).round(2)
        
        # 扁平化列名
        stats.columns = ['count', 'avg_total', 'median_total', 'max_total',
                        'avg_momentum', 'avg_value', 'avg_quality',
                        'avg_technical', 'avg_capital', 'avg_chip']
        
        stats = stats.sort_values('avg_total', ascending=False)
        stats = stats.reset_index()
        
        # 过滤样本过少的行业
        stats = stats[stats['count'] >= 5]
        
        logger.info(f"\n行业对比 ({trade_date}):")
        print(stats.head(10).to_string(index=False))
        return stats

    def analyze_trend(self, ts_code: str, start_date: int, end_date: int) -> pd.DataFrame:
        """单只股票评分趋势分析"""
        sql = """
        SELECT trade_date, 
               (COALESCE(m.momentum_score, 0) + COALESCE(v.value_score, 0) + 
                COALESCE(q.quality_score, 0) + COALESCE(t.technical_score, 0) + 
                COALESCE(c.capital_score, 0) + COALESCE(ch.chip_score, 0)) AS total_score,
               m.momentum_score, v.value_score, q.quality_score
        FROM dws_momentum_score m
        LEFT JOIN dws_value_score v ON m.trade_date = v.trade_date AND m.ts_code = v.ts_code
        LEFT JOIN dws_quality_score q ON m.trade_date = q.trade_date AND m.ts_code = q.ts_code
        LEFT JOIN dws_technical_score t ON m.trade_date = t.trade_date AND m.ts_code = t.ts_code
        LEFT JOIN dws_capital_score c ON m.trade_date = c.trade_date AND m.ts_code = c.ts_code
        LEFT JOIN dws_chip_score ch ON m.trade_date = ch.trade_date AND m.ts_code = ch.ts_code
        WHERE m.ts_code = :ts_code AND m.trade_date BETWEEN :start_date AND :end_date
        ORDER BY trade_date
        """
        return pd.read_sql(text(sql), self.engine, 
                          params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date})

    def screen_stocks(self, trade_date: int, criteria: Dict) -> pd.DataFrame:
        """自定义筛选 (复用 ScoreQuery 逻辑，但这里为了完整性保留)"""
        # 这里可以直接调用 _get_score_data 然后 filter
        df = self._get_score_data(trade_date, trade_date)
        
        for col, (min_val, max_val) in criteria.items():
            if col in df.columns:
                if min_val is not None:
                    df = df[df[col] >= min_val]
                if max_val is not None:
                    df = df[df[col] <= max_val]
        
        return df.sort_values('total_score', ascending=False)


def create_score_report(engine, trade_date: int, output_file: str = None):
    """生成综合分析报告"""
    analyzer = AdvancedAnalyzer(engine)
    
    if output_file is None:
        output_file = f'score_report_{trade_date}.xlsx'
        
    logger.info(f"正在生成分析报告: {output_file} ...")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 1. 全市场评分概览
        df_scores = analyzer._get_score_data(trade_date, trade_date)
        if not df_scores.empty:
            df_scores.sort_values('total_score', ascending=False).head(2000).to_excel(
                writer, sheet_name='全市场评分(Top2000)', index=False
            )
            
            # 统计摘要
            stats = {
                'count': len(df_scores),
                'avg_score': df_scores['total_score'].mean(),
                'median_score': df_scores['total_score'].median(),
                'std_score': df_scores['total_score'].std()
            }
            pd.DataFrame([stats]).to_excel(writer, sheet_name='统计摘要', index=False)
            
        # 2. 行业分析
        df_ind = analyzer.compare_by_industry(trade_date)
        if not df_ind.empty:
            df_ind.to_excel(writer, sheet_name='行业对比', index=False)
            
        # 3. 策略回测 (最近3个月)
        # end_dt = datetime.strptime(str(trade_date), "%Y%m%d")
        # start_dt = end_dt - pd.Timedelta(days=90)
        # start_date_int = int(start_dt.strftime("%Y%m%d"))
        
        # df_backtest = analyzer.backtest_score_strategy(start_date_int, trade_date)
        # if not df_backtest.empty:
        #     df_backtest.to_excel(writer, sheet_name='近3月回测', index=False)

    logger.info(f"报告生成完成: {os.path.abspath(output_file)}")
    return output_file


if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    
    # 数据库连接
    connection_string = "mysql+pymysql://root:19871019@localhost:3306/tushare_stock?charset=utf8mb4"
    engine = create_engine(connection_string)
    
    analyzer = AdvancedAnalyzer(engine)
    
    # 测试回测 (2025年)
    # analyzer.backtest_score_strategy(20250101, 20250601)
    
    # 测试行业对比
    analyzer.compare_by_industry(20260206)
    
    # 测试报告生成
    # create_score_report(engine, 20260206)
