"""
高级分析工具
提供因子回测、行业对比等功能
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


class AdvancedAnalyzer:
    """高级分析工具类"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def backtest_score_strategy(self, start_date: int, end_date: int, 
                                top_n: int = 50, holding_days: int = 20) -> pd.DataFrame:
        """
        回测评分选股策略
        
        策略逻辑：每次调仓选择评分最高的top_n只股票，持有holding_days天
        
        Parameters:
        -----------
        start_date : int
            回测起始日期
        end_date : int
            回测结束日期
        top_n : int
            每次选择的股票数量
        holding_days : int
            持有天数
        
        Returns:
        --------
        DataFrame with backtest results
        """
        logger.info(f"开始回测评分策略: {start_date} - {end_date}")
        logger.info(f"参数: Top {top_n} 股票, 持有 {holding_days} 天")
        
        # 获取评分数据
        query_scores = f"""
        SELECT 
            s.trade_date,
            s.ts_code,
            s.total_score,
            s.rank_all,
            p.close,
            p.pct_chg
        FROM ads_stock_score_daily s
        LEFT JOIN ods_daily p ON s.trade_date = p.trade_date AND s.ts_code = p.ts_code
        WHERE s.trade_date BETWEEN {start_date} AND {end_date}
        ORDER BY s.trade_date, s.total_score DESC
        """
        
        df = pd.read_sql(query_scores, self.engine)
        
        if df.empty:
            logger.warning("未找到评分数据")
            return pd.DataFrame()
        
        # 获取所有交易日
        trade_dates = sorted(df['trade_date'].unique())
        
        results = []
        current_portfolio = None
        buy_date = None
        
        for i, date in enumerate(trade_dates):
            # 检查是否需要调仓
            if current_portfolio is None or (buy_date and 
                len([d for d in trade_dates if buy_date <= d < date]) >= holding_days):
                
                # 选择当日评分Top N的股票
                top_stocks = df[df['trade_date'] == date].nlargest(top_n, 'total_score')
                current_portfolio = top_stocks['ts_code'].tolist()
                buy_date = date
                buy_prices = top_stocks.set_index('ts_code')['close'].to_dict()
                
                logger.info(f"{date}: 调仓，选择 {len(current_portfolio)} 只股票")
            
            # 计算当日收益
            portfolio_stocks = df[(df['trade_date'] == date) & 
                                (df['ts_code'].isin(current_portfolio))]
            
            if len(portfolio_stocks) > 0:
                daily_return = portfolio_stocks['pct_chg'].mean()
                
                results.append({
                    'trade_date': date,
                    'portfolio_size': len(portfolio_stocks),
                    'daily_return': daily_return,
                    'avg_score': portfolio_stocks['total_score'].mean()
                })
        
        df_results = pd.DataFrame(results)
        
        if not df_results.empty:
            # 计算累计收益
            df_results['cumulative_return'] = (1 + df_results['daily_return'] / 100).cumprod() - 1
            
            # 统计指标
            total_return = df_results['cumulative_return'].iloc[-1]
            sharpe = df_results['daily_return'].mean() / df_results['daily_return'].std() * np.sqrt(252)
            max_drawdown = self._calculate_max_drawdown(df_results['cumulative_return'])
            
            logger.info(f"\n回测结果:")
            logger.info(f"总收益率: {total_return*100:.2f}%")
            logger.info(f"夏普比率: {sharpe:.2f}")
            logger.info(f"最大回撤: {max_drawdown*100:.2f}%")
            logger.info(f"平均持仓数: {df_results['portfolio_size'].mean():.1f}")
        
        return df_results
    
    def _calculate_max_drawdown(self, cumulative_returns):
        """计算最大回撤"""
        running_max = cumulative_returns.cummax()
        drawdown = (cumulative_returns - running_max) / (1 + running_max)
        return drawdown.min()
    
    def compare_by_industry(self, trade_date: int, industry_col: str = 'industry_code') -> pd.DataFrame:
        """
        按行业对比评分
        
        Parameters:
        -----------
        trade_date : int
            交易日期
        industry_col : str
            行业字段名
        
        Returns:
        --------
        DataFrame with industry comparison
        """
        query = f"""
        SELECT 
            s.*,
            f.{industry_col}
        FROM ads_stock_score_daily s
        LEFT JOIN ads_features_stock_daily f 
            ON s.trade_date = f.trade_date AND s.ts_code = f.ts_code
        WHERE s.trade_date = {trade_date}
          AND f.{industry_col} IS NOT NULL
        """
        
        df = pd.read_sql(query, self.engine)
        
        if df.empty:
            logger.warning("未找到数据")
            return pd.DataFrame()
        
        # 按行业分组统计
        industry_stats = df.groupby(industry_col).agg({
            'total_score': ['mean', 'median', 'std', 'max', 'min'],
            'ts_code': 'count'
        }).round(2)
        
        industry_stats.columns = ['avg_score', 'median_score', 'std_score', 
                                 'max_score', 'min_score', 'stock_count']
        industry_stats = industry_stats.reset_index()
        industry_stats = industry_stats.sort_values('avg_score', ascending=False)
        
        logger.info(f"\n行业评分对比 ({trade_date}):")
        logger.info(f"共 {len(industry_stats)} 个行业")
        print(industry_stats.head(10).to_string(index=False))
        
        return industry_stats
    
    def find_score_reversal(self, lookback_days: int = 20, 
                           score_change_threshold: float = 20.0) -> pd.DataFrame:
        """
        寻找评分快速上升的股票（潜在黑马）
        
        Parameters:
        -----------
        lookback_days : int
            回看天数
        score_change_threshold : float
            评分变化阈值
        
        Returns:
        --------
        DataFrame with reversal stocks
        """
        # 获取最近的评分数据
        query = f"""
        SELECT 
            trade_date,
            ts_code,
            total_score,
            rank_all
        FROM ads_stock_score_daily
        WHERE trade_date >= (
            SELECT MAX(trade_date) - {lookback_days * 2}
            FROM ads_stock_score_daily
        )
        ORDER BY ts_code, trade_date
        """
        
        df = pd.read_sql(query, self.engine)
        
        if df.empty:
            return pd.DataFrame()
        
        # 计算评分变化
        df_change = df.groupby('ts_code').agg({
            'trade_date': ['first', 'last'],
            'total_score': ['first', 'last'],
            'rank_all': ['first', 'last']
        })
        
        df_change.columns = ['start_date', 'end_date', 'start_score', 
                            'end_score', 'start_rank', 'end_rank']
        df_change['score_change'] = df_change['end_score'] - df_change['start_score']
        df_change['rank_change'] = df_change['start_rank'] - df_change['end_rank']  # 排名上升为正
        
        # 筛选快速上升的股票
        reversal = df_change[
            (df_change['score_change'] >= score_change_threshold) &
            (df_change['rank_change'] > 0)
        ].sort_values('score_change', ascending=False)
        
        reversal = reversal.reset_index()
        
        logger.info(f"\n找到 {len(reversal)} 只评分快速上升的股票")
        if len(reversal) > 0:
            print(reversal.head(20).to_string(index=False))
        
        return reversal
    
    def factor_correlation_analysis(self, trade_date: int) -> pd.DataFrame:
        """
        分析各维度得分之间的相关性
        
        Parameters:
        -----------
        trade_date : int
            交易日期
        
        Returns:
        --------
        DataFrame with correlation matrix
        """
        query = f"""
        SELECT 
            momentum_score,
            value_score,
            quality_score,
            technical_score,
            capital_score,
            chip_score,
            total_score
        FROM ads_stock_score_daily
        WHERE trade_date = {trade_date}
        """
        
        df = pd.read_sql(query, self.engine)
        
        if df.empty:
            return pd.DataFrame()
        
        # 计算相关系数矩阵
        corr_matrix = df.corr().round(3)
        
        logger.info(f"\n因子相关性矩阵 ({trade_date}):")
        print(corr_matrix.to_string())
        
        return corr_matrix
    
    def screen_stocks(self, trade_date: int, criteria: Dict) -> pd.DataFrame:
        """
        自定义筛选股票
        
        Parameters:
        -----------
        trade_date : int
            交易日期
        criteria : dict
            筛选条件，例如:
            {
                'total_score': (70, 100),
                'momentum_score': (15, None),
                'value_score': (None, 15),
                'pe_ttm': (0, 30)
            }
        
        Returns:
        --------
        DataFrame
        """
        query = f"""
        SELECT 
            s.*,
            f.pe_ttm,
            f.pb,
            f.total_mv,
            f.roe
        FROM ads_stock_score_daily s
        LEFT JOIN ads_features_stock_daily f 
            ON s.trade_date = f.trade_date AND s.ts_code = f.ts_code
        WHERE s.trade_date = {trade_date}
        """
        
        df = pd.read_sql(query, self.engine)
        
        if df.empty:
            return pd.DataFrame()
        
        # 应用筛选条件
        for col, (min_val, max_val) in criteria.items():
            if col in df.columns:
                if min_val is not None:
                    df = df[df[col] >= min_val]
                if max_val is not None:
                    df = df[df[col] <= max_val]
        
        df = df.sort_values('total_score', ascending=False)
        
        logger.info(f"\n筛选结果: 找到 {len(df)} 只股票")
        logger.info(f"筛选条件: {criteria}")
        
        return df


def create_score_report(engine, trade_date: int, output_file: str = None):
    """
    生成评分分析报告
    
    Parameters:
    -----------
    engine : sqlalchemy.engine.Engine
        数据库连接
    trade_date : int
        交易日期
    output_file : str
        输出文件名
    """
    from stock_scoring_system import StockScoringSystem
    
    scorer = StockScoringSystem(engine)
    analyzer = AdvancedAnalyzer(engine)
    
    if output_file is None:
        output_file = f'score_report_{trade_date}.xlsx'
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 1. 综合统计
        stats = scorer.analyze_score_distribution(trade_date)
        pd.DataFrame([stats]).to_excel(writer, sheet_name='统计摘要', index=False)
        
        # 2. Top 100股票
        top_stocks = scorer.get_top_stocks(trade_date, top_n=100, min_score=0)
        top_stocks.to_excel(writer, sheet_name='Top100股票', index=False)
        
        # 3. 各维度Top 50
        query_all = f"""
        SELECT * FROM ads_stock_score_daily WHERE trade_date = {trade_date}
        """
        df_all = pd.read_sql(query_all, engine)
        
        for score_col in ['momentum_score', 'value_score', 'quality_score', 
                         'technical_score', 'capital_score', 'chip_score']:
            df_top = df_all.nlargest(50, score_col)[
                ['ts_code', score_col, 'total_score', 'rating', 'rank_all']
            ]
            sheet_name = score_col.replace('_score', '').capitalize()[:31]
            df_top.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # 4. 行业对比
        try:
            industry_stats = analyzer.compare_by_industry(trade_date)
            if not industry_stats.empty:
                industry_stats.to_excel(writer, sheet_name='行业对比', index=False)
        except Exception as e:
            logger.warning(f"行业对比分析失败: {e}")
        
        # 5. 因子相关性
        try:
            corr = analyzer.factor_correlation_analysis(trade_date)
            if not corr.empty:
                corr.to_excel(writer, sheet_name='因子相关性')
        except Exception as e:
            logger.warning(f"相关性分析失败: {e}")
    
    logger.info(f"\n分析报告已生成: {output_file}")
    return output_file


if __name__ == '__main__':
    # 示例使用
    DB_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'user': 'your_username',
        'password': 'your_password',
        'database': 'stock_db',
        'charset': 'utf8mb4'
    }
    
    connection_string = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset={DB_CONFIG['charset']}"
    )
    
    engine = create_engine(connection_string)
    analyzer = AdvancedAnalyzer(engine)
    
    # 1. 回测策略
    # backtest_results = analyzer.backtest_score_strategy(
    #     start_date=20260101,
    #     end_date=20260207,
    #     top_n=50,
    #     holding_days=20
    # )
    
    # 2. 行业对比
    # industry_comp = analyzer.compare_by_industry(trade_date=20260207)
    
    # 3. 寻找评分上升股票
    # reversal_stocks = analyzer.find_score_reversal(lookback_days=20)
    
    # 4. 自定义筛选
    # criteria = {
    #     'total_score': (70, 100),
    #     'momentum_score': (15, None),
    #     'pe_ttm': (0, 30)
    # }
    # filtered = analyzer.screen_stocks(trade_date=20260207, criteria=criteria)
    
    # 5. 生成完整报告
    # create_score_report(engine, trade_date=20260207)
