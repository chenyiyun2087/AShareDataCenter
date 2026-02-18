"""
股票评分查询模块

从DWS评分表读取预计算的评分数据，提供查询和分析功能。
评分计算已移至 scripts/etl/dws/scoring.py (纯SQL实现)
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


DIMENSION_SCORE_CAPS = {
    'momentum_score': 25,
    'value_score': 20,
    'quality_score': 20,
    'technical_score': 15,
    'capital_score': 10,
    'chip_score': 10,
}


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
        """获取指定日期所有股票的各维度评分，包含涨停标记"""
        sql = """
        SELECT 
            m.trade_date, m.ts_code,
            ds.name,
            od.pct_chg,
            CASE 
                -- 科创板(688)和创业板(3)涨跌停±20%
                WHEN (m.ts_code LIKE '688%' OR m.ts_code LIKE '3%') AND od.pct_chg >= 19.9 THEN '涨停'
                WHEN (m.ts_code LIKE '688%' OR m.ts_code LIKE '3%') AND od.pct_chg <= -19.9 THEN '跌停'
                -- 其他板块涨跌停±10%
                WHEN od.pct_chg >= 9.9 THEN '涨停'
                WHEN od.pct_chg <= -9.9 THEN '跌停'
                ELSE '' 
            END AS limit_flag,
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
        LEFT JOIN ods_daily od ON m.trade_date = od.trade_date AND m.ts_code = od.ts_code
        LEFT JOIN dim_stock ds ON m.ts_code = ds.ts_code
        WHERE m.trade_date = :trade_date
        """
        return pd.read_sql(text(sql), self.engine, params={"trade_date": trade_date})

    @staticmethod
    def get_dimension_score_caps() -> Dict[str, int]:
        """返回各维度理论满分。"""
        return DIMENSION_SCORE_CAPS.copy()
    
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
            m.vol_ratio_score, m.turnover_score, m.mtm_score, m.mtmma_score, m.momentum_score,
            v.pe_score, v.pb_score, v.ps_score, v.value_score,
            q.roe_score, q.margin_score, q.leverage_score, q.quality_score,
            t.macd_score, t.kdj_score, t.rsi_score, t.cci_score, t.bias_score, t.technical_score,
            c.elg_score, c.lg_score, c.margin_score, c.capital_score,
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
    '''
    策略回测 (--backtest)： 回测过去 3 个月评分策略的表现（Top 10 等权重持有）。
    python score/claude_score/score_query.py --backtest --num-stocks N  持仓股票数 (默认10)
    --initial-capital X  初始资金 (默认1000000)
    --hold N        持仓天数 (默认20)
    --commission X  佣金率 (默认0.0003=0.03%)
    --stamp-tax X   印花税 (默认0.001=0.1%)
    --slippage X    滑点 (默认0.001=0.1%)
    
    行业对比 (--industry)： 查看各行业平均评分排名，发现高分板块。
    python score/claude_score/score_query.py --industry
    
    生成报告 (--report)： 生成 Excel 格式的全方位分析报告（评分、行业、统计等）。
    python score/claude_score/score_query.py --report
    '''
    import sys
    import argparse
    import logging
    from score.claude_score.advanced_analysis import AdvancedAnalyzer, create_score_report
    
    # 配置日志输出到控制台
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    parser = argparse.ArgumentParser(description='股票评分查询与分析工具')
    parser.add_argument('codes', nargs='*', help='股票代码列表 (e.g. 000001.SZ)')
    parser.add_argument('--date', type=int, default=20260206, help='交易日期 (YYYYMMDD)')
    parser.add_argument('--top', type=int, default=10, help='显示Top N股票(查询/展示)')
    parser.add_argument('--industry', action='store_true', help='执行行业对比分析')
    parser.add_argument('--backtest', action='store_true', help='执行策略回测')
    parser.add_argument('--days', type=int, default=90, help='回测天数 (默认90天)')
    parser.add_argument('--num-stocks', '--top-n', dest='num_stocks', type=int, default=10, help='回测持仓股票数 (默认10)')
    parser.add_argument('--initial-capital', type=float, default=1_000_000.0, help='回测初始资金 (默认1000000)')
    parser.add_argument('--hold', type=int, default=20, help='持仓天数 (默认20天)')
    parser.add_argument('--commission', type=float, default=0.0003, help='佣金率 (默认0.03%%)')
    parser.add_argument('--stamp-tax', type=float, default=0.001, help='印花税 (默认0.1%%)')
    parser.add_argument('--slippage', type=float, default=0.001, help='滑点 (默认0.1%%)')
    parser.add_argument('--report', action='store_true', help='生成Excel分析报告')
    parser.add_argument('--host', default='localhost', help='数据库地址')
    parser.add_argument('--port', type=int, default=3306, help='数据库端口')
    parser.add_argument('--user', default='root', help='数据库用户')
    parser.add_argument('--password', default='', help='数据库密码')
    parser.add_argument('--database', default='tushare_stock', help='数据库名')
    
    args = parser.parse_args()
    
    engine = get_engine(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    q = ScoreQuery(engine)
    analyzer = AdvancedAnalyzer(engine)
    
    trade_date = args.date
    
    # 1. 行业分析
    if args.industry:
        analyzer.compare_by_industry(trade_date)
        sys.exit(0)
        
    # 2. 策略回测
    if args.backtest:
        import datetime
        end_dt = datetime.datetime.strptime(str(trade_date), "%Y%m%d")
        start_dt = end_dt - datetime.timedelta(days=args.days)
        start_date = int(start_dt.strftime("%Y%m%d"))
        
        analyzer.backtest_score_strategy(
            start_date=start_date, 
            end_date=trade_date,
            num_stocks=args.num_stocks,
            holding_days=args.hold,
            commission=args.commission,
            stamp_tax=getattr(args, 'stamp_tax'),
            slippage=args.slippage,
            initial_capital=args.initial_capital
        )
        sys.exit(0)
        
    # 3. 生成报告
    if args.report:
        create_score_report(engine, trade_date)
        sys.exit(0)
    
    # 4. 股票查询 (默认功能)
    if args.codes:
        print(f"查询股票: {args.codes}")
        q.print_stock_detail(trade_date, args.codes)
    else:
        # 默认展示Top N
        print(f"=== {trade_date} 评分统计 ===")
        stats = q.get_score_stats(trade_date)
        print(f"股票数: {stats.get('count', 0)}")
        print(f"平均分: {stats.get('avg_score', 0):.2f}")
        print(f"最高分: {stats.get('max_score', 0):.2f}")
        
        print(f"\n=== Top {args.top} 高分股票 (各维度详细分数) ===")
        print("维度满分: 动量25 | 价值20 | 质量20 | 技术15 | 资金10 | 筹码10 | 总分100")
        print("-" * 95)
        top = q.get_top_stocks(trade_date, top_n=args.top)
        display_cols = ['rank', 'ts_code', 'total_score', 
                       'momentum_score', 'value_score', 'quality_score',
                       'technical_score', 'capital_score', 'chip_score']
        # 重命名列以便显示
        top_display = top[display_cols].copy()
        top_display.columns = ['排名', '代码', '总分', '动量', '价值', '质量', '技术', '资金', '筹码']
        print(top_display.to_string(index=False))
        
        print("\n提示: 可使用 --help 查看更多高级分析功能")
