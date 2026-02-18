"""
高级分析工具
提供因子回测、行业对比等功能
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from typing import Dict
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
                                num_stocks: int = 50, holding_days: int = 20,
                                commission: float = 0.0003,  # 佣金率 0.03%
                                stamp_tax: float = 0.001,    # 印花税 0.1% (卖出)
                                slippage: float = 0.001,
                                initial_capital: float = 1_000_000.0,
                                top_n: int | None = None) -> pd.DataFrame:
        """
        回测评分选股策略 (修正版)
        
        修正内容:
        1. 使用T-1日评分在T日买入 (消除未来函数)
        2. 过滤T日涨停股 (涨停无法买入)
        3. 过滤停牌股 (pct_chg为空或0且成交量为0)
        4. 加入交易成本 (佣金+印花税+滑点)
        5. 调仓日不计算当日涨跌幅 (开盘买入，收益从T+1开始)
        
        策略逻辑：根据T-1日评分在T日执行日度再平衡，目标持仓为Top-N(N=num_stocks)，
        并通过日均替换仓位控制平均持有天数接近holding_days。
        """
        logger.info(f"开始回测评分策略: {start_date} - {end_date}")
        if top_n is not None and top_n > 0:
            num_stocks = top_n
        if num_stocks <= 0:
            raise ValueError("num_stocks must be > 0")
        if initial_capital <= 0:
            raise ValueError("initial_capital must be > 0")
        if holding_days <= 0:
            raise ValueError("holding_days must be > 0")

        logger.info(f"参数: 持仓股票数 {num_stocks}, 持有 {holding_days} 天")
        logger.info(f"初始资金: {initial_capital:,.2f}")
        logger.info(f"交易成本: 佣金{commission*100:.3f}%, 印花税{stamp_tax*100:.2f}%, 滑点{slippage*100:.2f}%")
        
        # 1. 获取评分数据
        df_scores = self._get_score_data(start_date, end_date)
        if df_scores.empty:
            logger.warning("未找到评分数据")
            return pd.DataFrame()
        
        # 2. 获取行情数据 - 需要包含涨停判断所需字段
        price_sql = """
        SELECT d.trade_date, d.ts_code, d.open, d.close, d.pct_chg, d.vol,
               CASE 
                   WHEN (d.ts_code LIKE '688%%' OR d.ts_code LIKE '3%%') AND d.pct_chg >= 19.9 THEN 1
                   WHEN d.pct_chg >= 9.9 THEN 1
                   ELSE 0 
               END AS is_limit_up,
               CASE 
                   WHEN (d.ts_code LIKE '688%%' OR d.ts_code LIKE '3%%') AND d.pct_chg <= -19.9 THEN 1
                   WHEN d.pct_chg <= -9.9 THEN 1
                   ELSE 0 
               END AS is_limit_down,
               CASE WHEN d.vol IS NULL OR d.vol = 0 THEN 1 ELSE 0 END AS is_suspended
        FROM ods_daily d
        WHERE d.trade_date BETWEEN :start_date AND :end_date
        """
        df_price = pd.read_sql(text(price_sql), self.engine, params={"start_date": start_date, "end_date": end_date})
        if df_price.empty:
            logger.warning("未找到行情数据")
            return pd.DataFrame()
            
        # 合并数据
        df = pd.merge(df_scores, df_price, on=['trade_date', 'ts_code'], how='inner')
        
        # 获取所有交易日
        trade_dates = sorted(df['trade_date'].unique())
        if len(trade_dates) < 2:
            logger.warning("交易日不足")
            return pd.DataFrame()
        
        # 构建T-1日评分字典: key=日期, value=前一日的评分DataFrame
        date_to_prev_scores = {}
        for i in range(1, len(trade_dates)):
            prev_date = trade_dates[i - 1]
            curr_date = trade_dates[i]
            # T-1日的评分用于T日选股
            prev_scores = df[df['trade_date'] == prev_date][['ts_code', 'total_score']].copy()
            date_to_prev_scores[curr_date] = prev_scores
        
        current_portfolio: list[str] = []
        holding_age: dict[str, int] = {}
        portfolio_value = float(initial_capital)
        daily_nav = []

        # 目标日均换手：按 holding_days 分摊，近似实现平均持有天数
        daily_replace_count = max(1, int(round(num_stocks / max(holding_days, 1))))

        for i, date in enumerate(trade_dates):
            day_return = 0.0
            is_rebalance_day = False
            transaction_cost = 0.0
            buy_count = 0
            sell_count = 0

            # 首日无 T-1 评分，无法建仓
            if i > 0 and date in date_to_prev_scores:
                is_rebalance_day = True
                prev_scores = date_to_prev_scores[date]

                # 获取T日行情数据 (判断涨停/停牌)
                today_data = df[df['trade_date'] == date].copy()

                # 合并T-1评分与T日行情
                selection_df = pd.merge(
                    prev_scores,
                    today_data[['ts_code', 'is_limit_up', 'is_suspended']],
                    on='ts_code',
                    how='inner',
                )

                # 过滤: 不能买入涨停股和停牌股
                eligible = selection_df[
                    (selection_df['is_limit_up'] == 0) &
                    (selection_df['is_suspended'] == 0)
                ].sort_values('total_score', ascending=False)

                if not eligible.empty:
                    rank_codes = eligible['ts_code'].tolist()
                    rank_map = {code: idx for idx, code in enumerate(rank_codes)}
                    top_pool = set(rank_codes[:num_stocks])

                    # 先让现有持仓年龄 +1
                    for code in list(holding_age.keys()):
                        holding_age[code] += 1

                    old_portfolio = list(current_portfolio)
                    old_set = set(old_portfolio)

                    # 候选卖出：优先卖出非Top池且持有更久且排名靠后
                    def _sell_key(code: str):
                        in_top = 1 if code in top_pool else 0
                        age = holding_age.get(code, 0)
                        rank = rank_map.get(code, 10**9)
                        return (in_top, -age, -rank)

                    sell_candidates = sorted(old_portfolio, key=_sell_key)
                    planned_sell = min(daily_replace_count, len(sell_candidates))
                    sell_list = sell_candidates[:planned_sell]

                    remain = [c for c in old_portfolio if c not in set(sell_list)]
                    remain_set = set(remain)

                    # 先从Top池补齐，再从全体rank补齐，确保持仓数=num_stocks
                    buy_list: list[str] = []
                    for code in rank_codes:
                        if code in remain_set or code in buy_list:
                            continue
                        buy_list.append(code)
                        if len(remain) + len(buy_list) >= num_stocks:
                            break

                    new_portfolio = remain + buy_list
                    new_portfolio = new_portfolio[:num_stocks]
                    new_set = set(new_portfolio)

                    # 交易成本按组合替换比例估算
                    sell_stocks = old_set - new_set
                    buy_stocks = new_set - old_set
                    sell_count = len(sell_stocks)
                    buy_count = len(buy_stocks)

                    sell_ratio = sell_count / max(len(old_set), 1)
                    buy_ratio = buy_count / max(len(new_set), 1)
                    sell_cost = sell_ratio * (commission + stamp_tax + slippage)
                    buy_cost = buy_ratio * (commission + slippage)
                    transaction_cost = sell_cost + buy_cost

                    current_portfolio = new_portfolio

                    # 维护持仓年龄
                    for code in list(holding_age.keys()):
                        if code not in new_set:
                            holding_age.pop(code, None)
                    for code in current_portfolio:
                        if code in old_set:
                            holding_age[code] = holding_age.get(code, 1)
                        else:
                            holding_age[code] = 1

                    excluded_limit_up = len(selection_df[selection_df['is_limit_up'] == 1])
                    excluded_suspended = len(selection_df[selection_df['is_suspended'] == 1])
                    logger.info(
                        f"{date}: 日度再平衡, 持仓{len(current_portfolio)}只, "
                        f"买入{buy_count}只, 卖出{sell_count}只 "
                        f"(排除涨停{excluded_limit_up}, 停牌{excluded_suspended})"
                    )

            # --- 收益计算 ---
            if current_portfolio:
                portfolio_data = df[(df['trade_date'] == date) & (df['ts_code'].isin(current_portfolio))]
                if not portfolio_data.empty:
                    avg_chg = portfolio_data['pct_chg'].mean()
                    day_return = (avg_chg / 100.0) - transaction_cost

            # 更新净值
            portfolio_value *= (1 + day_return)

            avg_holding_days = float(np.mean(list(holding_age.values()))) if holding_age else 0.0
            daily_nav.append({
                'trade_date': date,
                'nav': portfolio_value,
                'daily_return': day_return,
                'holdings_count': len(current_portfolio),
                'is_rebalance': is_rebalance_day,
                'buy_count': buy_count,
                'sell_count': sell_count,
                'avg_holding_days': avg_holding_days,
            })

        df_results = pd.DataFrame(daily_nav)

        if not df_results.empty:
            # 计算回撤
            df_results['max_nav'] = df_results['nav'].cummax()
            df_results['drawdown'] = (df_results['nav'] - df_results['max_nav']) / df_results['max_nav']

            total_ret = (df_results['nav'].iloc[-1] / float(initial_capital)) - 1
            max_dd = df_results['drawdown'].min()

            # 年化收益 (简单估算)
            days = len(trade_dates)
            annual_ret = (1 + total_ret) ** (252 / days) - 1 if days > 0 else 0

            # 计算调仓次数
            rebalance_count = int(df_results['is_rebalance'].sum())
            avg_hold = float(df_results['avg_holding_days'].replace(0, np.nan).mean()) if 'avg_holding_days' in df_results else 0.0

            logger.info(f"\n回测结果 (日度再平衡版):")
            logger.info(f"总收益率: {total_ret*100:.2f}%")
            logger.info(f"年化收益: {annual_ret*100:.2f}%")
            logger.info(f"最大回撤: {max_dd*100:.2f}%")
            logger.info(f"调仓次数: {rebalance_count}")
            logger.info(f"平均持有天数(样本均值): {avg_hold:.2f}")

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
    import argparse

    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

    parser = argparse.ArgumentParser(description='Claude评分高级分析工具')
    parser.add_argument('--date', type=int, default=20260206, help='分析日期(YYYYMMDD)')
    parser.add_argument('--host', default='localhost', help='数据库地址')
    parser.add_argument('--port', type=int, default=3306, help='数据库端口')
    parser.add_argument('--user', default='root', help='数据库用户')
    parser.add_argument('--password', default='', help='数据库密码')
    parser.add_argument('--database', default='tushare_stock', help='数据库名')
    args = parser.parse_args()

    connection_string = (
        f"mysql+pymysql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"
        f"?charset=utf8mb4"
    )
    engine = create_engine(connection_string)

    analyzer = AdvancedAnalyzer(engine)
    analyzer.compare_by_industry(args.date)
