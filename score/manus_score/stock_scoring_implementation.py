import pandas as pd
import numpy as np

class StockScorer:
    def __init__(self, weights=None):
        # 默认维度权重
        self.weights = weights or {
            'value': 0.20,
            'growth': 0.25,
            'quality': 0.25,
            'momentum': 0.15,
            'sentiment': 0.15
        }

    def normalize_rank(self, series, ascending=True):
        """
        使用排名法进行标准化，返回0-100的分数
        ascending=True: 数值越大，排名越靠前，分数越高 (如ROE)
        ascending=False: 数值越小，排名越靠前，分数越高 (如PE)
        """
        if series.isnull().all():
            return series.fillna(50) # 全为空则给中位数分
        
        ranks = series.rank(ascending=ascending, method='min', pct=True)
        return ranks * 100

    def calculate_value_score(self, df_basic):
        """
        计算价值维度得分
        输入: ods_daily_basic 的 DataFrame
        """
        # PE_TTM 越低越好
        pe_score = self.normalize_rank(df_basic['pe_ttm'], ascending=False)
        # PB 越低越好
        pb_score = self.normalize_rank(df_basic['pb'], ascending=False)
        # 股息率 越高越好
        dv_score = self.normalize_rank(df_basic['dv_ttm'], ascending=True)
        
        # 价值维度内部等权
        return (pe_score + pb_score + dv_score) / 3

    def calculate_quality_score(self, df_fina):
        """
        计算质量维度得分
        输入: ods_fina_indicator 的 DataFrame
        """
        # ROE 越高越好
        roe_score = self.normalize_rank(df_fina['roe'], ascending=True)
        # 毛利率 越高越好
        gp_score = self.normalize_rank(df_fina['grossprofit_margin'], ascending=True)
        # 资产负债率 越低越好
        debt_score = self.normalize_rank(df_fina['debt_to_assets'], ascending=False)
        
        return (roe_score + gp_score + debt_score) / 3

    def calculate_momentum_score(self, df_factor):
        """
        计算动量维度得分
        输入: ods_stk_factor 的 DataFrame
        """
        # 涨跌幅 越高越好
        chg_score = self.normalize_rank(df_factor['pct_change'], ascending=True)
        # RSI 越高越好 (简化处理，实际可能需要判断超买)
        rsi_score = self.normalize_rank(df_factor['rsi_12'], ascending=True)
        # MACD 越高越好
        macd_score = self.normalize_rank(df_factor['macd'], ascending=True)
        
        return (chg_score + rsi_score + macd_score) / 3

    def calculate_sentiment_score(self, df_flow, df_margin):
        """
        计算情绪维度得分
        输入: ods_moneyflow 和 ods_margin_detail 的 DataFrame
        """
        # 资金净流入额 越高越好
        flow_score = self.normalize_rank(df_flow['net_mf_amount'], ascending=True)
        # 融资余额 (简化处理，实际应看变化率)
        margin_score = self.normalize_rank(df_margin['rzye'], ascending=True)
        
        return (flow_score + margin_score) / 2

    def score_stocks(self, data_dict):
        """
        综合打分主函数
        data_dict: 包含各表 DataFrame 的字典
        """
        # 假设所有 DataFrame 都以 ts_code 为索引或包含 ts_code 列
        # 这里简化处理，假设数据已经按 ts_code 对齐
        
        v_score = self.calculate_value_score(data_dict['basic'])
        q_score = self.calculate_quality_score(data_dict['fina'])
        m_score = self.calculate_momentum_score(data_dict['factor'])
        s_score = self.calculate_sentiment_score(data_dict['flow'], data_dict['margin'])
        
        # 汇总综合得分
        final_df = pd.DataFrame({
            'ts_code': data_dict['basic']['ts_code'],
            'value_score': v_score,
            'quality_score': q_score,
            'momentum_score': m_score,
            'sentiment_score': s_score
        })
        
        # 暂时假设成长维度与质量维度共用部分数据或简化
        final_df['growth_score'] = q_score # 示例简化
        
        final_df['total_score'] = (
            final_df['value_score'] * self.weights['value'] +
            final_df['growth_score'] * self.weights['growth'] +
            final_df['quality_score'] * self.weights['quality'] +
            final_df['momentum_score'] * self.weights['momentum'] +
            final_df['sentiment_score'] * self.weights['sentiment']
        )
        
        return final_df.sort_values(by='total_score', ascending=False)

# 示例用法
if __name__ == "__main__":
    # 模拟数据
    data = {
        'basic': pd.DataFrame({
            'ts_code': ['000001.SZ', '600519.SH', '000858.SZ'],
            'pe_ttm': [10, 30, 25],
            'pb': [1.2, 8.0, 5.0],
            'dv_ttm': [3.5, 1.5, 2.0]
        }),
        'fina': pd.DataFrame({
            'ts_code': ['000001.SZ', '600519.SH', '000858.SZ'],
            'roe': [12, 30, 25],
            'grossprofit_margin': [30, 90, 75],
            'debt_to_assets': [80, 15, 20]
        }),
        'factor': pd.DataFrame({
            'ts_code': ['000001.SZ', '600519.SH', '000858.SZ'],
            'pct_change': [1.5, -0.5, 2.0],
            'rsi_12': [55, 45, 65],
            'macd': [0.1, -0.05, 0.2]
        }),
        'flow': pd.DataFrame({
            'ts_code': ['000001.SZ', '600519.SH', '000858.SZ'],
            'net_mf_amount': [1000, -500, 2000]
        }),
        'margin': pd.DataFrame({
            'ts_code': ['000001.SZ', '600519.SH', '000858.SZ'],
            'rzye': [50000, 100000, 80000]
        })
    }
    
    scorer = StockScorer()
    results = scorer.score_stocks(data)
    print("股票打分结果 (前3名):")
    print(results)
