基于您的TuShare数据底层架构，我设计了一套多维度量化打分算法框架。该框架融合了Barra风险模型、Fama-French五因子以及A股特色指标，适配您的数据表结构。
一、因子体系架构（适配您的数据表）
表格
复制
维度	因子类别	具体指标	数据来源表	权重建议
估值	价值因子	PE_TTM、PB、PS_TTM、股息率	ods_daily_basic	25%
质量	盈利因子	ROE、毛利率、净利率、资产负债率	ods_fina_indicator	25%
动量	技术因子	20日/60日收益率、MACD、RSI	ods_stk_factor	20%
情绪	资金因子	净流入额、融资余额变化、换手率	ods_moneyflow/ods_margin_detail	15%
风险	波动因子	ATR、20日波动率、筹码集中度	ods_cyq_perf/ods_stk_factor	15%
二、核心打分算法（Python实现）
Python
复制
import pandas as pd
import numpy as np
from scipy import stats
from sklearn.preprocessing import StandardScaler, RobustScaler
import pymysql
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class TuShareFactorScorer:
    """
    基于TuShare数据的多因子打分系统
    适配DDL中的表结构
    """
    
    def __init__(self, db_config):
        self.conn = pymysql.connect(**db_config)
        self.trade_date = self._get_latest_trade_date()
        
    def _get_latest_trade_date(self):
        """获取最新交易日"""
        sql = "SELECT MAX(cal_date) FROM dim_trade_cal WHERE is_open = 1"
        return pd.read_sql(sql, self.conn).iloc[0, 0]
    
    def fetch_factor_data(self, trade_date=None):
        """
        从ODS层获取原始因子数据
        适配您的DDL表结构
        """
        if trade_date is None:
            trade_date = self.trade_date
            
        # 1. 估值因子 (ods_daily_basic)
        sql_valuation = f"""
        SELECT 
            trade_date, ts_code,
            pe_ttm, pb, ps_ttm, dv_ttm,
            turnover_rate, turnover_rate_f, volume_ratio,
            total_mv, circ_mv
        FROM ods_daily_basic 
        WHERE trade_date = {trade_date}
        AND pe_ttm > 0 AND pe_ttm < 500  -- 剔除极端值
        AND pb > 0
        """
        df_val = pd.read_sql(sql_valuation, self.conn)
        
        # 2. 技术指标 (ods_stk_factor)
        sql_tech = f"""
        SELECT 
            trade_date, ts_code,
            pct_change, macd, kdj_k, kdj_d, rsi_6, rsi_12,
            boll_upper, boll_mid, boll_lower, cci, score as tech_score
        FROM ods_stk_factor
        WHERE trade_date = {trade_date}
        """
        df_tech = pd.read_sql(sql_tech, self.conn)
        
        # 3. 财务指标 (ods_fina_indicator - 取最新报告期)
        sql_fina = f"""
        SELECT 
            a.ts_code, a.roe, a.grossprofit_margin, 
            a.netprofit_margin, a.debt_to_assets,
            a.ann_date, a.end_date
        FROM ods_fina_indicator a
        INNER JOIN (
            SELECT ts_code, MAX(ann_date) as max_ann_date
            FROM ods_fina_indicator
            WHERE ann_date <= {trade_date}
            GROUP BY ts_code
        ) b ON a.ts_code = b.ts_code AND a.ann_date = b.max_ann_date
        """
        df_fina = pd.read_sql(sql_fina, self.conn)
        
        # 4. 资金流向 (ods_moneyflow_ths)
        sql_money = f"""
        SELECT 
            trade_date, ts_code,
            net_mf_amount, net_mf_vol,
            buy_lg_amount, sell_lg_amount,
            buy_elg_amount, sell_elg_amount
        FROM ods_moneyflow_ths
        WHERE trade_date = {trade_date}
        """
        df_money = pd.read_sql(sql_money, self.conn)
        
        # 5. 筹码分布 (ods_cyq_perf)
        sql_cyq = f"""
        SELECT 
            trade_date, ts_code,
            winner_rate, weight_avg,
            cost_5pct, cost_95pct
        FROM ods_cyq_perf
        WHERE trade_date = {trade_date}
        """
        df_cyq = pd.read_sql(sql_cyq, self.conn)
        
        # 合并所有因子
        df = df_val.merge(df_tech, on=['trade_date', 'ts_code'], how='left')
        df = df.merge(df_fina, on='ts_code', how='left')
        df = df.merge(df_money, on=['trade_date', 'ts_code'], how='left')
        df = df.merge(df_cyq, on=['trade_date', 'ts_code'], how='left')
        
        return df
    
    def calculate_derived_factors(self, df):
        """
        计算衍生因子（收益率、波动率等需要从dws层或计算）
        """
        # 获取前N日价格计算动量
        sql_momentum = f"""
        SELECT 
            ts_code,
            AVG(CASE WHEN trade_date <= {self.trade_date} 
                     AND trade_date > {self.trade_date - 20} THEN close END) as avg_close_20,
            AVG(CASE WHEN trade_date <= {self.trade_date - 20} 
                     AND trade_date > {self.trade_date - 60} THEN close END) as avg_close_60,
            STD(CASE WHEN trade_date <= {self.trade_date} 
                     AND trade_date > {self.trade_date - 20} THEN pct_chg END) as volatility_20
        FROM dwd_daily
        WHERE trade_date > {self.trade_date - 60} AND trade_date <= {self.trade_date}
        GROUP BY ts_code
        """
        df_mom = pd.read_sql(sql_momentum, self.conn)
        
        df = df.merge(df_mom, on='ts_code', how='left')
        
        # 计算动量因子
        df['momentum_20'] = (df['close'] - df['avg_close_20']) / df['avg_close_20'] * 100
        df['momentum_60'] = (df['avg_close_20'] - df['avg_close_60']) / df['avg_close_60'] * 100
        
        # 计算筹码集中度
        df['cyq_concentration'] = (df['cost_95pct'] - df['cost_5pct']) / df['weight_avg']
        
        # 计算主力资金流向占比
        df['main_force_ratio'] = (df['buy_lg_amount'] - df['sell_lg_amount']) / df['total_mv']
        
        return df
    
    def factor_preprocessing(self, df):
        """
        因子预处理：去极值、标准化、中性化
        """
        processed_df = df.copy()
        factor_cols = []
        
        # 1. 估值因子（低估值得分高，反向因子）
        valuation_factors = ['pe_ttm', 'pb', 'ps_ttm']
        for col in valuation_factors:
            if col in processed_df.columns:
                # MAD去极值
                processed_df[col] = self._winsorize_mad(processed_df[col])
                # 反向（值越小越好）
                processed_df[f'{col}_score'] = -self._zscore(processed_df[col])
                factor_cols.append(f'{col}_score')
        
        # 2. 质量因子（越高越好，正向因子）
        quality_factors = ['roe', 'grossprofit_margin', 'netprofit_margin']
        for col in quality_factors:
            if col in processed_df.columns:
                processed_df[col] = self._winsorize_mad(processed_df[col])
                processed_df[f'{col}_score'] = self._zscore(processed_df[col])
                factor_cols.append(f'{col}_score')
        
        # 3. 动量因子
        momentum_factors = ['momentum_20', 'momentum_60', 'macd']
        for col in momentum_factors:
            if col in processed_df.columns:
                processed_df[col] = self._winsorize_mad(processed_df[col])
                processed_df[f'{col}_score'] = self._zscore(processed_df[col])
                factor_cols.append(f'{col}_score')
        
        # 4. 情绪因子
        sentiment_factors = ['net_mf_amount', 'main_force_ratio', 'turnover_rate_f']
        for col in sentiment_factors:
            if col in processed_df.columns:
                processed_df[col] = self._winsorize_mad(processed_df[col])
                processed_df[f'{col}_score'] = self._zscore(processed_df[col])
                factor_cols.append(f'{col}_score')
        
        # 5. 风险因子（波动率越低越好，反向）
        risk_factors = ['volatility_20', 'cyq_concentration']
        for col in risk_factors:
            if col in processed_df.columns:
                processed_df[col] = self._winsorize_mad(processed_df[col])
                processed_df[f'{col}_score'] = -self._zscore(processed_df[col])
                factor_cols.append(f'{col}_score')
        
        return processed_df, factor_cols
    
    def _winsorize_mad(self, series, n=3):
        """MAD去极值"""
        median = series.median()
        mad = (series - median).abs().median()
        upper = median + n * 1.4826 * mad
        lower = median - n * 1.4826 * mad
        return series.clip(lower, upper)
    
    def _zscore(self, series):
        """Z-Score标准化"""
        return (series - series.mean()) / series.std()
    
    def calculate_weights_ic(self, df, factor_cols, forward_return_col='forward_return'):
        """
        基于IC值动态计算因子权重（IC加权法）
        """
        weights = {}
        
        for factor in factor_cols:
            # 计算Rank IC
            ic = df[factor].corr(df[forward_return_col], method='spearman')
            # 计算IR (IC稳定性)
            ir = ic / df[factor].std() if df[factor].std() != 0 else 0
            weights[factor] = max(0, ic * ir)  # 只保留正向预测因子
        
        # 归一化
        total = sum(weights.values())
        if total > 0:
            weights = {k: v/total for k, v in weights.items()}
        else:
            weights = {k: 1/len(factor_cols) for k in factor_cols}
            
        return weights
    
    def composite_score(self, df, factor_cols, weights=None, method='equal'):
        """
        计算综合得分
        
        method: 'equal'等权, 'ic' IC加权, 'pca' PCA合成
        """
        if method == 'equal':
            # 等权合成
            df['composite_score'] = df[factor_cols].mean(axis=1)
        elif method == 'ic' and weights:
            # IC加权
            df['composite_score'] = sum(df[factor] * weight for factor, weight in weights.items())
        elif method == 'pca':
            # PCA合成（降维去相关性）
            from sklearn.decomposition import PCA
            pca = PCA(n_components=1)
            df['composite_score'] = pca.fit_transform(df[factor_cols].fillna(0))
        
        return df
    
    def industry_neutralization(self, df, score_col='composite_score'):
        """
        行业中性化（可选）
        需要dim_stock表中的industry字段
        """
        sql_industry = "SELECT ts_code, industry FROM dim_stock"
        df_ind = pd.read_sql(sql_industry, self.conn)
        df = df.merge(df_ind, on='ts_code', how='left')
        
        # 行业内标准化
        df['neutral_score'] = df.groupby('industry')[score_col].transform(
            lambda x: (x - x.mean()) / x.std()
        )
        return df
    
    def generate_signals(self, df, score_col='composite_score', top_n=50):
        """
        生成交易信号
        """
        # 排序并分档
        df['rank'] = df[score_col].rank(ascending=False)
        df['quantile'] = pd.qcut(df[score_col], 5, labels=['Q1','Q2','Q3','Q4','Q5'])
        
        # 选股信号
        df['signal'] = 0
        df.loc[df['rank'] <= top_n, 'signal'] = 1  # 买入信号
        df.loc[df['quantile'] == 'Q1', 'signal'] = -1  # 卖出信号（最低分档）
        
        return df
    
    def run_pipeline(self, trade_date=None, method='equal', top_n=50):
        """
        完整流程
        """
        print(f"开始打分：交易日期 {trade_date or self.trade_date}")
        
        # 1. 获取数据
        df = self.fetch_factor_data(trade_date)
        print(f"获取原始数据：{len(df)} 只股票")
        
        # 2. 计算衍生因子
        df = self.calculate_derived_factors(df)
        
        # 3. 预处理
        df, factor_cols = self.factor_preprocessing(df)
        print(f"预处理完成，有效因子：{len(factor_cols)} 个")
        
        # 4. 计算权重（如有历史数据）
        weights = None
        if method == 'ic':
            # 这里需要获取未来收益数据计算IC，实际生产用滚动窗口
            weights = self.calculate_weights_ic(df, factor_cols)
            print("IC加权权重：", weights)
        
        # 5. 合成得分
        df = self.composite_score(df, factor_cols, weights, method)
        
        # 6. 行业中性化（可选）
        # df = self.industry_neutralization(df)
        
        # 7. 生成信号
        df = self.generate_signals(df, top_n=top_n)
        
        # 8. 保存结果到ADS层
        self.save_to_ads(df)
        
        return df.sort_values('composite_score', ascending=False)
    
    def save_to_ads(self, df):
        """
        保存打分结果到ADS层（扩展ads_features_stock_daily表）
        """
        result_df = df[['trade_date', 'ts_code', 'composite_score', 'rank', 'signal']].copy()
        result_df['updated_at'] = datetime.now()
        
        # 可以创建新表或更新现有表
        # 这里展示插入逻辑
        print(f"保存结果到ADS层：{len(result_df)} 条记录")
        return result_df

# 使用示例
if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'password',
        'database': 'tushare_stock',
        'charset': 'utf8mb4'
    }
    
    scorer = TuShareFactorScorer(db_config)
    top_stocks = scorer.run_pipeline(method='equal', top_n=50)
    print(top_stocks[['ts_code', 'composite_score', 'rank', 'signal']].head(20))
三、算法核心要点说明
1. 因子预处理流程
原始数据 → MAD去极值（剔除极端异常） → Z-Score标准化（消除量纲） → 方向调整（正向/反向因子）
2. 打分方法对比
表格
复制
方法	适用场景	优点	缺点
等权法	初期/因子等效	简单稳健	忽略因子有效性差异
IC加权	有历史回测数据	动态调整，优者权重高	需要计算IC稳定性
PCA合成	因子相关性高	去相关性，提取主成分	可解释性较弱
3. 关键SQL查询优化
您的数据表已建立合适索引（如idx_ts_date），上述查询会自动利用这些索引。对于大规模计算，建议：
使用PARTITION BY窗口函数计算滚动指标
对trade_date和ts_code建立联合索引
考虑将计算结果物化到DWS层避免重复计算
4. 彼得·林奇视角适配
考虑到您的投资风格，建议增加以下特色因子：
PEG因子：PE_TTM / 净利润增长率（您的DDL中有相关字段）
现金头寸：结合ods_fina_indicator中的经营现金流
机构持仓变化：可从ods_moneyflow的大单流向推断
