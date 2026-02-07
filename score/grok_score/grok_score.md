# 股票打分算法研究

## 多因子评分模型 (Multi-Factor Scoring Model)

基于您的数据库 schema（TuShare 数据源，针对中国 A 股市场），我推荐使用 **多因子评分模型 (Multi-Factor Scoring Model)** 来进行股票打分。该模型源于量化投资研究，特别是 Fama-French 模型的变体和 A 股市场实证研究（如 Barra CNE5 模型）。研究显示，在 A 股中，价值、质量、动量、流动性和规模因子组合能产生 3-8% 的年化超额收益。

**核心逻辑**：通过规范化（Z-Score）和加权聚合因子来计算复合分数，适用于日频数据和每日选股策略。

---

## 算法概述

### 1. 因子选择
*数据来源: ads_features_stock_daily, ods_daily_basic, ods_moneyflow*

*   **动量因子 (Momentum) [正向]**
    *   `ret_20`: 20日累计收益率
    *   `ret_60`: 60日累计收益率
*   **价值因子 (Value) [负向]**
    *   `pe_ttm`: 滚动市盈率
    *   `pb`: 市净率
*   **质量因子 (Quality) [正向]**
    *   `roe`: 净资产收益率
    *   `grossprofit_margin`: 毛利率
*   **偿债能力因子 (Solvency) [负向]**
    *   `debt_to_assets`: 资产负债率
*   **流动性因子 (Liquidity) [正向]**
    *   `turnover_rate`: 换手率
*   **规模因子 (Size) [可选/中性]**
    *   `circ_mv`: 流通市值 (可用于过滤超大盘股或进行风格暴露控制)

**额外增强**:
*   可整合 `ods_moneyflow` 中的净流入 (`net_mf_amount`)
*   或 `ods_stk_factor` 中的技术分数 (`score`)

### 2. 预处理与过滤
*   使用 `ads_universe_daily` 表 (或类似逻辑) 过滤可交易股票 (`is_tradable=1`, `is_suspended=0`)。
*   剔除 ST 股或上市不足 60 天的新股。

### 3. 算法步骤
1.  **数据提取**：从数据库查询指定 `trade_date` 的所有股票因子数据。
2.  **去极值与标准化 (Z-Score)**：
    *   对每个因子列 $X$，计算均值 $\mu$ 和标准差 $\sigma$。
    *   $Z = \frac{X - \mu}{\sigma}$
    *   对于负向因子（越低越好），乘以 -1。
3.  **加权聚合**：
    *   按类别分配权重，例如：
        *   动量: 30%
        *   价值: 30%
        *   质量: 20%
        *   偿债: 10%
        *   流动性: 10%
    *   计算复合分数: $Score = \sum (w_i \times Z_i)$
4.  **排名**：按分数降序排序，选取前 10% 为高分股票。

### 4. 回测建议
使用 `ods_daily` 的历史回报数据进行回测验证。在 A 股研究中，该模型的 Sharpe 比率通常可达 0.5-1.0。

---

## Python 代码实现示例

以下是使用 Python (pandas) 实现的评分逻辑。

```python
import pandas as pd
import numpy as np

# 假设 df 是查询到的 DataFrame，包含列：
# ts_code, ret_20, ret_60, pe_ttm, pb, roe, grossprofit_margin, debt_to_assets, turnover_rate
# 示例数据加载 (实际应从数据库读取)
# df = pd.read_sql("SELECT ...", con=engine) 

# 定义因子类别、权重和方向（1 正向，-1 负向）
factors = {
    'momentum':  {'cols': ['ret_20', 'ret_60'],         'weight': 0.3, 'dir': 1},
    'value':     {'cols': ['pe_ttm', 'pb'],             'weight': 0.3, 'dir': -1},
    'quality':   {'cols': ['roe', 'grossprofit_margin'], 'weight': 0.2, 'dir': 1},
    'solvency':  {'cols': ['debt_to_assets'],           'weight': 0.1, 'dir': -1},
    'liquidity': {'cols': ['turnover_rate'],            'weight': 0.1, 'dir': 1}
}

# Z-Score 标准化函数
def z_score(col, direction=1):
    mean = col.mean()
    std = col.std()
    if std == 0:
        return 0
    return direction * (col - mean) / std

# 计算分数
df['score'] = 0.0

for cat, info in factors.items():
    cat_score = 0.0
    for col in info['cols']:
        # 简单缺失值填充，生产环境建议用行业中位数填充
        if df[col].isnull().any():
            df[col] = df[col].fillna(df[col].median())
            
        cat_score += z_score(df[col], info['dir'])
    
    # 类别内取平均
    cat_score /= len(info['cols'])
    # 加权累加到总分
    df['score'] += cat_score * info['weight']

# 排序并输出前 10 高分股票
df_sorted = df.sort_values('score', ascending=False)
print(df_sorted[['ts_code', 'score']].head(10))

# 建议将分数存储在新表如 ads_stock_score_daily 中
```

## 高级扩展建议

1.  **机器学习预测 (ML Prediction)**
    *   使用 XGBoost 等模型预测未来回报（如 `ret_20`）。
    *   以因子作为特征，A 股实证研究显示 XGBoost 可提升方向预测准确率至 60-70%。

2.  **动态权重 (Dynamic Weighting)**
    *   根据市场波动率调整权重。例如：高波动市场环境配置更多权重给质量因子 (Quality)，低波动牛市环境配置更多给动量因子 (Momentum)。

3.  **因子有效性检验**
    *   A 股因子衰减较快，建议定期进行外样本测试（使用 2020 年以后的数据验证有效性）。