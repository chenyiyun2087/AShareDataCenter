# 股票打分算法设计

## 1. 引言

本算法旨在利用Tushare API获取的底层数据库数据，构建一个多维度股票打分系统，为股票投资提供量化参考。该系统将综合考虑股票的价值、成长、质量、动量和市场情绪等多个方面，通过对各项指标进行量化评分和加权汇总，最终得出一个综合评分，以辅助投资者进行股票筛选和决策。

## 2. 数据来源

本算法所需数据均来源于用户提供的Tushare ODS原始表，主要包括以下几类：

*   **日频行情表 (ods_daily)**：提供股票每日的开盘价、收盘价、最高价、最低价、成交量、成交额、涨跌幅等。
*   **日频指标表 (ods_daily_basic)**：包含换手率、市盈率 (PE)、市净率 (PB)、股息率等市场估值和交易活跃度指标。
*   **复权因子表 (ods_adj_factor)**：用于计算复权价格，确保价格数据的连续性和可比性。
*   **财务指标表 (ods_fina_indicator)**：提供ROE、毛利率、资产负债率、净利率、营业收入、总资产、股东权益等财务健康和盈利能力指标。
*   **融资融券明细表 (ods_margin_detail)**：反映市场融资融券交易情况，可用于分析市场情绪。
*   **资金流向表 (ods_moneyflow)**：提供个股资金流入流出数据，用于衡量市场资金对个股的关注度。
*   **每日筹码绩效表 (ods_cyq_perf)**：提供获利比例等筹码分布信息，可用于分析市场成本结构。
*   **股票因子表 (ods_stk_factor)**：包含MACD、KDJ、RSI、BOLL、CCI等技术指标，以及复权后的价格数据。

## 3. 算法设计原则

*   **多维度评估**：综合考虑基本面、技术面和市场面等多个维度，避免单一指标的片面性。
*   **量化可比**：所有指标均进行标准化或归一化处理，确保不同股票之间、不同指标之间具有可比性。
*   **动态调整**：算法参数和权重可根据市场环境和投资策略的变化进行调整。
*   **可解释性**：每个维度的评分和最终综合评分都应有清晰的逻辑和解释，方便用户理解和使用。

## 4. 评分维度与指标细化

本算法将股票评分划分为以下五个主要维度，每个维度下包含若干细化指标：

### 4.1. 价值维度 (Value)

价值投资是寻找被市场低估的股票。本维度主要关注股票的估值水平和资产质量。

| 指标名称 | 数据来源表 | 字段 | 计算方法/说明 |
| :------- | :--------- | :--- | :------------ |
| 市盈率 (PE) | `ods_daily_basic` | `pe_ttm` | 越低越好，反映公司盈利能力与股价的关系。 |
| 市净率 (PB) | `ods_daily_basic` | `pb` | 越低越好，反映公司净资产与股价的关系。 |
| 股息率 (Dividend Yield) | `ods_daily_basic` | `dv_ttm` | 越高越好，反映公司现金分红能力。 |
| 总市值 (Total Market Value) | `ods_daily_basic` | `total_mv` | 可作为规模因子，或用于与其他指标结合。 |

### 4.2. 成长维度 (Growth)

成长性是衡量公司未来发展潜力的重要指标。本维度主要关注公司的营收和利润增长情况。

| 指标名称 | 数据来源表 | 字段 | 计算方法/说明 |
| :------- | :--------- | :--- | :------------ |
| 营业收入增长率 | `ods_fina_indicator` | `op_income` | 计算同比或环比增长率，越高越好。 |
| 净利润增长率 | `ods_fina_indicator` | `netprofit_margin` | 结合`op_income`和`total_assets`等计算净利润，再计算增长率，越高越好。 |
| 净资产收益率 (ROE) 增长 | `ods_fina_indicator` | `roe` | 计算ROE的同比或环比增长率，越高越好。 |

### 4.3. 质量维度 (Quality)

质量维度关注公司的盈利能力、财务健康状况和经营效率。高质量的公司通常具有稳定的盈利和较低的风险。

| 指标名称 | 数据来源表 | 字段 | 计算方法/说明 |
| :------- | :--------- | :--- | :------------ |
| 净资产收益率 (ROE) | `ods_fina_indicator` | `roe` | 越高越好，衡量公司利用自有资本的效率。 |
| 毛利率 (Gross Profit Margin) | `ods_fina_indicator` | `grossprofit_margin` | 越高越好，反映公司产品或服务的盈利能力。 |
| 净利率 (Net Profit Margin) | `ods_fina_indicator` | `netprofit_margin` | 越高越好，反映公司最终盈利能力。 |
| 资产负债率 (Debt to Assets) | `ods_fina_indicator` | `debt_to_assets` | 越低越好，反映公司偿债能力和财务风险。 |

### 4.4. 动量维度 (Momentum)

动量策略认为，过去表现较好的股票在未来一段时间内仍将保持较好的表现。本维度主要关注股票的价格趋势和技术指标。

| 指标名称 | 数据来源表 | 字段 | 计算方法/说明 |
| :------- | :--------- | :--- | :------------ |
| 短期价格动量 | `ods_daily`, `ods_stk_factor` | `pct_chg`, `pct_change` | 计算过去5日、10日等涨跌幅，越高越好。 |
| 中期价格动量 | `ods_daily`, `ods_stk_factor` | `pct_chg`, `pct_change` | 计算过去20日、60日等涨跌幅，越高越好。 |
| MACD指标 | `ods_stk_factor` | `macd` | MACD金叉、柱线持续向上等为加分项。 |
| RSI指标 | `ods_stk_factor` | `rsi_6`, `rsi_12`, `rsi_24` | RSI处于强势区域为加分项。 |

### 4.5. 情绪维度 (Sentiment)

市场情绪反映了投资者对股票的整体看法和交易行为。本维度关注资金流向和融资融券数据。

| 指标名称 | 数据来源表 | 字段 | 计算方法/说明 |
| :------- | :--------- | :--- | :------------ |
| 资金净流入 | `ods_moneyflow` | `net_mf_amount` | 净流入额越大越好，反映市场资金关注度。 |
| 融资余额变化 | `ods_margin_detail` | `rzye` | 融资余额增加或维持高位可能为积极信号。 |
| 融券余额变化 | `ods_margin_detail` | `rqye` | 融券余额减少或维持低位可能为积极信号。 |
| 获利比例 | `ods_cyq_perf` | `winner_rate` | 获利比例越高，短期抛压可能越大，需结合其他指标判断。 |

## 5. 评分计算方法

### 5.1. 单一指标评分

对于每个细化指标，需要将其原始数值转换为0到100之间的分数。常用的方法包括：

1.  **排名法 (Rank-based Scoring)**：将所有股票在该指标上的数值进行排序，然后根据排位赋予分数。例如，排名前10%的股票得100分，后10%得0分，中间线性分布。
2.  **标准化法 (Standardization)**：将指标数值转换为标准分数 (Z-score)，然后通过Sigmoid函数或其他转换函数映射到0-100的区间。
3.  **分段计分法 (Segmented Scoring)**：根据指标的数值区间设定不同的分数。例如，PE < 20 得100分，20 <= PE < 30 得80分，以此类推。

具体采用哪种方法，需要根据指标的特性和分布情况进行选择。对于“越高越好”的指标，得分与数值正相关；对于“越低越好”的指标，得分与数值负相关。

### 5.2. 维度评分

每个维度的总分是其下属所有细化指标分数的加权平均。例如：

`维度A得分 = (指标A1得分 * 权重A1) + (指标A2得分 * 权重A2) + ...`

其中，`权重A1 + 权重A2 + ... = 1`。

### 5.3. 综合评分

最终的股票综合评分是所有维度评分的加权平均。例如：

`综合评分 = (价值维度得分 * 价值权重) + (成长维度得分 * 成长权重) + (质量维度得分 * 质量权重) + (动量维度得分 * 动量权重) + (情绪维度得分 * 情绪权重)`

其中，`价值权重 + 成长权重 + 质量权重 + 动量权重 + 情绪权重 = 1`。

## 6. 权重分配建议

初始权重分配可以采用相对均衡的策略，后续可根据回测结果和投资偏好进行优化调整。以下为初步建议：

| 维度 | 建议权重 |
| :--- | :------- |
| 价值 | 20% |
| 成长 | 25% |
| 质量 | 25% |
| 动量 | 15% |
| 情绪 | 15% |
| **总计** | **100%** |

每个维度内部的细化指标权重也需根据其重要性和有效性进行分配。

## 7. 数据预处理与清洗

*   **缺失值处理**：对于部分指标可能存在的缺失值，可以采用以下方法：
    *   **填充**：使用均值、中位数、众数或前一个有效值进行填充。
    *   **剔除**：如果缺失值过多，可以考虑剔除该指标或相关股票。
*   **异常值处理**：对极端异常值进行截断或替换，避免其对评分结果产生过大影响。
*   **数据类型转换**：确保所有用于计算的字段均为数值类型。
*   **日期处理**：交易日期和公告日期等需要转换为统一的日期格式进行处理。

## 8. 算法实现步骤概述

1.  **数据获取**：从ODS表中读取所需原始数据。
2.  **数据清洗与预处理**：处理缺失值、异常值，并进行数据类型转换。
3.  **指标计算**：根据上述定义，计算每个细化指标的原始数值。
4.  **指标评分**：将每个细化指标的原始数值转换为0-100的标准化分数。
5.  **维度评分**：根据细化指标权重，计算每个维度的综合分数。
6.  **综合评分**：根据维度权重，计算股票的最终综合评分。
7.  **结果输出**：输出股票列表及其综合评分，可按评分高低排序。

## 9. 参考文献

[1] Chen, Y. (2024). Analysis of stock selection strategy of multi-factorial model. *SHS Web of Conferences*, 181, 02025. [https://www.shs-conferences.org/articles/shsconf/pdf/2024/01/shsconf_icdeba2023_02025.pdf](https://www.shs-conferences.org/articles/shsconf/pdf/2024/01/shsconf_icdeba2023_02025.pdf)
[2] Ren, Y. (2023). A Study of Multifactor Quantitative Stock-Selection Model Based on Machine Learning. *Mathematics*, 11(16), 3502. [https://www.mdpi.com/2227-7390/11/16/3502](https://www.mdpi.com/2227-7390/11/16/3502)
[3] Investopedia. (n.d.). *Piotroski Score: 9 Criteria for Analyzing Value Stocks*. Retrieved from [https://www.investopedia.com/terms/p/piotroski-score.asp](https://www.investopedia.com/terms/p/piotroski-score.asp)
[4] MSCI. (n.d.). *MSCI Core Multiple-Factor Indexes Methodology*. Retrieved from [https://www.msci.com/documents/10199/ecc2cfae-0766-fa4f-8ce5-0fc3028272f3](https://www.msci.com/documents/10199/ecc2cfae-0766-fa4f-8ce5-0fc3028272f3)
[5] Bankrate. (2024, October 22). *8 Important Financial Ratios To Know When Analyzing A Stock*. Retrieved from [https://www.bankrate.com/investing/important-financial-ratios/](https://www.bankrate.com/investing/important-financial-ratios/)
[6] Schwab. (2024, August 20). *Buy, Hold, Sell: What Analyst Stock Ratings Mean*. Retrieved from [https://www.schwab.com/learn/story/buy-hold-sell-what-analyst-stock-ratings-mean](https://www.schwab.com/learn/story/buy-hold-sell-what-analyst-stock-ratings-mean)
[7] iShares. (2023, March 2). *Factor investing 101: The basics and its role in portfolios*. Retrieved from [https://www.ishares.com/us/investor-education/investment-strategies/what-is-factor-investing](https://www.ishares.com/us/investor-education/investment-strategies/what-is-factor-investing)
[8] Old School Value. (n.d.). *Piotroski F-Score Screening and Calculation to Beat the S&P500*. Retrieved from [https://www.oldschoolvalue.com/investment-tools/piotroski-f-score-screening-early-often/](https://www.oldschoolvalue.com/investment-tools/piotroski-f-score-screening-early-often/)
[9] Quant-Investing. (n.d.). *Piotroski F-Score*. Retrieved from [https://www.quant-investing.com/glossary/piotroski-f-score](https://www.quant-investing.com/glossary/piotroski-f-score)
[10] Wikipedia. (n.d.). *Piotroski F-score*. Retrieved from [https://en.wikipedia.org/wiki/Piotroski_F-score](https://en.wikipedia.org/wiki/Piotroski_F-score)
[11] Fidelity Institutional. (n.d.). *Quantitative Stock Selection Model*. Retrieved from [https://institutional.fidelity.com/app/item/RD_9906683/quantitative-stock-selection-model.html](https://institutional.fidelity.com/app/item/RD_9906683/quantitative-stock-selection-model.html)
[12] LPL Financial. (2025, July 22). *Exploring Quality, Value, and Momentum in Equity*. Retrieved from [https://www.lpl.com/research/street-view/factor-fiction-quality-value-and-momentum.html](https://www.lpl.com/research/street-view/factor-fiction-quality-value-and-momentum.html)
[13] Aberdeen Standard Investments. (n.d.). *Multi-factor: Why it takes value, quality, and momentum*. Retrieved from [https://www.aberdeeninvestments.com/en-us/institutional/insights-and-research/io-2024-multi-factor-why-it-takes-value-quality-momentum](https://www.aberdeeninvestments.com/en-us/institutional/insights-and-research/io-2024-multi-factor-why-it-takes-value-quality-momentum)
[14] Kavout. (2025, April 24). *Rank U.S. Stocks by Quality, Value, Growth, and Momentum*. Retrieved from [https://www.kavout.com/market-lens/how-to-pick-winning-stocks-with-ai-rank-u-s-stocks-by-quality-value-growth-and-momentum](https://www.kavout.com/market-lens/how-to-pick-winning-stocks-with-ai-rank-u-s-stocks-by-quality-value-growth-and-momentum)
[15] Zhang, J. (2025). A multifactor model using large language models and multimodal investor sentiment for stock selection. *Expert Systems with Applications*, 262, 121024. [https://www.sciencedirect.com/science/article/pii/S1059056025004447](https://www.sciencedirect.com/science/article/pii/S1059056025004447)
[16] Wang, K. (2025). Multifactor prediction model for stock market analysis based on deep learning. *Frontiers in Public Health*, 13, 11814281. [https://pmc.ncbi.nlm.nih.gov/articles/PMC11814281/](https://pmc.ncbi.nlm.nih.gov/articles/PMC11814281/)
[17] Reniers, C. (n.d.). *Adding sentiment to multifactor equity strategies*. Probability.nl. Retrieved from [https://probability.nl/wp-content/uploads/2021/02/SentimentFactorsProbability.pdf](https://probability.nl/wp-content/uploads/2021/02/SentimentFactorsProbability.pdf)
[18] Yahoo Finance. (2025, September 13). *What you need to know about quant investing in China*. Retrieved from [https://finance.yahoo.com/news/know-quant-investing-china-083002355.html](https://finance.yahoo.com/news/know-quant-investing-china-083002355.html)
[19] Jasper Capital. (n.d.). *The Relationship Between China A Quant Strategies and ...*. Retrieved from [https://jasperhk.com/new-blog-89](https://jasperhk.com/new-blog-89)
[20] MSCI. (n.d.). *Are You Really Capturing the Right Factors?*. Retrieved from [https://www.msci.com/downloads/web/msci-com/research-and-insights/paper/are-you-really-capturing-the-right-factors-unlocking-deeper-insights-in-china-a-share-factor-investing/Enhancing%20factor%20strategies%20China%20A%20share%20factor%20investing.pdf](https://www.msci.com/downloads/web/msci-com/research-and-insights/paper/are-you-really-capturing-the-right-factors-unlocking-deeper-insights-in-china-a-share-factor-investing/Enhancing%20factor%20strategies%20China%20A%20share%20factor%20investing.pdf)
[21] Premia Partners. (2025, May 7). *China A-shares Q1 2025 factor review*. Retrieved from [https://www.premia-partners.com/insight/china-a-shares-q1-2025-factor-review](https://www.premia-partners.com/insight/china-a-shares-q1-2025-factor-review)
