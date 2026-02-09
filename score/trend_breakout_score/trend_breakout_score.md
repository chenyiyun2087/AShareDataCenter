# 趋势突破评分（Trend Breakout）

## 目标
基于“突破 + 趋势 + 量能 + 相对强度 + 流动性 + 收敛 + 多头排列 + 乖离率 + 温和放量 + 筹码稳定性”的组合因子，
生成日频趋势突破评分，总分为加权和（0-100）。

## 因子权重与字段

| 因子 | 代码字段 | 权重 | 说明 |
| --- | --- | --- | --- |
| Breakout (突破) | `s_breakout` | 0.22 | 收盘价/20日最高价，越接近甚至突破新高得分越高 |
| Trend (趋势) | `s_trend` | 0.12 | 收盘>MA20、MA10>MA20、MA20斜率向上 |
| Volume (量能) | `s_volume` | 0.12 | 量能活跃度（量比/相对成交量） |
| RS (相对强度) | `s_rs` | 0.12 | 近20日涨幅，优选强于市场 |
| Liquidity (流动性) | `s_liquidity` | 0.10 | 成交额规模（20日均值） |
| Contraction (收敛) | `s_contraction` | 0.10 | 波动率收敛（短期/长期波动） |
| Bull Align (多头) | `s_bull_align` | 0.08 | MA5 > MA10 > MA20 |
| Bias (乖离率) | `s_bias` | 0.07 | 价格偏离MA20程度（倒U型） |
| Vol Mild (温和放量) | `s_vol_mild` | 0.04 | 量比接近1.5为最佳 |
| Chip (筹码) | `s_chip` | 0.03 | 筹码结构稳定性（集中度高、成本偏离低） |

## 数据来源与复用层

| 因子 | 主要表 | 备用/加工说明 |
| --- | --- | --- |
| Breakout | `dwd_daily` | 使用20日最高价窗口计算 |
| Trend/Bull Align/Bias | `dwd_daily` | 通过20/10/5日均线窗口计算 |
| Volume/Vol Mild | `dwd_daily` | 量比=当日成交量/20日均量 |
| RS | `dws_price_adj_daily` | 复权20日收益率 `qfq_ret_20` |
| Liquidity | `ads_features_stock_daily` | 优先用`amt_ma20`，缺失时用`dwd_daily`的20日均额 |
| Contraction | `dws_price_adj_daily` | 使用`qfq_ret_1`的5日/20日波动率比 |
| Chip | `dwd_chip_stability` | `chip_concentration` + `cost_deviation` 组合 |

## 评分规则要点

1. **Breakout**：`close/high_20` 做截断后按百分位标准化（0-100）。
2. **Trend**：趋势条件满足程度打分（100/70/40/0）。
3. **Volume**：量比按百分位标准化（0-100）。
4. **RS**：20日复权收益率按百分位标准化（0-100）。
5. **Liquidity**：20日均额按百分位标准化（0-100）。
6. **Contraction**：`vol_5/vol_20` 越低越好，按逆序百分位标准化（0-100）。
7. **Bull Align**：满足多头排列得100，否则0。
8. **Bias**：偏离度绝对值分段（≤2%:100，≤5%:70，≤8%:40，其他:10）。
9. **Vol Mild**：量比接近1.5时最高分，按距离衰减。
10. **Chip**：筹码集中度（高）与成本偏离（低）组合得分。

## 使用方式

在 `score/trend_breakout_score/score_query.py` 中提供 `TrendBreakoutScoreQuery`：

```python
from score.trend_breakout_score.score_query import TrendBreakoutScoreQuery, get_engine

engine = get_engine(password="your_password")
q = TrendBreakoutScoreQuery(engine)
df = q.get_scores(trade_date=20240105)
```

