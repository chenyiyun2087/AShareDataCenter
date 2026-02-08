# Mistery 520 战法

本包实现 520 战法信号生成，核心围绕 5 日均线（短期攻击线）与 20 日均线（中期生命线）。

## 信号定义

- **金叉买入**：5 日均线上穿 20 日均线，且 20 日均线向上；可选量能确认。
- **回踩低吸**：价格回踩 20 日均线不破（允许一定容差），5 日均线向上拐头，且 20 日均线向上。
- **减仓信号**：收盘价跌破 5 日均线。
- **离场信号**：5 日均线下穿 20 日均线（死叉）。

> 注意：20 日均线方向具有一票否决权，只有当 20 日均线呈上扬趋势时，金叉与回踩信号才生效。

## 快速使用

```python
import pandas as pd
from score.mistery import compute_520_signals, SignalConfig

# df 需包含 trade_date 与 close 列，可选 vol 列
signals = compute_520_signals(df, SignalConfig())
print(signals.tail())
```

## 数据库方式

```python
from score.mistery import Mistery520Strategy, get_engine

engine = get_engine(password="your_password")
strategy = Mistery520Strategy(engine=engine)
raw = strategy.load_daily_data("000001.SZ", 20240101, 20240531)
result = strategy.generate_signals(raw)
print(result.tail())
```
