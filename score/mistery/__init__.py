"""
Mistery 520 战法信号包。

基于 5 日均线与 20 日均线的交互生成信号：
- 金叉买入
- 回踩低吸
- 减仓
- 离场
"""

from .strategy import Mistery520Strategy, SignalConfig, compute_520_signals, get_engine

__all__ = ["Mistery520Strategy", "SignalConfig", "compute_520_signals", "get_engine"]
