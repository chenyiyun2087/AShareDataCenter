"""Optimizer configuration."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple, Optional


# Factor category names (7 alpha categories)
CATEGORY_NAMES: List[str] = [
    "momentum",
    "value",
    "quality",
    "technical",
    "capital",
    "chip",
    "size",
]


@dataclass
class OptimizerConfig:
    """Configuration for the walk-forward factor optimizer."""

    # --- Date ranges ---
    backtest_start: int = 20220101
    backtest_end: int = 20251231

    # Walk-forward folds: list of (train_end, test_start, test_end)
    # Derived automatically if empty
    folds: List[Tuple[int, int, int]] = field(default_factory=list)

    # Out-of-sample final test period
    oos_start: int = 20250701
    oos_end: int = 20251231

    # --- Portfolio parameters ---
    num_stocks: int = 5  # Number of stocks to hold
    top_n: Optional[int] = None  # Backward-compatible alias for num_stocks
    holding_days: int = 20  # Monthly rebalance
    initial_capital: float = 1_000_000.0  # Initial portfolio capital

    # --- Transaction costs ---
    commission: float = 0.0003  # 0.03% per trade
    stamp_tax: float = 0.001  # 0.1% sell only
    slippage: float = 0.001  # 0.1% slippage

    # --- Weight constraints ---
    weight_min: float = 0.05  # 5% minimum per category
    weight_max: float = 0.30  # 30% maximum per category

    # --- Optimization ---
    turnover_penalty: float = 0.002  # Penalty for portfolio turnover
    weight_round_step: float = 0.05  # Round to nearest 5%

    # --- Benchmark ---
    # CSI 500 (000905.SH) 未同步，使用沪深300；如需CSI 500需先同步 ods_index_daily
    benchmark_code: str = "000300.SH"  # CSI 300

    def get_default_folds(self) -> List[Tuple[int, int, int]]:
        """Return default walk-forward folds if none specified."""
        if self.folds:
            return self.folds
        return [
            # (train_end, test_start, test_end)
            (20230630, 20230701, 20231231),
            (20231231, 20240101, 20240630),
            (20240630, 20240701, 20241231),
            (20241231, 20250101, 20250630),
        ]


    def __post_init__(self) -> None:
        """Normalize aliases and validate key parameters."""
        if self.top_n is not None and self.top_n > 0:
            self.num_stocks = self.top_n
        self.top_n = self.num_stocks

        if self.num_stocks <= 0:
            raise ValueError("num_stocks must be > 0")
        if self.holding_days <= 0:
            raise ValueError("holding_days must be > 0")
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be > 0")
