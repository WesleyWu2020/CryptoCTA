from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from decimal import Decimal
from typing import ClassVar

import polars as pl

from cta_core.strategy_runtime.base import BaseStrategy, StrategyContext, StrategyDecision, StrategyDecisionType


@dataclass(frozen=True)
class RSIThresholdConfig:
    rsi_window: int = 14
    buy_threshold: float = 20.0
    sell_threshold: float = 70.0
    trend_fast_ema_window: int = 50
    trend_slow_ema_window: int = 200
    use_trend_filter: bool = True
    use_momentum_mode: bool = False
    adx_window: int = 14
    adx_threshold: float = 30.0
    use_adx_filter: bool = False
    adx_filter_mode: str = "min"
    quantity: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if self.rsi_window <= 1:
            raise ValueError("rsi_window must be > 1")
        if not (0 <= self.buy_threshold <= 100):
            raise ValueError("buy_threshold must be in [0, 100]")
        if not (0 <= self.sell_threshold <= 100):
            raise ValueError("sell_threshold must be in [0, 100]")
        if self.buy_threshold >= self.sell_threshold:
            raise ValueError("buy_threshold must be < sell_threshold")
        if self.trend_fast_ema_window <= 1:
            raise ValueError("trend_fast_ema_window must be > 1")
        if self.trend_slow_ema_window <= 1:
            raise ValueError("trend_slow_ema_window must be > 1")
        if self.trend_fast_ema_window >= self.trend_slow_ema_window:
            raise ValueError("trend_fast_ema_window must be < trend_slow_ema_window")
        if self.adx_window <= 1:
            raise ValueError("adx_window must be > 1")
        if not (0 <= self.adx_threshold <= 100):
            raise ValueError("adx_threshold must be in [0, 100]")
        if self.adx_filter_mode not in {"min", "max"}:
            raise ValueError("adx_filter_mode must be 'min' or 'max'")
        if self.quantity <= 0:
            raise ValueError("quantity must be > 0")


def _compute_rsi(closes: list[float], window: int) -> list[float | None]:
    if not closes:
        return []

    rsi: list[float | None] = [None for _ in closes]
    if len(closes) <= window:
        return rsi

    gains: list[float] = []
    losses: list[float] = []
    for idx in range(1, window + 1):
        change = closes[idx] - closes[idx - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    avg_gain = sum(gains) / window
    avg_loss = sum(losses) / window
    if avg_loss == 0:
        rsi[window] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi[window] = 100.0 - (100.0 / (1.0 + rs))

    for idx in range(window + 1, len(closes)):
        change = closes[idx] - closes[idx - 1]
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        avg_gain = ((avg_gain * (window - 1)) + gain) / window
        avg_loss = ((avg_loss * (window - 1)) + loss) / window

        if avg_loss == 0:
            rsi[idx] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[idx] = 100.0 - (100.0 / (1.0 + rs))

    return rsi


def _compute_ema(values: list[float], window: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (window + 1.0)
    out: list[float] = [values[0]]
    prev = values[0]
    for value in values[1:]:
        prev = alpha * value + (1.0 - alpha) * prev
        out.append(prev)
    return out


def _compute_dmi_adx(
    *,
    high: list[float],
    low: list[float],
    close: list[float],
    window: int,
) -> tuple[list[float | None], list[float | None], list[float | None]]:
    size = len(close)
    adx: list[float | None] = [None for _ in range(size)]
    plus_di: list[float | None] = [None for _ in range(size)]
    minus_di: list[float | None] = [None for _ in range(size)]
    if size <= window:
        return adx, plus_di, minus_di

    tr = [0.0 for _ in range(size)]
    plus_dm = [0.0 for _ in range(size)]
    minus_dm = [0.0 for _ in range(size)]

    for index in range(1, size):
        up_move = high[index] - high[index - 1]
        down_move = low[index - 1] - low[index]
        plus_dm[index] = up_move if up_move > down_move and up_move > 0 else 0.0
        minus_dm[index] = down_move if down_move > up_move and down_move > 0 else 0.0
        tr[index] = max(
            high[index] - low[index],
            abs(high[index] - close[index - 1]),
            abs(low[index] - close[index - 1]),
        )

    smoothed_tr = sum(tr[1 : window + 1])
    smoothed_plus_dm = sum(plus_dm[1 : window + 1])
    smoothed_minus_dm = sum(minus_dm[1 : window + 1])
    dx: list[float | None] = [None for _ in range(size)]

    for index in range(window, size):
        if index > window:
            smoothed_tr = smoothed_tr - (smoothed_tr / window) + tr[index]
            smoothed_plus_dm = smoothed_plus_dm - (smoothed_plus_dm / window) + plus_dm[index]
            smoothed_minus_dm = smoothed_minus_dm - (smoothed_minus_dm / window) + minus_dm[index]

        if smoothed_tr <= 0:
            plus_value = 0.0
            minus_value = 0.0
        else:
            plus_value = 100.0 * (smoothed_plus_dm / smoothed_tr)
            minus_value = 100.0 * (smoothed_minus_dm / smoothed_tr)
        plus_di[index] = plus_value
        minus_di[index] = minus_value

        denom = plus_value + minus_value
        dx[index] = 0.0 if denom <= 0 else 100.0 * abs(plus_value - minus_value) / denom

    adx_start = window * 2 - 1
    if size <= adx_start:
        return adx, plus_di, minus_di

    initial_dx = [value for value in dx[window : adx_start + 1] if value is not None]
    if len(initial_dx) != window:
        return adx, plus_di, minus_di

    adx_value = sum(initial_dx) / window
    adx[adx_start] = adx_value
    for index in range(adx_start + 1, size):
        if dx[index] is None:
            continue
        adx_value = ((adx_value * (window - 1)) + float(dx[index])) / window
        adx[index] = adx_value

    return adx, plus_di, minus_di


@dataclass
class RSIThresholdStrategy(BaseStrategy):
    config: RSIThresholdConfig = field(default_factory=RSIThresholdConfig)
    strategy_id: ClassVar[str] = "rsi_threshold"
    _long_open_by_symbol: dict[str, bool] = field(init=False, default_factory=dict, repr=False, compare=False)

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        if "close" not in bars.columns:
            raise ValueError("bars must contain close")

        close = [float(value) for value in bars.get_column("close").to_list()]
        high = (
            [float(value) for value in bars.get_column("high").to_list()]
            if "high" in bars.columns
            else close
        )
        low = (
            [float(value) for value in bars.get_column("low").to_list()]
            if "low" in bars.columns
            else close
        )
        rsi = _compute_rsi(close, self.config.rsi_window)
        ema_fast = _compute_ema(close, self.config.trend_fast_ema_window)
        ema_slow = _compute_ema(close, self.config.trend_slow_ema_window)
        adx, plus_di, minus_di = _compute_dmi_adx(high=high, low=low, close=close, window=self.config.adx_window)
        trend_long_ok = [
            index >= self.config.trend_slow_ema_window - 1 and ema_fast[index] > ema_slow[index]
            for index in range(len(close))
        ]
        if self.config.adx_filter_mode == "max":
            adx_trend_ok = [value is not None and value < self.config.adx_threshold for value in adx]
        else:
            adx_trend_ok = [value is not None and value > self.config.adx_threshold for value in adx]
        if self.config.use_momentum_mode:
            rsi_buy_signal = [
                value is not None
                and value > self.config.sell_threshold
                and ((not self.config.use_trend_filter) or trend_long_ok[index])
                and ((not self.config.use_adx_filter) or adx_trend_ok[index])
                for index, value in enumerate(rsi)
            ]
            rsi_sell_signal = [value is not None and value < self.config.buy_threshold for value in rsi]
        else:
            rsi_buy_signal = [
                value is not None
                and value < self.config.buy_threshold
                and ((not self.config.use_trend_filter) or trend_long_ok[index])
                and ((not self.config.use_adx_filter) or adx_trend_ok[index])
                for index, value in enumerate(rsi)
            ]
            rsi_sell_signal = [value is not None and value > self.config.sell_threshold for value in rsi]

        return bars.with_columns(
            pl.Series("rsi", rsi),
            pl.Series("ema_fast", ema_fast),
            pl.Series("ema_slow", ema_slow),
            pl.Series("trend_long_ok", trend_long_ok),
            pl.Series("plus_di", plus_di),
            pl.Series("minus_di", minus_di),
            pl.Series("adx", adx),
            pl.Series("adx_trend_ok", adx_trend_ok),
            pl.Series("rsi_buy_signal", rsi_buy_signal),
            pl.Series("rsi_sell_signal", rsi_sell_signal),
        )

    def on_start(self, context: StrategyContext) -> None:
        self.set_long_open(symbol=context.symbol, is_open=False)

    def on_bar(self, context: StrategyContext) -> list[StrategyDecision]:
        if context.bars.height == 0:
            return []

        current_bar = context.current_bar
        long_open = self._long_open_by_symbol.get(context.symbol, False)
        if bool(current_bar.get("rsi_buy_signal")) and not long_open:
            self.set_long_open(symbol=context.symbol, is_open=True)
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.ENTER_LONG,
                    size=self.config.quantity,
                    reason="rsi_below_buy_threshold",
                )
            ]

        if bool(current_bar.get("rsi_sell_signal")) and long_open:
            self.set_long_open(symbol=context.symbol, is_open=False)
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.EXIT_LONG,
                    reason="rsi_above_sell_threshold",
                )
            ]

        return []

    def on_finish(self, context: StrategyContext) -> None:
        self.set_long_open(symbol=context.symbol, is_open=False)

    def set_long_open(self, *, symbol: str, is_open: bool) -> None:
        self._long_open_by_symbol[symbol] = is_open

    @classmethod
    def register_cli_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--rsi-window", type=int, default=14)
        parser.add_argument("--rsi-buy-threshold", type=float, default=20.0)
        parser.add_argument("--rsi-sell-threshold", type=float, default=70.0)
        parser.add_argument("--rsi-momentum-mode", action="store_true")
        parser.add_argument("--adx-window", type=int, default=14)
        parser.add_argument("--adx-threshold", type=float, default=30.0)
        parser.add_argument("--enable-adx-filter", action="store_true")
        parser.add_argument("--adx-filter-mode", choices=("min", "max"), default="min")
        parser.add_argument("--trend-fast-ema-window", type=int, default=50)
        parser.add_argument("--trend-slow-ema-window", type=int, default=200)
        parser.add_argument("--disable-trend-filter", action="store_true")
        parser.add_argument("--atr-window", type=int, default=14)
        parser.add_argument("--atr-stop-multiplier", type=float, default=2.0)
        parser.add_argument("--atr-trailing-multiplier", type=float, default=2.0)
        parser.add_argument("--max-hold-bars", type=int, default=None)
        parser.add_argument("--enable-partial-take-profit", action="store_true")
        parser.add_argument("--take-profit-r-multiple", type=float, default=1.0)
        parser.add_argument("--take-profit-fraction", type=float, default=0.5)
        parser.add_argument("--cooldown-bars", type=int, default=0)

    @classmethod
    def config_from_args(cls, args: argparse.Namespace) -> RSIThresholdConfig:
        return RSIThresholdConfig(
            rsi_window=args.rsi_window,
            buy_threshold=args.rsi_buy_threshold,
            sell_threshold=args.rsi_sell_threshold,
            trend_fast_ema_window=args.trend_fast_ema_window,
            trend_slow_ema_window=args.trend_slow_ema_window,
            use_trend_filter=not bool(getattr(args, "disable_trend_filter", False)),
            use_momentum_mode=bool(getattr(args, "rsi_momentum_mode", False)),
            adx_window=args.adx_window,
            adx_threshold=args.adx_threshold,
            use_adx_filter=bool(getattr(args, "enable_adx_filter", False)),
            adx_filter_mode=args.adx_filter_mode,
            quantity=Decimal("1"),
        )


__all__ = ["RSIThresholdConfig", "RSIThresholdStrategy"]
