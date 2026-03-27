from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RiskContext:
    symbol: str
    order_notional: Decimal
    equity: Decimal
    day_pnl: Decimal
    losing_streak: int
    symbol_notional: Decimal


@dataclass(frozen=True)
class RiskResult:
    allowed: bool
    rule: str
    detail: str


class RiskEngine:
    def __init__(
        self,
        *,
        max_daily_loss: Decimal,
        max_losing_streak: int = 3,
        max_symbol_notional_ratio: Decimal = Decimal("0.4"),
    ):
        self._max_daily_loss = max_daily_loss
        self._max_losing_streak = max_losing_streak
        self._max_symbol_notional_ratio = max_symbol_notional_ratio

    def check(self, ctx: RiskContext) -> RiskResult:
        if ctx.day_pnl < 0 and abs(ctx.day_pnl) > self._max_daily_loss:
            return RiskResult(
                False,
                "daily_max_loss",
                f"day_pnl={ctx.day_pnl} exceeds max_daily_loss={self._max_daily_loss}",
            )
        if ctx.losing_streak >= self._max_losing_streak:
            return RiskResult(
                False,
                "losing_streak",
                f"losing_streak={ctx.losing_streak} reaches max_losing_streak={self._max_losing_streak}",
            )
        if ctx.equity <= 0:
            return RiskResult(False, "symbol_risk_budget", "equity must be positive")

        ratio = ctx.symbol_notional / ctx.equity
        if ratio > self._max_symbol_notional_ratio:
            return RiskResult(
                False,
                "symbol_risk_budget",
                (
                    f"symbol_notional/equity={ratio} exceeds "
                    f"max_symbol_notional_ratio={self._max_symbol_notional_ratio}"
                ),
            )
        return RiskResult(True, "pass", "ok")
