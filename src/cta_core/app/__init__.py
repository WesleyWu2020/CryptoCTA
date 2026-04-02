from __future__ import annotations

import importlib

__all__ = ["run_backtest", "run_turtle_backtest", "write_backtest_output"]

_EXPORTS = {
    "run_backtest": ("cta_core.app.backtest_runner", "run_backtest"),
    "run_turtle_backtest": ("cta_core.app.turtle_backtest", "run_turtle_backtest"),
    "write_backtest_output": ("cta_core.app.turtle_backtest", "write_backtest_output"),
}


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _EXPORTS[name]
    module = importlib.import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
