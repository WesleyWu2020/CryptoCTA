from __future__ import annotations

import argparse

from cta_core.config.run_config import RunConfig
from cta_core.strategy_runtime.registry import get_strategy_class, list_strategy_ids


def _bootstrap_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--strategy", choices=list_strategy_ids())
    parser.add_argument("--preset", default=None)
    parser.add_argument("--list-strategies", action="store_true")
    return parser.parse_known_args(argv)[0]


def _map_preset_use_flag(
    *,
    key: str,
    value: object,
    parser_dests: set[str],
) -> dict[str, object]:
    if not key.startswith("use_") or not isinstance(value, bool):
        return {}

    suffix = key.removeprefix("use_")
    enable_key = f"enable_{suffix}"
    disable_key = f"disable_{suffix}"

    mapped: dict[str, object] = {}
    if enable_key in parser_dests:
        mapped[enable_key] = value
    if disable_key in parser_dests:
        mapped[disable_key] = not value
    return mapped


def _apply_preset_defaults(parser: argparse.ArgumentParser, preset_id: str) -> None:
    from cta_core.app.strategy_presets import get_backtest_strategy

    preset = get_backtest_strategy(preset_id)
    parser_dests = {action.dest for action in parser._actions}

    preset_defaults: dict[str, object] = {}
    for key, value in preset.defaults.items():
        if key in parser_dests:
            preset_defaults[key] = value
            continue
        mapped = _map_preset_use_flag(key=key, value=value, parser_dests=parser_dests)
        for mapped_key, mapped_value in mapped.items():
            preset_defaults.setdefault(mapped_key, mapped_value)

    parser.set_defaults(**preset_defaults)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    bootstrap = _bootstrap_args(argv)

    parser = argparse.ArgumentParser(description="Run registered strategy backtests.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list-strategies", action="store_true", help="print registered strategy ids")
    group.add_argument("--strategy", choices=list_strategy_ids(), help="build or execute a registered strategy")

    RunConfig.register_cli_args(parser)

    if bootstrap.strategy:
        strategy_cls = get_strategy_class(bootstrap.strategy)
        if hasattr(strategy_cls, "register_cli_args"):
            strategy_cls.register_cli_args(parser)

    if bootstrap.preset is not None:
        _apply_preset_defaults(parser, bootstrap.preset)

    return parser.parse_args(argv)


__all__ = ["parse_args"]
