from cta_core.config.settings import AppSettings


def test_settings_defaults():
    settings = AppSettings(symbols=["BTCUSDT", "ETHUSDT"], intervals=["15m", "1h"])
    assert settings.timezone == "UTC"
    assert settings.exchange == "binance_um"
    assert settings.symbols == ["BTCUSDT", "ETHUSDT"]
