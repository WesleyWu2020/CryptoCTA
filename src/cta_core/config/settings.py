from pydantic import BaseModel, Field


class AppSettings(BaseModel):
    exchange: str = "binance_um"
    timezone: str = "UTC"
    symbols: list[str] = Field(min_length=1)
    intervals: list[str] = Field(min_length=1)
