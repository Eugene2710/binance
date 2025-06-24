from datetime import datetime
from pydantic import BaseModel
from src.models.service_level.binance_klines import SingleKline


class SingleKlineDTO(BaseModel):
    """
    DTO for SingleKline into ClickHouse
    """
    symbol: str
    open_time: datetime  # e.g. 2025-01-06 00:00:00 (ms resolution, cos it is parsed from unix timestamp)
    open_price: float  # e.g. "0.01634790"
    high_price: float  # e.g. "0.80000000"
    low_price: float  # e.g. "0.01575800"
    close_price: float  # e.g. "0.01577100"
    volume: float  # e.g. "148976.11427815"
    close_time: datetime  # e.g. 1499644799999
    quote_asset_volume: float  # e.g. "2434.19055334"
    number_of_trades: int  # e.g. 308
    taker_buy_base_asset_volume: float  # e.g. "1756.87402397"
    taker_buy_quote_asset_volume: float  # e.g. "28.46694368"
    ignore: str  # e.g. "0" (unused field)
    created_at: datetime | None = None  # Populated at DB level if not provided.

    @staticmethod
    def from_single_kline(input: SingleKline) -> "SingleKlineDTO":
        return SingleKlineDTO(
            symbol=input.symbol,
            open_time=datetime.fromtimestamp(input.open_time),
            open_price=float(input.open_price),
            high_price=float(input.high_price),
            low_price=float(input.low_price),
            close_price=float(input.close_price),
            volume=float(input.volume),
            close_time=datetime.fromtimestamp(input.close_time),
            quote_asset_volume=float(input.quote_asset_volume),
            number_of_trades=input.number_of_trades,
            taker_buy_base_asset_volume=float(input.taker_buy_base_asset_volume),
            taker_buy_quote_asset_volume=float(input.taker_buy_quote_asset_volume),
            ignore=input.ignore,
        )