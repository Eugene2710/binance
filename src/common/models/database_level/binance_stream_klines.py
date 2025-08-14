from pydantic import BaseModel
from pprint import pprint
from datetime import datetime

from src.common.models.service_level.binance_stream_klines import (
    BinanceStreamedKlines,
    BinanceStreamedKlinesMetadata,
)


class BinanceStreamedKlinesDTO(BaseModel):
    symbol: str
    open_time: datetime  # aka start time e.g. 2025-01-06 00:00:00 (ms resolution, cos it is parsed from unix timestamp)
    close_time: datetime  # e.g. 1499644799999
    open_price: float  # e.g. "0.01634790"
    high_price: float  # e.g. "0.80000000"
    low_price: float  # e.g. "0.01575800"
    close_price: float  # e.g. "0.01577100"
    volume: float  # aka base asset volume e.g. "148976.11427815"
    quote_asset_volume: float  # e.g. "2434.19055334"
    number_of_trades: int  # e.g. 308
    taker_buy_base_asset_volume: float  # e.g. "1756.87402397"
    taker_buy_quote_asset_volume: float  # e.g. "28.46694368"
    ignore: str  # e.g. "0" (unused field)
    created_at: datetime | None = None  # Populated at DB level if not provided.

    @staticmethod
    def from_single_kline(input: BinanceStreamedKlines):
        return BinanceStreamedKlinesDTO(
            symbol=input.k.s,
            open_time=datetime.fromtimestamp(
                input.k.t / 1000
            ),  # initial value was in ms
            close_time=datetime.fromtimestamp(
                input.k.T / 1000
            ),  # initial value was in ms
            open_price=float(input.k.o),
            high_price=float(input.k.h),
            low_price=float(input.k.l),
            close_price=float(input.k.c),
            volume=float(input.k.v),
            quote_asset_volume=float(input.k.q),
            number_of_trades=input.k.n,
            taker_buy_base_asset_volume=float(input.k.V),
            taker_buy_quote_asset_volume=float(input.k.Q),
            ignore=input.k.B,
        )


if __name__ == "__main__":
    sample_streamed_kline: BinanceStreamedKlines = BinanceStreamedKlines(
        e="kline",
        E=1754495086023,
        s="BTCUSDT",
        k=BinanceStreamedKlinesMetadata(
            t=1754495085000,
            T=1754495085999,
            s="BTCUSDT",
            i="1s",
            f=5136784691,
            L=5136784694,
            o="115211.88000000",
            c="115211.88000000",
            h="115211.88000000",
            l="115211.87000000",
            v="0.03644000",
            n=4,
            x=True,
            q="4198.32054920",
            V="0.00064000",
            Q="73.73560320",
            B="0",
        ),
    )
    binance_streamed_kline_dto: BinanceStreamedKlinesDTO = (
        BinanceStreamedKlinesDTO.from_single_kline(sample_streamed_kline)
    )
    pprint(binance_streamed_kline_dto)
