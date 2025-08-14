from pydantic import BaseModel
from pprint import pprint
from typing import Any


class BinanceStreamedKlinesMetadata(BaseModel):
    t: int  # Kline start time
    T: int  # Kline close time
    s: str  # Symbol
    i: str  # Interval, e.g. "1s", "1m"
    f: int  # First trade ID
    L: int  # Last trade ID
    o: str  # Open price
    c: str  # Close price
    h: str  # High price
    l: str  # noqa: E741 Low price
    v: str  # Base asset volume
    n: int  # Number of trades
    x: bool  # Is this kline closed?
    q: str  # Quote asset volume
    V: str  # Taker buy base asset volume
    Q: str  # Taker buy quote asset volume
    B: str  # ignore


class BinanceStreamedKlines(BaseModel):
    e: str  # Event type
    E: int  # Event time
    s: str  # Symbol
    k: BinanceStreamedKlinesMetadata


if __name__ == "__main__":
    sample_json: dict[str, Any] = {
        "e": "kline",
        "E": 1754495086023,
        "s": "BTCUSDT",
        "k": {
            "t": 1754495085000,
            "T": 1754495085999,
            "s": "BTCUSDT",
            "i": "1s",
            "f": 5136784691,
            "L": 5136784694,
            "o": "115211.88000000",
            "c": "115211.88000000",
            "h": "115211.88000000",
            "l": "115211.87000000",
            "v": "0.03644000",
            "n": 4,
            "x": True,
            "q": "4198.32054920",
            "V": "0.00064000",
            "Q": "73.73560320",
            "B": "0",
        },
    }
    binance_streamed_kline: BinanceStreamedKlines = (
        BinanceStreamedKlines.model_validate(sample_json)
    )
    pprint(binance_streamed_kline)
