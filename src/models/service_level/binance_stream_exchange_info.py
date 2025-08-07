from pydantic import BaseModel
from pprint import pprint
from typing import Any

class BinanceStreamedExchangeInfo(BaseModel):
    e: str  # Event type
    E: int  # Event time
    s: str  # Symbol
    c: str  # Close
    o: str  # Open
    h: str  # High
    l: str  # Low
    v: str  # Total traded base asset volume
    q: str  # Total traded quote asset volume


if __name__ == "__main__":
    sample_json: list[dict[str, Any]] = [
        {'e': '24hrMiniTicker', 'E': 1754491665001, 's': 'REDUSDT', 'c': '0.38680000', 'o': '0.38090000',
         'h': '0.39440000', 'l': '0.36570000', 'v': '4818440.80000000', 'q': '1815004.16383000'},
        {'e': '24hrMiniTicker', 'E': 1754491665008, 's': 'REDUSDC', 'c': '0.38690000', 'o': '0.38120000',
         'h': '0.41000000', 'l': '0.36590000', 'v': '582442.60000000', 'q': '219857.92475000'},
        {'e': '24hrMiniTicker', 'E': 1754491665009, 's': 'REDFDUSD', 'c': '0.38750000', 'o': '0.38180000',
         'h': '0.39500000', 'l': '0.36660000', 'v': '219195.10000000', 'q': '82606.94623000'},
    ]
    for tickers in sample_json:
        exchange_info: BinanceStreamedExchangeInfo = BinanceStreamedExchangeInfo.model_validate(tickers)
        pprint(exchange_info)
