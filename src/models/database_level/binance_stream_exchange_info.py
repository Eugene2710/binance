from pydantic import BaseModel
from datetime import datetime
from pprint import pprint

from src.models.service_level.binance_stream_exchange_info import BinanceStreamedExchangeInfo


class TickerStreamDTO(BaseModel):
    symbol: str
    price: float # the close amount is taken
    time: datetime
    created_at: datetime | None = None # Populated at DB level if not provided

    @staticmethod
    def from_stream_exchange_info(input: BinanceStreamedExchangeInfo) -> "TickerStreamDTO":
        return TickerStreamDTO(
            symbol=input.s,
            price=float(input.c),
            # note that timestamp is in milliseconds hence the need to divide by 1000
            time=datetime.fromtimestamp(input.E / 1000),
        )

if __name__ == "__main__":
    sample_exchange_info: BinanceStreamedExchangeInfo = BinanceStreamedExchangeInfo(e='24hrMiniTicker', E=1754491665009, s='REDFDUSD', c='0.38750000', o='0.38180000', h='0.39500000', l='0.36660000', v='219195.10000000', q='82606.94623000')
    ticker_stream_dto: TickerStreamDTO = TickerStreamDTO.from_stream_exchange_info(sample_exchange_info)
    pprint(ticker_stream_dto)
