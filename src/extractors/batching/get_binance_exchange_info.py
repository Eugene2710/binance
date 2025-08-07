from asyncio import AbstractEventLoop, new_event_loop
from typing import Any
import aiohttp
from pprint import pprint
from tenacity import retry, wait_fixed, stop_after_attempt

from src.models.service_level.binance_exchange_info import ExchangeInfo


class BinanceExchangeInfoExtractor:
    @staticmethod
    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True
    )
    async def extract(symbol: str) -> ExchangeInfo:
        """
        Get Exchange Ticker Info so that these ticker symbols can be used for further querying against the API
        https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints

        TODO
        1. Nest into try catch, and include logging
        """
        # url: str = "https://api.binance.com/api/v3/exchangeInfo"
        url: str = f"https://api.binance.com/api/v3/exchangeInfo?symbol={symbol}"
        async with aiohttp.ClientSession() as client:
            async with client.get(url, ssl=False) as response:
                if response.status == 200:
                    data: dict[str, Any] = await response.json()
                    """
                    possible symbol status: PRE_TRADING, TRADING, POST_TRADING, END_OF_DAY, HALT, AUCTION_MATCH, BREAK
                    https://developers.binance.com/docs/binance-spot-api-docs/enums#account-and-symbol-permissions
                    """
                    exchange_info: ExchangeInfo = ExchangeInfo.model_validate(data)
                    return exchange_info
                else:
                    raise Exception(f"Received non-status code 200: {response.status}")


if __name__ == "__main__":
    binance_exchange_info_extractor: BinanceExchangeInfoExtractor = BinanceExchangeInfoExtractor()
    event_loop: AbstractEventLoop = new_event_loop()
    res: ExchangeInfo = event_loop.run_until_complete(binance_exchange_info_extractor.extract("ETHUSDT"))
    pprint(res)

