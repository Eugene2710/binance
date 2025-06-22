from asyncio import AbstractEventLoop, new_event_loop
from typing import Any
import aiohttp
from pprint import pprint
from datetime import datetime
from urllib.parse import urlencode
from tenacity import retry, wait_fixed, stop_after_attempt

from src.models.binance_klines import Klines


class BinanceKlinesExtractor:
    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True
    )
    async def extract(
            self,
            symbol: str,
            interval: str,
            start_time: datetime | None = None,
            end_time: datetime | None = None,
            limit: int | None = None
    ) -> Klines:
        """
        TODO
        1. Nest into try catch, and include logging
        2. Implement enum data class
        """
        base: str = f"https://api.binance.com/api/v3/klines"
        start_time_int: int|None = int(start_time.timestamp())*1000 if start_time else None
        print(start_time_int)
        end_time_int: int | None = int(end_time.timestamp())*1000 if end_time else None
        print(end_time_int)

        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            **{k:v for k, v in {
                "startTime": start_time_int,
                "endTime": end_time_int,
                "limit": limit
            }.items() if v is not None}
        }
        url: str = f"{base}?{urlencode(params)}"

        async with aiohttp.ClientSession() as client:
            async with client.get(url, ssl=False) as response:
                if response.status == 200:
                    data: dict[str, Any] = await response.json()
                    k_lines: Klines = Klines.from_json(symbol=symbol, raw_data=data)
                    return k_lines
                else:
                    raise aiohttp.ClientError(f"Received non-status code 200: {response.status}")


if __name__ == "__main__":
    binance_klines_extractor: BinanceKlinesExtractor = BinanceKlinesExtractor()
    event_loop: AbstractEventLoop = new_event_loop()
    res: Klines = event_loop.run_until_complete(binance_klines_extractor.extract(
        symbol="BTCUSDT",
        interval="1m",
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 1, 8, 20)
    ))
    pprint(res)





