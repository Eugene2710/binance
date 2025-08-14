from asyncio import AbstractEventLoop, new_event_loop
import json
from typing import AsyncGenerator, Any
import aiohttp
from aiohttp import WSMessage

"""
Streams from Binance 
Docs: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#trade-streams
Note:
To enable combined stream payloads, use "/stream/", e.g. "wss://stream.binance.com:9443/stream/!miniTicker@arr"
To disable combined stream payloads, use "/ws", e.g ""wss://stream.binance.com:9443/ws/!miniTicker@arr""

Payload for kline/candlestick streams for UTC: list[dict[str, Any] = 
{
  "e": "kline",         // Event type
  "E": 1672515782136,   // Event time
  "s": "BNBBTC",        // Symbol
  "k": {
    "t": 1672515780000, // Kline start time
    "T": 1672515839999, // Kline close time
    "s": "BNBBTC",      // Symbol
    "i": "1m",          // Interval
    "f": 100,           // First trade ID
    "L": 200,           // Last trade ID
    "o": "0.0010",      // Open price
    "c": "0.0020",      // Close price
    "h": "0.0025",      // High price
    "l": "0.0015",      // Low price
    "v": "1000",        // Base asset volume
    "n": 100,           // Number of trades
    "x": false,         // Is this kline closed?
    "q": "1.0000",      // Quote asset volume
    "V": "500",         // Taker buy base asset volume
    "Q": "0.500",       // Taker buy quote asset volume
    "B": "123456"       // Ignore
  }
}

connection string format: wss://stream.binance.com:9443/ws/<symbol>@kline_<interval>
e.g wss://stream.binance.com:9443/ws/btcusdt@kline_1m (Bitcoin 1-minute klines)
e.g wss://stream.binance.com:9443/ws/bnbbtc@kline_5m (BNB/BTC 5-minute klines)
Valid intervals are: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
"""


async def get_klines_socket() -> AsyncGenerator[list[dict[str, Any]], None]:
    conn_string: str = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"
    try:
        # happy path
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(conn_string, ssl=False) as ws:
                # ws: ClientWebSocketResponse
                async for msg in ws:
                    # technically redundant: to help pycharm understand msg has .type and .data attributes
                    msg: WSMessage  # type: ignore[no-redef]
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        msg_string: str = msg.data
                        msg_dict: list[dict[str, Any]] = json.loads(msg_string)
                        yield msg_dict
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        raise ValueError("Websocket connection closed.")
                    elif msg.type == aiohttp.WSMsgType.error:
                        raise ValueError("Websocket encountered error.")
    except aiohttp.ClientError as e:
        raise Exception(f"Client error occurred: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error occurred: {e}")


async def main() -> None:
    async for event in get_klines_socket():
        print(event)


if __name__ == "__main__":
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(main())
