import os
from datetime import datetime
from typing import Any
from sqlalchemy import text, create_engine, Engine
from sqlalchemy.exc import OperationalError
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from dotenv import load_dotenv

from src.models.database_level.binance_klines import SingleKlineDTO


class KlinesDAO:
    INSERT_SQL = text(
        """
        INSERT INTO klines_append_only
          (symbol, open_time, open_price, high_price, low_price,
           close_price, volume, close_time, quote_asset_volume,
           number_of_trades, taker_buy_base_asset_volume,
           taker_buy_quote_asset_volume, ignore)
        VALUES
          (:symbol, :open_time, :open_price, :high_price, :low_price,
           :close_price, :volume, :close_time, :quote_asset_volume,
           :number_of_trades, :taker_buy_base_asset_volume,
           :taker_buy_quote_asset_volume, :ignore)
        """
    )
    def __init__(self, connection_string: str) ->  None:
        """
        connection_string format: "clickhousedb+connect://localhost:8123/default"
        """
        self._engine: Engine = create_engine(
            connection_string, connect_args={"verify": False} # a flag to disable TLS certificate checks over HTTPS
        )

    @retry(
        stop=stop_after_attempt(5),  # give up after 5 tries
        wait=wait_fixed(2),  # wait 1 seconds between retries
        retry=retry_if_exception_type(OperationalError),  # only on OperationalError
        reraise=True,  # re-raise the last exception if still failing
    )
    def insert(self, input: list[SingleKlineDTO]) -> None:
        """
        Bulk inserts a batch of SingleKline DTOs into ClickHouse via raw SQL.
        ClickHouse's native protocol will send all params in one go.
        """
        if not input:
            return
        params: list[dict[str, Any]] = []
        for k in input:
            params.append(
                {
                    "symbol": k.symbol,
                    "open_time": k.open_time,  # datetime or str
                    "open_price": k.open_price,
                    "high_price": k.high_price,
                    "low_price": k.low_price,
                    "close_price": k.close_price,
                    "volume": k.volume,
                    "close_time": k.close_time,  # datetime or str
                    "quote_asset_volume": k.quote_asset_volume,
                    "number_of_trades": k.number_of_trades,
                    "taker_buy_base_asset_volume": k.taker_buy_base_asset_volume,
                    "taker_buy_quote_asset_volume": k.taker_buy_quote_asset_volume,
                    "ignore": k.ignore,
                }
            )
        try:
            with self._engine.begin() as conn:
                conn.execute(self.INSERT_SQL, params)
        except OperationalError as oe:
            print(f"Operational Error inserting klines: {oe}")
            raise
        except Exception as e:
            print(f"Unexpected error inserting klines: {e}")
            raise


if __name__ == "__main__":
    load_dotenv()
    connection_string: str = os.getenv("CLICKHOUSE_URL")
    dao: KlinesDAO = KlinesDAO(connection_string)
    dummy_input: list[SingleKlineDTO] = [
        SingleKlineDTO(
            symbol="ETHUSDT",
            open_time=datetime(2025, 1, 6, 0, 0, 0),
            open_price=0.01634790,
            high_price=0.80000000,
            low_price=0.01575800,
            close_price=0.01577100,
            volume=148976.11427815,
            close_time=datetime(2025, 1, 6, 0, 1, 39, 999000),  # e.g. +1m 39.999s
            quote_asset_volume=2434.19055334,
            number_of_trades=308,
            taker_buy_base_asset_volume=1756.87402397,
            taker_buy_quote_asset_volume=28.46694368,
            ignore="0",
            # created_at is omitted so ClickHouse will default it server-side
        )
    ]
    print("Inserting")
    dao.insert(input=dummy_input)