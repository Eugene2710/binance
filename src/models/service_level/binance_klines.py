from pydantic import BaseModel


class Kline(BaseModel):
    symbol: str
    open_time: int  # e.g. 1499040000000
    open_price: str  # e.g. "0.01634790"
    high_price: str  # e.g. "0.80000000"
    low_price: str  # e.g. "0.01575800"
    close_price: str  # e.g. "0.01577100"
    volume: str  # e.g. "148976.11427815"
    close_time: int  # e.g. 1499644799999
    quote_asset_volume: str  # e.g. "2434.19055334"
    number_of_trades: int  # e.g. 308
    taker_buy_base_asset_volume: str  # e.g. "1756.87402397"
    taker_buy_quote_asset_volume: str  # e.g. "28.46694368"
    ignore: str  # e.g. "0" (unused field)

    @staticmethod
    def create_kline(symbol: str, data: list[str | int]) -> "Kline":
        return Kline(
            symbol=symbol,
            open_time=data[0],
            open_price=data[1],
            high_price=data[2],
            low_price=data[3],
            close_price=data[4],
            volume=data[5],
            close_time=data[6],
            quote_asset_volume=data[7],
            number_of_trades=data[8],
            taker_buy_base_asset_volume=data[9],
            taker_buy_quote_asset_volume=data[10],
            ignore=data[11],
        )


class Klines(BaseModel):
    klines: list[Kline]

    @staticmethod
    def from_json(symbol: str, raw_data: list[list[str | int]]) -> "Klines":
        """
        Converts raw JSON (a list of lists) into a Klines object
        """
        parsed_klines = [
            Kline(
                symbol=symbol,
                open_time=item[0],
                open_price=item[1],
                high_price=item[2],
                low_price=item[3],
                close_price=item[4],
                volume=item[5],
                close_time=item[6],
                quote_asset_volume=item[7],
                number_of_trades=item[8],
                taker_buy_base_asset_volume=item[9],
                taker_buy_quote_asset_volume=item[10],
                ignore=item[11],
            )
            for item in raw_data
        ]

        return Klines(klines=parsed_klines)


if __name__ == "__main__":
    raw_data = [
        [
            1499040000000,
            "0.01634790",
            "0.80000000",
            "0.01575800",
            "0.01577100",
            "148976.11427815",
            1499644799999,
            "2434.19055334",
            308,
            "1756.87402397",
            "28.46694368",
            "0",
        ]
    ]

    klines_obj: Klines = Klines.from_json(symbol="ETHUSDT", raw_data=raw_data)
    print(klines_obj)
