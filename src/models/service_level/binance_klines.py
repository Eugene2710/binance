from pydantic import BaseModel


class SingleKline(BaseModel):
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
    def create_kline(symbol: str, data: list[str | int]) -> "SingleKline":
        return SingleKline(
            symbol=symbol,
            open_time=int(data[0]),
            open_price=str(data[1]),
            high_price=str(data[2]),
            low_price=str(data[3]),
            close_price=str(data[4]),
            volume=str(data[5]),
            close_time=int(data[6]),
            quote_asset_volume=str(data[7]),
            number_of_trades=int(data[8]),
            taker_buy_base_asset_volume=str(data[9]),
            taker_buy_quote_asset_volume=str(data[10]),
            ignore=str(data[11]),
        )


class Klines(BaseModel):
    klines: list[SingleKline]

    @staticmethod
    def from_json(symbol: str, raw_data: list[list[str | int]]) -> "Klines":
        """
        Converts raw JSON (a list of lists) into a Klines object
        """
        parsed_klines = [
            SingleKline(
                symbol=symbol,
                open_time=int(item[0]),
                open_price=str(item[1]),
                high_price=str(item[2]),
                low_price=str(item[3]),
                close_price=str(item[4]),
                volume=str(item[5]),
                close_time=int(item[6]),
                quote_asset_volume=str(item[7]),
                number_of_trades=int(item[8]),
                taker_buy_base_asset_volume=str(item[9]),
                taker_buy_quote_asset_volume=str(item[10]),
                ignore=str(item[11]),
            )
            for item in raw_data
        ]

        return Klines(klines=parsed_klines)


if __name__ == "__main__":
    raw_data: list[list[str | int]] = [
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
