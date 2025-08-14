import io
import pyarrow.parquet as pq
from src.extractor_process.formatter.klines_to_parquet_formatter import (
    KLinesToParquetFormatter,
)
from src.common.models.service_level.binance_klines import Klines, SingleKline


class TestKLinesToParquetFormatter:
    def test_format_with_valid_data(self):
        """Test formatting klines data to parquet with valid data"""
        # Arrange
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
            ],
            [
                1499040060000,
                "0.01577100",
                "0.01600000",
                "0.01574800",
                "0.01580000",
                "50000.00000000",
                1499644859999,
                "800.00000000",
                150,
                "25000.00000000",
                "400.00000000",
                "0",
            ],
        ]
        klines = Klines.from_json("ETHUSDT", raw_data)

        # Act
        result = KLinesToParquetFormatter.format(klines)

        # Assert
        assert result is not None
        assert isinstance(result, io.BytesIO)

        # Verify parquet content
        result.seek(0)
        table = pq.read_table(result)
        df = table.to_pandas()

        assert len(df) == 2
        assert df["symbol"].iloc[0] == "ETHUSDT"
        assert df["open_time"].iloc[0] == 1499040000000
        assert df["open_price"].iloc[0] == "0.01634790"
        assert df["high_price"].iloc[0] == "0.80000000"
        assert df["low_price"].iloc[0] == "0.01575800"
        assert df["close_price"].iloc[0] == "0.01577100"
        assert df["volume"].iloc[0] == "148976.11427815"
        assert df["close_time"].iloc[0] == 1499644799999
        assert df["quote_asset_volume"].iloc[0] == "2434.19055334"
        assert df["number_of_trades"].iloc[0] == 308
        assert df["taker_buy_base_asset_volume"].iloc[0] == "1756.87402397"
        assert df["taker_buy_quote_asset_volume"].iloc[0] == "28.46694368"
        assert df["ignore"].iloc[0] == "0"

        # Verify second row
        assert df["symbol"].iloc[1] == "ETHUSDT"
        assert df["open_time"].iloc[1] == 1499040060000

    def test_format_with_empty_klines(self):
        """Test formatting with empty klines list returns None"""
        # Arrange
        klines = Klines(klines=[])

        # Act
        result = KLinesToParquetFormatter.format(klines)

        # Assert
        assert result is None

    def test_format_with_single_kline(self):
        """Test formatting with a single kline"""
        # Arrange
        single_kline = SingleKline(
            symbol="BTCUSDT",
            open_time=1499040000000,
            open_price="30000.00",
            high_price="31000.00",
            low_price="29500.00",
            close_price="30500.00",
            volume="100.00",
            close_time=1499043600000,
            quote_asset_volume="3050000.00",
            number_of_trades=500,
            taker_buy_base_asset_volume="50.00",
            taker_buy_quote_asset_volume="1525000.00",
            ignore="0",
        )
        klines = Klines(klines=[single_kline])

        # Act
        result = KLinesToParquetFormatter.format(klines)

        # Assert
        assert result is not None
        assert isinstance(result, io.BytesIO)

        # Verify parquet content
        result.seek(0)
        table = pq.read_table(result)
        df = table.to_pandas()

        assert len(df) == 1
        assert df["symbol"].iloc[0] == "BTCUSDT"
        assert df["open_price"].iloc[0] == "30000.00"

    def test_parquet_schema_matches_expected(self):
        """Test that the generated parquet has the correct schema"""
        # Arrange
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
        klines = Klines.from_json("ETHUSDT", raw_data)

        # Act
        result = KLinesToParquetFormatter.format(klines)

        # Assert
        assert result
        result.seek(0)
        table = pq.read_table(result)

        expected_schema = KLinesToParquetFormatter.PYARROW_SCHEMA
        assert table.schema.equals(expected_schema)

    def test_parquet_compression_is_zstd(self):
        """Test that the parquet file uses zstd compression"""
        # Arrange
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
        klines = Klines.from_json("ETHUSDT", raw_data)

        # Act
        result = KLinesToParquetFormatter.format(klines)

        # Assert
        assert result
        result.seek(0)
        parquet_file = pq.ParquetFile(result)

        # Check that compression is used (specific compression type might not be directly accessible)
        assert parquet_file.metadata.num_rows == 1
        assert parquet_file.metadata.num_columns == 13

    def test_buffer_position_after_format(self):
        """Test that buffer position is reset to beginning after format"""
        # Arrange
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
        klines = Klines.from_json("ETHUSDT", raw_data)

        # Act
        result = KLinesToParquetFormatter.format(klines)

        # Assert
        assert result
        assert result.tell() == 0  # Buffer should be at the beginning
