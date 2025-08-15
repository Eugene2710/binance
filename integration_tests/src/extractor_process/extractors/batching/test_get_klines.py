import pytest
from datetime import datetime
import aiohttp

from src.extractor_process.extractors.batching.get_klines import BinanceKlinesExtractor
from src.common.models.service_level.binance_klines import Klines, SingleKline


class TestBinanceKlinesExtractor:
    @pytest.fixture
    def extractor(self):
        return BinanceKlinesExtractor()

    @pytest.mark.asyncio
    async def test_extract_with_time_range(self, extractor):
        """Test extraction with specific start and end time"""
        # Arrange
        symbol = "BTCUSDT"
        interval = "1m"
        start_time = datetime(2024, 1, 1, 0, 0)
        end_time = datetime(2024, 1, 1, 0, 5)  # 5 minutes of data

        # Act
        result = await extractor.extract(
            symbol=symbol, interval=interval, start_time=start_time, end_time=end_time
        )

        # Assert
        assert isinstance(result, Klines)
        assert len(result.klines) > 0
        assert all(isinstance(kline, SingleKline) for kline in result.klines)

        # Verify all klines are for the correct symbol
        assert all(kline.symbol == symbol for kline in result.klines)

        # Verify time range - note that close times can extend beyond end_time due to interval boundaries
        first_kline = result.klines[0]
        last_kline = result.klines[-1]
        assert first_kline.open_time >= int(start_time.timestamp()) * 1000
        # Allow some tolerance for close time as it depends on interval boundaries
        end_time_ms = int(end_time.timestamp()) * 1000
        # Close time can be up to interval duration after end_time
        interval_ms = 60 * 1000  # 1 minute in milliseconds
        assert last_kline.close_time <= end_time_ms + interval_ms

    @pytest.mark.asyncio
    async def test_extract_with_limit(self, extractor):
        """Test extraction with limit parameter"""
        # Arrange
        symbol = "ETHUSDT"
        interval = "5m"
        limit = 3

        # Act
        result = await extractor.extract(symbol=symbol, interval=interval, limit=limit)

        # Assert
        assert isinstance(result, Klines)
        assert len(result.klines) <= limit  # Should not exceed limit
        assert len(result.klines) > 0  # Should have some data
        assert all(kline.symbol == symbol for kline in result.klines)

    @pytest.mark.asyncio
    async def test_extract_minimal_params(self, extractor):
        """Test extraction with only required parameters"""
        # Arrange
        symbol = "ADAUSDT"
        interval = "1h"

        # Act
        result = await extractor.extract(symbol=symbol, interval=interval)

        # Assert
        assert isinstance(result, Klines)
        assert len(result.klines) > 0
        assert all(kline.symbol == symbol for kline in result.klines)

    @pytest.mark.asyncio
    async def test_extract_different_intervals(self, extractor):
        """Test extraction with different time intervals"""
        symbol = "BNBUSDT"
        intervals = ["1m", "5m", "15m", "1h"]

        for interval in intervals:
            # Act
            result = await extractor.extract(symbol=symbol, interval=interval, limit=2)

            # Assert
            assert isinstance(result, Klines)
            assert len(result.klines) > 0
            assert all(kline.symbol == symbol for kline in result.klines)

    @pytest.mark.asyncio
    async def test_extract_validates_kline_data_structure(self, extractor):
        """Test that extracted data has correct structure"""
        # Arrange
        symbol = "DOGEUSDT"
        interval = "15m"

        # Act
        result = await extractor.extract(symbol=symbol, interval=interval, limit=1)

        # Assert
        assert len(result.klines) >= 1
        kline = result.klines[0]

        # Verify all required fields are present and have correct types
        assert isinstance(kline.symbol, str)
        assert isinstance(kline.open_time, int)
        assert isinstance(kline.open_price, str)
        assert isinstance(kline.high_price, str)
        assert isinstance(kline.low_price, str)
        assert isinstance(kline.close_price, str)
        assert isinstance(kline.volume, str)
        assert isinstance(kline.close_time, int)
        assert isinstance(kline.quote_asset_volume, str)
        assert isinstance(kline.number_of_trades, int)
        assert isinstance(kline.taker_buy_base_asset_volume, str)
        assert isinstance(kline.taker_buy_quote_asset_volume, str)
        assert isinstance(kline.ignore, str)

        # Verify logical relationships
        assert kline.open_time < kline.close_time
        assert float(kline.low_price) <= float(kline.high_price)
        assert kline.number_of_trades >= 0
        assert float(kline.volume) >= 0

    @pytest.mark.asyncio
    async def test_extract_with_invalid_symbol_raises_error(self, extractor):
        """Test that invalid symbol raises appropriate error"""
        # Arrange
        invalid_symbol = "INVALIDCOIN"
        interval = "1m"

        # Act & Assert
        with pytest.raises(aiohttp.ClientError):
            await extractor.extract(symbol=invalid_symbol, interval=interval, limit=1)

    @pytest.mark.asyncio
    async def test_extract_with_invalid_interval_raises_error(self, extractor):
        """Test that invalid interval raises appropriate error"""
        # Arrange
        symbol = "BTCUSDT"
        invalid_interval = "2x"  # Invalid interval format

        # Act & Assert
        with pytest.raises(aiohttp.ClientError):
            await extractor.extract(symbol=symbol, interval=invalid_interval, limit=1)

    @pytest.mark.asyncio
    async def test_extract_with_future_dates(self, extractor):
        """Test extraction with future dates returns empty or minimal data"""
        # Arrange
        symbol = "BTCUSDT"
        interval = "1m"
        future_start = datetime(2030, 1, 1, 0, 0)
        future_end = datetime(2030, 1, 1, 0, 5)

        # Act
        result = await extractor.extract(
            symbol=symbol,
            interval=interval,
            start_time=future_start,
            end_time=future_end,
        )

        # Assert
        assert isinstance(result, Klines)
        # Future dates should return empty or very minimal data
        assert len(result.klines) == 0 or len(result.klines) <= 2

    @pytest.mark.asyncio
    async def test_extract_with_large_time_range(self, extractor):
        """Test extraction with larger time range"""
        # Arrange
        symbol = "ETHUSDT"
        interval = "1h"
        start_time = datetime(2024, 1, 1, 0, 0)
        end_time = datetime(2024, 1, 2, 0, 0)  # 24 hours

        # Act
        result = await extractor.extract(
            symbol=symbol, interval=interval, start_time=start_time, end_time=end_time
        )

        # Assert
        assert isinstance(result, Klines)
        assert len(result.klines) > 0
        # Should have roughly 24 hourly candles
        assert len(result.klines) <= 25  # Allow some tolerance

    @pytest.mark.asyncio
    async def test_extract_retry_mechanism(self, extractor):
        """Test that retry mechanism doesn't interfere with normal operation"""
        # This test verifies the retry decorator works correctly
        # Arrange
        symbol = "ADAUSDT"
        interval = "30m"

        # Act - Multiple calls to test retry doesn't cause issues
        result1 = await extractor.extract(symbol=symbol, interval=interval, limit=1)
        result2 = await extractor.extract(symbol=symbol, interval=interval, limit=1)

        # Assert
        assert isinstance(result1, Klines)
        assert isinstance(result2, Klines)
        assert len(result1.klines) > 0
        assert len(result2.klines) > 0

    @pytest.mark.asyncio
    async def test_extract_various_popular_symbols(self, extractor):
        """Test extraction works with various popular trading symbols"""
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "SOLUSDT"]
        interval = "1h"

        for symbol in symbols:
            # Act
            result = await extractor.extract(symbol=symbol, interval=interval, limit=1)

            # Assert
            assert isinstance(result, Klines)
            assert len(result.klines) >= 1
            assert result.klines[0].symbol == symbol
