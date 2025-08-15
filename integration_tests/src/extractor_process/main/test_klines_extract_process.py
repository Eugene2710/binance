from typing import Sequence

import pytest
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
import pyarrow.parquet as pq
import io
import uuid

from src.extractor_process.main import KLinesExtractProcess
from src.extractor_process.extractors.batching.get_klines import BinanceKlinesExtractor
from src.extractor_process.formatter.klines_to_parquet_formatter import (
    KLinesToParquetFormatter,
)
from src.extractor_process.s3_writer.s3_writer import S3Writer
from src.extractor_process.config import DataSourceConfig
from mypy_boto3_s3.type_defs import ObjectIdentifierTypeDef


class TestKLinesExtractProcess:
    @pytest.fixture
    def s3_client(self) -> S3Client:
        return boto3.client("s3")

    @pytest.fixture
    def crypto_bucket(self) -> str:
        # Generate unique bucket name with UUID4 suffix
        return f"crypto-test-{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def data_source_config(self, crypto_bucket: str) -> DataSourceConfig:
        return DataSourceConfig(
            bucket_name=crypto_bucket, source_path="data_sources/klines_pricing/btcusd"
        )

    @pytest.fixture
    def setup_localstack(self, crypto_bucket: str, s3_client: S3Client):
        """Setup LocalStack S3 client for testing using environment variables"""
        # Setup: Create bucket
        try:
            s3_client.create_bucket(Bucket=crypto_bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                raise

        yield  # This is where the test runs

        # Teardown: Delete all objects in bucket, then delete bucket
        try:
            # Delete all objects in the bucket first
            response = s3_client.list_objects_v2(Bucket=crypto_bucket)
            if "Contents" in response:
                objects_to_delete: Sequence[ObjectIdentifierTypeDef] = [
                    {"Key": obj["Key"]} for obj in response["Contents"]
                ]
                s3_client.delete_objects(
                    Bucket=crypto_bucket, Delete={"Objects": objects_to_delete}
                )

            # Delete the bucket
            s3_client.delete_bucket(Bucket=crypto_bucket)
        except ClientError:
            # Ignore errors during cleanup
            pass

    @pytest.fixture
    def extractor(self):
        return BinanceKlinesExtractor()

    @pytest.fixture
    def formatter(self):
        return KLinesToParquetFormatter()

    @pytest.fixture
    def s3_writer(self):
        return S3Writer()

    @pytest.fixture
    def extract_process(self, extractor, formatter, s3_writer, data_source_config):
        return KLinesExtractProcess(extractor, formatter, s3_writer, data_source_config)

    @pytest.mark.asyncio
    async def test_run_full_integration_with_real_api(
        self, setup_localstack, extract_process, crypto_bucket: str, s3_client: S3Client
    ):
        """Test complete integration with real Binance API and LocalStack S3"""
        # Arrange
        symbol = "BTCUSDT"
        interval = "1m"
        start_time = datetime(2024, 1, 1, 0, 0)
        end_time = datetime(2024, 1, 1, 0, 5)  # Just 5 minutes of data

        # Act
        await extract_process.run(
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            limit=5,
        )

        # Assert
        # List objects in the crypto bucket to find our uploaded file
        response = s3_client.list_objects_v2(
            Bucket=crypto_bucket, Prefix="data_sources/klines_pricing/btcusd/"
        )

        assert "Contents" in response
        uploaded_files = response["Contents"]
        assert len(uploaded_files) >= 1

        # Get the uploaded file
        uploaded_file = uploaded_files[0]
        file_key = uploaded_file["Key"]

        # Verify the file name pattern
        assert file_key.startswith("data_sources/klines_pricing/btcusd/btcusd_")
        assert file_key.endswith(".parquet")

        # Download and verify the parquet content
        obj_response = s3_client.get_object(Bucket=crypto_bucket, Key=file_key)
        parquet_data = obj_response["Body"].read()

        # Verify it's valid parquet data
        parquet_buffer = io.BytesIO(parquet_data)
        table = pq.read_table(parquet_buffer)
        df = table.to_pandas()

        # Verify the data structure
        expected_columns = [
            "symbol",
            "open_time",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ]

        assert all(col in df.columns for col in expected_columns)
        assert len(df) > 0  # Should have some data
        assert df["symbol"].iloc[0] == symbol

    @pytest.mark.asyncio
    async def test_run_with_no_limit(
        self, setup_localstack, extract_process, crypto_bucket: str, s3_client: S3Client
    ):
        """Test run without limit parameter (uses default)"""
        # Arrange
        symbol = "ETHUSDT"
        interval = "5m"
        start_time = datetime(2024, 1, 1, 0, 0)
        end_time = datetime(2024, 1, 1, 1, 0)  # 1 hour of 5m data

        # Act
        await extract_process.run(
            symbol=symbol, interval=interval, start_time=start_time, end_time=end_time
        )

        # Assert
        response = s3_client.list_objects_v2(
            Bucket=crypto_bucket, Prefix="data_sources/klines_pricing/btcusd/"
        )

        assert "Contents" in response
        uploaded_files = response["Contents"]
        assert len(uploaded_files) >= 1

    @pytest.mark.asyncio
    async def test_run_with_minimal_params(
        self, setup_localstack, extract_process, crypto_bucket: str, s3_client: S3Client
    ):
        """Test run with only required parameters"""
        # Arrange
        symbol = "ADAUSDT"
        interval = "1h"

        # Act
        await extract_process.run(symbol=symbol, interval=interval, limit=3)

        # Assert
        response = s3_client.list_objects_v2(
            Bucket=crypto_bucket, Prefix="data_sources/klines_pricing/btcusd/"
        )

        assert "Contents" in response
        uploaded_files = response["Contents"]
        assert len(uploaded_files) >= 1

    @pytest.mark.asyncio
    async def test_run_generates_unique_timestamps(
        self, setup_localstack, extract_process, crypto_bucket: str, s3_client: S3Client
    ):
        """Test that multiple runs generate files with unique timestamps"""
        # Arrange
        symbol = "DOGEUSDT"
        interval = "15m"

        # Act - run twice quickly
        await extract_process.run(symbol=symbol, interval=interval, limit=2)
        await extract_process.run(symbol=symbol, interval=interval, limit=2)

        # Assert
        response = s3_client.list_objects_v2(
            Bucket=crypto_bucket, Prefix="data_sources/klines_pricing/btcusd/"
        )

        assert "Contents" in response
        uploaded_files = response["Contents"]
        assert len(uploaded_files) >= 2

        # Verify all files have unique names
        file_keys = [obj["Key"] for obj in uploaded_files]
        assert len(file_keys) == len(set(file_keys))  # All unique

    @pytest.mark.asyncio
    async def test_run_with_invalid_symbol_raises_exception(
        self, setup_localstack, extract_process
    ):
        """Test that invalid symbol raises appropriate exception"""
        # Arrange
        invalid_symbol = "INVALIDCOIN"
        interval = "1m"

        # Act & Assert
        with pytest.raises(Exception):  # Should raise some exception from API
            await extract_process.run(symbol=invalid_symbol, interval=interval, limit=1)

    @pytest.mark.asyncio
    async def test_run_error_handling_and_logging(
        self, extractor, formatter, s3_writer, data_source_config, caplog
    ):
        """Test error handling and logging"""
        # Arrange - create process with dependencies
        extract_process = KLinesExtractProcess(
            extractor, formatter, s3_writer, data_source_config
        )

        symbol = "BTCUSDT"
        interval = "1m"

        # Mock the S3Writer to raise an exception
        original_upload = s3_writer.upload_fileobj

        def mock_upload(*args, **kwargs):
            raise ClientError(
                error_response={"Error": {"Code": "NoSuchBucket"}},
                operation_name="upload_fileobj",
            )

        s3_writer.upload_fileobj = mock_upload

        # Act & Assert
        with pytest.raises(ClientError):
            await extract_process.run(symbol=symbol, interval=interval, limit=1)

        # Verify logging occurred
        assert "Starting KLines extraction" in caplog.text
        assert "Failed to process KLines extraction" in caplog.text

        # Restore original method
        s3_writer.upload_fileobj = original_upload

    def test_process_initialization(
        self, extractor, formatter, s3_writer, data_source_config
    ):
        """Test that KLinesExtractProcess initializes correctly with dependencies"""
        # Act
        process = KLinesExtractProcess(
            extractor, formatter, s3_writer, data_source_config
        )

        # Assert
        assert process.extractor is extractor
        assert process.formatter is formatter
        assert process.s3_writer is s3_writer
        assert process.config is data_source_config

    @pytest.mark.asyncio
    async def test_run_with_custom_current_time(
        self, setup_localstack, extract_process, crypto_bucket: str, s3_client: S3Client
    ):
        """Test that custom current_time parameter generates predictable file names"""
        # Arrange
        symbol = "BTCUSDT"
        interval = "1m"
        custom_time = datetime(2024, 6, 15, 14, 30, 45, 123456)
        expected_timestamp = "20240615143045123456"

        # Act
        await extract_process.run(
            symbol=symbol, interval=interval, limit=1, current_time=custom_time
        )

        # Assert
        response = s3_client.list_objects_v2(
            Bucket=crypto_bucket, Prefix="data_sources/klines_pricing/btcusd/"
        )

        assert "Contents" in response
        uploaded_files = response["Contents"]
        assert len(uploaded_files) == 1

        file_key = uploaded_files[0]["Key"]
        expected_filename = f"btcusd_{expected_timestamp}.parquet"
        assert file_key.endswith(expected_filename)
