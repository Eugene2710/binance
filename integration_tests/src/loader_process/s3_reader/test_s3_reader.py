import io
import pytest
import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from src.loader_process.s3_reader.s3_reader import S3Reader


class TestS3Reader:
    @pytest.fixture
    def s3_client(self) -> S3Client:
        return boto3.client("s3")

    @pytest.fixture
    def test_bucket(self) -> str:
        return "test-bucket"

    @pytest.fixture
    def setup_localstack(self, test_bucket: str, s3_client: S3Client):
        """Setup LocalStack S3 client for testing using environment variables"""
        try:
            s3_client.create_bucket(Bucket=test_bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                raise

    def test_download_fileobj_success(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test successful file download from LocalStack S3"""
        # Arrange
        test_data = b"test parquet data"
        path = "data/test.parquet"

        # Upload test data first
        s3_client.put_object(Bucket=test_bucket, Key=path, Body=test_data)

        reader = S3Reader()

        # Act
        buffer = reader.download_fileobj(test_bucket, path)

        # Assert
        buffer.seek(0)
        downloaded_data = buffer.read()
        assert downloaded_data == test_data

    def test_download_fileobj_with_different_params(
        self, setup_localstack, s3_client: S3Client
    ):
        """Test download with different bucket and path parameters"""
        # Arrange
        crypto_bucket = "crypto-data-lake"
        try:
            s3_client.create_bucket(Bucket=crypto_bucket)
        except ClientError:
            pass  # Bucket may already exist

        test_data = b"klines data in parquet format"
        path = "binance/klines/BTCUSDT/2025/01/15/data.parquet"

        # Upload test data first
        s3_client.put_object(Bucket=crypto_bucket, Key=path, Body=test_data)

        reader = S3Reader()

        # Act
        buffer = reader.download_fileobj(crypto_bucket, path)

        # Assert
        buffer.seek(0)
        downloaded_data = buffer.read()
        assert downloaded_data == test_data

    def test_download_fileobj_client_error_with_invalid_bucket(self, setup_localstack):
        """Test that ClientError is raised for invalid bucket"""
        # Arrange
        reader = S3Reader()
        invalid_bucket = "non-existent-bucket-that-does-not-exist"
        path = "test/path.parquet"

        # Act & Assert
        with pytest.raises(ClientError):
            reader.download_fileobj(invalid_bucket, path)

    def test_download_fileobj_client_error_with_invalid_key(
        self, setup_localstack, test_bucket: str
    ):
        """Test that ClientError is raised for non-existent key"""
        # Arrange
        reader = S3Reader()
        non_existent_path = "non/existent/file.parquet"

        # Act & Assert
        with pytest.raises(ClientError):
            reader.download_fileobj(test_bucket, non_existent_path)

    def test_download_fileobj_empty_file(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test downloading an empty file"""
        # Arrange
        empty_data = b""
        path = "empty.parquet"

        # Upload empty file first
        s3_client.put_object(Bucket=test_bucket, Key=path, Body=empty_data)

        reader = S3Reader()

        # Act
        buffer = reader.download_fileobj(test_bucket, path)

        # Assert
        buffer.seek(0)
        downloaded_data = buffer.read()
        assert downloaded_data == empty_data

    def test_multiple_downloads_same_instance(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test multiple downloads using the same S3Reader instance"""
        # Arrange
        bucket2 = "test-bucket-2"
        try:
            s3_client.create_bucket(Bucket=bucket2)
        except ClientError:
            pass

        reader = S3Reader()

        # Upload test data
        data1 = b"data1"
        data2 = b"data2"
        path1 = "path1"
        path2 = "path2"

        s3_client.put_object(Bucket=test_bucket, Key=path1, Body=data1)
        s3_client.put_object(Bucket=bucket2, Key=path2, Body=data2)

        # Act - perform multiple downloads
        buffer1 = reader.download_fileobj(test_bucket, path1)
        buffer2 = reader.download_fileobj(bucket2, path2)

        # Assert
        buffer1.seek(0)
        buffer2.seek(0)
        assert buffer1.read() == data1
        assert buffer2.read() == data2

    def test_download_fileobj_integration_with_real_data_format(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test download with data that resembles real parquet format"""
        # Arrange
        # Simulate parquet-like binary data
        parquet_like_data = b"\x50\x41\x52\x31"  # PAR1 header signature
        path = "binance/klines/ETHUSDT/year=2025/month=01/day=15/hour=10/data.parquet"

        # Upload test data first
        s3_client.put_object(Bucket=test_bucket, Key=path, Body=parquet_like_data)

        reader = S3Reader()

        # Act
        buffer = reader.download_fileobj(test_bucket, path)

        # Assert
        buffer.seek(0)
        downloaded_data = buffer.read()
        assert downloaded_data == parquet_like_data

    def test_download_fileobj_large_data(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test downloading larger data to verify multipart handling"""
        # Arrange
        # Create 1MB of test data
        large_data = b"A" * (1024 * 1024)
        path = "large/data.parquet"

        # Upload test data first
        s3_client.put_object(Bucket=test_bucket, Key=path, Body=large_data)

        reader = S3Reader()

        # Act
        buffer = reader.download_fileobj(test_bucket, path)

        # Assert
        buffer.seek(0)
        downloaded_data = buffer.read()
        assert len(downloaded_data) == len(large_data)
        assert downloaded_data == large_data

    def test_download_fileobj_buffer_position_reset(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test that returned buffer is positioned at the beginning"""
        # Arrange
        test_data = b"test data for position check"
        path = "position/test.parquet"

        # Upload test data first
        s3_client.put_object(Bucket=test_bucket, Key=path, Body=test_data)

        reader = S3Reader()

        # Act
        buffer = reader.download_fileobj(test_bucket, path)

        # Assert - buffer should be positioned at start for immediate reading
        # Note: The current implementation doesn't reset position, so we test current behavior
        downloaded_data = buffer.read()
        assert downloaded_data == test_data

    def test_download_fileobj_with_special_characters_in_path(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test download with special characters in the file path"""
        # Arrange
        test_data = b"special path test data"
        path = "special/path with spaces/file-name_with-special@chars.parquet"

        # Upload test data first
        s3_client.put_object(Bucket=test_bucket, Key=path, Body=test_data)

        reader = S3Reader()

        # Act
        buffer = reader.download_fileobj(test_bucket, path)

        # Assert
        buffer.seek(0)
        downloaded_data = buffer.read()
        assert downloaded_data == test_data

    def test_download_fileobj_returns_bytesio_instance(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test that the method returns a BytesIO instance"""
        # Arrange
        test_data = b"instance type test"
        path = "type/test.parquet"

        # Upload test data first
        s3_client.put_object(Bucket=test_bucket, Key=path, Body=test_data)

        reader = S3Reader()

        # Act
        buffer = reader.download_fileobj(test_bucket, path)

        # Assert
        assert isinstance(buffer, io.BytesIO)
        buffer.seek(0)
        assert buffer.read() == test_data
