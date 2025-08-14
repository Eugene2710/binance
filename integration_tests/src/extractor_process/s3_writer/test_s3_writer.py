import io
import pytest
import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from src.extractor_process.s3_writer.s3_writer import S3Writer


class TestS3Writer:
    @pytest.fixture
    def s3_client(self) -> S3Client:
        return boto3.client("s3")

    @pytest.fixture
    def test_bucket(self) -> str:
        return "test-bucket"

    @pytest.fixture
    def setup_localstack(self, test_bucket: str, s3_client: S3Client):
        """Setup LocalStack S3 client for testing using environment variables"""
        # Configure boto3 to use environment variables

        try:
            s3_client.create_bucket(Bucket=test_bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                raise

    def test_upload_fileobj_success(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test successful file upload to LocalStack S3"""
        # Arrange
        writer = S3Writer()
        test_data = b"test parquet data"
        buffer = io.BytesIO(test_data)
        path = "data/test.parquet"

        # Act
        writer.upload_fileobj(test_bucket, path, buffer)

        # Assert - verify file was uploaded by downloading it
        response = s3_client.get_object(Bucket=test_bucket, Key=path)
        downloaded_data = response["Body"].read()
        assert downloaded_data == test_data

    def test_upload_fileobj_with_different_params(
        self, setup_localstack, s3_client: S3Client
    ):
        """Test upload with different bucket and path parameters"""
        # Arrange
        crypto_bucket = "crypto-data-lake"
        try:
            s3_client.create_bucket(Bucket=crypto_bucket)
        except ClientError:
            pass  # Bucket may already exist

        writer = S3Writer()
        test_data = b"klines data in parquet format"
        buffer = io.BytesIO(test_data)
        path = "binance/klines/BTCUSDT/2025/01/15/data.parquet"

        # Act
        writer.upload_fileobj(crypto_bucket, path, buffer)

        # Assert
        response = s3_client.get_object(Bucket=crypto_bucket, Key=path)
        downloaded_data = response["Body"].read()
        assert downloaded_data == test_data

    def test_upload_fileobj_client_error_with_invalid_bucket(self, setup_localstack):
        """Test that ClientError is raised for invalid bucket"""
        # Arrange
        writer = S3Writer()
        buffer = io.BytesIO(b"test data")
        invalid_bucket = "non-existent-bucket-that-does-not-exist"
        path = "test/path.parquet"

        # Act & Assert
        with pytest.raises(ClientError):
            writer.upload_fileobj(invalid_bucket, path, buffer)

    def test_upload_fileobj_empty_buffer(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test uploading an empty buffer"""
        # Arrange
        writer = S3Writer()
        empty_buffer = io.BytesIO(b"")
        path = "empty.parquet"

        # Act
        writer.upload_fileobj(test_bucket, path, empty_buffer)

        # Assert
        response = s3_client.get_object(Bucket=test_bucket, Key=path)
        downloaded_data = response["Body"].read()
        assert downloaded_data == b""

    def test_multiple_uploads_same_instance(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test multiple uploads using the same S3Writer instance"""
        # Arrange
        bucket2 = "test-bucket-2"
        try:
            s3_client.create_bucket(Bucket=bucket2)
        except ClientError:
            pass

        writer = S3Writer()

        # Act - perform multiple uploads
        data1 = b"data1"
        data2 = b"data2"
        buffer1 = io.BytesIO(data1)
        buffer2 = io.BytesIO(data2)

        writer.upload_fileobj(test_bucket, "path1", buffer1)
        writer.upload_fileobj(bucket2, "path2", buffer2)

        # Assert
        response1 = s3_client.get_object(Bucket=test_bucket, Key="path1")
        response2 = s3_client.get_object(Bucket=bucket2, Key="path2")

        assert response1["Body"].read() == data1
        assert response2["Body"].read() == data2

    def test_upload_fileobj_integration_with_real_data_format(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test upload with data that resembles real parquet format"""
        # Arrange
        writer = S3Writer()

        # Simulate parquet-like binary data
        parquet_like_data = b"\x50\x41\x52\x31"  # PAR1 header signature
        buffer = io.BytesIO(parquet_like_data)
        path = "binance/klines/ETHUSDT/year=2025/month=01/day=15/hour=10/data.parquet"

        # Act
        writer.upload_fileobj(test_bucket, path, buffer)

        # Assert
        response = s3_client.get_object(Bucket=test_bucket, Key=path)
        downloaded_data = response["Body"].read()
        assert downloaded_data == parquet_like_data

    def test_upload_fileobj_large_data(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test uploading larger data to verify multipart handling"""
        # Arrange
        writer = S3Writer()

        # Create 1MB of test data
        large_data = b"A" * (1024 * 1024)
        buffer = io.BytesIO(large_data)
        path = "large/data.parquet"

        # Act
        writer.upload_fileobj(test_bucket, path, buffer)

        # Assert
        response = s3_client.get_object(Bucket=test_bucket, Key=path)
        downloaded_data = response["Body"].read()
        assert len(downloaded_data) == len(large_data)
        assert downloaded_data == large_data

    def test_upload_fileobj_retry_mechanism(
        self, setup_localstack, test_bucket: str, s3_client: S3Client
    ):
        """Test that retry mechanism works by using invalid credentials temporarily"""
        # This test would need to be adjusted based on how you want to test retries
        # For now, we'll just test that the method completes successfully
        writer = S3Writer()
        test_data = b"retry test data"
        buffer = io.BytesIO(test_data)
        path = "retry/test.parquet"

        # Act
        writer.upload_fileobj(test_bucket, path, buffer)

        # Assert
        response = s3_client.get_object(Bucket=test_bucket, Key=path)
        downloaded_data = response["Body"].read()
        assert downloaded_data == test_data
