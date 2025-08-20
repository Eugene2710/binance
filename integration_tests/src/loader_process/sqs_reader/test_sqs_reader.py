import io
import json
import os
import pandas as pd
import pytest
import time
import uuid
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
import boto3

from src.loader_process.sqs_reader.sqs_reader import SQSReader


class TestSQSReader:
    @pytest.fixture
    def s3_client(self) -> S3Client:
        return boto3.client("s3")

    @pytest.fixture
    def sqs_client(self) -> SQSClient:
        return boto3.client("sqs")

    @pytest.fixture
    def test_bucket_name(self) -> str:
        return f"crypto-test-{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def test_queue_name(self) -> str:
        return f"klines-notifications-test-{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def setup_test_infrastructure(
        self,
        s3_client: S3Client,
        sqs_client: SQSClient,
        test_bucket_name: str,
        test_queue_name: str,
    ):
        """Setup test S3 bucket, SQS queue, and notification configuration"""
        queue_url = None
        queue_arn = None

        try:
            # Create S3 bucket
            try:
                s3_client.create_bucket(Bucket=test_bucket_name)
            except ClientError as e:
                if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                    raise

            # Create SQS queue
            try:
                response = sqs_client.create_queue(QueueName=test_queue_name)
                queue_url = response["QueueUrl"]
            except ClientError as e:
                if e.response["Error"]["Code"] == "QueueAlreadyExists":
                    response = sqs_client.get_queue_url(QueueName=test_queue_name)
                    queue_url = response["QueueUrl"]
                else:
                    raise

            # Get queue ARN
            attrs_response = sqs_client.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["QueueArn"]
            )
            queue_arn = attrs_response["Attributes"]["QueueArn"]

            # Configure S3 bucket notification
            notification_config = {
                "QueueConfigurations": [
                    {
                        "Id": "klines-parquet-notification-test",
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {
                                        "Name": "prefix",
                                        "Value": "data_sources/klines_pricing/btcusd/",
                                    },
                                    {"Name": "suffix", "Value": ".parquet"},
                                ]
                            }
                        },
                    }
                ]
            }

            s3_client.put_bucket_notification_configuration(
                Bucket=test_bucket_name, NotificationConfiguration=notification_config
            )

            # Set environment variable for SQS reader
            original_queue_url = os.environ.get("SQS_QUEUE_URL")
            os.environ["SQS_QUEUE_URL"] = queue_url

            yield {
                "bucket_name": test_bucket_name,
                "queue_url": queue_url,
                "queue_arn": queue_arn,
                "original_queue_url": original_queue_url,
            }

        finally:
            # Cleanup
            try:
                # Delete all objects in bucket
                objects_response = s3_client.list_objects_v2(Bucket=test_bucket_name)
                if "Contents" in objects_response:
                    delete_keys = [
                        {"Key": obj["Key"]} for obj in objects_response["Contents"]
                    ]
                    s3_client.delete_objects(
                        Bucket=test_bucket_name, Delete={"Objects": delete_keys}
                    )

                # Delete bucket
                s3_client.delete_bucket(Bucket=test_bucket_name)
            except ClientError:
                pass  # Bucket might not exist or might not be empty

            try:
                # Purge and delete queue
                if queue_url:
                    sqs_client.purge_queue(QueueUrl=queue_url)
                    # Wait a bit for purge to complete
                    time.sleep(2)
                    sqs_client.delete_queue(QueueUrl=queue_url)
            except ClientError:
                pass  # Queue might not exist

    def create_sample_klines_parquet(self) -> bytes:
        """Create sample parquet data matching the klines schema"""
        data = {
            "symbol": ["BTCUSD", "BTCUSD", "BTCUSD"],
            "open_time": [
                pd.Timestamp("2025-01-15 10:00:00"),
                pd.Timestamp("2025-01-15 10:01:00"),
                pd.Timestamp("2025-01-15 10:02:00"),
            ],
            "open_price": [50000.0, 50100.0, 50200.0],
            "high_price": [50150.0, 50250.0, 50350.0],
            "low_price": [49950.0, 50050.0, 50150.0],
            "close_price": [50100.0, 50200.0, 50300.0],
            "volume": [10.5, 12.3, 8.7],
            "close_time": [
                pd.Timestamp("2025-01-15 10:00:59.999"),
                pd.Timestamp("2025-01-15 10:01:59.999"),
                pd.Timestamp("2025-01-15 10:02:59.999"),
            ],
            "quote_asset_volume": [525525.0, 617460.0, 437610.0],
            "number_of_trades": [156, 189, 134],
            "taker_buy_base_asset_volume": [6.2, 7.8, 4.9],
            "taker_buy_quote_asset_volume": [310620.0, 391560.0, 246470.0],
            "ignore": ["", "", ""],
            "created_at": [
                pd.Timestamp("2025-01-15 10:05:00"),
                pd.Timestamp("2025-01-15 10:05:00"),
                pd.Timestamp("2025-01-15 10:05:00"),
            ],
        }

        df = pd.DataFrame(data)

        # Convert to parquet bytes
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        return buffer.getvalue()

    def test_sqs_reader_initialization_with_valid_env(self, setup_test_infrastructure):
        """Test SQS reader initializes correctly with valid environment variable"""
        reader = SQSReader()
        assert reader._queue_url == setup_test_infrastructure["queue_url"]

    def test_sqs_reader_initialization_without_env_var(self):
        """Test SQS reader raises error without environment variable"""
        original_value = os.environ.get("SQS_QUEUE_URL")
        try:
            os.environ.pop("SQS_QUEUE_URL", None)
            with pytest.raises(
                ValueError, match="SQS_QUEUE_URL environment variable is required"
            ):
                SQSReader()
        finally:
            if original_value:
                os.environ["SQS_QUEUE_URL"] = original_value

    def test_s3_notification_triggers_sqs_message(
        self, setup_test_infrastructure, s3_client: S3Client
    ):
        """Test that uploading a parquet file to S3 triggers SQS notification"""
        # Arrange
        parquet_data = self.create_sample_klines_parquet()
        s3_key = "data_sources/klines_pricing/btcusd/sample_data.parquet"

        reader = SQSReader()

        # Act - Upload file to S3 (should trigger notification)
        s3_client.put_object(
            Bucket=setup_test_infrastructure["bucket_name"],
            Key=s3_key,
            Body=parquet_data,
        )

        # Wait a moment for notification to be processed
        time.sleep(2)

        # Read messages from SQS
        notifications = reader.get_s3_notifications(max_messages=5)

        # Assert
        assert len(notifications) >= 1
        notification = notifications[0]
        assert notification.bucket_name == setup_test_infrastructure["bucket_name"]
        assert notification.object_key == s3_key
        assert notification.event_name.startswith("ObjectCreated:Put")

    def test_parse_s3_notification_message(self, setup_test_infrastructure):
        """Test parsing of S3 notification message structure"""
        # Arrange
        reader = SQSReader()

        # Sample S3 notification message structure
        sample_message = {
            "Body": json.dumps(
                {
                    "Records": [
                        {
                            "eventName": "s3:ObjectCreated:Put",
                            "s3": {
                                "bucket": {"name": "crypto-test"},
                                "object": {
                                    "key": "data_sources/klines_pricing/btcusd/test.parquet"
                                },
                            },
                        }
                    ]
                }
            )
        }

        # Act
        notifications = reader._parse_s3_notification(sample_message)

        # Assert
        assert len(notifications) == 1
        notification = notifications[0]
        assert notification.bucket_name == "crypto-test"
        assert (
            notification.object_key == "data_sources/klines_pricing/btcusd/test.parquet"
        )
        assert notification.event_name == "s3:ObjectCreated:Put"

    def test_parse_sns_wrapped_s3_notification(self, setup_test_infrastructure):
        """Test parsing of SNS-wrapped S3 notification message"""
        # Arrange
        reader = SQSReader()

        # Sample SNS-wrapped S3 notification
        sample_message = {
            "Body": json.dumps(
                {
                    "Message": json.dumps(
                        {
                            "Records": [
                                {
                                    "eventName": "s3:ObjectCreated:Post",
                                    "s3": {
                                        "bucket": {"name": "crypto-test"},
                                        "object": {
                                            "key": "data_sources/klines_pricing/btcusd/sns_test.parquet"
                                        },
                                    },
                                }
                            ]
                        }
                    )
                }
            )
        }

        # Act
        notifications = reader._parse_s3_notification(sample_message)

        # Assert
        assert len(notifications) == 1
        notification = notifications[0]
        assert notification.bucket_name == "crypto-test"
        assert (
            notification.object_key
            == "data_sources/klines_pricing/btcusd/sns_test.parquet"
        )
        assert notification.event_name == "s3:ObjectCreated:Post"

    def test_delete_messages_success(
        self, setup_test_infrastructure, sqs_client: SQSClient
    ):
        """Test successful deletion of SQS messages"""
        # Arrange
        reader = SQSReader()

        # Send a test message
        sqs_client.send_message(
            QueueUrl=setup_test_infrastructure["queue_url"], MessageBody="test message"
        )

        # Read the message
        messages = reader._read_messages(max_messages=1)
        assert len(messages) == 1

        # Act - Delete the message
        reader.delete_messages(messages)

        # Assert - Message should be gone (verify with direct SQS client call)
        # Use the SQS client directly with shorter wait time to verify deletion
        for attempt in range(10):
            time.sleep(0.5)
            response = sqs_client.receive_message(
                QueueUrl=setup_test_infrastructure["queue_url"],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=1,
            )
            remaining_messages = response.get("Messages", [])
            if len(remaining_messages) == 0:
                break

        assert (
            len(remaining_messages) == 0
        ), f"Message still exists after {attempt + 1} attempts"

    def test_delete_messages_empty_list(self, setup_test_infrastructure):
        """Test delete_messages with empty list does nothing"""
        # Arrange
        reader = SQSReader()

        # Act & Assert - Should not raise any exception
        reader.delete_messages([])

    def test_read_messages_with_wait_time(self, setup_test_infrastructure):
        """Test read_messages respects max_messages parameter"""
        # Arrange
        reader = SQSReader()

        # Act - Should return quickly even with no messages due to test setup
        messages = reader._read_messages(max_messages=1)

        # Assert
        assert len(messages) == 1
        # In a real scenario this would wait up to 20 seconds, but LocalStack should return faster

    def test_end_to_end_notification_flow(
        self, setup_test_infrastructure, s3_client: S3Client
    ):
        """Test complete end-to-end flow from S3 upload to SQS message processing"""
        # Arrange
        parquet_data = self.create_sample_klines_parquet()
        s3_key = "data_sources/klines_pricing/btcusd/end_to_end_test.parquet"

        reader = SQSReader()

        # Act 1 - Upload file to S3
        s3_client.put_object(
            Bucket=setup_test_infrastructure["bucket_name"],
            Key=s3_key,
            Body=parquet_data,
        )

        # Wait for notification
        time.sleep(2)

        # Act 2 - Read and parse notifications
        notifications = reader.get_s3_notifications(max_messages=5)

        # Assert
        assert len(notifications) >= 1

        # Find our specific notification
        our_notification = None
        for notification in notifications:
            if notification.object_key == s3_key:
                our_notification = notification
                break

        assert our_notification is not None
        assert our_notification.bucket_name == setup_test_infrastructure["bucket_name"]
        assert our_notification.object_key == s3_key
        assert our_notification.event_name.startswith("ObjectCreated:Put")

    def test_notification_filtering_by_prefix_and_suffix(
        self, setup_test_infrastructure, s3_client: S3Client
    ):
        """Test that only files matching prefix and suffix trigger notifications"""
        # Arrange
        reader = SQSReader()

        # Upload file that should NOT trigger notification (wrong prefix)
        s3_client.put_object(
            Bucket=setup_test_infrastructure["bucket_name"],
            Key="wrong_prefix/test.parquet",
            Body=b"test data",
        )

        # Upload file that should NOT trigger notification (wrong suffix)
        s3_client.put_object(
            Bucket=setup_test_infrastructure["bucket_name"],
            Key="data_sources/klines_pricing/btcusd/test.txt",
            Body=b"test data",
        )

        # Upload file that SHOULD trigger notification
        parquet_data = self.create_sample_klines_parquet()
        s3_client.put_object(
            Bucket=setup_test_infrastructure["bucket_name"],
            Key="data_sources/klines_pricing/btcusd/valid_file.parquet",
            Body=parquet_data,
        )

        # Wait for any notifications
        time.sleep(3)

        # Act
        notifications = reader.get_s3_notifications(max_messages=10)

        # Assert - Should only get notification for the valid file
        valid_notifications = [
            n
            for n in notifications
            if n.object_key == "data_sources/klines_pricing/btcusd/valid_file.parquet"
        ]
        assert len(valid_notifications) >= 1

        # Should not get notifications for invalid files
        invalid_notifications = [
            n
            for n in notifications
            if n.object_key
            in [
                "wrong_prefix/test.parquet",
                "data_sources/klines_pricing/btcusd/test.txt",
            ]
        ]
        assert len(invalid_notifications) == 0

    def test_multiple_file_uploads_multiple_notifications(
        self, setup_test_infrastructure, s3_client: S3Client
    ):
        """Test that multiple file uploads generate multiple notifications"""
        # Arrange
        reader = SQSReader()
        parquet_data = self.create_sample_klines_parquet()

        file_keys = [
            "data_sources/klines_pricing/btcusd/file1.parquet",
            "data_sources/klines_pricing/btcusd/file2.parquet",
            "data_sources/klines_pricing/btcusd/file3.parquet",
        ]

        # Act - Upload multiple files
        for key in file_keys:
            s3_client.put_object(
                Bucket=setup_test_infrastructure["bucket_name"],
                Key=key,
                Body=parquet_data,
            )

        # Wait for notifications
        time.sleep(3)

        notifications = reader.get_s3_notifications(max_messages=10)

        # Assert
        notification_keys = [n.object_key for n in notifications]
        for expected_key in file_keys:
            assert expected_key in notification_keys