import io
import os
from typing import Any

import pandas as pd
import pytest
import time
import uuid
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import (
    ObjectIdentifierTypeDef,
    NotificationConfigurationTypeDef,
)
from mypy_boto3_sqs import SQSClient
import boto3
import clickhouse_connect

from src.loader_process.main import LoaderProcess


class TestLoaderProcess:
    @pytest.fixture
    def s3_client(self) -> S3Client:
        return boto3.client("s3")

    @pytest.fixture
    def sqs_client(self) -> SQSClient:
        return boto3.client("sqs")

    @pytest.fixture
    def clickhouse_client(self):
        # Create client without specifying database (connect to default first)
        return clickhouse_connect.create_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            username=os.getenv("CLICKHOUSE_USERNAME", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "password"),
            database="default",  # Always use default database for setup
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        )

    @pytest.fixture
    def test_database_name(self) -> str:
        return f"test_loader_{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def test_bucket_name(self) -> str:
        return f"crypto-test-{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def test_queue_name(self) -> str:
        return f"klines-notifications-test-{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def test_table_names(self, test_database_name: str):
        return {
            "append_only": f"{test_database_name}.klines_append_only",
            "rmt": f"{test_database_name}.klines_rmt",
            "temp_template": f"{test_database_name}.klines_temp",
            "mv": f"{test_database_name}.klines_mv",
        }

    @pytest.fixture
    def setup_test_infrastructure(
            self,
            s3_client: S3Client,
            sqs_client: SQSClient,
            clickhouse_client,
            test_database_name: str,
            test_bucket_name: str,
            test_queue_name: str,
            test_table_names: dict[str, Any],
    ):
        """Setup complete test infrastructure including ClickHouse, S3, and SQS"""
        queue_url = None
        queue_arn = None

        try:
            # 1. Create test database in ClickHouse
            clickhouse_client.command(
                f"CREATE DATABASE IF NOT EXISTS {test_database_name}"
            )

            # 2. Create ClickHouse tables (based on resources/sql/klines.sql)
            # Create temp table template first (needed by ClickHouseLoader)
            create_tables_sql = f"""
            CREATE TABLE {test_table_names['append_only']}(
                symbol String,
                open_time DateTime64(3),
                open_price Float64,
                high_price Float64,
                low_price Float64,
                close_price Float64,
                volume Float64,
                close_time DateTime64(3),
                quote_asset_volume Float64,
                number_of_trades Int64,
                taker_buy_base_asset_volume Float64,
                taker_buy_quote_asset_volume Float64,
                ignore String,
                created_at DateTime64(3) DEFAULT now()
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(open_time)
            ORDER BY (symbol, open_time);

            CREATE TABLE {test_table_names['rmt']} AS {test_table_names['append_only']}
            ENGINE = ReplacingMergeTree(created_at)
            PARTITION BY toYYYYMM(open_time)
            ORDER BY (symbol, open_time);

            CREATE MATERIALIZED VIEW {test_table_names['mv']} TO {test_table_names['rmt']}
            AS
            SELECT * from {test_table_names['append_only']};

            CREATE TABLE {test_table_names['temp_template']}(
                symbol String,
                open_time DateTime64(3),
                open_price Float64,
                high_price Float64,
                low_price Float64,
                close_price Float64,
                volume Float64,
                close_time DateTime64(3),
                quote_asset_volume Float64,
                number_of_trades Int64,
                taker_buy_base_asset_volume Float64,
                taker_buy_quote_asset_volume Float64,
                ignore String,
                created_at DateTime64(3) DEFAULT now()
            )
            ENGINE = MergeTree
            ORDER BY (symbol, open_time);
            """

            for statement in create_tables_sql.strip().split(";"):
                if statement.strip():
                    clickhouse_client.command(statement.strip())

            # 3. Create S3 bucket
            try:
                s3_client.create_bucket(Bucket=test_bucket_name)
            except ClientError as e:
                if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                    raise

            # 4. Create SQS queue with short visibility timeout for testing
            try:
                response = sqs_client.create_queue(
                    QueueName=test_queue_name,
                    Attributes={
                        "VisibilityTimeout": "2"  # 2 seconds for faster test execution
                    }
                )
                queue_url = response["QueueUrl"]
            except ClientError as e:
                if e.response["Error"]["Code"] == "QueueAlreadyExists":
                    response = sqs_client.get_queue_url(QueueName=test_queue_name)
                    queue_url = response["QueueUrl"]
                else:
                    raise

            # 5. Get queue ARN
            attrs_response = sqs_client.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["QueueArn"]
            )
            queue_arn = attrs_response["Attributes"]["QueueArn"]

            # 6. Configure S3 bucket notification
            notification_config: NotificationConfigurationTypeDef = {
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

            # 7. Set environment variables for the LoaderProcess
            os.environ["SQS_QUEUE_URL"] = queue_url
            os.environ["CLICKHOUSE_DATABASE"] = test_database_name

            yield {
                "database_name": test_database_name,
                "bucket_name": test_bucket_name,
                "queue_url": queue_url,
                "queue_arn": queue_arn,
                "table_names": test_table_names,
            }

        finally:
            # Cleanup
            try:
                # Drop ClickHouse database
                clickhouse_client.command(
                    f"DROP DATABASE IF EXISTS {test_database_name}"
                )
            except Exception:
                pass

            try:
                # Delete all objects in S3 bucket
                objects_response = s3_client.list_objects_v2(Bucket=test_bucket_name)
                if "Contents" in objects_response:
                    delete_keys: list[ObjectIdentifierTypeDef] = [
                        {"Key": obj["Key"]} for obj in objects_response["Contents"]
                    ]
                    s3_client.delete_objects(
                        Bucket=test_bucket_name, Delete={"Objects": delete_keys}
                    )

                # Delete S3 bucket
                s3_client.delete_bucket(Bucket=test_bucket_name)
            except ClientError:
                pass

            try:
                # Purge and delete SQS queue
                if queue_url:
                    sqs_client.purge_queue(QueueUrl=queue_url)
                    time.sleep(2)
                    sqs_client.delete_queue(QueueUrl=queue_url)
            except ClientError:
                pass

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

    def test_loader_process_end_to_end(
            self,
            setup_test_infrastructure,
            s3_client: S3Client,
            sqs_client: SQSClient,
            clickhouse_client,
    ):
        """Test complete end-to-end LoaderProcess flow"""
        # Arrange
        test_data = self.create_sample_klines_parquet()
        s3_key = "data_sources/klines_pricing/btcusd/test_data.parquet"

        # Create LoaderProcess instance
        loader_process = LoaderProcess(
            append_only_table="klines_append_only",
            rmt_table="klines_rmt",
            temp_table_template="klines_temp",
            columns=[
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
                "created_at",
            ],
            max_messages_per_batch=10,
            poll_interval_seconds=1,
        )

        # Upload file to S3 (triggers notification)
        s3_client.put_object(
            Bucket=setup_test_infrastructure["bucket_name"], Key=s3_key, Body=test_data
        )

        # Wait for notification to be processed
        time.sleep(3)

        # Act - Run one processing cycle
        loader_process.run_once()

        # Assert 1 - Data should be in ClickHouse RMT table (wait for materialized view)
        rmt_table = setup_test_infrastructure["table_names"]["rmt"]

        # Retry logic for materialized view to populate RMT table
        rows = []
        for attempt in range(10):
            result = clickhouse_client.query(f"SELECT * FROM {rmt_table}")
            rows = result.result_rows
            if len(rows) == 3:
                break
            time.sleep(1)

        assert (
                len(rows) == 3
        ), f"Expected 3 rows in RMT table after {attempt + 1} attempts, got {len(rows)}"

        # Verify data content
        symbols = [row[0] for row in rows]
        assert all(
            symbol == "BTCUSD" for symbol in symbols
        ), "All symbols should be BTCUSD"

        # Verify prices
        open_prices = [row[2] for row in rows]
        expected_open_prices = [50000.0, 50100.0, 50200.0]
        assert (
                open_prices == expected_open_prices
        ), f"Expected {expected_open_prices}, got {open_prices}"

        # Assert 2 - SQS message should be deleted
        # Wait a bit for deletion to take effect
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
        ), "SQS message should be deleted after successful processing"

    def test_loader_process_with_processing_failure(
            self,
            setup_test_infrastructure,
            s3_client: S3Client,
            sqs_client: SQSClient,
            clickhouse_client,
    ):
        """Test LoaderProcess behavior when processing fails"""
        # Arrange - Upload invalid parquet data
        invalid_data = b"this is not valid parquet data"
        s3_key = "data_sources/klines_pricing/btcusd/invalid_data.parquet"

        # Create LoaderProcess instance
        loader_process = LoaderProcess(
            append_only_table="klines_append_only",
            rmt_table="klines_rmt",
            temp_table_template="klines_temp",
            columns=[
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
                "created_at",
            ],
        )

        # Upload invalid file to S3
        s3_client.put_object(
            Bucket=setup_test_infrastructure["bucket_name"],
            Key=s3_key,
            Body=invalid_data,
        )

        # Wait for notification
        time.sleep(3)

        # Act - Run one processing cycle (should fail)
        loader_process.run_once()

        # Assert 1 - No data should be in ClickHouse (wait to be sure)
        rmt_table = setup_test_infrastructure["table_names"]["rmt"]

        # Wait and check that no data appears
        for attempt in range(5):
            result = clickhouse_client.query(f"SELECT COUNT(*) FROM {rmt_table}")
            count = result.result_rows[0][0]
            if count == 0:
                break
            time.sleep(1)

        assert count == 0, "No data should be inserted when processing fails"

        # Assert 2 - SQS message should NOT be deleted (for retry)
        # Wait for the visibility timeout to expire so messages become available again
        time.sleep(3)  # Wait longer than the 2-second visibility timeout

        remaining_messages = []
        for attempt in range(10):
            response = sqs_client.receive_message(
                QueueUrl=setup_test_infrastructure["queue_url"],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=1,
            )
            remaining_messages = response.get("Messages", [])
            if len(remaining_messages) >= 1:
                break
            time.sleep(1)

        assert (
                len(remaining_messages) >= 1
        ), "SQS message should remain for retry when processing fails"

    def test_loader_process_multiple_files(
            self,
            setup_test_infrastructure,
            s3_client: S3Client,
            sqs_client: SQSClient,
            clickhouse_client,
    ):
        """Test LoaderProcess with multiple files"""
        # Arrange
        test_data = self.create_sample_klines_parquet()
        file_keys = [
            "data_sources/klines_pricing/btcusd/file1.parquet",
            "data_sources/klines_pricing/btcusd/file2.parquet",
            "data_sources/klines_pricing/btcusd/file3.parquet",
        ]

        loader_process = LoaderProcess(
            append_only_table="klines_append_only",
            rmt_table="klines_rmt",
            temp_table_template="klines_temp",
            columns=[
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
                "created_at",
            ],
        )

        # Upload multiple files
        for key in file_keys:
            s3_client.put_object(
                Bucket=setup_test_infrastructure["bucket_name"], Key=key, Body=test_data
            )

        # Wait for notifications
        time.sleep(3)

        # Act
        loader_process.run_once()

        # Assert - Should have 9 rows total (3 rows per file × 3 files)
        rmt_table = setup_test_infrastructure["table_names"]["rmt"]

        # Retry logic for materialized view to populate RMT table with all data
        count = 0
        for attempt in range(10):
            result = clickhouse_client.query(f"SELECT COUNT(*) FROM {rmt_table}")
            count = result.result_rows[0][0]
            if count == 9:
                break
            time.sleep(1)

        assert (
                count == 9
        ), f"Expected 9 rows (3 files × 3 rows each), got {count} after {attempt + 1} attempts"

        # Assert - All messages should be deleted
        remaining_messages = []
        for attempt in range(10):
            time.sleep(1)
            response = sqs_client.receive_message(
                QueueUrl=setup_test_infrastructure["queue_url"],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1,
            )
            remaining_messages = response.get("Messages", [])
            if len(remaining_messages) == 0:
                break

        assert (
                len(remaining_messages) == 0
        ), f"All SQS messages should be deleted after successful processing, but {len(remaining_messages)} remain after {attempt + 1} attempts"

    def test_loader_process_deduplication(
            self, setup_test_infrastructure, s3_client: S3Client, clickhouse_client
    ):
        """Test that ReplacingMergeTree deduplication works correctly"""
        # Arrange - Create data with duplicate entries (same symbol + open_time)
        duplicate_data = {
            "symbol": ["BTCUSD", "BTCUSD"],  # Same symbol
            "open_time": [
                pd.Timestamp("2025-01-15 10:00:00"),  # Same timestamp
                pd.Timestamp("2025-01-15 10:00:00"),  # Same timestamp
            ],
            "open_price": [50000.0, 51000.0],  # Different prices (second should win)
            "high_price": [50150.0, 51150.0],
            "low_price": [49950.0, 50950.0],
            "close_price": [50100.0, 51100.0],
            "volume": [10.5, 20.5],
            "close_time": [
                pd.Timestamp("2025-01-15 10:00:59.999"),
                pd.Timestamp("2025-01-15 10:00:59.999"),
            ],
            "quote_asset_volume": [525525.0, 1051050.0],
            "number_of_trades": [156, 312],
            "taker_buy_base_asset_volume": [6.2, 12.4],
            "taker_buy_quote_asset_volume": [310620.0, 621240.0],
            "ignore": ["", ""],
            "created_at": [
                pd.Timestamp("2025-01-15 10:05:00"),  # Earlier timestamp
                pd.Timestamp("2025-01-15 10:06:00"),  # Later timestamp (should win)
            ],
        }

        df = pd.DataFrame(duplicate_data)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        duplicate_parquet_data = buffer.getvalue()

        loader_process = LoaderProcess(
            append_only_table="klines_append_only",
            rmt_table="klines_rmt",
            temp_table_template="klines_temp",
            columns=[
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
                "created_at",
            ],
        )

        # Upload file with duplicates
        s3_client.put_object(
            Bucket=setup_test_infrastructure["bucket_name"],
            Key="data_sources/klines_pricing/btcusd/duplicate_data.parquet",
            Body=duplicate_parquet_data,
        )

        time.sleep(3)

        # Act
        loader_process.run_once()

        # Wait for data to appear in RMT table first
        rmt_table = setup_test_infrastructure["table_names"]["rmt"]

        # Wait for materialized view to populate data
        rows_count = 0
        for attempt in range(10):
            result = clickhouse_client.query(f"SELECT COUNT(*) FROM {rmt_table}")
            rows_count = result.result_rows[0][0]
            if rows_count > 0:
                break
            time.sleep(1)

        assert (
                rows_count > 0
        ), f"No data appeared in RMT table after {attempt + 1} attempts"

        # Force ClickHouse to merge parts for deduplication
        clickhouse_client.command(f"OPTIMIZE TABLE {rmt_table} FINAL")

        # Wait for optimization to complete and check deduplication
        final_rows = []
        for attempt in range(10):
            result = clickhouse_client.query(
                f"SELECT * FROM {rmt_table} FINAL ORDER BY created_at DESC"
            )
            final_rows = result.result_rows
            if len(final_rows) == 1:
                break
            time.sleep(1)

        assert (
                len(final_rows) == 1
        ), f"Should have 1 row after deduplication, got {len(final_rows)} after {attempt + 1} attempts"

        # Assert - Should have the latest data (higher open_price from second record)
        row = final_rows[0]
        open_price = row[2]
        assert (
                open_price == 51000.0
        ), f"Expected latest open_price 51000.0, got {open_price}"
