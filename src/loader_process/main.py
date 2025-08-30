import time
import signal
import sys
from types import FrameType
from mypy_boto3_sqs.type_defs import MessageTypeDef
from src.loader_process.loaders.clickhouse_loader import ClickHouseLoader
from src.loader_process.s3_reader.s3_reader import S3Reader
from src.loader_process.sqs_reader.sqs_reader import SQSReader, S3PutNotificationMessage
from src.common.logging import setup_logger

logger = setup_logger(__name__)


class LoaderProcess:
    """
    Orchestrates the data loading pipeline from SQS notifications to Clickhouse

    Flow:
    1. Poll SQS for SQS file notifications (max of 10 at a time)
    2. Download each file from S3 using S3Reader
    3. Load each file into Clickhouse using ClickHouseLoader
    4. Delete processed messages from SQS

    The process runs continuously until stopped via signal or error.
    """
    def __init__(
            self,
            append_only_table: str,
            rmt_table: str,
            temp_table_template: str,
            columns: list[str],
            max_messages_per_batch: int = 10,
            poll_interval_seconds: int = 5,
    ) -> None:
        self._loader: ClickHouseLoader = ClickHouseLoader(
            append_only_table=append_only_table,
            rmt_table=rmt_table,
            temp_table_template=temp_table_template,
            columns=columns,
        )
        self._s3_reader: S3Reader() = S3Reader()
        self._sqs_reader: SQSReader = SQSReader()
        self._max_messages_per_batch: int = max_messages_per_batch
        self._poll_interval_seconds: int = poll_interval_seconds
        self._running: bool = True

        # setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler) # check
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum: int, frame_type: FrameType) -> None:
        """
        Handles shutdown signals gracefully
        """
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._running = False

    def _process_s3_notifications(self, raw_messages: list[MessageTypeDef]) -> bool:
        """
        Process a batch of S3 notifications by downloading files and loading into ClickHouse

        Check the raw messages' event type: We only want to ingest S3:PutObject Events
        """
        if not raw_messages:
            return True

        successful_files = []
        failed_files = []

        logger.info((f"Processing {len(raw_messages)} S3 notifications"))

        for notification in raw_messages:
            try:
                # for each raw message from S3 notification SQS reader read, parse it using the SQS reader
                parsed_notifications: list[S3PutNotificationMessage] = self._sqs_reader.parse_and_filter_put_s3_notification(notification)
                for parsed_notification in parsed_notifications:
                    try:
                        logger.info(f"Processing file: s3://{parsed_notification.bucket_name}/{parsed_notification.object_key}")
                        # Download file from S3
                        file_buffer = self._s3_reader.download_fileobj(
                            bucket_name=parsed_notification.bucket_name, path=parsed_notification.object_key
                        )
                        # Load file into ClickHouse
                        self._loader.load(file_buffer)
                        successful_files.append(parsed_notification.object_key)
                    except Exception as e:
                        logger.error(f"Failed to process from path {parsed_notification.object_key}: {e}")
                        failed_files.append(parsed_notification.object_key)
            except Exception as e:
                logger.error(f"Failed to parse notification message: {e}")
                failed_files.append("unknown_file")

        # Log summary
        if successful_files:
            logger.info(f"Successfully processed {len(successful_files)} files: {successful_files}")
        if failed_files:
            logger.error(f"Failed to process {len(failed_files)} files: {failed_files}")
        # Return True only if all files were processed successfully
        return len(failed_files) == 0

    def _process_batch(self) -> None:
        """
        Process one batch of SQS messages
        """
        try:
            # Get S3 notifications from SQS
            raw_messages = self._sqs_reader.read_messages(
                max_messages=self._max_messages_per_batch
            )

            # Process all notifications
            all_successful = self._process_s3_notifications(raw_messages)

            if all_successful:
                # Delete messages from SQS only if all files processed successfully
                self._sqs_reader.delete_messages(raw_messages)
                logger.info(
                    f"Deleted {len(raw_messages)} messages from SQS after successful processing"
                )
            else:
                logger.warning(
                    "Not deleting SQS messages due to processing failures - messages will be retried"
                )

        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            raise

    def run(self) -> None:
        """
        Run the process continuously

        In a while loop:
        1. Poll SQSReader for new files (up to max_messages_per_batch at at time
        2. For each batch of files, read each S3 file into BytesIO with a S3Reader
        3. Insert each file sequentially into ClickHouse with the loader
        4. Finally, delete the messages from SQS (only if all files have been processed successfully)
        """
        logger.info(
            f"Starting LoaderProcess with max_messages_per_batch={self._max_messages_per_batch}, poll_interval={self._poll_interval_seconds}s"
        )

        try:
            while self._running:
                self._process_batch()

                # Sleep between polling cycles
                if self._running:
                    time.sleep(self._poll_interval_seconds)

        except KeyboardInterrupt:
            logger.info(f"Received keyboard interrupt, shutting down")
        except Exception as e:
            logger.error(f"Fatal error in loader process: {e}")
            raise
        finally:
            logger.info("LoaderProcess stopped")

    def run_once(self) -> None:
        logger.info("Running single batch processing processor")
        self._process_batch()

def main() -> None:
    """
    Main entry point for loader process.

    Configures the loader for klines data and starts the process.
    """
    try:
        loader_process: LoaderProcess = LoaderProcess(
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
            ]
        )
        loader_process.run()
    except Exception as e:
        logger.info(f"Failed to start loader process: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

