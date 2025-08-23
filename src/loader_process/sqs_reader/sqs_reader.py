import boto3
import json
import os
from typing import Any
from pydantic import BaseModel
from mypy_boto3_sqs.type_defs import (
    ReceiveMessageResultTypeDef,
    MessageTypeDef,
    DeleteMessageBatchRequestEntryTypeDef,
)
from mypy_boto3_sqs import SQSClient

from src.common.logging import setup_logger

logger = setup_logger(__name__)


class S3PutNotificationMessage(BaseModel):
    """
    Represents a parsed S3 notification message from SQS

    1. bucket_name - Required
    - Used to construct S3 client calls and identify which bucket to access
    - Essential for downloading/processing the file
    2. object_key - Required
    - The full path to the file in S3 (e.g., "klines/BTCUSDT/2023/01/file.parquet")
    - Essential for identifying and accessing the specific file

    To identify file location: use bucket_name + object_key

    Potentially useful additional fields:
    - event_name - When you want to parse/delete event based on event name, e.g "ObjectCreated:Put", "ObjectRemoved:Delete"
    - event_time - When the event occurred (useful for ordering/deduplication)
    - object_size - File size (useful for batching/resource planning)
    - object_etag - File version identifier (useful for deduplication)

    TODO: include event_time down the road because there could be a case where given files were uploaded in a batch,
    we want the files to be uploaded in order of upload. If the later file was uploaded first before the one later,
    the data uploaded eventually might not be the most up to date one
    """

    bucket_name: str
    object_key: str


class SQSReader:
    """
    Responsible for reading new files and their file paths from S3
    Read from SQS klines-notifications queue
    As part of the setup, it takes in the
    - Queue url: set as an environment variable

    And subscribe to the queue, 10 messages at a time (bc the max is 10)
    And deletes messages up to 10 at a time.
    """

    def __init__(self) -> None:
        self._sqs_client: SQSClient = boto3.client("sqs")
        self._queue_url: str = os.getenv("SQS_QUEUE_URL", "")
        if not self._queue_url:
            raise ValueError("SQS_QUEUE_URL environment variable is required")

    def read_messages(self, max_messages: int = 10) -> list[MessageTypeDef]:
        """
        Consumes up to max messages from SQS
        """
        try:
            # Block here for up to 20 seconds, or till SQS receives a max of 10 messages
            response: ReceiveMessageResultTypeDef = self._sqs_client.receive_message(
                QueueUrl=self._queue_url,
                MaxNumberOfMessages=min(max_messages, 10),
                WaitTimeSeconds=20,  # long polling
                MessageAttributeNames=["All"],
            )
            messages: list[MessageTypeDef] = response.get("Messages", [])
            logger.info(f"Recieved {len(messages)} messages from SQS quque")
            return messages
        except Exception:
            logger.exception(
                f"Failed to read messages from SQS queue {self._queue_url}"
            )
            raise

    def delete_messages(self, messages: list[MessageTypeDef] | None) -> None:
        """
        Deletes messages from SQS
        """
        # messages can be of type None
        if not messages:
            return
        try:
            entries: list[DeleteMessageBatchRequestEntryTypeDef] = [
                {"Id": str(i), "ReceiptHandle": message["ReceiptHandle"]}
                for i, message in enumerate(messages)
            ]
            # delete messages in batches of 10
            for i in range(0, len(entries), 10):
                # DeleteMessageBatchRequestTypeDef requires "QueueUrl" and "Entries" of type Sequence[DeleteMessageBatchRequestEntryTypeDef]
                # and DeleteMessageBatchRequestEntryTypeDef requires "Id" and "ReceiptHandle"
                # TLDR: to delete a message, you need the Id and ReceiptHandle
                batch = entries[i : i + 10]
                response = self._sqs_client.delete_message_batch(
                    QueueUrl=self._queue_url, Entries=batch
                )

                if "Failed" in response and response["Failed"]:
                    logger.error(f"Failed to delete messages: {response['Failed']}")
            logger.info(f"Successfully deleted {len(messages)} messages from SQS")

        except Exception:
            logger.exception(
                f"Failed to delete messages from SQS queue {self._queue_url}"
            )

    def parse_and_filter_put_s3_notification(
        self, message: MessageTypeDef
    ) -> list[S3PutNotificationMessage]:
        """
        Parse S3 notification message from SQS and filter for ObjectCreated:Put
        """
        try:
            # message["Body"] is a json string
            body: dict[str, Any] = json.loads(message["Body"])

            records = body.get("Records", [])

            notifications: list[S3PutNotificationMessage] = []
            for record in records:
                # refer to https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html for event message structure
                s3_info: dict[str, Any] = record.get("s3", {})
                bucket_name: str = s3_info["bucket"]["name"]
                object_key: str = s3_info["object"]["key"]
                event_name: str = record.get("eventName", "unknown")
                if event_name == "ObjectCreated:Put":
                    notifications.append(
                        S3PutNotificationMessage(
                            bucket_name=bucket_name,
                            object_key=object_key,
                        )
                    )

            logger.info(f"Parsed {len(notifications)} S3 notifications from message")
            return notifications

        except (json.JSONDecodeError, KeyError):
            logger.exception(f"Failed to parse S3 notification message: {message}")
            raise