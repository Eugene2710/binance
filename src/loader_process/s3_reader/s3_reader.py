import boto3
import io
from botocore.exceptions import ClientError
from src.common.logging import setup_logger

logger = setup_logger(__name__)


class S3Reader:
    """
    Responsible for reading a file into an in-memory buffer into S3
    """

    def __init__(self) -> None:
        self._s3_client = boto3.client("s3")

    def download_fileobj(self, bucket_name: str, path: str) -> io.BytesIO:
        try:
            buffer = io.BytesIO()
            self._s3_client.download_fileobj(
                Bucket=bucket_name, Key=path, Fileobj=buffer
            )
            buffer.seek(0)
            return buffer
        except ClientError:
            logger.exception(f"Failed to upload buffer to s3://{bucket_name}/{path}")
            raise
