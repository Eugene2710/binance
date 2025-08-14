import boto3
import io
from botocore.exceptions import ClientError
from src.common.logging import setup_logger

logger = setup_logger(__name__)


class S3Writer:
    """
    Responsible for writing an in-memory buffer into S3
    """

    def __init__(self) -> None:
        self._s3_client = boto3.client("s3")

    def upload_fileobj(self, bucket_name: str, path: str, buffer: io.BytesIO) -> None:
        try:
            self._s3_client.upload_fileobj(Bucket=bucket_name, Key=path, Fileobj=buffer)
        except ClientError:
            logger.exception(f"Failed to upload buffer to s3://{bucket_name}/{path}")
            raise
