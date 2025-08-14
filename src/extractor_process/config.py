from pydantic import BaseModel


class DataSourceConfig(BaseModel):
    """
    Configuration for data source metadata including S3 bucket and path information
    """

    bucket_name: str
    source_path: str
