import io
from abc import ABC


class BaseLoader(ABC):
    """
    Base class which takes in a io.BytesIO loaded from S3 to be inserted into the DB.
    """

    def load(self, data: io.BytesIO) -> None:
        raise NotImplementedError()
