import io
from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from pydantic import BaseModel

ExtractedData = TypeVar("ExtractedData", bound=BaseModel)


class Formatter(ABC, Generic[ExtractedData]):
    @staticmethod
    @abstractmethod
    def format(data: ExtractedData) -> io.BytesIO | None:
        """
        Formats a Pydantic Data Class from the extractor into an in-memory file ready to be saved to S3
        """
        raise NotImplementedError()
