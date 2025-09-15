from typing import Optional

from .base import DataType


class FileStorageError(Exception):
    pass


class File(DataType[bytes]):
    def __init__(self, content: str, encoding: str = "utf-8"):
        self.content = content
        self.encoding = encoding

    def to_storage_format(self) -> bytes:
        try:
            return self.content.encode(self.encoding)
        except UnicodeEncodeError as e:
            raise FileStorageError(f"Failed to encode content with {self.encoding}: {e}")

    @classmethod
    def from_storage_format(cls, data: bytes, encoding: str = "utf-8") -> Optional["File"]:
        try:
            return cls(data.decode(encoding), encoding)
        except UnicodeDecodeError:
            return None
