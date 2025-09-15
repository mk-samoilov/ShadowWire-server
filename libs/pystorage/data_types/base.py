from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Generic, Type

T = TypeVar("T")

class DataType(ABC, Generic[T]):
    @abstractmethod
    def to_storage_format(self) -> T:
        pass

    @classmethod
    @abstractmethod
    def from_storage_format(cls: Type["DataType[T]"], data: T, **kwargs) -> Optional["DataType[T]"]:
        pass
