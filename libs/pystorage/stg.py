from pathlib import Path
from typing import Optional, List, Union, Type

from .data_types.file import File
from .data_types.table import Table, TableRecord
from .data_types.json import JsonData
from .backends.file_backend import FileStorageBackend, StorageBackendError
from .data_types.base import DataType


class StorageError(Exception):
    pass


class Storage:
    def __init__(self, storage_name: str, storage_path: str = "storages", backend = FileStorageBackend):
        if not storage_name:
            raise ValueError("storage_name cannot be empty")

        self.storage_name = storage_name
        self.storage_path = Path(storage_path)
        self.backend = backend(self.storage_path, storage_name)

        self._ensure_storage_exists()
        self._initialize_storage()

    def _ensure_storage_exists(self) -> None:
        if not self.storage_path.exists():
            self.storage_path.mkdir(parents=True)

    def _initialize_storage(self) -> None:
        if not self.exists():
            try:
                self.backend.save({})
            except StorageBackendError as e:
                raise StorageError(f"Failed to initialize storage: {e}")

    def write(self, key: str, item: Union[File, Table, TableRecord, JsonData], if_not_exist: bool = False) -> bool:
        try:
            data = self.backend.load() or {}
            if if_not_exist and key in data:
                return False
            data[key] = item.to_storage_format()
            return self.backend.save(data)
        except StorageBackendError as e:
            raise StorageError(f"Failed to write item with key {key}: {e}")

    def read(self, key: str, item_type: Type[DataType], **kwargs) -> Optional[Union[File, Table, TableRecord, JsonData]]:
        try:
            data = self.backend.load()
            if data is None or key not in data:
                return None
            return item_type.from_storage_format(data[key], **kwargs)
        except StorageBackendError as e:
            raise StorageError(f"Failed to read item with key {key}: {e}")

    def delete(self, key: str) -> bool:
        try:
            data = self.backend.load()
            if data is None or key not in data:
                return False
            del data[key]
            return self.backend.save(data)
        except StorageBackendError as e:
            raise StorageError(f"Failed to delete item with key {key}: {e}")

    def exists(self, key: Optional[str] = None) -> bool:
        try:
            if not self.storage_path.exists():
                return False
            if key is None:
                return True
            data = self.backend.load()
            return data is not None and key in data
        except StorageBackendError as e:
            raise StorageError(f"Failed to check existence: {e}")

    def list_items(self) -> List[str]:
        try:
            data = self.backend.load()
            return list(data.keys()) if data is not None else []
        except StorageBackendError as e:
            raise StorageError(f"Failed to list items: {e}")

    def list_storages(self) -> List[str]:
        return [
            f.name[len("stg_"):-len(".stg")]
            for f in self.storage_path.glob("stg_*.stg")
            if f.is_file()
        ]
