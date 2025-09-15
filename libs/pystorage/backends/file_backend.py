from pathlib import Path
from typing import Optional, Dict, Union

import zlib
import pickle
import pandas as pd


class StorageBackendError(Exception):
    pass


class FileStorageBackend:
    def __init__(self, storage_path: Path, storage_name: str):
        self.storage_path = storage_path / f"stg_{storage_name}.stg"

    def load(self) -> Optional[Dict[str, Union[bytes, pd.DataFrame, Dict]]]:
        if not self.storage_path.exists():
            return None
        try:
            with self.storage_path.open("rb") as f:
                compressed_data = f.read()
            decompressed_data = zlib.decompress(compressed_data)
            return pickle.loads(decompressed_data)
        except (zlib.error, pickle.PicklingError) as e:
            raise StorageBackendError(f"Failed to load storage: {e}")

    def save(self, data: Dict[str, Union[bytes, pd.DataFrame, Dict]]) -> bool:
        try:
            serialized_data = pickle.dumps(data)
            compressed_data = zlib.compress(serialized_data)
            with self.storage_path.open("wb") as f:
                f.write(compressed_data)
            return True
        except (zlib.error, pickle.PicklingError) as e:
            raise StorageBackendError(f"Failed to save storage: {e}")
