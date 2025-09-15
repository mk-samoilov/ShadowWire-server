from typing import Optional, Dict, Union

import json

from .base import DataType


class JsonStorageError(Exception):
    pass


class JsonData(DataType[Dict]):
    def __init__(self, content: Union[str, Dict], validate_json: bool = True):
        self.validate_json = validate_json
        try:
            if isinstance(content, str):
                self.data = json.loads(content) if validate_json else json.loads(content, strict=False)
            else:
                self.data = content
                if validate_json:
                    json.dumps(content)  # Validate JSON compatibility
        except json.JSONDecodeError as e:
            raise JsonStorageError(f"Invalid JSON content: {e}")

    def to_storage_format(self) -> Dict:
        return self.data

    @classmethod
    def from_storage_format(cls, data: Dict, validate_json: bool = True) -> Optional["JsonData"]:
        try:
            return cls(data, validate_json)
        except Exception:
            return None
