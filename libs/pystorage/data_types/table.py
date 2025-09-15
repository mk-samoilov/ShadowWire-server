from typing import Optional, List, Dict, Union

import pandas as pd

from .base import DataType


class TableStorageError(Exception):
    pass


class Table(DataType[pd.DataFrame]):
    def __init__(self, data: List[Dict], columns: Optional[List[str]] = None, schema: Optional[Dict[str, type]] = None):
        self.schema = schema or {}
        self.dataframe = pd.DataFrame(data, columns=columns) if columns else pd.DataFrame(data)
        if self.schema:
            for col, dtype in self.schema.items():
                if col in self.dataframe.columns:
                    if dtype != list:  # Skip type casting for list type
                        try:
                            self.dataframe[col] = self.dataframe[col].astype(dtype)
                        except Exception as e:
                            raise TableStorageError(f"Failed to apply schema for column {col}: {e}")
                    else:
                        # Validate that all values in the column are lists
                        if not all(isinstance(val, list) or pd.isna(val) for val in self.dataframe[col]):
                            raise TableStorageError(f"Column {col} contains non-list values, but schema specifies list type")

    def to_storage_format(self) -> pd.DataFrame:
        return self.dataframe

    @classmethod
    def from_storage_format(cls, data: pd.DataFrame, schema: Optional[Dict[str, type]] = None) -> Optional["Table"]:
        try:
            return cls(data.to_dict("records"), columns=list(data.columns), schema=schema)
        except Exception:
            return None

    def add_record(self, record: Union["TableRecord", Dict]) -> bool:
        try:
            record_dict = record.record if isinstance(record, TableRecord) else record
            new_row = pd.DataFrame([record_dict])
            if self.schema:
                for col, dtype in self.schema.items():
                    if col in new_row.columns:
                        if dtype != list:
                            try:
                                new_row[col] = new_row[col].astype(dtype)
                            except Exception as e:
                                raise TableStorageError(f"Failed to apply schema for column {col}: {e}")
                        else:
                            if not isinstance(new_row[col].iloc[0], list) and not pd.isna(new_row[col].iloc[0]):
                                raise TableStorageError(f"Column {col} in new record is not a list, but schema specifies list type")
            self.dataframe = pd.concat([self.dataframe, new_row], ignore_index=True)
            return True
        except Exception:
            return False

    def remove_record(self, index: Optional[int] = None, condition: Optional[Dict] = None) -> bool:
        try:
            if index is not None and condition is not None:
                raise ValueError("Cannot specify both index and condition")

            if index is not None:
                if index < 0 or index >= len(self.dataframe):
                    return False
                self.dataframe = self.dataframe.drop(index).reset_index(drop=True)
                return True

            elif condition is not None:
                mask = True
                for key, value in condition.items():
                    mask &= self.dataframe[key] == value
                if not mask.any():
                    return False
                self.dataframe = self.dataframe[~mask].reset_index(drop=True)
                return True

            else:
                raise ValueError("Must specify either index or condition")
        except Exception:
            return False


class TableRecord(DataType[Dict]):
    def __init__(self, record: Dict):
        self.record = record

    def to_storage_format(self) -> Dict:
        return self.record

    @classmethod
    def from_storage_format(cls, data: Dict, **kwargs) -> Optional["TableRecord"]:
        try:
            return cls(data)
        except Exception:
            return None
