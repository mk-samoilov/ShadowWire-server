import os
import pickle
import hashlib

from pathlib import Path
from typing import Optional, Dict, Union, Any

from application.libs.pycrypter.core import Crypter


class EncryptedStorageBackendError(Exception):
    pass


class EncryptedStorageBackend:
    MASTER_KEY = "master_key_v0.2.7".encode()
    
    def __init__(self, storage_path: Path, storage_name: str):
        self.storage_path = storage_path / f"stg_{storage_name}.stg"
        self.master_crypter = Crypter(self.MASTER_KEY)
        
    def _get_key_for_element(self, element_name: str) -> bytes:
        try:
            if not self.storage_path.exists():
                keys_table = {}
                new_key = self._generate_key_for_element(element_name)
                keys_table[element_name] = new_key
                self._save_keys_table(keys_table)
                return new_key
            
            with self.storage_path.open("rb") as f:
                encrypted_data = f.read()
            
            if not encrypted_data or len(encrypted_data) < 16:
                keys_table = {}
                new_key = self._generate_key_for_element(element_name)
                keys_table[element_name] = new_key
                self._save_keys_table(keys_table)
                return new_key
            
            try:
                decrypted_data = self.master_crypter.decrypt(encrypted_data)
                storage_data = pickle.loads(decrypted_data)
                
                keys_table = storage_data.get("_stg_keys", {})
                
                if element_name in keys_table:
                    return keys_table[element_name]
                else:
                    new_key = self._generate_key_for_element(element_name)
                    keys_table[element_name] = new_key
                    self._save_keys_table(keys_table)
                    return new_key
            except Exception:
                keys_table = {}
                new_key = self._generate_key_for_element(element_name)
                keys_table[element_name] = new_key
                self._save_keys_table(keys_table)
                return new_key
                
        except Exception as e:
            raise EncryptedStorageBackendError(f"Failed to get key for element {element_name}: {e}")
    
    @staticmethod
    def _generate_key_for_element(element_name: str) -> bytes:
        random_data = os.urandom(32)
        combined = f"{element_name}_{random_data.hex()}".encode()
        return hashlib.sha256(combined).digest()
    
    def _save_keys_table(self, keys_table: Dict[str, bytes]) -> None:
        try:
            storage_data = {}
            if self.storage_path.exists():
                with self.storage_path.open("rb") as f:
                    encrypted_data = f.read()
                if encrypted_data and len(encrypted_data) >= 16:
                    try:
                        decrypted_data = self.master_crypter.decrypt(encrypted_data)
                        storage_data = pickle.loads(decrypted_data)
                    except Exception:
                        storage_data = {}
            
            storage_data["_stg_keys"] = keys_table
            
            serialized_data = pickle.dumps(storage_data)
            encrypted_data = self.master_crypter.encrypt(serialized_data)

            with self.storage_path.open("wb") as f:
                f.write(encrypted_data)

        except Exception as e:
            raise EncryptedStorageBackendError(f"Failed to save keys table: {e}")
    
    def _remove_key_for_element(self, element_name: str) -> None:
        try:
            if not self.storage_path.exists():
                return
            
            with self.storage_path.open("rb") as f:
                encrypted_data = f.read()
            
            if not encrypted_data or len(encrypted_data) < 16:
                return
            
            try:
                decrypted_data = self.master_crypter.decrypt(encrypted_data)
                storage_data = pickle.loads(decrypted_data)
                
                keys_table = storage_data.get("_stg_keys", {})
                
                if element_name in keys_table:
                    del keys_table[element_name]
                    self._save_keys_table(keys_table)

            except Exception:
                pass
                
        except Exception as e:
            raise EncryptedStorageBackendError(f"Failed to remove key for element {element_name}: {e}")
    
    def load(self) -> Optional[Dict[str, Union[bytes, Any]]]:
        if not self.storage_path.exists():
            return None
            
        try:
            with self.storage_path.open("rb") as f:
                encrypted_data = f.read()
            
            if not encrypted_data:
                return None
            
            if len(encrypted_data) < 16:
                return None
            
            decrypted_data = self.master_crypter.decrypt(encrypted_data)
            storage_data = pickle.loads(decrypted_data)
            
            if isinstance(storage_data, dict) and "_stg_keys" in storage_data:
                user_data = {k: v for k, v in storage_data.items() if k != "_stg_keys"}
                return user_data
            
            return storage_data
            
        except Exception:
            return None
    
    def save(self, data: Dict[str, Union[bytes, Any]]) -> bool:
        try:
            storage_data = {}
            if self.storage_path.exists():
                with self.storage_path.open("rb") as f:
                    encrypted_data = f.read()

                if encrypted_data and len(encrypted_data) >= 16:
                    try:
                        decrypted_data = self.master_crypter.decrypt(encrypted_data)
                        storage_data = pickle.loads(decrypted_data)
                    except Exception:
                        storage_data = {}
            
            combined_data = {**storage_data, **data}
            
            serialized_data = pickle.dumps(combined_data)
            encrypted_data = self.master_crypter.encrypt(serialized_data)
            with self.storage_path.open("wb") as f:
                f.write(encrypted_data)
            
            return True
            
        except Exception as e:
            raise EncryptedStorageBackendError(f"Failed to save encrypted storage: {e}")
    
    def read_element(self, element_name: str) -> Optional[Any]:
        try:
            if not self.storage_path.exists():
                return None
            
            with self.storage_path.open("rb") as f:
                encrypted_data = f.read()
            
            if not encrypted_data or len(encrypted_data) < 16:
                return None
            
            try:
                decrypted_data = self.master_crypter.decrypt(encrypted_data)
                storage_data = pickle.loads(decrypted_data)
                
                if element_name not in storage_data:
                    return None
                
                return storage_data[element_name]

            except Exception:
                return None
            
        except Exception as e:
            raise EncryptedStorageBackendError(f"Failed to read element {element_name}: {e}")
    
    def write_element(self, element_name: str, element_data: Any) -> bool:
        try:
            storage_data = {}
            if self.storage_path.exists():
                with self.storage_path.open("rb") as f:
                    encrypted_data = f.read()
                if encrypted_data and len(encrypted_data) >= 16:
                    try:
                        decrypted_data = self.master_crypter.decrypt(encrypted_data)
                        storage_data = pickle.loads(decrypted_data)
                    except Exception:
                        storage_data = {}
            
            storage_data[element_name] = element_data
            
            return self.save(storage_data)
            
        except Exception as e:
            raise EncryptedStorageBackendError(f"Failed to write element {element_name}: {e}")
    
    def delete_element(self, element_name: str) -> bool:
        try:
            if not self.storage_path.exists():
                return True
            
            with self.storage_path.open("rb") as f:
                encrypted_data = f.read()
            
            if not encrypted_data or len(encrypted_data) < 16:
                return True
            
            try:
                decrypted_data = self.master_crypter.decrypt(encrypted_data)
                storage_data = pickle.loads(decrypted_data)
                
                if element_name not in storage_data:
                    return True
                
                del storage_data[element_name]
                
                self._remove_key_for_element(element_name)
                
                return self.save(storage_data)
            except Exception:
                return True
            
        except Exception as e:
            raise EncryptedStorageBackendError(f"Failed to delete element {element_name}: {e}")
    
    def list_elements(self) -> list:
        try:
            if not self.storage_path.exists():
                return []
            
            with self.storage_path.open("rb") as f:
                encrypted_data = f.read()
            
            if not encrypted_data or len(encrypted_data) < 16:
                return []
            
            try:
                decrypted_data = self.master_crypter.decrypt(encrypted_data)
                storage_data = pickle.loads(decrypted_data)
                
                return [key for key in storage_data.keys() if key != "_stg_keys"]
            except Exception:
                return []
            
        except Exception as e:
            raise EncryptedStorageBackendError(f"Failed to list elements: {e}")
    
    def element_exists(self, element_name: str) -> bool:
        try:
            if not self.storage_path.exists():
                return False
            
            with self.storage_path.open("rb") as f:
                encrypted_data = f.read()
            
            if not encrypted_data or len(encrypted_data) < 16:
                return False
            
            try:
                decrypted_data = self.master_crypter.decrypt(encrypted_data)
                storage_data = pickle.loads(decrypted_data)
                
                return element_name in storage_data
            except Exception:
                return False
            
        except Exception as e:
            raise EncryptedStorageBackendError(f"Failed to check element existence: {e}")


class StorageError(Exception):
    pass
