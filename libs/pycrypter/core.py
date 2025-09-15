import hashlib
import pickle
import os

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

from .exceptions import DecryptFileError, EncryptFileError


class Crypter:
    def __init__(self, key: str | bytes):
        self.key = self.format_key(key)
        self._backend = default_backend()

    @staticmethod
    def format_key(key: str | bytes) -> bytes:
        if isinstance(key, bytes):
            return hashlib.sha256(key).digest()
        elif isinstance(key, str):
            return hashlib.sha256(key.encode()).digest()
        raise ValueError("Key must be string or bytes")

    def encrypt(self, data: bytes) -> bytes:
        iv = os.urandom(12)
        cipher = Cipher(algorithms.AES(self.key), modes.GCM(iv), backend=self._backend)

        encryptor = cipher.encryptor()
        encrypted_data = encryptor.update(data) + encryptor.finalize()

        return iv + encrypted_data + encryptor.tag

    def decrypt(self, data: bytes) -> bytes:
        if len(data) < 28:
            raise DecryptFileError("Invalid encrypted data: too short")

        iv = data[:12]
        tag = data[-16:]
        encrypted_data = data[12:-16]

        cipher = Cipher(algorithms.AES(self.key), modes.GCM(iv, tag), backend=self._backend)
        decryptor = cipher.decryptor()
        
        return decryptor.update(encrypted_data) + decryptor.finalize()


class CryptedFile:
    def __init__(self, filename: str, key: str | bytes):
        self.filename = str(filename)
        self.crypter = Crypter(key=key)

    def read(self):
        try:
            with open(self.filename, "rb") as file:
                encrypted_data = file.read()
                if not encrypted_data:
                    raise DecryptFileError("File is empty")
                decrypted_data = self.crypter.decrypt(encrypted_data)
                return pickle.loads(decrypted_data)
        except (pickle.UnpicklingError, UnicodeDecodeError, EOFError) as e:
            raise DecryptFileError from e

    def write(self, new_data: object):
        if new_data is None:
            raise EncryptFileError("None is not writeable.")

        try:
            serialized_data = pickle.dumps(new_data)
            encrypted_data = self.crypter.encrypt(serialized_data)
            with open(self.filename, "wb") as file:
                file.write(encrypted_data)
        except (TypeError, pickle.PicklingError) as e:
            raise EncryptFileError from e
