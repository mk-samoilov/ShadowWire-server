import threading
import logging

from typing import Optional
from queue import Queue, Empty

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import dh
from cryptography.hazmat.backends import default_backend


class DHParameterCache:
    def __init__(self, key_size: int = 512, pool_size: int = 128):
        self.key_size = key_size
        self.pool_size = pool_size
        self._parameters: Optional[dh.DHParameters] = None
        self._private_keys_pool: Queue = Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._initialized = False

        self._initialize()
    
    def _initialize(self):
        if self._initialized:
            return
            
        with self._lock:
            if self._initialized:
                return
                
            try:
                logging.info(f"Initialization of DH parameters with a key size of {self.key_size} bits...")

                self._parameters = dh.generate_parameters(generator=2, key_size=self.key_size)

                logging.debug(f"Generating pool of {self.pool_size} private keys...")
                for _ in range(self.pool_size):
                    private_key = self._parameters.generate_private_key()
                    self._private_keys_pool.put(private_key)
                
                self._initialized = True
                logging.debug("DH parameters and key pool initialized successfully")
                
            except Exception as e:
                logging.error(f"Error creating DH parameters: {e}")
                raise
    
    def get_parameters(self) -> dh.DHParameters:
        if not self._initialized:
            self._initialize()
        return self._parameters
    
    def get_private_key(self) -> dh.DHPrivateKey:
        if not self._initialized:
            self._initialize()
            
        try:
            return self._private_keys_pool.get_nowait()
        except Empty:
            logging.warning("Private key pool is empty, generate new key")
            return self._parameters.generate_private_key()
    
    def return_private_key(self, private_key: dh.DHPrivateKey):
        try:
            self._private_keys_pool.put_nowait(private_key)
        except:
            pass
    
    def get_parameter_numbers(self):
        return self.get_parameters().parameter_numbers()


class OptimizedDHKeyExchange:
    def __init__(self, key_size: int = 512, pool_size: int = 100):
        self.cache = DHParameterCache(key_size, pool_size)
        self._hash_algorithm = hashes.BLAKE2b(64)
        
    def generate_session_key(self, shared_key: bytes) -> bytes:
        digest = hashes.Hash(self._hash_algorithm, backend=default_backend())
        digest.update(shared_key)

        return digest.finalize()[:32]
    
    def get_parameters_for_client(self) -> tuple[bytes, bytes]:
        pn = self.cache.get_parameter_numbers()
        
        p_bytes = pn.p.to_bytes((pn.p.bit_length() + 7) // 8, byteorder="big")
        g_bytes = pn.g.to_bytes((pn.g.bit_length() + 7) // 8, byteorder="big")
        
        return p_bytes, g_bytes
    
    def create_server_keypair(self) -> tuple[dh.DHPrivateKey, bytes]:
        private_key = self.cache.get_private_key()
        public_key = private_key.public_key()
        
        public_bytes = public_key.public_numbers().y.to_bytes(
            (public_key.key_size + 7) // 8, byteorder="big"
        )
        
        return private_key, public_bytes
    
    def derive_shared_key(self, server_private_key: dh.DHPrivateKey, 
                         client_public_y: int, pn) -> bytes:
        client_public_key = dh.DHPublicNumbers(client_public_y, pn).public_key()
        shared_key = server_private_key.exchange(client_public_key)

        return self.generate_session_key(shared_key)
    
    def cleanup_private_key(self, private_key: dh.DHPrivateKey):
        self.cache.return_private_key(private_key)


_global_dh_exchange: Optional[OptimizedDHKeyExchange] = None
_global_lock = threading.Lock()


def get_dh_exchange(key_size: int = 512, pool_size: int = 100) -> OptimizedDHKeyExchange:
    global _global_dh_exchange
    
    with _global_lock:
        if _global_dh_exchange is None:
            _global_dh_exchange = OptimizedDHKeyExchange(key_size, pool_size)
        return _global_dh_exchange
