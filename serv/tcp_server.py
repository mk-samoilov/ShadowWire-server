import socket
import threading
import logging
import struct

from configparser import ConfigParser
from typing import List, Tuple, Callable

from .dh_optimizer import get_dh_exchange
from libs.pycrypter import Crypter


class ClientConnection:
    def __init__(self, client_socket, client_address, server_instance):
        self.client_socket = client_socket
        self.client_address = client_address
        self.server_instance = server_instance
        self.request_handle_func = self.server_instance.request_handle_func

        self.running = True
        self.crypter = None
        self.dh_exchange = get_dh_exchange(key_size=512, pool_size=128)

    def _recv_exact(self, num_bytes: int) -> bytes:
        chunks = []
        received = 0
        while received < num_bytes:
            chunk = self.client_socket.recv(num_bytes - received)
            if not chunk:
                return b""

            chunks.append(chunk)
            received += len(chunk)

        return b"".join(chunks)

    def __init_session__(self):
        try:
            p_bytes, g_bytes = self.dh_exchange.get_parameters_for_client()

            self.client_socket.sendall(struct.pack("!I", len(p_bytes)) + p_bytes)
            self.client_socket.sendall(struct.pack("!I", len(g_bytes)) + g_bytes)

            server_private_key, server_public_bytes = self.dh_exchange.create_server_keypair()

            self.client_socket.sendall(struct.pack("!I", len(server_public_bytes)) + server_public_bytes)

            client_public_length_bytes = self._recv_exact(4)
            if not client_public_length_bytes:
                raise ConnectionResetError("Client closed connection while sending public key length")
            client_public_length = struct.unpack("!I", client_public_length_bytes)[0]
            client_public_bytes = self._recv_exact(client_public_length)
            if not client_public_bytes or len(client_public_bytes) != client_public_length:
                raise ConnectionResetError("Client closed connection while sending public key bytes")
            client_public_y = int.from_bytes(client_public_bytes, byteorder="big")

            pn = self.dh_exchange.cache.get_parameter_numbers()

            self.session_key = self.dh_exchange.derive_shared_key(
                server_private_key, client_public_y, pn
            )

            self.crypter = Crypter(self.session_key)

            self.dh_exchange.cleanup_private_key(server_private_key)

            logging.debug(
                f"{'[Server ' + self.server_instance.title_ + '] - ' if self.server_instance.title_ else ''}" + \
                f"Server session key for {self.client_address[0]}: {self.session_key.hex()}")

        except ConnectionResetError:
            self.stop()
        except Exception:
            logging.exception(
                f"{'[Server ' + self.server_instance.title_ + '] - ' if self.server_instance.title_ else ''}" + \
                f"Key exchange failed for client {self.client_address[0]}:")
            raise

    def handle(self):
        try:
            self.__init_session__()

            while self.running:
                try:
                    length_bytes = self._recv_exact(4)
                    if not length_bytes:
                        break
                    pkg_length = struct.unpack("!I", length_bytes)[0]

                    trans_length_bytes = self._recv_exact(4)
                    if not trans_length_bytes:
                        break
                    trans_length = struct.unpack("!I", trans_length_bytes)[0]

                    encrypted_trans_code = self._recv_exact(trans_length)
                    if not encrypted_trans_code or len(encrypted_trans_code) != trans_length:
                        break

                    encrypted_data = self._recv_exact(pkg_length)
                    if not encrypted_data or len(encrypted_data) != pkg_length:
                        break

                    transaction_code = self.crypter.decrypt(encrypted_trans_code).decode("utf-8")
                    data = self.crypter.decrypt(encrypted_data)

                    logging.info(
                        f"{'[Server ' + self.server_instance.title_ + '] - ' if self.server_instance.title_ else ''}" + \
                        f"Client {self.client_address[0]} sent package of code '{transaction_code}'")
                    self.process_request(data, transaction_code)

                except socket.timeout:
                    continue

                except ConnectionResetError:
                    break

                except Exception:
                    if self.running:
                        logging.exception(f"Error handling client {self.client_address[0]}")
                    break

        except Exception:
            logging.exception(f"Error in client handler for {self.client_address[0]}")
        finally:
            self.close_connection()

    def process_request(self, data: bytes, transaction_code: str):
        r_data, r_trans = self.request_handle_func(pkg=data, transaction_code=transaction_code)
        self.send_pkg(pkg=r_data, transaction_code=r_trans)

    def send_pkg(self, pkg: bytes, transaction_code: str):
        max_retries = 3
        for attempt in range(max_retries):
            trans_code_bytes = transaction_code.encode("utf-8")
            encrypted_trans_code = self.crypter.encrypt(trans_code_bytes)
            encrypted_pkg = self.crypter.encrypt(pkg)
            pkg_data = (
                    struct.pack("!I", len(encrypted_pkg)) +
                    struct.pack("!I", len(encrypted_trans_code)) +
                    encrypted_trans_code +
                    encrypted_pkg
            )
            self.client_socket.sendall(pkg_data)
            return

    def close_connection(self):
        try:
            self.client_socket.close()
            logging.info(
                f"{'[Server ' + self.server_instance.title_ + '] - ' if self.server_instance.title_ else ''}" + \
                f"Client {self.client_address[0]} disconnected")

        except Exception:
            logging.exception(
                f"{'[Server ' + self.server_instance.title_ + '] - ' if self.server_instance.title_ else ''}" + \
                f"Error closing connection for client {self.client_address[0]}:")

    def stop(self):
        self.running = False
        try:
            if hasattr(self, 'client_socket') and self.client_socket:
                self.client_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass


class TCPServer:
    def __init__(self, conf: ConfigParser, request_handle_func: Callable, title_: str | None = None):
        self.conf = conf
        self.request_handle_func = request_handle_func

        self.title_ = title_

        self.handling = False
        self.clients: List[Tuple[ClientConnection, threading.Thread]] = []

    def _bind_socket(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.conf["client_tcp_endpoint"]["host"], self.conf["client_tcp_endpoint"].getint("port")))
        self.socket.settimeout(1.0)

        self.handling = True

    def main(self):
        self._bind_socket()

        try:
            self.socket.listen(self.conf["client_tcp_endpoint"].getint("max_available_connections"))
            logging.info(
                f"{'Server ' + self.title_ + ' ' if self.title_ else ''}Endpoint started on {
                self.conf["client_tcp_endpoint"]["host"]
                }:{self.conf["client_tcp_endpoint"]["port"]}"
            )

            while self.handling:
                self.main_loop()

        except Exception:
            logging.exception(
                f"{'[Server ' + self.title_ + '] - ' if self.title_ else ''}Error in main server loop:")

    def main_loop(self):
        self.clients = [(conn, th) for conn, th in self.clients if conn.running]

        for conn, th in self.clients:
            if not conn.running and th.is_alive():
                th.join(timeout=1.0)

        try:
            client_socket, client_address = self.socket.accept()
            logging.info(
                f"{'[Server ' + self.title_ + '] - ' if self.title_ else ''}" + \
                f"Connected client from {client_address[0]}")
            c_handler = ClientConnection(client_socket, client_address, self)
            client_thread = threading.Thread(target=c_handler.handle)
            client_thread.start()
            self.clients.append((c_handler, client_thread))

        except socket.timeout:
            pass

        except Exception:
            if self.handling:
                logging.exception(
                    f"Server '{self.title_ + '\' ' if self.title_ else ' '}- Error accepting client connection:")

    def stop(self):
        logging.info(f"Stopping server '{self.title_ + '\' ' if self.title_ else ''}...")
        self.handling = False

        for handler, thread in self.clients:
            handler.stop()
            if thread.is_alive():
                thread.join(timeout=5.0)

        if hasattr(self, "socket") and self.socket:
            try:
                self.socket.close()
            except Exception:
                logging.exception(f"Error closing server '{self.title_ + ' ' if self.title_ else ''} socket:")

        logging.info(f"Server '{self.title_ + '\' ' if self.title_ else ''}shut down")
