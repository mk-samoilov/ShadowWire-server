import logging
import os
import signal

from datetime import datetime
from importlib import import_module
from pathlib import Path

from .tcp_server import TCPServer

from .client_request_handler.cr_handler import cr_handler as crh
from .config_parser import load_config
from .db_api import MainAppDatabaseAPI


class ServiceCore:
    DATA_DIR = str(Path(__file__).resolve().parent.parent) + "/data"

    DEFAULT_CONFIG_FILE = DATA_DIR + "/app_config.conf"

    VERSION_FILE = DATA_DIR + "/vers/server_version"
    CRYPT_TCP_PROTOCOL_VERSION_FILE = DATA_DIR + "/vers/crypt_tcp_protocol_version"
    CRYPT_DB_PROTOCOL_VERSION_FILE = DATA_DIR + "/vers/crypt_db_protocol_version"

    def __init__(self, args):
        self.args = args
        self.conf = load_config(file=args.config if args.config else self.DEFAULT_CONFIG_FILE)

        self._make_dirs()
        self.__setup_logging__()

        self.version, self.crypt_tcp_protocol_version, self.crypt_db_protocol_version = self.load_version()

        logging.info(f"Service core initialized [{self.version} " + \
                     f"tcp_p{self.crypt_tcp_protocol_version} db_p{self.crypt_db_protocol_version}]")

        self.c_tcp_serv = TCPServer(conf=self.conf, request_handle_func=crh)

        self.__setup_db__()
        self.__setup_signal_handlers__()
        self._stopping = False

    def load_version(self) -> (str, int, int):
        with open(file=self.VERSION_FILE, mode="r", encoding="UTF-8") as vers_file:
            vers = str(vers_file.readline(0))

        with open(file=self.CRYPT_TCP_PROTOCOL_VERSION_FILE, mode="r", encoding="UTF-8") as proto_vers_file_tcp:
            crypt_tcp_proto_vers = int(proto_vers_file_tcp.readline(0))

        with open(file=self.CRYPT_DB_PROTOCOL_VERSION_FILE, mode="r", encoding="UTF-8") as proto_vers_file_db:
            crypt_db_proto_vers = int(proto_vers_file_db.readline(0))

        return vers, crypt_tcp_proto_vers, crypt_db_proto_vers

    def _make_dirs(self):
        os.makedirs(self.conf["paths"]["logs_dir"], exist_ok=True)
        os.makedirs(self.conf["paths"]["plugins_dir"], exist_ok=True)

    def __setup_logging__(self):
        log_filename = f"{self.conf['paths']['logs_dir']}/log_{datetime.now().strftime('%Y-%m-%d')}.log"

        logging.basicConfig(
            level=getattr(import_module("logging"), self.conf["logging"]["level"]),
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_filename, encoding="utf-8", mode="a"),
                logging.StreamHandler()
            ]
        )

    def __setup_db__(self):
        self.db_api = MainAppDatabaseAPI()

    def _define_cr_server(self):
        def request_handler_constructor(transaction_code, pkg):
            return crh(transaction_code=transaction_code, pkg=pkg, db_api=self.db_api)

        self.c_tcp_serv.request_handle_func = request_handler_constructor
        self.c_tcp_serv.title_ = "CRH"

    def _start(self):
        self._define_cr_server()
        self.c_tcp_serv.main()

    def _stop(self):
        if not self._stopping:
            self._stopping = True
            self.c_tcp_serv.stop()

    def __setup_signal_handlers__(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, _frame):
        logging.info(f"Received signal {signum}, shutting down gracefully...")
        self._stop()

    def loop(self):
        try:
            self._start()
        except KeyboardInterrupt:
            logging.info("Received KeyboardInterrupt, shutting down gracefully...")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
        finally:
            self._stop()
