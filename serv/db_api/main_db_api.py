from configparser import ConfigParser
from pathlib import Path

import logging
import os

from libs.pycrypter import gen_key

from .databaser import PDB


def build_conf_of_pdb(app_conf: ConfigParser):
    db_conf = app_conf["db"]

    user = db_conf["role"]
    user_password = db_conf["role_passwd"]
    host = db_conf["db_host"]
    port = db_conf["db_port"]
    database_name = db_conf["db_name"]

    return None, host, port, database_name, user, user_password, None, False


class MainAppDatabaseAPI:
    KEYS_PATH = str(Path(__file__).resolve().parent.parent.parent) + "/data/keys"

    KEYS_TO_MAKE = [
        "crypt_accounts_key.bin",
        "crypt_messages_key.bin"
    ]

    def __init__(self, app_conf: ConfigParser):
        self.db = PDB(*build_conf_of_pdb(app_conf=app_conf))

        self.db.init_schema()

        self._make_keys()

        logging.info(f"Database '{build_conf_of_pdb(app_conf=app_conf)[3]}' initialized successfully by role " + \
                     build_conf_of_pdb(app_conf=app_conf)[4])

    def _make_keys(self):
        for key_ in self.KEYS_TO_MAKE:
            key_path = self.KEYS_PATH + f"/{key_}"
            if not os.path.exists(key_path):
                with open(file=key_path, mode="wb") as key_file:
                    key_file.write(gen_key(len_=512))
            else:
                with open(file=key_path, mode="rb") as key_file:
                    key_data = key_file.read()
                    if len(key_data) < 32:
                        logging.warning(f"Key file {key_} is too short, regenerating...")
                        with open(file=key_path, mode="wb") as key_file:
                            key_file.write(gen_key(len_=512))
