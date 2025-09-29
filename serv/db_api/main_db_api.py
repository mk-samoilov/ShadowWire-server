from configparser import ConfigParser

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
    def __init__(self, app_conf: ConfigParser):
        self.db = PDB(*build_conf_of_pdb(app_conf=app_conf))
