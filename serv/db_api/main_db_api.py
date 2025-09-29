from configparser import ConfigParser

from .databaser import PDB


class MainAppDatabaseAPI:
    def __init__(self, app_conf: ConfigParser):
            self.db = PDB()
