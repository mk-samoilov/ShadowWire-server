from pathlib import Path

import configparser
import os


CORE_DIR = Path(__file__).resolve().parent

DATA_DIR = str(Path(__file__).resolve().parent.parent) + "/data"

DEFAULT_CONFIG_FILE = DATA_DIR + "/app_config.conf"


def load_config(file: str):
    config = configparser.ConfigParser()

    if not os.path.exists(file):
        config["paths"] = \
            {
                "logs_dir": str(CORE_DIR.parent) + "/logs",
                "plugins_dir": str(CORE_DIR) + "/plugins"
            }

        config["db"] = \
            {
                "role": "< change these field in config file>",
                "role_passwd": "< change these field in config file >",
                "db_host": "localhost",
                "db_port": 5432,
                "db_name": "shadow_wire_db"
            }

        config["logging"] = \
            {
                "level": "DEBUG"
            }

        config["client_tcp_endpoint"] = \
            {
                "host": "0.0.0.0",
                "port": "5477",
                "max_available_connections": 950
            }

        with open(file=file, mode="w", encoding="UTF-8") as configfile:
            config.write(fp=configfile)
    else:
        config.read(file)

    return config


def gen_config_util(args):
    config_file_path = args.config if args.config else DEFAULT_CONFIG_FILE

    if not os.path.exists(config_file_path):
        load_config(file=config_file_path)
        print("configuration file generated")

    else:
        print("error: configuration file already exists")
