from pathlib import Path

import configparser
import os


CORE_DIR_PATH = Path(__file__).resolve().parent


def load_config(file: str):
    config = configparser.ConfigParser()

    if not os.path.exists(file):
        config["paths"] = \
            {
                "logs_dir": str(CORE_DIR_PATH.parent) + "/logs",
                "plugins_dir": str(CORE_DIR_PATH) + "/plugins"
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
