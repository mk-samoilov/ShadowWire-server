import argparse

from . import run_repl


def starter():
    arg_parser = argparse.ArgumentParser(description="shadow_wire.server")

    arg_parser.add_argument(
        "-c", "--config",
        type=str,
        help="Path to the configuration file")

    arg_parser.add_argument(
        "-i", "--gen_conf_file",
        action="store_true",
        help="If set, the server won't start — it will just generate a basic config file.")

    args = arg_parser.parse_args()
    run_repl(args=args)


if __name__ == "__main__":
    starter()
