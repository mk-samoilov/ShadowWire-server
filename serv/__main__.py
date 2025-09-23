import argparse

from . import run_repl


def starter():
    arg_parser = argparse.ArgumentParser(description="m_crypted:server")

    arg_parser.add_argument(
        "-c", "--config",
        type=str,
        help="Path to the configuration file")

    args = arg_parser.parse_args()
    run_repl(args=args)


if __name__ == "__main__":
    starter()
