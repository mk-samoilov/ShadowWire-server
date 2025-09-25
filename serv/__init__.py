from .config_parser import gen_config_util


def run_repl(args):
    if args.gen_conf_file:
        gen_config_util(args=args)

