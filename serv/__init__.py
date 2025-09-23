from .core import ServiceCore


def run_repl(args):
    repl = ServiceCore(args=args)
    repl.loop()
