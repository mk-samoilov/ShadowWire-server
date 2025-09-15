import secrets


def gen_key(len_: int = 256) -> bytes:
    return secrets.token_bytes(int(len_))
