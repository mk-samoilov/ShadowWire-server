from importlib import import_module
from pathlib import Path

import json

from ..db_api import MainAppDatabaseAPI


def add_request_uuid_to_response(response_data: bytes, request_uuid: str = None) -> bytes:
    if not request_uuid:
        return response_data

    try:
        response_json = json.loads(response_data.decode(encoding="utf-8"))
        if isinstance(response_json, tuple) and len(response_json) == 2:
            result, data = response_json
            if data is None:
                data = {}
            elif not isinstance(data, dict):
                data = {"data": data}

            data["request_uuid"] = request_uuid
            return json.dumps((result, data)).encode(encoding="utf-8")
        else:
            return response_data

    except (json.JSONDecodeError, UnicodeDecodeError, UnicodeEncodeError):
        return response_data


def cr_handler(transaction_code: str, pkg: bytes, db_api: MainAppDatabaseAPI) -> (bytes, str):
    request_uuid = None

    try:
        pkg_data = json.loads(pkg.decode(encoding="utf-8"))
        if isinstance(pkg_data, dict) and "request_uuid" in pkg_data:
            request_uuid = pkg_data.pop("request_uuid")
    except (json.JSONDecodeError, UnicodeDecodeError):
        pkg_data = {}

    if transaction_code == "CONNECTION_TEST":
        original_pkg = pkg.decode(encoding="utf-8")
        response_data = original_pkg.encode(encoding="utf-8")

        return add_request_uuid_to_response(response_data, request_uuid), "CONNECTION_TEST:RESPONSE"
    else:
        if pkg_data is None:
            pkg_data = {}

        try:
            module = import_module(name=".app_functions", package=__package__)
            response_data, response_type = getattr(module, transaction_code.lower())(db_api=db_api, **pkg_data)
            return add_request_uuid_to_response(response_data, request_uuid), response_type

        except AttributeError:
            data_dir = str(Path(__file__).resolve().parent.parent.parent) + "/data"
            error_codes_file = data_dir + "/fuh_exit_codes.json"
            error_codes = json.loads(open(file=error_codes_file, mode="r", encoding="UTF-8").read())

            result = next(item for item in error_codes if item[0] == "invalid_transaction_code")
            response_data = json.dumps((result, None)).encode()

            return add_request_uuid_to_response(response_data, request_uuid), "ERROR:RESPONSE"
