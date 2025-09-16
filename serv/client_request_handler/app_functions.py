import hashlib
import json
import logging

from pathlib import Path

from ..db_api import MainApplicationStorage


exit_codes_file = str(Path(__file__).resolve().parent) + "/transactions_exit_codes.json"
exit_codes = json.loads(open(file=exit_codes_file, mode="r", encoding="UTF-8").read())


def reg_account(stg: MainApplicationStorage, username: str, password: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = str(username)
    password = str(password)

    if stg.check_user_is_exist(username=username):
        _exit_code_s = "username_already_used"

    if not _exit_code_s:
        try:
            password_hash = hashlib.sha256(str(password).encode()).hexdigest()
            stg.make_user_r(username=str(username), password_hash=password_hash)
            _exit_code_s = "ok"

        except Exception as err:
            _exit_code_s = "server_other_error"
            logging.error(err)

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "REG_ACCOUNT:RESPONSE"


def login(stg: MainApplicationStorage, username: str, password: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = str(username)
    password = str(password)

    if not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        if not stg.check_user_passwd(username=username, password_hash=password_hash):
            _exit_code_s = "invalid_password"
        else:
            try:
                token = stg.generate_token(username=username, password_hash=password_hash)
                if not token:
                    _exit_code_s = "invalid_credentials"
                else:
                    _exit_code_s = "ok"
                    result = next(item for item in exit_codes if item[0] == _exit_code_s)
                    return json.dumps((result, {"token": token})).encode(), "LOGIN:RESPONSE"
            except Exception as err:
                _exit_code_s = "server_other_error"
                logging.error(err)

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "LOGIN:RESPONSE"


def change_username(stg: MainApplicationStorage, token: str, new_username: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if not username:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"
    elif stg.check_user_is_exist(username=new_username):
        _exit_code_s = "username_already_used"

    if not _exit_code_s:
        try:
            if stg.change_username(old_username=username, new_username=new_username):
                _exit_code_s = "ok"
            else:
                _exit_code_s = "server_other_error"
        except Exception as e:
            logging.error(f"Nickname change failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "CHANGE_NICKNAME:RESPONSE"


def change_password(stg: MainApplicationStorage, token: str, old_password: str, new_password: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            old_password_hash = hashlib.sha256(old_password.encode()).hexdigest()
            if not stg.check_user_passwd(username=username, password_hash=old_password_hash):
                _exit_code_s = "invalid_password"
            else:
                new_password_hash = hashlib.sha256(new_password.encode()).hexdigest()
                if stg.change_password(username=username, new_password_hash=new_password_hash):
                    _exit_code_s = "ok"
                else:
                    _exit_code_s = "server_other_error"
        except Exception as e:
            logging.error(f"Password change failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "CHANGE_PASSWORD:RESPONSE"


def create_chat(stg: MainApplicationStorage, token: str, participants: list[str], name: str = None) -> tuple[
    bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"

    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            for participant in participants:
                if not stg.check_user_is_exist(participant):
                    _exit_code_s = "invalid_participant"
                    break
            if not _exit_code_s:
                chat_id = stg.create_chat(username, participants, name)
                _exit_code_s = "ok"
                result = next(item for item in exit_codes if item[0] == _exit_code_s)
                return json.dumps((result, {"chat_id": chat_id})).encode(), "CREATE_CHAT:RESPONSE"
        except Exception as e:
            logging.error(f"Chat creation failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "CREATE_CHAT:RESPONSE"


def delete_chat(stg: MainApplicationStorage, token: str, chat_id: int) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"

    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            chat = stg.get_chat_by_id(chat_id, username)
            if not chat:
                _exit_code_s = "chat_not_found"
            else:
                if stg.delete_chat(chat_id):
                    _exit_code_s = "ok"
                else:
                    _exit_code_s = "chat_not_found"

        except Exception as e:
            logging.error(f"Chat deletion failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "DELETE_CHAT:RESPONSE"


def add_participant_to_chat(stg: MainApplicationStorage, token: str, chat_id: int, username_to_add: str) -> tuple[
    bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"

    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            chat = stg.get_chat_by_id(chat_id, username)
            if not chat:
                _exit_code_s = "chat_not_found"
            elif not chat.get("is_owner", False):
                _exit_code_s = "not_chat_owner"
            elif not stg.check_user_is_exist(username_to_add):
                _exit_code_s = "invalid_participant"
            else:
                if stg.add_participant_to_chat(chat_id, username_to_add):
                    _exit_code_s = "ok"
                else:
                    _exit_code_s = "chat_not_found"

        except Exception as e:
            logging.error(f"Add participant failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "ADD_PARTICIPANT_TO_CHAT:RESPONSE"


def remove_participant_from_chat(stg: MainApplicationStorage, token: str, chat_id: int, username_to_remove: str) -> \
tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            chat = stg.get_chat_by_id(chat_id, username)
            if not chat:
                _exit_code_s = "chat_not_found"
            elif not chat.get("is_owner", False):
                _exit_code_s = "not_chat_owner"
            elif not stg.check_user_is_exist(username_to_remove):
                _exit_code_s = "invalid_participant"
            else:
                if stg.remove_participant_from_chat(chat_id, username_to_remove):
                    _exit_code_s = "ok"
                else:
                    _exit_code_s = "chat_not_found"
        except Exception as e:
            logging.error(f"Remove participant failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "REMOVE_PARTICIPANT_FROM_CHAT:RESPONSE"


def get_chat_by_id(stg: MainApplicationStorage, token: str, chat_id: int) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            chat = stg.get_chat_by_id(chat_id, username)
            if not chat:
                _exit_code_s = "chat_not_found"
            else:
                _exit_code_s = "ok"
                result = next(item for item in exit_codes if item[0] == _exit_code_s)
                return json.dumps((result, chat)).encode(), "GET_CHAT_BY_ID:RESPONSE"

        except Exception as e:
            logging.error(f"Chat retrieval failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "GET_CHAT_BY_ID:RESPONSE"


def get_user_chats(stg: MainApplicationStorage, token: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            user_chats = stg.get_user_chats(username=username)
            _exit_code_s = "ok"
            result = next(item for item in exit_codes if item[0] == _exit_code_s)
            return json.dumps((result, {"chats": user_chats})).encode(), "GET_USER_CHATS:RESPONSE"
        except Exception as e:
            logging.error(f"User chats retrieval failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "GET_USER_CHATS:RESPONSE"


def change_chat_name(stg: MainApplicationStorage, token: str, chat_id: int, new_name: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"
    elif not new_name or new_name.strip() == "":
        _exit_code_s = "server_other_error"

    if not _exit_code_s:
        try:
            chat = stg.get_chat_by_id(chat_id, username)
            if not chat:
                _exit_code_s = "chat_not_found"
            else:
                if stg.change_chat_name(chat_id, new_name):
                    _exit_code_s = "ok"
                else:
                    _exit_code_s = "chat_not_found"

        except Exception as e:
            logging.error(f"Chat name change failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "CHANGE_CHAT_NAME:RESPONSE"


def delete_message(stg: MainApplicationStorage, token: str, m_id: int) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)
    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            if stg.delete_message(m_id, username):
                _exit_code_s = "ok"
            else:
                _exit_code_s = "message_not_found_or_not_owner"
        except Exception as e:
            logging.error(f"Message deletion failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "DELETE_MESSAGE:RESPONSE"


def edit_message(stg: MainApplicationStorage, token: str, m_id: int, new_payload: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)
    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            if stg.edit_message(m_id, username, new_payload):
                _exit_code_s = "ok"
            else:
                _exit_code_s = "message_not_found_or_not_owner"
        except Exception as e:
            logging.error(f"Message edit failed: {str(e)}")
            _exit_code_s = "message_encryption_filed"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "EDIT_MESSAGE:RESPONSE"


def send_message(stg: MainApplicationStorage, token: str, chat_id: int, payload: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            stg.make_message_r(username=username, payload=payload, chat_id=chat_id)
            _exit_code_s = "ok"

        except ValueError as e:
            logging.error(f"Message sending failed: {str(e)}")
            _exit_code_s = "invalid_chat_id"

        except Exception as e:
            logging.error(f"Message encryption failed: {str(e)}")
            _exit_code_s = "message_encryption_filed"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "SEND_MESSAGE_TOKEN:RESPONSE"


def read_messages(stg: MainApplicationStorage, token: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)
    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            messages = stg.get_messages_own_user(username=username)
            messages_serializable = []
            for msg in messages:
                m_id, payload_bytes, sender, chat_id, chat_name, created_at = msg
                messages_serializable.append([
                    m_id,
                    payload_bytes.decode() if isinstance(payload_bytes, bytes) else str(payload_bytes),
                    sender,
                    chat_id,
                    chat_name,
                    created_at
                ])

            _exit_code_s = "ok"
            result = next(item for item in exit_codes if item[0] == _exit_code_s)
            return (json.dumps((result, messages_serializable)).encode(),
                    "READ_MESSAGES_TOKEN:RESPONSE")
        except Exception as e:
            logging.error(f"Message retrieval failed: {str(e)}")
            _exit_code_s = "message_decryption_filed"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "READ_MESSAGES_TOKEN:RESPONSE"


def verify_token(stg: MainApplicationStorage, token: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username):
        _exit_code_s = "account_not_found"
    else:
        _exit_code_s = "ok"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps(
        (result, {"username": username} if _exit_code_s == "ok" else None)).encode(), "VERIFY_TOKEN:RESPONSE"


def delete_token(stg: MainApplicationStorage, token: str, r_token_id: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            if stg.delete_token_by_id(r_token_id, username):
                _exit_code_s = "ok"
            else:
                _exit_code_s = "token_not_owner"
        except Exception as e:
            logging.error(f"Token deletion failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "DELETE_TOKEN:RESPONSE"


def get_user_tokens(stg: MainApplicationStorage, token: str) -> tuple[bytes, str]:
    _exit_code_s: str | None = None
    username = stg.validate_token(token)

    if username is None:
        _exit_code_s = "invalid_token"
    elif not stg.check_user_is_exist(username=username):
        _exit_code_s = "account_not_found"

    if not _exit_code_s:
        try:
            user_tokens = stg.get_user_tokens(username=username)
            _exit_code_s = "ok"
            result = next(item for item in exit_codes if item[0] == _exit_code_s)
            return json.dumps((result, {"tokens": user_tokens})).encode(), "GET_USER_TOKENS:RESPONSE"
        except Exception as e:
            logging.error(f"Token retrieval failed: {str(e)}")
            _exit_code_s = "server_other_error"

    result = next(item for item in exit_codes if item[0] == _exit_code_s)
    return json.dumps((result, None)).encode(), "GET_USER_TOKENS:RESPONSE"
