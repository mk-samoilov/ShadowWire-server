from datetime import datetime
from pathlib import Path

import os
import logging
import uuid

from libs.pycrypter import Crypter, gen_key
from libs.pystorage import *

from libs.encrypted_storage_be_addon import EncryptedStorageBackend


class MainApplicationStorage:
    KEYS_PATH = str(Path(__file__).resolve().parent.parent.parent) + "/data/keys"

    KEYS_TO_MAKE = [
        "crypt_messages_key.bin"
    ]

    TABLES = [
        {"name": "accounts_table", "schema": {"account_id": int, "username": str, "password_hash": str,
                                              "verify_token": str}},
        {"name": "messages_table", "schema": {"message_id": int, "chat_uuid": str, "sender": str, "percipient": str,
                                              "payload": bytes, "pds": tuple[bool, bool], "created_at": str}}
    ]

    def __init__(self, storage_path: str):
        self.storage = Storage(
            storage_name="MAS",
            storage_path=str(storage_path),
            backend=EncryptedStorageBackend
        )

        self._make_keys()
        self._make_tables()

        logging.info("Storage 'MAS' initialized")

    def _make_keys(self):
        for key_ in self.KEYS_TO_MAKE:
            key_path = self.KEYS_PATH + f"/{key_}"
            if not os.path.exists(key_path):
                with open(file=key_path, mode="wb") as key_file:
                    key_file.write(gen_key(len_=512))
            else:
                with open(file=key_path, mode="rb") as key_file:
                    key_data = key_file.read()
                    if len(key_data) < 32:
                        logging.warning(f"Key file {key_} is too short, regenerating...")
                        with open(file=key_path, mode="wb") as key_file:
                            key_file.write(gen_key(len_=512))

    def _make_tables(self):
        for table_ in self.TABLES:
            self.storage.write(table_["name"], Table(data=[], schema=table_["schema"]), True)

    def check_user_is_exist(self, username: str):
        table_ = self.storage.read(key="users_table", item_type=Table)
        if table_ is None:
            return False
        for r in table_.dataframe.to_dict("records"):
            if r["username"] == str(username): return True
        return False

    @staticmethod
    def _get_record_id(table_: Table, id_key: str):
        if table_ is None:
            return 0
        try:
            last_record = table_.dataframe.to_dict("records")[-1]
            if str(id_key) not in last_record:
                last_record_id = 0
            else:
                last_record_id = last_record[str(id_key)] + 1

        except IndexError:
            return 0

        return int(last_record_id)

    def make_user_r(self, username: str, password_hash: str):
        table_ = self.storage.read(key="users_table", item_type=Table)
        if table_ is None:
            table_ = Table(data=[], schema=self.TABLES[0]["schema"])

        new_record = TableRecord(record={
            "user_id": int(self._get_record_id(table_, id_key="user_id")),
            "username": str(username),
            "password_hash": str(password_hash),
            "created_at": str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            "_account_is_active": True
        })

        table_.add_record(record=new_record)
        self.storage.write(key="users_table", item=table_)

    def check_user_passwd(self, username: str, password_hash: str):
        table_ = self.storage.read(key="users_table", item_type=Table)
        if table_ is None:
            return False
        for r in table_.dataframe.to_dict("records"):
            if r["username"] == str(username) and r["password_hash"] == str(password_hash): return True

        return False

    def generate_token(self, username: str, password_hash: str):
        if not self.check_user_passwd(username, password_hash):
            return None

        table_ = self.storage.read(key="tokens_table", item_type=Table)
        if table_ is None:
            table_ = Table(data=[], schema=self.TABLES[1]["schema"])

        token = str(uuid.uuid4())
        new_record = TableRecord(record={
            "t_id": int(self._get_record_id(table_, id_key="t_id")),
            "token": token,
            "username": str(username),
            "created_at": str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        })

        table_.add_record(record=new_record)
        self.storage.write(key="tokens_table", item=table_)

        return token

    def validate_token(self, token: str):
        table_ = self.storage.read(key="tokens_table", item_type=Table)
        if table_ is None:
            return None
        for r in table_.dataframe.to_dict("records"):
            if r["token"] == str(token):
                return r["username"]

        return None

    def change_password(self, username: str, new_password_hash: str):
        table_ = self.storage.read(key="users_table", item_type=Table)

        mask = table_.dataframe["username"] == str(username)
        if len(table_.dataframe[mask]) == 0:
            return False

        table_.dataframe.loc[mask, "password_hash"] = str(new_password_hash)
        self.storage.write(key="users_table", item=table_)
        return True

    def get_messages_own_user(self, username: str):
        username = str(username)

        table_ = self.storage.read(key="messages_table", item_type=Table)
        chat_table = self.storage.read(key="chats_table", item_type=Table)

        if table_ is None or chat_table is None:
            return []

        records = table_.dataframe.to_dict("records")

        crypter_l1 = Crypter(key=open(file=f"{self.KEYS_PATH}/crypt_messages_key.bin", mode="rb").read())

        user_messages = []
        chat_dict = {}
        for r in chat_table.dataframe.to_dict("records"):
            if username in r["participants"] or r["owner"] == username:
                chat_dict[r["chat_id"]] = r["name"]

        for rcd in records:
            if rcd["chat_id"] in chat_dict:
                try:
                    if not rcd["payload_bytes"] or len(rcd["payload_bytes"]) == 0:
                        logging.warning(f"Skipping message ID {rcd['message_id']}: empty payload bytes")
                        continue

                    payload_decrypted_l1 = crypter_l1.decrypt(rcd["payload_bytes"])
                    if len(payload_decrypted_l1) == 0:
                        logging.warning(
                            f"Skipping invalid message ID {rcd['message_id']}: " + \
                            f"payload is empty")
                        continue

                    payload_decrypted = payload_decrypted_l1
                    user_messages.append(
                        (rcd["message_id"], payload_decrypted, rcd["sender"], rcd["chat_id"],
                         chat_dict[rcd["chat_id"]], rcd["created_at"]))

                except Exception as e:
                    logging.error(f"Failed to decrypt message ID {rcd['message_id']}: {str(e)}")
                    continue

        return user_messages

    def make_message_r(self, username: str, payload: str, chat_id: int):
        table_ = self.storage.read(key="messages_table", item_type=Table)
        chat_table = self.storage.read(key="chats_table", item_type=Table)

        chat = None
        for r in chat_table.dataframe.to_dict("records"):
            if r["chat_id"] == int(chat_id):
                chat = r
                break

        if not chat or (username not in chat["participants"] and username != chat["owner"]):
            raise ValueError("Invalid chat_id or user not in chat")

        crypter_l1 = Crypter(key=open(file=f"{self.KEYS_PATH}/crypt_messages_key.bin", mode="rb").read())

        if not payload or len(payload.strip()) == 0:
            raise ValueError("Message payload cannot be empty")

        payload_bytes = payload.encode()
        payload_crypted_l1 = crypter_l1.encrypt(payload_bytes)
        payload_crypted = payload_crypted_l1

        new_record = {
            "message_id": int(self._get_record_id(table_, id_key="message_id")),
            "sender": str(username),
            "payload_bytes": payload_crypted,
            "chat_id": int(chat_id),
            "created_at": str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        }

        table_.add_record(record=TableRecord(record=new_record))
        self.storage.write(key="messages_table", item=table_)

    def delete_message(self, message_id: int, username: str):
        table_ = self.storage.read(key="messages_table", item_type=Table)
        records = table_.dataframe.to_dict("records")
        for i, r in enumerate(records):
            if r["message_id"] == int(message_id) and r["sender"] == str(username):
                table_.remove_record(i)

                self.storage.write(key="messages_table", item=table_)
                return True

        return False

    def _delete_messages_by_chat_id(self, chat_id: int):
        table_ = self.storage.read(key="messages_table", item_type=Table)
        if table_ is None:
            return
        records = table_.dataframe.to_dict("records")
        indices_to_remove = []
        for i, r in enumerate(records):
            if r["chat_id"] == int(chat_id):
                indices_to_remove.append(i)

        for i in sorted(indices_to_remove, reverse=True):
            table_.remove_record(i)

        self.storage.write(key="messages_table", item=table_)
