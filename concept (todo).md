## ShadowWire - Privacy Message Communication System

<br>

## Database Tables (Schemes):

### Accounts Table
| Column            | Type | Description                    |
|-------------------|------|--------------------------------|
| **id**            | INT  | Account ID                     |
| **username**      | STR  | Unique Username/Nickname       |
| **password_hash** | STR  | Password hash                  |
| **verify_token**  | STR  | Token for verification account |

### Messages Table (for temporary storage until reading)
| Column         | Type                                  | Description                                            |
|----------------|---------------------------------------|--------------------------------------------------------|
| **id**         | INT                                   | Message ID                                             |
| **chat_uuid**  | STR                                   | Chat UUID                                              |
| **sender**     | STR                                   | Sender Username/Nickname                               |
| **percipient** | STR                                   | Percipient Username/Nickname                           |
| ** payload**   | BYTES                                 | Bytes Payload (encoded text/images)                    |
| **pds**        | TUPLE[BOOL(sender), BOOL(percipient)] | Consent to delete message from database (upon receipt) |

<br>

## Transactions:

### Accounts transactions:
#### *REGISTER_ACCOUNT* (username: str, password_hash: str) >> ["ok"]
#### *GEN_VERIFY_TOKEN* (username: str, password: str) >> ["ok", <verify_token>]
#### *CHECK_ACCOUNT_ACCESS_BY_PASSWORD* (username: str, password: str) >> ["ok"]
#### *VERIFY_TOKEN* (target_username: str, token: str) >> ["ok"]

### Messages transactions:
#### *SEND_MSG* (chat_uuid: str, username: str, password: str, payload: bytes) >> ["ok"] 
#### *UPDATE_PDS* (username: str, password: str, pds: bytes) >> ["ok"]

#### *READ_ALL_MESSAGES* (username: str, password: str, last_num: int) >> ["ok", <messages>]
#### *READ_MESSAGES_OF_CHAT* (username: str, password: str, last_num: int, chat_uuid: str) >> ["ok", <messages>]

<br>


