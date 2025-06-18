"""Bolt wire protocol implementation."""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional


class OpCode(IntEnum):
    """Operation codes for Bolt protocol."""
    PUT = 1
    GET = 2
    DEL = 3
    DB_SWITCH = 4
    GET_ALL = 5
    AUTH = 6
    AUTH_OK = 7
    AUTH_FAIL = 8
    STATS = 9
    CLUSTER_STATUS = 10
    SETEX = 11
    TTL = 12
    MGET = 13
    MSET = 14
    MDEL = 15
    METRICS = 16
    # Counter operations
    INCR = 20
    DECR = 21
    INCRBY = 22
    # List operations
    LPUSH = 30
    RPUSH = 31
    LPOP = 32
    RPOP = 33
    LRANGE = 34
    LLEN = 35
    # Set operations
    SADD = 40
    SREM = 41
    SMEMBERS = 42
    SCARD = 43
    SISMEMBER = 44
    # Utility operations
    EXISTS = 50
    TYPE = 51
    KEYS = 52
    # User management
    USER_ADD = 60
    USER_DEL = 61
    USER_LIST = 62
    USER_PASSWD = 63
    USER_ROLE = 64
    WHOAMI = 65


# Header size: code(2) + db_id_len(4) + key_len(4) + value_len(4) = 14 bytes
HEADER_SIZE = 14


@dataclass
class Message:
    """
    Bolt protocol message.

    Wire protocol format:
    1. code (u16) - 2 bytes, big-endian
    2. database_id_length (u32) - 4 bytes, big-endian
    3. key_length (u32) - 4 bytes, big-endian
    4. value_length (u32) - 4 bytes, big-endian
    5. database_id (bytes) - if length > 0
    6. key (bytes)
    7. value (bytes)
    """
    code: int
    key: str
    value: str
    not_found: bool = False
    database_id: str = ""  # Empty string = Default, non-empty = Custom

    def encode(self) -> bytes:
        """Encode message to bytes for sending."""
        key_bytes = self.key.encode('utf-8')
        value_bytes = self.value.encode('utf-8')
        db_id_bytes = self.database_id.encode('utf-8') if self.database_id else b''

        # Build header: code(u16) + db_id_len(u32) + key_len(u32) + value_len(u32)
        header = struct.pack(
            '>HIII',  # Big-endian: unsigned short, 3x unsigned int
            self.code,
            len(db_id_bytes),
            len(key_bytes),
            len(value_bytes)
        )

        # Build body: db_id + key + value
        body = db_id_bytes + key_bytes + value_bytes

        return header + body

    @classmethod
    def decode(cls, header: bytes, body: bytes) -> 'Message':
        """
        Decode message from header and body bytes.

        Args:
            header: 14-byte header
            body: Variable-length body
        """
        # Parse header
        code, db_id_len, key_len, value_len = struct.unpack('>HIII', header)

        # Parse body
        offset = 0

        database_id = ""
        if db_id_len > 0:
            database_id = body[offset:offset + db_id_len].decode('utf-8')
            offset += db_id_len

        key = body[offset:offset + key_len].decode('utf-8') if key_len > 0 else ""
        offset += key_len

        value = body[offset:offset + value_len].decode('utf-8') if value_len > 0 else ""

        return cls(
            code=code,
            key=key,
            value=value,
            not_found=False,
            database_id=database_id,
        )

    @classmethod
    def auth(cls, username: str, password: str) -> 'Message':
        """Create authentication message."""
        return cls(
            code=OpCode.AUTH,
            key=username,
            value=password,
        )

    @classmethod
    def put(cls, key: str, value: str, database: Optional[str] = None) -> 'Message':
        """Create PUT message."""
        return cls(
            code=OpCode.PUT,
            key=key,
            value=value,
            database_id=database or "",
        )

    @classmethod
    def get(cls, key: str, database: Optional[str] = None) -> 'Message':
        """Create GET message."""
        return cls(
            code=OpCode.GET,
            key=key,
            value="",
            database_id=database or "",
        )

    @classmethod
    def delete(cls, key: str, database: Optional[str] = None) -> 'Message':
        """Create DEL message."""
        return cls(
            code=OpCode.DEL,
            key=key,
            value="",
            database_id=database or "",
        )

    @classmethod
    def cluster_status(cls) -> 'Message':
        """Create CLUSTER_STATUS message."""
        return cls(code=OpCode.CLUSTER_STATUS, key="", value="")
