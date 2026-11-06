"""Record format of the write-ahead log.

Every record is::

    +---------+------+----------+------------+-----+-------+
    | crc32   | op   | key_len  | value_len  | key | value |
    | 4 bytes | 1 B  | 4 bytes  | 4 bytes    |     |       |
    +---------+------+----------+------------+-----+-------+

The CRC (big endian) covers everything after the CRC field. A record that is
cut short or whose checksum does not match marks the end of the valid log.
"""

import struct
import zlib
from typing import BinaryIO, Iterator, Optional, Tuple

OP_PUT = 1
OP_DELETE = 2

_HEADER = struct.Struct(">IBII")  # crc, op, key_len, value_len
HEADER_SIZE = _HEADER.size
MAX_FIELD = 64 * 1024 * 1024  # sanity bound so a corrupt length cannot trigger a huge read


def encode_record(op: int, key: bytes, value: bytes = b"") -> bytes:
    if op not in (OP_PUT, OP_DELETE):
        raise ValueError(f"unknown op {op}")
    if len(key) > MAX_FIELD or len(value) > MAX_FIELD:
        raise ValueError("key or value too large")
    body = struct.pack(">BII", op, len(key), len(value)) + key + value
    return struct.pack(">I", zlib.crc32(body)) + body


def read_records(handle: BinaryIO) -> Iterator[Tuple[int, bytes, bytes, int]]:
    """Yield ``(op, key, value, end_offset)`` for every valid record.

    Stops silently at the first truncated or corrupt record; the caller can
    compare the last ``end_offset`` with the file size to see how much of the
    tail was discarded.
    """
    offset = handle.tell()
    while True:
        header = handle.read(HEADER_SIZE)
        if len(header) < HEADER_SIZE:
            return
        crc, op, key_len, value_len = _HEADER.unpack(header)
        if op not in (OP_PUT, OP_DELETE) or key_len > MAX_FIELD or value_len > MAX_FIELD:
            return
        payload = handle.read(key_len + value_len)
        if len(payload) < key_len + value_len:
            return
        body = header[4:] + payload
        if zlib.crc32(body) != crc:
            return
        offset += HEADER_SIZE + key_len + value_len
        yield op, payload[:key_len], payload[key_len:], offset
