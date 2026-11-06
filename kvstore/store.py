"""The key-value store."""

import os
import threading
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Union

from .wal import OP_DELETE, OP_PUT, encode_record, read_records

Bytes = Union[bytes, str]


class ClosedError(RuntimeError):
    """The store was used after ``close()``."""


@dataclass(frozen=True)
class RecoveryInfo:
    records_replayed: int
    bytes_discarded: int  # corrupt or truncated tail removed at open


def _b(value: Bytes) -> bytes:
    return value.encode("utf-8") if isinstance(value, str) else bytes(value)


class KeyValueStore:
    """Durable key-value store.

    Writes are first appended to a log file and only then applied to an
    in-memory dict, so the log is the source of truth. On open the log is
    replayed; a torn write at the tail (power loss mid-append) or any record
    with a bad checksum ends the replay and the tail is truncated, leaving a
    log that ends on a record boundary.

    ``sync=True`` calls ``fsync`` after every write: a returned ``put`` has
    survived power loss. ``sync=False`` only guards against process crashes
    (data sits in the OS page cache) and is much faster.

    ``compact()`` rewrites the log with one record per live key into a
    temporary file and swaps it in with an atomic ``os.replace``.
    """

    def __init__(self, path: str, sync: bool = True, auto_compact_ratio: Optional[float] = None):
        self.path = path
        self._sync = sync
        self._auto_ratio = auto_compact_ratio
        self._lock = threading.RLock()
        self._data: Dict[bytes, bytes] = {}
        self._records = 0  # records currently in the log file
        self._closed = False
        # A leftover temp file means a compaction was interrupted: the original
        # log is still complete, so the partial copy is simply discarded.
        tmp = path + ".compact"
        if os.path.exists(tmp):
            os.remove(tmp)
        self.recovery = self._recover()
        self._log = open(path, "ab")

    # -- recovery ------------------------------------------------------------

    def _recover(self) -> RecoveryInfo:
        if not os.path.exists(self.path):
            return RecoveryInfo(0, 0)
        valid_end = 0
        replayed = 0
        with open(self.path, "rb") as handle:
            for op, key, value, end in read_records(handle):
                if op == OP_PUT:
                    self._data[key] = value
                else:
                    self._data.pop(key, None)
                replayed += 1
                valid_end = end
        size = os.path.getsize(self.path)
        if valid_end < size:
            with open(self.path, "r+b") as handle:
                handle.truncate(valid_end)
                handle.flush()
                os.fsync(handle.fileno())
        self._records = replayed
        return RecoveryInfo(replayed, size - valid_end)

    # -- helpers ---------------------------------------------------------------

    def _check_open(self) -> None:
        if self._closed:
            raise ClosedError("store is closed")

    def _append(self, op: int, key: bytes, value: bytes = b"") -> None:
        self._log.write(encode_record(op, key, value))
        self._log.flush()
        if self._sync:
            os.fsync(self._log.fileno())
        self._records += 1

    # -- public API ----------------------------------------------------------

    def put(self, key: Bytes, value: Bytes) -> None:
        k, v = _b(key), _b(value)
        with self._lock:
            self._check_open()
            self._append(OP_PUT, k, v)
            self._data[k] = v
            self._maybe_compact()

    def get(self, key: Bytes, default: Optional[bytes] = None) -> Optional[bytes]:
        with self._lock:
            self._check_open()
            return self._data.get(_b(key), default)

    def delete(self, key: Bytes) -> bool:
        k = _b(key)
        with self._lock:
            self._check_open()
            if k not in self._data:
                return False
            self._append(OP_DELETE, k)  # tombstone
            del self._data[k]
            self._maybe_compact()
            return True

    def keys(self) -> List[bytes]:
        with self._lock:
            self._check_open()
            return sorted(self._data)

    def __contains__(self, key: Bytes) -> bool:
        with self._lock:
            self._check_open()
            return _b(key) in self._data

    def __len__(self) -> int:
        with self._lock:
            self._check_open()
            return len(self._data)

    def items(self) -> Iterator:
        with self._lock:
            self._check_open()
            return iter(sorted(self._data.items()))

    # -- compaction ----------------------------------------------------------

    def _maybe_compact(self) -> None:
        if self._auto_ratio is None:
            return
        live = max(len(self._data), 1)
        if self._records >= 16 and self._records / live >= self._auto_ratio:
            self.compact()

    def compact(self) -> int:
        """Rewrite the log keeping only live keys; returns bytes reclaimed."""
        with self._lock:
            self._check_open()
            before = os.path.getsize(self.path)
            tmp = self.path + ".compact"
            with open(tmp, "wb") as out:
                for key in sorted(self._data):
                    out.write(encode_record(OP_PUT, key, self._data[key]))
                out.flush()
                os.fsync(out.fileno())
            self._log.close()
            os.replace(tmp, self.path)  # atomic on POSIX and Windows
            self._fsync_directory()
            self._log = open(self.path, "ab")
            self._records = len(self._data)
            return before - os.path.getsize(self.path)

    def _fsync_directory(self) -> None:
        try:
            fd = os.open(os.path.dirname(os.path.abspath(self.path)), os.O_RDONLY)
        except OSError:  # not supported on every platform
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def stats(self) -> Dict[str, int]:
        with self._lock:
            self._check_open()
            return {
                "live_keys": len(self._data),
                "log_records": self._records,
                "dead_records": self._records - len(self._data),
                "file_bytes": os.path.getsize(self.path),
            }

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._log.flush()
                if self._sync:
                    os.fsync(self._log.fileno())
                self._log.close()
                self._closed = True

    def __enter__(self) -> "KeyValueStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
