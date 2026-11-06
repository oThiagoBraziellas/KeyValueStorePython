import os
import tempfile
import threading
import unittest

from kvstore import ClosedError, KeyValueStore
from kvstore.wal import OP_PUT, encode_record


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "kv.db")

    def tearDown(self):
        self.tmp.cleanup()

    def open(self, **kw):
        kw.setdefault("sync", False)
        return KeyValueStore(self.path, **kw)


class BasicTests(Base):
    def test_put_get_delete(self):
        with self.open() as s:
            s.put("a", "1")
            s.put(b"b", b"\x00\xff")
            self.assertEqual(s.get("a"), b"1")
            self.assertEqual(s.get("b"), b"\x00\xff")
            self.assertTrue(s.delete("a"))
            self.assertFalse(s.delete("a"))
            self.assertIsNone(s.get("a"))
            self.assertEqual(s.get("a", b"d"), b"d")
            self.assertEqual(len(s), 1)

    def test_overwrite_and_empty_value(self):
        with self.open() as s:
            s.put("k", "old")
            s.put("k", "")
            self.assertEqual(s.get("k"), b"")
            self.assertIn("k", s)

    def test_keys_sorted_and_unicode(self):
        with self.open() as s:
            for k in ["b", "ä", "a"]:
                s.put(k, "x")
            self.assertEqual(s.keys(), sorted([b"b", "ä".encode(), b"a"]))

    def test_use_after_close(self):
        s = self.open()
        s.close()
        with self.assertRaises(ClosedError):
            s.get("a")
        s.close()  # idempotent

    def test_sync_mode_writes(self):
        with self.open(sync=True) as s:
            s.put("durable", "yes")
        with self.open() as s:
            self.assertEqual(s.get("durable"), b"yes")


class PersistenceTests(Base):
    def test_reopen_restores_state(self):
        with self.open() as s:
            s.put("a", "1")
            s.put("b", "2")
            s.put("a", "3")
            s.delete("b")
        with self.open() as s:
            self.assertEqual(s.get("a"), b"3")
            self.assertIsNone(s.get("b"))
            self.assertEqual(s.recovery.records_replayed, 4)
            self.assertEqual(s.recovery.bytes_discarded, 0)

    def test_delete_survives_restart(self):
        with self.open() as s:
            s.put("x", "1")
            s.delete("x")
        with self.open() as s:
            self.assertNotIn("x", s)

    def test_crash_without_close(self):
        s = self.open()
        s.put("a", "1")
        s.put("b", "2")
        # Simulate a killed process: never call close().
        s2 = self.open()
        self.assertEqual(s2.get("b"), b"2")
        s2.close()
        s._log.close()


class CrashRecoveryTests(Base):
    def _seed(self, n=5):
        with self.open() as s:
            for i in range(n):
                s.put(f"key{i}", f"value{i}")
        return os.path.getsize(self.path)

    def test_torn_write_is_discarded(self):
        size = self._seed()
        partial = encode_record(OP_PUT, b"half", b"written")[:-4]
        with open(self.path, "ab") as f:
            f.write(partial)
        with self.open() as s:
            self.assertEqual(len(s), 5)
            self.assertNotIn("half", s)
            self.assertEqual(s.recovery.bytes_discarded, len(partial))
            self.assertEqual(os.path.getsize(self.path), size)  # tail truncated
            s.put("after", "recovery")
        with self.open() as s:  # log is still well formed after new appends
            self.assertEqual(s.get("after"), b"recovery")
            self.assertEqual(s.recovery.bytes_discarded, 0)

    def test_every_possible_truncation_point_recovers_a_prefix(self):
        self._seed(4)
        with open(self.path, "rb") as f:
            original = f.read()
        for cut in range(len(original) + 1):
            with open(self.path, "wb") as f:
                f.write(original[:cut])
            with self.open() as s:
                keys = s.keys()
                # Result is always a prefix of the writes, never garbage.
                self.assertEqual(keys, [f"key{i}".encode() for i in range(len(keys))])
                for i in range(len(keys)):
                    self.assertEqual(s.get(f"key{i}"), f"value{i}".encode())

    def test_corrupt_record_stops_replay(self):
        self._seed(5)
        with open(self.path, "rb") as f:
            data = bytearray(f.read())
        record_len = len(encode_record(OP_PUT, b"key0", b"value0"))
        data[record_len * 2 + 12] ^= 0xFF  # flip a byte inside the third record
        with open(self.path, "wb") as f:
            f.write(bytes(data))
        with self.open() as s:
            self.assertEqual(s.keys(), [b"key0", b"key1"])
            self.assertEqual(s.recovery.bytes_discarded, record_len * 3)

    def test_garbage_file(self):
        with open(self.path, "wb") as f:
            f.write(os.urandom(200))
        with self.open() as s:
            self.assertEqual(len(s), 0)
            s.put("a", "1")
        with self.open() as s:
            self.assertEqual(s.get("a"), b"1")


class CompactionTests(Base):
    def test_compaction_shrinks_log_and_keeps_data(self):
        with self.open() as s:
            for i in range(200):
                s.put("hot", f"v{i}")
            s.put("cold", "c")
            s.put("gone", "g")
            s.delete("gone")
            before = s.stats()
            reclaimed = s.compact()
            after = s.stats()
            self.assertGreater(reclaimed, 0)
            self.assertLess(after["file_bytes"], before["file_bytes"])
            self.assertEqual(after["log_records"], 2)
            self.assertEqual(after["dead_records"], 0)
            self.assertEqual(s.get("hot"), b"v199")
            self.assertNotIn("gone", s)
            s.put("new", "n")  # still writable after the swap
        with self.open() as s:
            self.assertEqual(s.keys(), [b"cold", b"hot", b"new"])

    def test_leftover_temp_file_from_interrupted_compaction_is_ignored(self):
        with self.open() as s:
            s.put("a", "1")
        with open(self.path + ".compact", "wb") as f:
            f.write(b"partial garbage")
        with self.open() as s:
            self.assertEqual(s.get("a"), b"1")
        self.assertFalse(os.path.exists(self.path + ".compact"))

    def test_auto_compaction(self):
        with self.open(auto_compact_ratio=4) as s:
            for i in range(100):
                s.put("k", str(i))
            self.assertLess(s.stats()["log_records"], 30)
            self.assertEqual(s.get("k"), b"99")


class ConcurrencyTests(Base):
    def test_concurrent_writers(self):
        with self.open() as s:
            def worker(n):
                for i in range(100):
                    s.put(f"t{n}-{i}", str(i))

            threads = [threading.Thread(target=worker, args=(n,)) for n in range(6)]
            [t.start() for t in threads]
            [t.join() for t in threads]
            self.assertEqual(len(s), 600)
        with self.open() as s:
            self.assertEqual(len(s), 600)
            self.assertEqual(s.recovery.bytes_discarded, 0)


if __name__ == "__main__":
    unittest.main()
