"""Command line interface."""

import argparse
import sys
from typing import List, Optional

from .store import KeyValueStore


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="kvstore", description="Append-only key-value store")
    p.add_argument("--path", default="kv.db", help="log file (default: kv.db)")
    p.add_argument("--no-sync", action="store_true", help="skip fsync on every write")
    sub = p.add_subparsers(dest="command", required=True)
    put = sub.add_parser("put", help="store a value")
    put.add_argument("key")
    put.add_argument("value")
    sub.add_parser("get", help="print a value").add_argument("key")
    sub.add_parser("delete", help="remove a key").add_argument("key")
    sub.add_parser("keys", help="list keys")
    sub.add_parser("compact", help="rewrite the log without dead records")
    sub.add_parser("stats", help="show log statistics")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    with KeyValueStore(args.path, sync=not args.no_sync) as store:
        if store.recovery.bytes_discarded:
            print(
                f"warning: discarded {store.recovery.bytes_discarded} bytes of damaged log tail",
                file=sys.stderr,
            )
        if args.command == "put":
            store.put(args.key, args.value)
            print("OK")
        elif args.command == "get":
            value = store.get(args.key)
            if value is None:
                print("(nil)", file=sys.stderr)
                return 1
            print(value.decode("utf-8", errors="replace"))
        elif args.command == "delete":
            if not store.delete(args.key):
                print("(nil)", file=sys.stderr)
                return 1
            print("OK")
        elif args.command == "keys":
            for key in store.keys():
                print(key.decode("utf-8", errors="replace"))
        elif args.command == "compact":
            print(f"reclaimed {store.compact()} bytes")
        else:
            for name, value in store.stats().items():
                print(f"{name}: {value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
