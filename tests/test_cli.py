import contextlib
import io
import os
import tempfile
import unittest

from kvstore.cli import main


def run(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = main(list(argv))
    return rc, out.getvalue(), err.getvalue()


class CliTests(unittest.TestCase):
    def test_flow(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "cli.db")
            base = ["--path", db, "--no-sync"]
            self.assertEqual(run(*base, "put", "name", "ada")[0], 0)
            self.assertEqual(run(*base, "get", "name")[1].strip(), "ada")
            self.assertEqual(run(*base, "keys")[1].strip(), "name")
            for _ in range(5):
                run(*base, "put", "name", "ada")
            self.assertIn("reclaimed", run(*base, "compact")[1])
            self.assertIn("live_keys: 1", run(*base, "stats")[1])
            self.assertEqual(run(*base, "delete", "name")[0], 0)
            self.assertEqual(run(*base, "get", "name")[0], 1)
            self.assertEqual(run(*base, "delete", "name")[0], 1)

    def test_warns_about_damaged_tail(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "cli.db")
            run("--path", db, "--no-sync", "put", "a", "1")
            with open(db, "ab") as f:
                f.write(b"\x00\x01\x02")
            rc, out, err = run("--path", db, "--no-sync", "get", "a")
            self.assertEqual((rc, out.strip()), (0, "1"))
            self.assertIn("discarded 3 bytes", err)


if __name__ == "__main__":
    unittest.main()
