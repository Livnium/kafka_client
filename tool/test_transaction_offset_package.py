#!/usr/bin/env python3
"""Offline transaction-probe source closure and launcher cleanup regressions."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


PROBE = Path(__file__).resolve().parent / "transaction_offset_probe"


class TransactionOffsetPackageTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="probe package ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.probe = self.root / "package" / "tool" / "transaction_offset_probe"
        shutil.copytree(PROBE, self.probe)
        self.bin = self.root / "fake jdk" / "bin"
        self.bin.mkdir(parents=True)
        self.builds = self.root / "builds"
        self.builds.mkdir()
        self.compiler_receipt = self.root / "compiler.json"
        self.java_receipt = self.root / "java.json"
        self.env = dict(
            os.environ,
            JAVA_HOME=str(self.bin.parent),
            KAFKA_HOME=str(self.root / "external kafka"),
            KAFKA_SECURE_USERNAME="synthetic-user",
            KAFKA_SECURE_PASSWORD="synthetic-password",
            TMPDIR=str(self.builds),
            PROBE_DIRECTORY=str(self.probe),
            COMPILER_RECEIPT=str(self.compiler_receipt),
            JAVA_RECEIPT=str(self.java_receipt),
        )
        self.write_command(
            "javac",
            """import json, os, pathlib, sys
args = sys.argv[1:]
sources = [pathlib.Path(arg) for arg in args if arg.endswith('.java')]
expected = pathlib.Path(os.environ['PROBE_DIRECTORY']).resolve()
if len(sources) != 2 or any(not path.is_file() or path.resolve().parent != expected for path in sources):
    sys.exit(72)
build = pathlib.Path(args[args.index('-d') + 1])
if not build.is_dir():
    sys.exit(73)
pathlib.Path(os.environ['COMPILER_RECEIPT']).write_text(json.dumps({'args': args, 'sources': [path.name for path in sources], 'build': str(build)}))
sys.exit(int(os.environ.get('COMPILER_EXIT', '0')))
""",
        )
        self.write_command(
            "java",
            """import json, os, pathlib, sys
pathlib.Path(os.environ['JAVA_RECEIPT']).write_text(json.dumps({'args': sys.argv[1:], 'stdin': sys.stdin.read()}))
sys.exit(int(os.environ.get('JAVA_EXIT', '0')))
""",
        )

    def write_command(self, name, body):
        path = self.bin / name
        path.write_text("#!/usr/bin/env python3\n" + body)
        path.chmod(0o700)

    def run_launcher(self):
        return subprocess.run(
            ["bash", str(self.probe / "run.sh"), "--hold", "SSL", "commit"],
            cwd=self.root,
            env=self.env,
            input="commit\n",
            capture_output=True,
            text=True,
            timeout=10,
        )

    def assert_cleaned(self):
        self.assertEqual(list(self.builds.iterdir()), [])

    def test_runs_with_only_packaged_probe_sources(self):
        self.assertFalse((self.root / "package" / "verification").exists())
        result = self.run_launcher()
        self.assertEqual(result.returncode, 0, result.stderr)
        compiler = json.loads(self.compiler_receipt.read_text())
        self.assertEqual(
            compiler["sources"],
            ["OffsetFetchWireOracle.java", "PendingTransactionalOffsetProbe.java"],
        )
        self.assertIn("-proc:none", compiler["args"])
        java = json.loads(self.java_receipt.read_text())
        self.assertEqual(
            java["args"][-4:],
            ["PendingTransactionalOffsetProbe", "--hold", "SSL", "commit"],
        )
        self.assertEqual(java["stdin"], "commit\n")
        self.assert_cleaned()

    def test_missing_packaged_oracle_fails_before_java(self):
        (self.probe / "OffsetFetchWireOracle.java").unlink()
        self.assertEqual(self.run_launcher().returncode, 72)
        self.assertFalse(self.java_receipt.exists())
        self.assert_cleaned()

    def test_compile_failure_is_retained_and_cleaned(self):
        self.env["COMPILER_EXIT"] = "17"
        self.assertEqual(self.run_launcher().returncode, 17)
        self.assertTrue(self.compiler_receipt.exists())
        self.assertFalse(self.java_receipt.exists())
        self.assert_cleaned()

    def test_java_failure_is_retained_and_cleaned(self):
        self.env["JAVA_EXIT"] = "19"
        self.assertEqual(self.run_launcher().returncode, 19)
        self.assertTrue(self.java_receipt.exists())
        self.assert_cleaned()


if __name__ == "__main__":
    unittest.main()
