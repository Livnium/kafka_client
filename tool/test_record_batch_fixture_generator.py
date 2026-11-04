# Copyright Livnium.
# SPDX-License-Identifier: Apache-2.0
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import generate_record_batch_fixtures as generator


class RecordBatchFixtureGeneratorTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.home = self.root / "kafka"
        (self.home / "libs").mkdir(parents=True)
        self.source = self.root / "RecordBatchOracle.java"
        self.source.write_bytes(b"// synthetic test source\r\nclass RecordBatchOracle {}\r\n")
        self.fixture = self.root / "fixture.json"
        self.fixture.write_text("original\n", encoding="utf-8")
        self.hashes = {}
        for name, contents in [("kafka-clients-4.0.2.jar", b"synthetic kafka"),
                               ("slf4j-api-1.7.36.jar", b"synthetic logging")]:
            (self.home / "libs" / name).write_bytes(contents)
            self.hashes[name] = hashlib.sha256(contents).hexdigest()
        for name, value in [("SOURCE", self.source), ("FIXTURE", self.fixture),
                            ("JARS", self.hashes)]:
            mocking = patch.object(generator, name, value)
            mocking.start()
            self.addCleanup(mocking.stop)
        self.output = "\n".join(json.dumps({
            "name": name, "wire": "", "lastBatchOffset": "0", "records": []
        }) for name in generator.CASES)

    def completed(self):
        return [subprocess.CompletedProcess([], 0, "", ""),
                subprocess.CompletedProcess([], 0, self.output, "")]

    def invoke(self, *args):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return generator.main(["--kafka-home", str(self.home), *args])

    def test_only_verified_pinned_jars_enter_classpath(self):
        (self.home / "libs" / "unrelated.jar").write_bytes(b"not loaded")
        with patch.object(generator.subprocess, "run", side_effect=self.completed()) as run:
            artifact = json.loads(generator.generate(self.home))
        self.assertEqual(artifact["oracle"]["jarsSha256"], self.hashes)
        compile_args, java_args = [call.args[0] for call in run.call_args_list]
        expected = [str((self.home / "libs" / name).resolve()) for name in self.hashes]
        self.assertEqual(compile_args[compile_args.index("-cp") + 1].split(os.pathsep), expected)
        self.assertEqual(java_args[java_args.index("-cp") + 1].split(os.pathsep)[1:], expected)
        for call in run.call_args_list:
            self.assertEqual(call.kwargs["timeout"], 60)
            self.assertTrue(call.kwargs["check"])
        self.assertIn("-J-Xmx128m", compile_args)
        self.assertIn("-Xmx128m", java_args)

    def test_wrong_jar_hash_prevents_any_process(self):
        (self.home / "libs" / "kafka-clients-4.0.2.jar").write_bytes(b"changed")
        with patch.object(generator.subprocess, "run") as run:
            with self.assertRaisesRegex(RuntimeError, "Unexpected SHA256"):
                generator.generate(self.home)
            run.assert_not_called()

    def test_missing_jar_prevents_any_process(self):
        (self.home / "libs" / "slf4j-api-1.7.36.jar").unlink()
        with patch.object(generator.subprocess, "run") as run:
            with self.assertRaises(FileNotFoundError):
                generator.generate(self.home)
            run.assert_not_called()

    def test_generation_is_deterministic_and_normalizes_source_newlines(self):
        with patch.object(generator.subprocess, "run", side_effect=self.completed() * 2):
            first = generator.generate(self.home)
            second = generator.generate(self.home)
        self.assertEqual(first, second)
        expected = hashlib.sha256(b"// synthetic test source\nclass RecordBatchOracle {}\n").hexdigest()
        self.assertEqual(json.loads(first)["oracle"]["generatorSha256"], expected)
        self.assertEqual(self.fixture.read_text(encoding="utf-8"), "original\n")

    def test_missing_or_reordered_cases_fail_generation(self):
        for lines in [self.output.splitlines()[:-1], self.output.splitlines()[::-1]]:
            with self.subTest(lines=len(lines)):
                completed = self.completed()
                completed[1].stdout = "\n".join(lines)
                with patch.object(generator.subprocess, "run", side_effect=completed):
                    with self.assertRaisesRegex(RuntimeError, "unexpected case list"):
                        generator.generate(self.home)

    def test_check_mismatch_never_rewrites_fixture(self):
        with patch.object(generator, "generate", return_value="different\n"):
            self.assertEqual(self.invoke("--check"), 1)
        self.assertEqual(self.fixture.read_text(encoding="utf-8"), "original\n")

    def test_default_mode_checks_without_writing(self):
        before = self.fixture.stat().st_mtime_ns
        with patch.object(generator, "generate", return_value="original\n"):
            self.assertEqual(self.invoke(), 0)
        self.assertEqual(self.fixture.stat().st_mtime_ns, before)

    def test_explicit_write_replaces_fixture_with_generated_bytes(self):
        with patch.object(generator, "generate", return_value="replacement\n"):
            self.assertEqual(self.invoke("--write"), 0)
        self.assertEqual(self.fixture.read_bytes(), b"replacement\n")

    def test_missing_kafka_home_is_an_argument_error(self):
        with patch.dict(os.environ, {}, clear=True), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                generator.main([])
        self.assertEqual(error.exception.code, 2)

    def test_process_failure_cannot_replace_existing_fixture(self):
        for phase in ["javac", "java"]:
            with self.subTest(phase=phase):
                failure = subprocess.CalledProcessError(1, phase, stderr="synthetic failure")
                steps = [failure] if phase == "javac" else [self.completed()[0], failure]
                with patch.object(generator.subprocess, "run", side_effect=steps):
                    self.assertEqual(self.invoke("--write"), 1)
                self.assertEqual(self.fixture.read_text(encoding="utf-8"), "original\n")


if __name__ == "__main__":
    unittest.main()
