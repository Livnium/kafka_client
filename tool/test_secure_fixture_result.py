import base64
import pathlib
import subprocess
import tempfile
import unittest

from secure_fixture_result import mtls_summary


class MtlsSummaryTests(unittest.TestCase):
    def test_output_discards_every_raw_material_representation(self):
        # Deliberate synthetic leak injection, not real secret material.
        raw = b'synthetic-private-key-and-certificate\x00\xff'
        representations = [raw.decode('latin1'), str(list(raw)), raw.hex(),
                           base64.b64encode(raw).decode(),
                           '-----BEGIN PRIVATE KEY-----\nsynthetic\n-----END PRIVATE KEY-----',
                           'synthetic-container-password', '/private/fixture/client.p12']
        text = '\n'.join(representations) + '\n00:01 +10: All tests passed!\n'
        summary, status = mtls_summary(text, 0, 'complete', '4.0.2')
        self.assertEqual(status, 0)
        self.assertIn('passed_cases=10', summary)
        for value in representations:
            self.assertNotIn(value, summary)

    def test_missing_success_or_nonterminal_stage_fails_closed(self):
        for text, stage in [('', 'complete'), ('Some tests failed.', 'complete'),
                            ('00:01 +10: All tests passed!\n', 'tests')]:
            summary, status = mtls_summary(text, 0, stage, '4.0.2')
            self.assertNotEqual(status, 0)
            self.assertIn('tests=not-passed', summary)

    def test_sdk_colored_success_uses_only_safe_numeric_count(self):
        summary, status = mtls_summary(
            '00:01 \x1b[32m+10\x1b[0m: All tests passed!\x1b[0m\n',
            0, 'complete', '4.0.2')
        self.assertEqual(status, 0)
        self.assertIn('passed_cases=10', summary)
        self.assertNotIn('\x1b', summary)

    def test_failures_and_untrusted_fields_never_echo_input(self):
        summary, status = mtls_summary('secret', 130, 'secret', 'secret')
        self.assertEqual(status, 130)
        self.assertNotIn('secret', summary)
        self.assertIn('stage=unknown', summary)
        self.assertIn('Kafka unknown', summary)

    def test_runner_defaults_and_explicit_listener_policy(self):
        root = pathlib.Path(__file__).resolve().parent
        source = (root / 'run_secure_broker_integration.sh').read_text()
        self.assertIn('CLIENT_AUTH=none', source)
        self.assertIn('CLIENT_AUTH=required', source)
        for listener in ('sasl_ssl', 'scram'):
            self.assertIn(f'listener.name.{listener}.ssl.client.auth=$CLIENT_AUTH', source)
        self.assertIn("from secure_fixture_result import mtls_summary", source)
        self.assertIn('report_status=$?', source)
        wrapper = (root / 'run_mtls_broker_integration.sh').read_text()
        self.assertIn('export KAFKA_SECURE_MTLS=1', wrapper)
        self.assertIn('mtls_broker_integration_test.dart', wrapper)
        subprocess.run(['bash', '-n', str(root / 'run_secure_broker_integration.sh'),
                        str(root / 'run_mtls_broker_integration.sh')], check=True)

    def test_cleanup_reports_success_only_after_verified_removal(self):
        source = pathlib.Path(__file__).with_name('run_secure_broker_integration.sh').read_text()
        cleanup = source.split('  cleanup_status=0\n', 1)[1].split('\n}\ntrap cleanup EXIT', 1)[0]
        for fail_remove in (False, True):
            with self.subTest(fail_remove=fail_remove), tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary)
                private = root / 'private'
                private.mkdir()
                result = root / 'summary.txt'
                script = ('status=0\n' +
                          ('rm() { return 1; }\n' if fail_remove else '') +
                          'cleanup_status=0\n' + cleanup)
                import os
                env = dict(os.environ, RUN_DIR=str(private), RESULT=str(result))
                process = subprocess.run(['bash', '-c', script], env=env,
                                         capture_output=True, text=True)
                self.assertEqual(process.returncode, 1 if fail_remove else 0)
                if fail_remove:
                    self.assertIn('cleanup failed', process.stderr)
                    self.assertNotIn('removed;', process.stdout)
                    self.assertIn('Cleanup failed', result.read_text())
                else:
                    self.assertFalse(private.exists())
                    self.assertIn('keys/credentials removed;', process.stdout)


if __name__ == '__main__':
    unittest.main()
