#!/usr/bin/env python3
"""No-broker regression for secure fixture failure and private cleanup."""
import contextlib
import io
import json
import os
import pathlib
import signal
import tempfile
import unittest
from unittest.mock import Mock, patch

import run_secure_cluster_integration as runner


class SecureRunnerTest(unittest.TestCase):
    def exercise_second_topic_failure(self, version):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            home = root / 'kafka'
            (home / 'libs').mkdir(parents=True)
            (home / f'libs/kafka-clients-{version}.jar').touch()
            output = root / 'runs'
            cluster = Mock()
            cluster.processes = {}
            cluster.generations = {1: 1, 2: 1, 3: 1}
            cluster.command.side_effect = [None, RuntimeError('second topic creation failed')]
            process = Mock()
            process.wait.return_value = 0
            process.poll.return_value = 0
            captured = {}

            def fake_test(*args, **kwargs):
                env = kwargs['env']
                captured.update({key: env[key] for key in
                                 ('KAFKA_SECURE_CLUSTER_USERNAME',
                                  'KAFKA_SECURE_CLUSTER_PASSWORD')})
                for value in captured.values():
                    kwargs['stdout'].write(value + '\n')
                kwargs['stdout'].flush()
                return process

            def fake_certificates(run, store_password):
                captured['store'] = store_password
                (run / 'server.key').write_text('synthetic private test key')
                (run / 'server.p12').write_bytes(b'synthetic private test store')

            saved = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
            try:
                with patch.dict(os.environ, KAFKA_HOME=str(home), KAFKA_VERSION=version,
                                KAFKA_SECURE_CLUSTER_RUN_ROOT=str(output)), \
                        patch.object(runner, 'certificates', side_effect=fake_certificates), \
                        patch.object(runner, 'SecureCluster', return_value=cluster), \
                        patch.object(runner, 'Control'), \
                        patch.object(runner.subprocess, 'Popen', side_effect=fake_test) as popen, \
                        patch.object(runner.sys, 'argv', ['runner']), \
                        contextlib.redirect_stdout(io.StringIO()), \
                        contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(runner.main(), 1)
                self.assertEqual(popen.call_count, 1)
                result = json.loads(next(output.glob('result-*.json')).read_text())
                self.assertEqual(result['kafka_version'], version)
                self.assertEqual(result['exit_code'], 1)
                self.assertEqual(result['test_exit_code'], 1)
                self.assertIn('second topic creation failed', result['failure'])
                self.assertTrue(result['private_fixture_removed'])
                self.assertTrue(result['owned_processes_stopped'])
                self.assertTrue(result['retained_output_secret_scan_passed'])
                self.assertEqual(list(output.glob('private.*')), [])
                retained = ''.join(path.read_text() for path in output.iterdir())
                for value in captured.values():
                    self.assertNotIn(value, retained)
                self.assertIn('[REDACTED]', retained)
            finally:
                for signum, handler in saved.items():
                    signal.signal(signum, handler)

    def test_second_topic_failure_cannot_reuse_first_mode_success(self):
        self.exercise_second_topic_failure('4.0.2')

    def test_non_default_release_is_recorded_on_failure(self):
        self.exercise_second_topic_failure('4.4.0')


if __name__ == '__main__':
    unittest.main()
