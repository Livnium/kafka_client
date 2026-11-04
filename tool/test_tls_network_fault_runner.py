#!/usr/bin/env python3
"""No-broker regressions: failure status, credential redaction and private cleanup."""
import contextlib
import io
import json
import os
import pathlib
import signal
import tempfile
import unittest
from unittest.mock import Mock, patch

import run_tls_network_fault_integration as runner


class TlsFaultRunnerTest(unittest.TestCase):
    def exercise(self, unreadable=False, resolved=True):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            checkout = root / 'checkout'
            checkout.mkdir()
            package_config = checkout / '.dart_tool/package_config.json'
            test_entry = root / 'test-package/bin/test.dart'
            if resolved:
                # The Python-only job has no real Dart package resolution.
                # Own every file the mocked runner needs instead of reading
                # the developer checkout's ignored package_config/cache.
                package_config.parent.mkdir()
                test_entry.parent.mkdir(parents=True)
                test_entry.touch()
                package_config.write_text(json.dumps(dict(configVersion=2, packages=[
                    dict(name='test', rootUri=test_entry.parent.parent.as_uri()),
                ])))
            home = root / 'kafka'
            (home / 'libs').mkdir(parents=True)
            (home / 'libs/kafka-clients-4.0.2.jar').touch()
            output = root / 'runs'
            proxy = Mock()
            proxy.thread.is_alive.return_value = False
            proxy.snapshot.return_value = {'active': 0, 'workers': 0}
            control = Mock()
            control.thread.is_alive.return_value = False
            process = Mock()
            process.wait.return_value = 0
            process.poll.return_value = 0
            captured = {}
            launches = []

            def fake_test(*args, **kwargs):
                launches.append((args[0], kwargs['cwd']))
                env = kwargs['env']
                for key in ('KAFKA_TLS_FAULT_USERNAME', 'KAFKA_TLS_FAULT_PASSWORD'):
                    captured[key] = env[key]
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
                with contextlib.ExitStack() as stack:
                    stack.enter_context(patch.object(runner, 'ROOT', checkout))
                    stack.enter_context(patch.dict(os.environ, KAFKA_HOME=str(home), KAFKA_VERSION='4.0.2',
                                                   KAFKA_TLS_FAULT_RUN_ROOT=str(output)))
                    stack.enter_context(patch.object(runner, 'certificates', side_effect=fake_certificates))
                    stack.enter_context(patch.object(runner.BrokerFixture, 'format'))
                    stack.enter_context(patch.object(runner.BrokerFixture, 'start'))
                    command = stack.enter_context(patch.object(runner.BrokerFixture, 'command',
                        side_effect=[None, RuntimeError('second topic creation failed')] if not unreadable else [None, None]))
                    stack.enter_context(patch.object(runner, 'OpaqueFaultProxy', return_value=proxy))
                    stack.enter_context(patch.object(runner, 'FileControl', return_value=control))
                    popen = stack.enter_context(patch.object(runner.subprocess, 'Popen', side_effect=fake_test))
                    stack.enter_context(patch.object(runner.sys, 'argv', ['runner']))
                    stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                    stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
                    if unreadable:
                        stack.enter_context(patch.object(runner, 'read_diagnostics',
                                                         side_effect=OSError('injected log read failure')))
                        with self.assertRaisesRegex(RuntimeError, 'Incomplete fixture cleanup'):
                            runner.main()
                    else:
                        self.assertEqual(runner.main(), 1)
                self.assertEqual(command.call_count, 2 if resolved else 0)
                self.assertEqual(popen.call_count, (2 if unreadable else 1) if resolved else 0)
                for arguments, cwd in launches:
                    self.assertEqual(cwd, checkout)
                    self.assertIn(f'--packages={package_config}', arguments)
                    self.assertIn(str(test_entry), arguments)
                result = json.loads(next(output.glob('result-*.json')).read_text())
                self.assertEqual(result['exit_code'], 1)
                if unreadable:
                    self.assertIn('injected log read failure', str(result['cleanup_errors']))
                elif resolved:
                    self.assertEqual(result['test_exit_code'], 1)
                    self.assertIn('second topic creation failed', result['failure'])
                else:
                    self.assertEqual(result['stage'], 'startup')
                    self.assertIn('FileNotFoundError', result['failure'])
                    self.assertIn('package_config.json', result['failure'])
                for field in ('private_fixture_removed', 'owned_process_stopped',
                              'test_process_stopped', 'proxy_stopped', 'controls_stopped',
                              'ports_reusable', 'retained_output_secret_scan_passed'):
                    self.assertTrue(result[field], field)
                self.assertEqual(list(output.glob('private.*')), [])
                retained = ''.join(path.read_text() for path in output.iterdir())
                for value in captured.values():
                    self.assertNotIn(value, retained)
                if resolved and not unreadable:
                    self.assertIn('[REDACTED]', retained)
            finally:
                for signum, handler in saved.items():
                    signal.signal(signum, handler)

    def test_second_topic_failure_does_not_reuse_first_mode_success(self):
        self.exercise()

    def test_unreadable_diagnostics_does_not_skip_private_cleanup(self):
        self.exercise(unreadable=True)

    def test_missing_package_config_stops_before_topic_or_test_commands(self):
        self.exercise(resolved=False)


if __name__ == '__main__':
    unittest.main()
