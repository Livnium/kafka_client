import contextlib
import io
import json
import os
from pathlib import Path
import sys
import signal
from unittest import mock
import unittest

import run_bootstrap_tls_fallback_integration as runner

class RunnerTests(unittest.TestCase):
    def test_child_failure_redacted_and_removed(self):
        seen = []
        def fixture(path):
            seen.append(path)
            self.assertEqual(path.stat().st_mode & 0o777, 0o700)
            (path / 'private.key').write_text('private-test-material')
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = runner.execute([sys.executable, '-c', "import os,sys; print(os.environ['KAFKA_BOOTSTRAP_TLS_PASSWORD']); sys.exit(7)"], fixture_builder=fixture, guard_fn=lambda:None)
        self.assertFalse(result['ok'])
        self.assertEqual(result['exit_code'],7)
        self.assertTrue(result['cleaned'])
        self.assertFalse(seen[0].exists())
        self.assertEqual(output.getvalue(),'')
        self.assertNotIn(str(seen[0]),str(result))

    def test_generation_failure_removes_partial_keys(self):
        seen = []
        def fixture(path):
            seen.append(path)
            (path / 'private.key').write_text('private-test-material')
            raise RuntimeError('secret fixture failure')
        result = runner.execute(['not-executed'], fixture_builder=fixture, guard_fn=lambda:None)
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'],'certificates')
        self.assertTrue(result['cleaned'])
        self.assertFalse(seen[0].exists())

    def test_json_status_only_no_secret_fields(self):
        command = "import json; print(json.dumps(dict(type='testDone', testID=9, result='error', hidden=False, secret='do-not-retain'))); print('do-not-retain')"
        result = runner.execute([sys.executable, '-c', command], fixture_builder=lambda p:None, guard_fn=lambda:None)
        self.assertNotIn('do-not-retain',str(result))
        self.assertEqual(result['tests']['failed_test_ids'],[9])
        self.assertEqual(result['tests']['failed'],1)
        self.assertTrue(result['cleaned'])

    def test_complete_allowlisted_matrix_required(self):
        events = []
        for index, name in enumerate(sorted(runner.REPORTER_NAMES)):
            events.extend([dict(type='testStart', test=dict(id=index, name=name)),
                           dict(type='testDone', testID=index, result='success', hidden=False, skipped=False)])
        events.append(dict(type='done', success=True))
        code = 'print(' + repr('\n'.join(json.dumps(event) for event in events)) + ')'
        result = runner.execute([sys.executable, '-c', code], fixture_builder=lambda p:None, guard_fn=lambda:None)
        self.assertTrue(result['ok'])
        self.assertTrue(result['complete_test_matrix'])
        result = runner.execute([sys.executable, '-c', 'pass'], fixture_builder=lambda p:None, guard_fn=lambda:None)
        self.assertFalse(result['ok'])
        self.assertFalse(result['complete_test_matrix'])

    def test_sigterm_cleans_child_fixture_and_restores_handlers(self):
        seen = []
        previous = signal.getsignal(signal.SIGTERM)
        def fixture(path):
            seen.append(path)
            (path / 'private.key').write_text('owned synthetic material')
        code = 'import os,signal,time; time.sleep(.05); os.kill(os.getppid(),signal.SIGTERM); time.sleep(20)'
        result = runner.execute([sys.executable, '-c', code], fixture_builder=fixture, guard_fn=lambda:None)
        self.assertEqual(result['stage'],'interrupted')
        self.assertFalse(result['ok'])
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['process_finished'])
        self.assertTrue(result['reader_finished'])
        self.assertTrue(result['reader_ok'])
        self.assertFalse(seen[0].exists())
        self.assertEqual(signal.getsignal(signal.SIGTERM),previous)

    def test_live_root_rss_unreadable_fails_closed(self):
        with mock.patch.object(Path,'read_text',side_effect=PermissionError), mock.patch.object(runner,'process_exists',return_value=True):
            with self.assertRaises(RuntimeError):
                runner.group_rss(os.getpid())
        with mock.patch.object(Path,'read_text',return_value='State: S (sleeping)\n'), mock.patch.object(runner,'process_exists',return_value=True):
            with self.assertRaises(RuntimeError):
                runner.group_rss(os.getpid())
        with mock.patch.object(runner,'group_rss',side_effect=RuntimeError):
            result = runner.execute([sys.executable,'-c','import time; time.sleep(20)'], fixture_builder=lambda p:None, guard_fn=lambda:None)
        self.assertEqual(result['stage'],'rss_unavailable')
        self.assertFalse(result['ok'])
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['reader_finished'])

    def test_child_exit_during_rss_sampling_preserves_complete_matrix(self):
        events = []
        for index, name in enumerate(sorted(runner.REPORTER_NAMES)):
            events.extend([dict(type='testStart', test=dict(id=index, name=name)),
                           dict(type='testDone', testID=index, result='success', hidden=False, skipped=False)])
        events.append(dict(type='done', success=True))
        payload = repr('\n'.join(json.dumps(event) for event in events))
        real_popen = runner.subprocess.Popen
        children = []
        def capture(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            children.append(process)
            return process
        def disappear_during_sample(pid):
            # The sample started with a live owned child. Force its exact exit
            # before raising the same unavailable-/proc error as the race.
            self.assertEqual(pid, children[0].pid)
            children[0].wait(timeout=5)
            raise RuntimeError('rss unavailable')
        code = 'import time; time.sleep(.05); print(' + payload + ')'
        with mock.patch.object(runner.subprocess, 'Popen', side_effect=capture), \
                mock.patch.object(runner, 'group_rss', side_effect=disappear_during_sample) as sample:
            result = runner.execute([sys.executable, '-c', code],
                                    fixture_builder=lambda p: None, guard_fn=lambda: None)
        sample.assert_called_once()
        self.assertTrue(result['complete_test_matrix'])
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['process_finished'])
        self.assertTrue(result['reader_finished'])
        self.assertTrue(result['ok'], result)
        self.assertEqual(result['exit_code'], 0)
        self.assertEqual(result['stage'], 'child')

    def test_nonzero_child_exit_during_rss_sampling_stays_failure(self):
        real_popen = runner.subprocess.Popen
        children = []
        def capture(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            children.append(process)
            return process
        def disappear_during_sample(pid):
            self.assertEqual(pid, children[0].pid)
            children[0].wait(timeout=5)
            raise RuntimeError('rss unavailable')
        with mock.patch.object(runner.subprocess, 'Popen', side_effect=capture), \
                mock.patch.object(runner, 'group_rss', side_effect=disappear_during_sample) as sample:
            result = runner.execute([sys.executable, '-c',
                                     'import sys,time; time.sleep(.05); sys.exit(7)'],
                                    fixture_builder=lambda p: None, guard_fn=lambda: None)
        sample.assert_called_once()
        self.assertFalse(result['ok'])
        self.assertEqual(result['exit_code'], 7)
        self.assertEqual(result['stage'], 'child')
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['process_finished'])

    def test_uncertain_child_exit_during_rss_sampling_stays_failure(self):
        real_popen = runner.subprocess.Popen
        children = []
        def capture(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            children.append(process)
            return process
        def unavailable_and_poll_uncertain(pid):
            self.assertEqual(pid, children[0].pid)
            # The initial live check worked; only the fresh race-resolution
            # poll fails. Cleanup still uses the real child's wait method.
            children[0].poll = mock.Mock(side_effect=OSError('private-poll'))
            raise RuntimeError('rss unavailable')
        with mock.patch.object(runner.subprocess, 'Popen', side_effect=capture), \
                mock.patch.object(runner, 'group_rss', side_effect=unavailable_and_poll_uncertain):
            result = runner.execute([sys.executable, '-c', 'import time; time.sleep(20)'],
                                    fixture_builder=lambda p: None, guard_fn=lambda: None)
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'], 'rss_unavailable')
        self.assertNotIn('exit_code', result)
        self.assertNotIn('private-poll', str(result))
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['process_finished'])
        self.assertTrue(result['reader_finished'])

    def test_cleanup_failure_is_fixed_failure(self):
        seen = []
        real_remove = runner.shutil.rmtree
        try:
            with mock.patch.object(runner.shutil,'rmtree',side_effect=OSError('private path')):
                result = runner.execute([sys.executable,'-c','pass'], fixture_builder=lambda p:seen.append(p), guard_fn=lambda:None)
            self.assertFalse(result['ok'])
            self.assertFalse(result['cleaned'])
            self.assertEqual(result['stage'],'fixture_cleanup_failed')
            self.assertNotIn('private path',str(result))
        finally:
            for path in seen:
                real_remove(path)

    def test_stop_signals_owned_group_once(self):
        process = mock.Mock(pid=12345,returncode=0)
        process._fixture_stopped = False
        with mock.patch.object(runner.os,'killpg') as kill:
            self.assertTrue(runner.stop(process))
            self.assertTrue(runner.stop(process))
            kill.assert_called_once_with(12345,signal.SIGKILL)

    def test_timeout_removes_fixture(self):
        result = runner.execute([sys.executable,'-c','import time; time.sleep(20)'], fixture_builder=lambda p:None, guard_fn=lambda:None, timeout=.05)
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'],'timeout')
        self.assertTrue(result['cleaned'])

if __name__ == '__main__':
    unittest.main()
