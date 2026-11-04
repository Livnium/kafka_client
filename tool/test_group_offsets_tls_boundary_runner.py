import contextlib
import io
import json
import sys
import unittest
from unittest import mock

import run_bootstrap_tls_fallback_integration as bootstrap
import run_group_offsets_tls_boundary_integration as runner


def events_for(names=None):
    events = []
    for index, name in enumerate(sorted(runner.REPORTER_NAMES if names is None else names)):
        events.extend([
            dict(type='testStart', test=dict(id=index, name=name)),
            dict(type='testDone', testID=index, result='success', hidden=False, skipped=False),
        ])
    events.append(dict(type='done', success=True))
    return events


def command_for(events):
    return [sys.executable, '-c', 'print(' + repr('\n'.join(json.dumps(e) for e in events)) + ')']


class GroupOffsetsTlsBoundaryRunnerTests(unittest.TestCase):
    def run_events(self, events):
        return runner.execute(command_for(events), fixture_builder=lambda p: None,
                              guard_fn=lambda: None)

    def test_complete_exact_nonzero_matrix_passes(self):
        self.assertEqual(len(runner.CASE_NAMES), 27)
        self.assertLessEqual(len(runner.CASE_NAMES), 64)
        result = self.run_events(events_for())
        self.assertTrue(result['ok'])
        self.assertTrue(result['complete_test_matrix'])
        self.assertEqual(result['expected_tests'], len(runner.CASE_NAMES))
        self.assertEqual({c['name'] for c in result['tests']['cases']}, runner.CASE_NAMES)
        self.assertTrue(result['cleaned'])

    def test_empty_output_and_done_only_never_pass(self):
        for events in ([], [dict(type='done', success=True)]):
            with self.subTest(events=events):
                result = self.run_events(events)
                self.assertFalse(result['ok'])
                self.assertFalse(result['complete_test_matrix'])
                self.assertTrue(result['cleaned'])

    def test_accidental_empty_allowlist_never_passes(self):
        with mock.patch.object(runner, 'CASE_NAMES', frozenset()), \
                mock.patch.object(runner._BASE, 'CASE_NAMES', frozenset()), \
                mock.patch.object(runner._BASE, 'REPORTER_NAMES', {}):
            result = self.run_events([dict(type='done', success=True)])
        self.assertFalse(result['ok'])

    def test_missing_case_fails(self):
        names = list(runner.REPORTER_NAMES)
        result = self.run_events(events_for(names[1:]))
        self.assertFalse(result['ok'])
        self.assertFalse(result['complete_test_matrix'])

    def test_duplicate_known_case_fails(self):
        names = sorted(runner.REPORTER_NAMES)
        names[-1] = names[0]
        result = self.run_events(events_for(names))
        self.assertFalse(result['ok'])
        self.assertFalse(result['complete_test_matrix'])

    def test_unknown_name_fails_without_retaining_name(self):
        names = sorted(runner.REPORTER_NAMES)
        names[-1] = 'private-untrusted-reporter-name'
        result = self.run_events(events_for(names))
        self.assertFalse(result['ok'])
        self.assertNotIn('private-untrusted-reporter-name', str(result))

    def test_extra_unknown_test_fails(self):
        result = self.run_events(events_for([*runner.REPORTER_NAMES, 'unknown-case']))
        self.assertFalse(result['ok'])
        self.assertNotIn('unknown-case', str(result))

    def test_skip_failure_or_reporter_false_fail(self):
        for mutation in ('skip', 'failure', 'reporter'):
            events = events_for()
            if mutation == 'skip':
                events[1]['skipped'] = True
            elif mutation == 'failure':
                events[1]['result'] = 'error'
                events[1]['message'] = 'private-error-detail'
            else:
                events[-1]['success'] = False
            with self.subTest(mutation=mutation):
                result = self.run_events(events)
                self.assertFalse(result['ok'])
                self.assertNotIn('private-error-detail', str(result))

    def test_baseline_runner_uses_independent_module_and_matrix(self):
        self.assertIsNot(bootstrap, runner._BASE)
        self.assertEqual(len(bootstrap.CASE_NAMES), 22)
        self.assertTrue(all(name.startswith('validated bootstrap TLS fallback ')
                            for name in bootstrap.REPORTER_NAMES))
        self.run_events(events_for())
        self.assertEqual(len(bootstrap.CASE_NAMES), 22)
        self.assertNotEqual(bootstrap.CASE_NAMES, runner.CASE_NAMES)

    def test_child_failure_suppresses_secrets_and_removes_keys(self):
        seen = []
        def fixture(path):
            seen.append(path)
            self.assertEqual(path.stat().st_mode & 0o777, 0o700)
            (path / 'private.key').write_text('owned synthetic key')
        output = io.StringIO()
        command = [sys.executable, '-c', "import os,sys; print(os.environ['KAFKA_BOOTSTRAP_TLS_PASSWORD']); print(os.environ['KAFKA_BOOTSTRAP_TLS_FIXTURE_DIR']); sys.exit(7)"]
        with contextlib.redirect_stdout(output):
            result = runner.execute(command, fixture_builder=fixture, guard_fn=lambda: None)
        self.assertFalse(result['ok'])
        self.assertEqual(result['exit_code'], 7)
        self.assertEqual(output.getvalue(), '')
        self.assertNotIn(str(seen[0]), str(result))
        self.assertNotIn('owned synthetic key', str(result))
        self.assertTrue(result['cleaned'])
        self.assertFalse(seen[0].exists())

    def test_certificate_builder_failure_removes_partial_keys(self):
        seen = []
        def fixture(path):
            seen.append(path)
            (path / 'private.key').write_text('owned synthetic key')
            raise RuntimeError('private-builder-error')
        result = runner.execute(['not-executed'], fixture_builder=fixture, guard_fn=lambda: None)
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'], 'certificates')
        self.assertNotIn('private-builder-error', str(result))
        self.assertTrue(result['cleaned'])
        self.assertFalse(seen[0].exists())

    def test_fixed_rss_guard_fails_closed_and_cleans(self):
        with mock.patch.object(runner._BASE, 'group_rss', return_value=768 * 1024**2 + 1):
            result = runner.execute([sys.executable, '-c', 'import time; time.sleep(20)'],
                                    fixture_builder=lambda p: None, guard_fn=lambda: None)
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'], 'rss_guard')
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['process_finished'])
        self.assertTrue(result['reader_finished'])

    def test_unavailable_rss_fails_closed(self):
        with mock.patch.object(runner._BASE, 'group_rss', side_effect=RuntimeError('private-rss')):
            result = runner.execute([sys.executable, '-c', 'import time; time.sleep(20)'],
                                    fixture_builder=lambda p: None, guard_fn=lambda: None)
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'], 'rss_unavailable')
        self.assertNotIn('private-rss', str(result))
        self.assertTrue(result['cleaned'])

    def test_host_guard_prevents_child_and_fixture(self):
        builder = mock.Mock()
        result = runner.execute(['not-executed'], fixture_builder=builder,
                                guard_fn=mock.Mock(side_effect=RuntimeError('private-guard')))
        builder.assert_not_called()
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'], 'guard')
        self.assertNotIn('private-guard', str(result))
        self.assertTrue(result['cleaned'])

    def test_timeout_removes_fixture(self):
        result = runner.execute([sys.executable, '-c', 'import time; time.sleep(20)'],
                                fixture_builder=lambda p: None, guard_fn=lambda: None, timeout=.05)
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'], 'timeout')
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['process_finished'])

    def test_child_exit_during_rss_sampling_preserves_complete_matrix(self):
        real_popen = runner._BASE.subprocess.Popen
        children = []
        def capture(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            children.append(process)
            return process
        def disappear_during_sample(pid):
            self.assertEqual(pid, children[0].pid)
            children[0].wait(timeout=5)
            raise RuntimeError('rss unavailable')
        command = command_for(events_for())
        command[-1] = 'import time; time.sleep(.05); ' + command[-1]
        with mock.patch.object(runner._BASE.subprocess, 'Popen', side_effect=capture), \
                mock.patch.object(runner._BASE, 'group_rss', side_effect=disappear_during_sample) as sample:
            result = runner.execute(command, fixture_builder=lambda p: None,
                                    guard_fn=lambda: None)
        sample.assert_called_once()
        self.assertTrue(result['complete_test_matrix'])
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['process_finished'])
        self.assertTrue(result['reader_finished'])
        self.assertTrue(result['ok'], result)
        self.assertEqual(result['exit_code'], 0)
        self.assertEqual(result['stage'], 'child')

    def test_rss_failure_waits_for_delayed_confirmed_exit(self):
        real_popen = runner._BASE.subprocess.Popen
        children = []
        def capture(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            children.append(child)
            return child
        def missing_before_exit(pid):
            self.assertEqual(pid, children[0].pid)
            self.assertIsNone(children[0].poll())
            raise RuntimeError('rss unavailable')
        # The child is genuinely live when sampled, then emits the complete
        # owned matrix and exits inside the bounded confirmation interval.
        command = command_for(events_for())
        command[-1] = 'import time; time.sleep(.02); ' + command[-1]
        with mock.patch.object(runner._BASE.subprocess, 'Popen', side_effect=capture), \
                mock.patch.object(runner._BASE, 'group_rss', side_effect=missing_before_exit) as sample:
            result = runner.execute(command, fixture_builder=lambda p: None,
                                    guard_fn=lambda: None)
        sample.assert_called_once()
        self.assertTrue(result['ok'], result)
        self.assertEqual(result['exit_code'], 0)
        self.assertEqual(result['stage'], 'child')
        self.assertTrue(result['complete_test_matrix'])
        self.assertTrue(result['cleaned'])

    def test_rss_failure_live_child_exceeds_confirmation_interval(self):
        real_popen = runner._BASE.subprocess.Popen
        observed_waits = []
        def capture(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            original_wait = child.wait
            def wait(timeout=None):
                observed_waits.append(timeout)
                return original_wait(timeout=timeout)
            child.wait = wait
            return child
        with mock.patch.object(runner._BASE.subprocess, 'Popen', side_effect=capture), \
                mock.patch.object(runner._BASE, 'group_rss', side_effect=RuntimeError('rss unavailable')) as sample:
            result = runner.execute([sys.executable, '-c', 'import time; time.sleep(20)'],
                                    fixture_builder=lambda p: None, guard_fn=lambda: None)
        sample.assert_called_once()
        self.assertEqual(observed_waits[0], .1)
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'], 'rss_unavailable')
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['process_finished'])
        self.assertTrue(result['reader_finished'])

    def test_rss_exit_confirmation_wait_error_fails_closed(self):
        real_popen = runner._BASE.subprocess.Popen
        observed_waits = []
        def capture(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            original_wait = child.wait
            def wait(timeout=None):
                observed_waits.append(timeout)
                if len(observed_waits) == 1:
                    raise OSError('private-wait-error')
                return original_wait(timeout=timeout)
            child.wait = wait
            return child
        with mock.patch.object(runner._BASE.subprocess, 'Popen', side_effect=capture), \
                mock.patch.object(runner._BASE, 'group_rss', side_effect=RuntimeError('rss unavailable')):
            result = runner.execute([sys.executable, '-c', 'import time; time.sleep(20)'],
                                    fixture_builder=lambda p: None, guard_fn=lambda: None)
        self.assertEqual(observed_waits[0], .1)
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'], 'rss_unavailable')
        self.assertNotIn('private-wait-error', str(result))
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['process_finished'])
        self.assertTrue(result['reader_finished'])

    def test_rss_exit_confirmation_wait_respects_remaining_deadline(self):
        real_popen = runner._BASE.subprocess.Popen
        observed_waits = []
        def capture(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            original_wait = child.wait
            def wait(timeout=None):
                observed_waits.append(timeout)
                return original_wait(timeout=timeout)
            child.wait = wait
            return child
        # Monotonic calls: deadline, host-guard scheduling, host-guard check,
        # timeout check, then the exact remaining-deadline calculation.
        with mock.patch.object(runner._BASE.subprocess, 'Popen', side_effect=capture), \
                mock.patch.object(runner._BASE.time, 'monotonic', side_effect=[100, 100, 100.02, 100.02, 100.03]), \
                mock.patch.object(runner._BASE, 'group_rss', side_effect=RuntimeError('rss unavailable')):
            result = runner.execute([sys.executable, '-c', 'import time; time.sleep(20)'],
                                    fixture_builder=lambda p: None, guard_fn=lambda: None, timeout=.05)
        self.assertAlmostEqual(observed_waits[0], .02)
        self.assertLess(observed_waits[0], .1)
        self.assertFalse(result['ok'])
        self.assertEqual(result['stage'], 'rss_unavailable')
        self.assertTrue(result['cleaned'])
        self.assertTrue(result['process_finished'])
        self.assertTrue(result['reader_finished'])

    def test_main_emits_only_fixed_status_envelope(self):
        output = io.StringIO()
        fake = dict(ok=False, cleaned=True, stage='child')
        with mock.patch.object(sys, 'argv', ['runner', '--', 'cached-command']), \
                mock.patch.object(runner, 'execute', return_value=fake), \
                contextlib.redirect_stdout(output):
            self.assertEqual(runner.main(), 1)
        prefix, payload = output.getvalue().strip().split(' ', 1)
        self.assertEqual(prefix, 'GROUP_OFFSETS_TLS_BOUNDARY_FIXTURE')
        self.assertEqual(json.loads(payload), fake)


if __name__ == '__main__':
    unittest.main()
