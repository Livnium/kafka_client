import contextlib
import io
import json
import pathlib
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

import run_retry_waiter_probe as runner


class ReportTest(unittest.TestCase):
    def row(self, mode='legacy', iterations=10):
        return dict(passed=True, guard_failure=None, failure=None,
                    after_close_waiters=0, network_used=False,
                    mode=mode, iterations=iterations, retries=iterations,
                    operations=2 * iterations, source_method='_safeRetry',
                    synthetic_operation=True, sdk_private_mirrors=True,
                    dart_version='3.12.0 (test fixture)',
                    rss_limit_bytes=runner.RSS_LIMIT, rss_sample_interval_ms=100,
                    wall_limit_seconds=120, elapsed_ms=100, close_elapsed_us=1,
                    initial_rss_bytes=1000, sampled_peak_rss_bytes=2000,
                    final_rss_bytes=1500,
                    samples=[dict(completed_retries=n,
                                  retained_close_listeners=n if mode == 'legacy' else 0,
                                  active_removable_waiters=0, rss_bytes=1000,
                                  elapsed_ms=1)
                             for n in runner.expected_checkpoints(iterations)])

    def report(self, row=None):
        return 'RETRY_WAITER_PROBE ' + json.dumps(self.row() if row is None else row) + '\n'

    def valid(self, row, mode='legacy', iterations=10):
        return runner.check_report(self.report(row), 0, None, mode, iterations)

    def test_complete_reports_at_checkpoint_boundaries(self):
        for mode in ('legacy', 'removable'):
            for iterations in (1, 9, 10, 11, 100, 1000, 10000):
                self.assertTrue(self.valid(self.row(mode, iterations), mode, iterations))

    def test_requires_exact_success_report(self):
        for text in ['', 'RETRY_WAITER_PROBE {', self.report() * 2,
                     self.report([]), self.report(True)]:
            self.assertFalse(runner.check_report(text, 0, None, 'legacy', 10))

    def test_guard_and_exit_are_independent_gates(self):
        for status, guard in ((1, None), (None, None), (0, 'RSS limit')):
            self.assertFalse(runner.check_report(self.report(), status, guard, 'legacy', 10))

    def test_every_required_field_is_required(self):
        for key in self.row():
            row = self.row()
            del row[key]
            self.assertFalse(self.valid(row), key)

    def test_wrong_fields_and_types_fail(self):
        for key, value in [('passed', False), ('failure', 'bad'),
                           ('guard_failure', 'bad'), ('after_close_waiters', 1),
                           ('after_close_waiters', False), ('network_used', True),
                           ('iterations', True), ('iterations', 10.0),
                           ('retries', 9), ('operations', 19), ('mode', 'removable'),
                           ('source_method', 'other'), ('synthetic_operation', 1),
                           ('sdk_private_mirrors', False), ('dart_version', ''),
                           ('rss_limit_bytes', runner.RSS_LIMIT + 1),
                           ('rss_sample_interval_ms', 200), ('wall_limit_seconds', 121),
                           ('elapsed_ms', 120001), ('close_elapsed_us', -1),
                           ('final_rss_bytes', runner.RSS_LIMIT + 1),
                           ('final_rss_bytes', 2001),
                           ('sampled_peak_rss_bytes', 999)]:
            row = self.row()
            row[key] = value
            self.assertFalse(self.valid(row), (key, value))
        self.assertFalse(self.valid(self.row(), 'removable'))
        self.assertFalse(self.valid(self.row(), iterations=11))
        self.assertFalse(self.valid(self.row(), iterations=True))

    def test_bad_missing_or_duplicate_trajectory_fails(self):
        for samples in (None, {}, [], [None], self.row()['samples'][:-1],
                        self.row()['samples'] + [self.row()['samples'][-1]]):
            row = self.row()
            row['samples'] = samples
            self.assertFalse(self.valid(row))
        for key in self.row()['samples'][0]:
            row = self.row()
            del row['samples'][0][key]
            self.assertFalse(self.valid(row), key)
        for key, value in [('completed_retries', True), ('completed_retries', 9),
                           ('retained_close_listeners', 0), ('active_removable_waiters', 1),
                           ('rss_bytes', 2001), ('elapsed_ms', 101), ('elapsed_ms', 0)]:
            row = self.row()
            row['samples'][-1][key] = value
            self.assertFalse(self.valid(row), (key, value))
        row = self.row('removable')
        row['samples'][-1]['retained_close_listeners'] = 10
        self.assertFalse(self.valid(row, 'removable'))

    def test_limits_are_fixed(self):
        self.assertEqual(runner.RSS_LIMIT, 805306368)
        self.assertEqual(runner.WALL_LIMIT, 120)


class CleanupTest(unittest.TestCase):
    @patch.object(runner.os, 'killpg', side_effect=ProcessLookupError)
    def test_kill_race_requires_confirmed_exit(self, kill):
        process = Mock(pid=123)
        process.poll.side_effect = [None, 0]
        runner.stop_owned_process(process)
        kill.assert_called_once_with(123, signal.SIGTERM)
        process.poll.side_effect = [None, None]
        with self.assertRaises(ProcessLookupError):
            runner.stop_owned_process(process)

    @patch.object(runner.os, 'killpg')
    def test_escalates_only_owned_group_on_timeout(self, kill):
        process = Mock(pid=123)
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired('fixture', 5), 0]
        runner.stop_owned_process(process)
        self.assertEqual(kill.call_args_list,
                         [unittest.mock.call(123, signal.SIGTERM),
                          unittest.mock.call(123, signal.SIGKILL)])

    @patch.object(runner.os, 'killpg')
    def test_finished_leader_is_not_signaled(self, kill):
        process = Mock(pid=123)
        process.poll.return_value = 0
        runner.stop_owned_process(process)
        kill.assert_not_called()


class FinalGuardTest(unittest.TestCase):
    def run_fake(self, resources, clock):
        process = Mock(pid=123)
        process.poll.return_value = 0
        def launch(command, stdout, **kwargs):
            stdout.write(ReportTest().report())
            return process
        with tempfile.TemporaryDirectory() as temporary:
            target = pathlib.Path(temporary) / 'run'
            with patch.object(runner, 'read_resources', side_effect=resources), \
                    patch.object(runner.time, 'monotonic', side_effect=clock), \
                    patch.object(runner.subprocess, 'Popen', side_effect=launch), \
                    contextlib.redirect_stdout(io.StringIO()):
                code = runner.main([str(target), '--', '/fake/dart',
                                    'tool/measure_retry_waiters.dart', 'legacy', '10'])
            return code, json.loads((target / 'result.json').read_text())

    def test_complete_success_preserves_provenance(self):
        healthy = dict(disk_free_bytes=3 * 1024**3, mem_available_bytes=2 * 1024**3)
        code, result = self.run_fake([healthy, healthy], [0, 1, 1])
        self.assertEqual(code, 0)
        self.assertTrue(result['passed'])
        self.assertIn('tool/measure_retry_waiters.dart', result['file_sha256_before_run'])
        self.assertEqual(len(result['host_resources_first_last']), 2)

    def test_final_resource_check_catches_immediately_exited_child(self):
        healthy = dict(disk_free_bytes=3 * 1024**3, mem_available_bytes=2 * 1024**3)
        unhealthy = dict(healthy, mem_available_bytes=0)
        code, result = self.run_fake([healthy, unhealthy], [0, 1, 1])
        self.assertEqual(code, 1)
        self.assertIn('MemAvailable', result['guard_failure'])
        self.assertTrue(result['owned_process_stopped'])

    def test_final_wall_check_catches_immediately_exited_child(self):
        healthy = dict(disk_free_bytes=3 * 1024**3, mem_available_bytes=2 * 1024**3)
        code, result = self.run_fake([healthy, healthy], [0, 121, 121])
        self.assertEqual(code, 1)
        self.assertIn('wall deadline', result['guard_failure'])

    def test_invalid_cli_does_not_launch(self):
        with patch.object(runner.subprocess, 'Popen') as launch, \
                contextlib.redirect_stderr(io.StringIO()):
            for tail in (['invalid', '10'], ['legacy', '10001'], ['legacy', 'x']):
                self.assertEqual(runner.main(['unused', '--', '/fake/dart', 'probe', *tail]), 64)
            launch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
