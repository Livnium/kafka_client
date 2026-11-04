"""No broker or child process is started by these guard regressions."""
import pathlib
import signal
import tempfile
import unittest
from unittest.mock import patch
import check_guarded_soak_cleanup as cleanup
import run_guarded_soak as soak


class GuardTest(unittest.TestCase):
    def test_cleanup_check_rejects_failure_without_actual_interruption(self):
        receipt = {'exit_code': 1, 'owned_process_stopped': True,
                   'data_removed': True, 'control_removed': True,
                   'cleanup_errors': []}
        injection = {'progress': {'records': 8}, 'termination_called': True,
                     'interruption_observed': False}
        self.assertFalse(cleanup.check_passed(2, injection, receipt, True))
        injection['interruption_observed'] = True
        self.assertTrue(cleanup.check_passed(2, injection, receipt, True))

    def test_cleanup_check_requires_positive_healthy_progress(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            run = root / 'run.owned'
            run.mkdir()
            log = run / 'tests.log'
            log.write_text('SOAK_PROGRESS {"records":0,"health_failure":null}\n'
                           'SOAK_PROGRESS {"records":8,"health_failure":"failed"}\n')
            self.assertIsNone(cleanup.active_progress(root))
            with log.open('a') as output:
                output.write('SOAK_PROGRESS {"records":16,"health_failure":null}\n')
            self.assertEqual(cleanup.active_progress(root)['records'], 16)

    def test_cleanup_check_ignores_incomplete_progress(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            run = root / 'run.owned'
            run.mkdir()
            (run / 'tests.log').write_text('SOAK_PROGRESS {"records":')
            self.assertIsNone(cleanup.active_progress(root))

    def test_boundary_and_low_resources(self):
        self.assertIsNone(soak.violation({'disk_free_bytes': 2 * soak.GIB,
                                         'mem_available_bytes': soak.GIB}))
        for sample in ({'disk_free_bytes': 2 * soak.GIB - 1, 'mem_available_bytes': soak.GIB},
                       {'disk_free_bytes': 2 * soak.GIB, 'mem_available_bytes': soak.GIB - 1}):
            self.assertIsNotNone(soak.violation(sample))

    def test_monitor_signals_only_itself(self):
        guard = soak.Guard(pathlib.Path('.'), sample=lambda _: {
            'disk_free_bytes': 0, 'mem_available_bytes': soak.GIB})
        with patch.object(guard.stopping, 'wait', return_value=False), patch.object(soak.os, 'kill') as kill:
            guard.watch()
        kill.assert_called_once_with(soak.os.getpid(), signal.SIGTERM)
        self.assertIsNotNone(guard.reason)

    def test_sampling_failure_fails_closed(self):
        def broken(_):
            raise OSError('unavailable')
        guard = soak.Guard(pathlib.Path('.'), sample=broken)
        self.assertFalse(guard.check())
        self.assertIn('sampling failed', guard.reason)

    def test_preflight_refuses_runner(self):
        with patch.object(soak.Guard, 'check', return_value=False), patch.object(soak, 'run_operational_integration') as runner:
            self.assertEqual(soak.main(runner=runner.main), 2)
            runner.main.assert_not_called()

    def test_tripped_guard_overrides_success(self):
        original_init = soak.Guard.__init__
        def initialize(guard, *args, **kwargs):
            original_init(guard, *args, **kwargs)
            guard.reason = 'simulated low disk'
        with patch.object(soak.Guard, '__init__', initialize), patch.object(soak.Guard, 'check', return_value=True), patch.object(soak.Guard, 'watch'):
            self.assertEqual(soak.main(runner=lambda: 0), 2)

    def test_guard_cannot_silently_pass(self):
        with patch.object(soak.Guard, 'check', return_value=True), patch.object(soak.Guard, 'watch') as watch:
            def run():
                raise KeyboardInterrupt()
            self.assertEqual(soak.main(runner=run), 130)
            self.assertTrue(watch.called)


if __name__ == '__main__':
    unittest.main()
