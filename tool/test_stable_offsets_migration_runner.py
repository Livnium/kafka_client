"""Fast migration-wrapper regressions. No Kafka/JVM/compiler processes."""
import pathlib
import tempfile
import unittest
from unittest.mock import patch

import run_stable_offsets_migration_integration as runner
import run_combined_secure_fault_integration as original
import test_combined_secure_fault_runner as inherited


class MigrationRunnerTests(unittest.TestCase):
    def run_mocked(self, *args, diagnostic_failure=False, **kwargs):
        def collect(cluster, values):
            if diagnostic_failure:
                raise OSError('injected broker diagnostic failure')
            return dict(sampler_stopped=True, effective_controlled_setting={
                str(n): ['replica.fetch.wait.max.ms=5000'] for n in range(1, 4)})
        with patch.object(inherited, 'runner', runner), patch.object(
                inherited.FakeCluster, 'collect_diagnostics', collect, create=True):
            return inherited.RunnerTests.run_mocked(self, *args, **kwargs)

    def test_mode_selection_and_explicit_command_preserved(self):
        self.assertEqual(runner.selected_modes('sasl'), ['sasl'])
        self.assertEqual(runner.selected_modes('tls,sasl'), ['tls', 'sasl'])
        with self.assertRaises(ValueError):
            runner.selected_modes('sasl,tls')
        self.assertEqual(runner.resolved_command(['--', '/cached/dart', 'test']),
                         ['/cached/dart', 'test'])
        self.assertIn('group_stable_offsets_migration_integration_test.dart',
                      pathlib.Path(runner.__file__).read_text())

    def test_owned_config_lag_only_safety_unchanged_and_sampler_stops(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = pathlib.Path(temporary)
            cluster = runner.ProxiedCluster(run, run, list(range(19000, 19018)),
                                            'synthetic-user', 'synthetic-pass', 'synthetic-store')
            try:
                for node in range(1, 4):
                    config = (run / f'node-{node}/server.properties').read_text()
                    self.assertEqual(config.count('replica.fetch.wait.max.ms=5000'), 1)
                    self.assertIn('unclean.leader.election.enable=false', config)
                    self.assertIn('min.insync.replicas=2', config)
                    self.assertIn('ssl.client.auth=none', config)
                self.assertEqual(cluster.env['KAFKA_HEAP_OPTS'], '-Xms128m -Xmx256m')
                cluster.diagnostic_targets.add(14)
                (run / 'node-1/broker-1.log').write_text(
                    'DEBUG __consumer_offsets-14 synthetic-pass synthetic-user\n'
                    'unrelated private line\n')
                result = cluster.collect_diagnostics(['synthetic-pass', 'synthetic-user'])
                self.assertTrue(result['sampler_stopped'])
                self.assertIn('[REDACTED]', result['broker_logs']['node-1/broker-1.log'])
                self.assertNotIn('synthetic-pass', str(result))
                self.assertNotIn('unrelated private line', str(result))
            finally:
                cluster.diagnostic_stop.set()
                cluster.diagnostic_thread.join(timeout=3)
                self.assertFalse(cluster.diagnostic_thread.is_alive())

    def test_constructor_failure_starts_no_sampler(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = pathlib.Path(temporary)
            with patch.object(runner.threading.Thread, 'start') as start, patch.object(
                    runner.pathlib.Path, 'read_text', side_effect=OSError('injected config failure')):
                with self.assertRaises(OSError):
                    runner.ProxiedCluster(run, run, list(range(19000, 19018)), 'u', 'p', 's')
                start.assert_not_called()

    def test_success_modes_and_cleanup(self):
        status, result, count = self.run_mocked((0,), modes='sasl')
        self.assertEqual((status, count), (0, 1))
        self.assertEqual(result['requested_modes'], ['sasl'])
        self.assertTrue(result['broker_diagnostics']['sampler_stopped'])
        self.assertEqual(result['cleanup_errors'], [])

    def test_failure_sticky_and_no_second_mode(self):
        status, result, count = self.run_mocked((19,))
        self.assertEqual((status, count), (19, 1))
        self.assertNotIn('sasl', result['mode_results'])

    def test_diagnostic_failure_preserves_private_removal(self):
        status, result, _ = self.run_mocked((0,), modes='sasl', diagnostic_failure=True)
        self.assertNotEqual(status, 0)
        self.assertTrue(result['private_fixture_removed'])
        self.assertTrue(any('broker diagnostics' in e for e in result['cleanup_errors']))

    def test_guard_limits_and_preflight_unchanged(self):
        self.assertEqual(runner.ResourceGuard.MAX_RSS, original.ResourceGuard.MAX_RSS)
        self.assertIs(runner.Guard, original.Guard)
        status, result, count = self.run_mocked(preflight=False)
        self.assertNotEqual(status, 0)
        self.assertEqual(count, 0)
        self.assertEqual(result['stage'], 'resource-preflight')
        status, result, _ = self.run_mocked((0,), modes='sasl', trip=True)
        self.assertNotEqual(status, 0)
        self.assertEqual(result['guard']['reason'], 'injected guard failure')


if __name__ == '__main__':
    unittest.main()
