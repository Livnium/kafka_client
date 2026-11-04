"""Fast isolated regressions: no Java, compiler, broker or child process."""
import contextlib
import json
import pathlib
import socket
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import run_combined_secure_fault_integration as runner


class FakeProcess:
    def __init__(self, code=None):
        self.code = code
    def poll(self):
        return self.code
    def wait(self, timeout=None):
        return self.code


class FakeCluster:
    def __init__(self, *args):
        self.processes, self.generations = {}, {}
    def start(self, node):
        self.processes[node] = FakeProcess()
        self.generations[node] = self.generations.get(node, 0) + 1
    def stop(self, node, crash=False):
        if node in self.processes:
            self.processes[node].code = -9 if crash else 0
    def ready(self, nodes):
        pass
    def format(self):
        pass
    def command(self, *args):
        pass


class FakeProxy:
    def __init__(self, *args):
        self.mode = 'pass'
        self.connections, self.workers = set(), set()
        self.thread = threading.Thread(target=lambda: None)
    def snapshot(self):
        return dict(ok=True, mode=self.mode, active=0, workers=0,
                    dropped_upstream=7, dropped_downstream=9)
    def control(self, action):
        if action != 'disconnect':
            self.mode = action
    def shutdown(self):
        self.thread.join()


class FakeFileControl:
    def __init__(self, *args):
        self.thread = threading.Thread(target=lambda: None)
        self.stopping = threading.Event()
    def shutdown(self):
        self.thread.join()


class ControlTests(unittest.TestCase):
    def setUp(self):
        self.cluster = FakeCluster()
        for node in range(1, 4):
            self.cluster.start(node)
        self.proxies = [FakeProxy() for _ in range(3)]
        self.control = runner.CombinedControl(self.cluster, self.proxies)

    def test_overlap_is_real_state_not_requested_action(self):
        self.control.control('blackhole')
        result = self.control.control('crash:2')
        self.assertTrue(result['ok'])
        self.assertEqual(result['action'], 'crash:2')
        self.assertEqual(result['nodes']['2'], dict(running=False, generation=1))
        self.assertTrue(result['nodes']['1']['running'])
        self.assertTrue(result['nodes']['3']['running'])
        self.assertEqual([p['node'] for p in result['proxies']], [1, 2, 3])
        self.assertTrue(all(p['mode'] == 'blackhole' for p in result['proxies']))
        self.assertTrue(all(p['dropped_downstream'] == 9 for p in result['proxies']))
        self.control.control('start:2')
        self.assertEqual(self.control.control('status')['nodes']['2'], dict(running=True, generation=2))
        self.assertFalse(result['nodes']['2']['running'])  # Earlier evidence is immutable.

    def test_validation_rejects_nonowned_nodes_and_unbounded_modes(self):
        for action in (None, {}, 1, 'start:0', 'crash:4', 'crash:01', 'drop_down', 'start:1;kill', 'CRASH:1'):
            with self.subTest(action=action), self.assertRaises(ValueError):
                self.control.control(action)
        self.assertTrue(all(p.poll() is None for p in self.cluster.processes.values()))

    def test_closing_rejects_queued_actions(self):
        self.control.closing.set()
        with self.assertRaisesRegex(RuntimeError, 'closing'):
            self.control.control('crash:1')
        self.assertIsNone(self.cluster.processes[1].poll())

    def test_all_three_transport_proxies_and_bounded_events(self):
        for i in range(140):
            result = self.control.control('fragment' if i % 2 else 'pass')
            self.assertTrue(all(p['mode'] == result['action'] for p in result['proxies']))
        self.assertEqual(len(result['events']), 128)
        self.assertEqual(len(self.control.control('status')['events']), 128)
        self.assertIn('timestamp', result['events'][0])

    def test_real_opaque_byte_drop_and_crashed_node_snapshot(self):
        # Plain synthetic bytes exercise the opaque transport without a broker.
        import time
        with socket.socket() as upstream:
            upstream.bind(('127.0.0.1', 0))
            upstream.listen(1)
            proxy = runner.OpaqueFaultProxy(0, upstream.getsockname()[1])
            proxy.thread.start()
            try:
                with socket.create_connection(proxy.listener.getsockname()) as client:
                    accepted, _ = upstream.accept()
                    with accepted:
                        adapter = runner.CombinedControl(self.cluster, [proxy, *self.proxies[1:]])
                        adapter.control('blackhole')
                        client.sendall(b'synthetic opaque outbound')
                        accepted.sendall(b'synthetic opaque inbound')
                        deadline = time.monotonic() + 2
                        while time.monotonic() < deadline:
                            state = proxy.snapshot()
                            if state['dropped_upstream'] and state['dropped_downstream']:
                                break
                            time.sleep(.01)
                        state = adapter.control('crash:2')
                        self.assertFalse(state['nodes']['2']['running'])
                        self.assertTrue(state['nodes']['1']['running'])
                        actual = state['proxies'][0]
                        self.assertEqual(actual['node'], 1)
                        self.assertEqual(actual['mode'], 'blackhole')
                        self.assertGreater(actual['dropped_upstream'], 0)
                        self.assertGreater(actual['dropped_downstream'], 0)
                        self.assertEqual(actual['active'], 1)
                        self.assertLessEqual(actual['workers'], 32)
            finally:
                proxy.shutdown()
            self.assertEqual(proxy.snapshot()['active'], 0)
            self.assertEqual(proxy.snapshot()['workers'], 0)

    def test_file_control_contract_and_invalid_request(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            control = runner.FileControl(directory, self.control)
            control.thread.start()
            try:
                import time
                for action, ok in (('blackhole', True), ('crash:2', True), ('crash:4', False)):
                    path = directory / 'one.request'
                    response = directory / 'one.response'
                    response.unlink(missing_ok=True)
                    path.write_text(json.dumps(dict(action=action)))
                    deadline = time.monotonic() + 2
                    while not response.exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    result = json.loads(response.read_text())
                    self.assertEqual(result['ok'], ok)
                    if action == 'crash:2':
                        self.assertFalse(result['nodes']['2']['running'])
                        self.assertTrue(all(p['mode'] == 'blackhole' for p in result['proxies']))
            finally:
                control.shutdown()


class RunnerTests(unittest.TestCase):
    def run_mocked(self, codes=(0, 0), diagnostics_error=False, preflight=True, trip=False, modes="tls,sasl"):
        with tempfile.TemporaryDirectory() as temporary, contextlib.ExitStack() as stack:
            root = pathlib.Path(temporary)
            home = root / 'kafka'
            (home / 'libs').mkdir(parents=True)
            (home / 'libs/kafka-clients-4.0.2.jar').touch()
            results = root / 'results'
            stack.enter_context(patch.dict(runner.os.environ, KAFKA_HOME=str(home), KAFKA_COMBINED_RUN_ROOT=str(results), KAFKA_COMBINED_MODES=modes))
            stack.enter_context(patch.object(runner.sys, 'argv', ['runner', '--', '/not-executed']))
            stack.enter_context(patch.object(runner, 'certificates'))
            stack.enter_context(patch.object(runner, 'ProxiedCluster', FakeCluster))
            stack.enter_context(patch.object(runner, 'OpaqueFaultProxy', FakeProxy))
            stack.enter_context(patch.object(runner, 'BoundedFileControl', FakeFileControl))
            stack.enter_context(patch.object(runner, 'terminate', lambda p: None))
            popen = stack.enter_context(patch.object(runner.subprocess, 'Popen', side_effect=[c if isinstance(c, Exception) else FakeProcess(c) for c in codes]))
            stack.enter_context(patch.object(runner.Guard, 'check', return_value=preflight))
            def watch(guard):
                if trip:
                    guard.reason = 'injected guard failure'
            stack.enter_context(patch.object(runner.ResourceGuard, 'watch', watch))
            if diagnostics_error:
                stack.enter_context(patch.object(runner, 'read_diagnostics', side_effect=OSError('injected diagnostics failure')))
            status = runner.main()
            result = json.loads(next(results.glob('result-*.json')).read_text())
            self.assertFalse(list(results.glob('private.*')))
            self.assertTrue(result['private_fixture_removed'])
            self.assertTrue(result['ports_reusable'])
            self.assertTrue(result['guard']['stopped'])
            self.assertTrue(result['owned_processes_stopped'])
            self.assertTrue(result['test_process_stopped'])
            return status, result, popen.call_count

    def test_both_modes_success(self):
        status, result, count = self.run_mocked()
        self.assertEqual(status, 0)
        self.assertEqual(count, 2)
        self.assertEqual(set(result['mode_results']), {'tls', 'sasl'})
        self.assertEqual(result['requested_modes'], ['tls', 'sasl'])

    def test_mode_parser_is_strict(self):
        for value, expected in (('tls', ['tls']), ('sasl', ['sasl']),
                                ('tls,sasl', ['tls', 'sasl'])):
            self.assertEqual(runner.selected_modes(value), expected)
        for invalid in ('', 'both', 'sasl,tls', 'tls,tls', 'tls, sasl', 'TLS',
                        'tls,sasl,', ' tls', None):
            with self.subTest(value=invalid), self.assertRaises(ValueError):
                runner.selected_modes(invalid)

    def test_isolated_modes_are_recorded_and_only_run_once(self):
        for mode in ('tls', 'sasl'):
            with self.subTest(mode=mode):
                status, result, count = self.run_mocked((0,), modes=mode)
                self.assertEqual(status, 0)
                self.assertEqual(count, 1)
                self.assertEqual(result['requested_modes'], [mode])
                self.assertEqual(set(result['mode_results']), {mode})

    def test_invalid_mode_fails_before_work_and_removes_private_directory(self):
        status, result, count = self.run_mocked(modes='sasl,tls')
        self.assertNotEqual(status, 0)
        self.assertEqual(count, 0)
        self.assertEqual(result['stage'], 'mode-selection')
        self.assertIn('KAFKA_COMBINED_MODES', result['failure'])
        self.assertEqual(result['mode_results'], {})

    def test_second_mode_failure_is_sticky(self):
        status, result, count = self.run_mocked((0, 17))
        self.assertEqual(status, 17)
        self.assertEqual(result['mode_results']['sasl']['exit_code'], 17)
        self.assertEqual(result['test_exit_code'], 17)

    def test_second_mode_spawn_exception_cannot_inherit_tls_success(self):
        status, result, count = self.run_mocked((0, OSError('injected second spawn failure')))
        self.assertNotEqual(status, 0)
        self.assertEqual(result['stage'], 'sasl-tests')
        self.assertIn('second spawn failure', result['failure'])
        self.assertEqual(result['mode_results']['tls']['exit_code'], 0)
        self.assertFalse(result['mode_results']['sasl']['completed'])

    def test_first_failure_stops_second_mode(self):
        status, result, count = self.run_mocked((19,))
        self.assertEqual(status, 19)
        self.assertEqual(count, 1)
        self.assertNotIn('sasl', result['mode_results'])

    def test_diagnostic_failure_cannot_skip_private_removal(self):
        status, result, count = self.run_mocked(diagnostics_error=True)
        self.assertNotEqual(status, 0)
        self.assertIn('test diagnostics', result['cleanup_errors'][0])

    def test_preflight_fail_closed(self):
        status, result, count = self.run_mocked(preflight=False)
        self.assertNotEqual(status, 0)
        self.assertEqual(count, 0)
        self.assertEqual(result['stage'], 'resource-preflight')

    def test_guard_trip_cannot_report_success(self):
        status, result, count = self.run_mocked(trip=True)
        self.assertNotEqual(status, 0)
        self.assertEqual(result['guard']['reason'], 'injected guard failure')

    def test_redaction(self):
        self.assertEqual(runner.scrub('hello user pass /private/key', ['user', 'pass', '/private']),
                         'hello [REDACTED] [REDACTED] [REDACTED]/key')

    def test_rss_limit_fixed_and_sampling_fail_closed(self):
        process = Mock(pid=12345)
        process.poll.return_value = None
        guard = runner.ResourceGuard(pathlib.Path('.'), lambda: process)
        with patch.object(pathlib.Path, 'read_text', return_value='VmRSS: 786433 kB\n'):
            with self.assertRaisesRegex(RuntimeError, 'exceeded 768'):
                guard.rss_check()
        with patch.object(pathlib.Path, 'read_text', side_effect=OSError('gone')):
            with self.assertRaisesRegex(RuntimeError, 'sampling failed'):
                guard.rss_check()
        process.poll.return_value = 0
        guard.rss_check()

    def test_guard_notifies_once_and_stops_on_sampling_failure(self):
        notify = Mock()
        guard = runner.ResourceGuard(pathlib.Path('.'), lambda: None, notify=notify)
        with patch.object(guard.stopping, 'wait', return_value=False), patch.object(
                guard, 'rss_check', side_effect=RuntimeError('sampling unavailable')):
            guard.watch()
        notify.assert_called_once_with()
        self.assertEqual(guard.reason, 'sampling unavailable')

    def test_advertised_endpoints_only_are_rewritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = pathlib.Path(temporary)
            ports = list(range(19000, 19018))
            cluster = runner.ProxiedCluster(run, run, ports, 'synthetic', 'synthetic', 'synthetic')
            for node in range(1, 4):
                config = (run / f'node-{node}/server.properties').read_text()
                self.assertIn(f'listeners=SSL://127.0.0.1:{ports[node-1]},', config)
                self.assertIn(f'advertised.listeners=SSL://localhost:{ports[node+11]},SASL_SSL://localhost:{ports[node+14]},', config)
                self.assertIn('inter.broker.listener.name=PLAINTEXT', config)
            self.assertEqual(cluster.env['KAFKA_HEAP_OPTS'], '-Xms128m -Xmx256m')


if __name__ == '__main__':
    unittest.main()
