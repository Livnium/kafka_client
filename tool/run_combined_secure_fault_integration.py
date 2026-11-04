#!/usr/bin/env python3
"""Owned three-node secure Kafka plus six bounded opaque loopback proxies.

Only synthetic ephemeral credentials/certificates; all private fixture files are
removed. Plaintext controller/admin/interbroker plumbing is local-only. Requires
an independently verified Kafka distribution and resolved local Dart packages.
No resolver, publication, external service or capacity certification is involved.
KAFKA_COMBINED_MODES optionally isolates tls or sasl; default tls,sasl stops at
the first failing mode. Isolated runs do not replace the default combined result.
"""
import collections
import json
import os
import pathlib
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse

from kafka_distribution import selected_distribution

from run_cluster_integration import ROOT, terminate
from run_guarded_soak import Guard
from run_operational_integration import FileControl
from run_secure_cluster_integration import SecureCluster, certificates
from run_tls_network_fault_integration import OpaqueFaultProxy, read_diagnostics


class ProxiedCluster(SecureCluster):
    def __init__(self, home, run, ports, username, password, store_password):
        super().__init__(home, run, ports[:12], username, password, store_password)
        for node in range(1, 4):
            path = run / f'node-{node}/server.properties'
            config = path.read_text()
            old = (f'advertised.listeners=SSL://localhost:{ports[node-1]},'
                   f'SASL_SSL://localhost:{ports[node+2]},')
            new = (f'advertised.listeners=SSL://localhost:{ports[node+11]},'
                   f'SASL_SSL://localhost:{ports[node+14]},')
            if config.count(old) != 1:
                raise RuntimeError('Secure advertised listener rewrite failed')
            path.write_text(config.replace(old, new, 1))

    def command(self, name, *args):
        # CLI JVMs are owned too, including during host-guard interruption.
        process = None
        try:
            process = subprocess.Popen([str(self.home / 'bin' / name), *args],
                env=dict(self.env, LOG_DIR=str(self.run / 'cli-logs')),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                start_new_session=True)
            output, _ = process.communicate(timeout=60)
            with (self.run / 'commands.log').open('a') as log:
                log.write(f'{name} {args}\n{output}\n')
            if process.returncode:
                raise subprocess.CalledProcessError(process.returncode, [name])
            return output
        finally:
            terminate(process)


class CombinedControl:
    def __init__(self, cluster, proxies, lock=None):
        self.cluster, self.proxies = cluster, proxies
        self.lock = lock or threading.RLock()
        self.events = collections.deque(maxlen=128)
        self.closing = threading.Event()

    def snapshot(self):
        return dict(nodes={str(node): dict(
            running=node in self.cluster.processes and self.cluster.processes[node].poll() is None,
            generation=self.cluster.generations.get(node, 0)) for node in range(1, 4)},
            proxies=[dict(node=node, **proxy.snapshot())
                     for node, proxy in enumerate(self.proxies, 1)])

    def control(self, action):
        if not isinstance(action, str) or action not in (
                'status', 'pass', 'blackhole', 'fragment', 'disconnect',
                'crash:1', 'crash:2', 'crash:3', 'start:1', 'start:2', 'start:3'):
            raise ValueError('Only bounded transport actions and owned nodes 1..3 supported')
        with self.lock:
            if self.closing.is_set():
                raise RuntimeError('Fixture control is closing')
            if ':' in action:
                operation, text = action.split(':')
                node = int(text)
                if operation == 'crash':
                    self.cluster.stop(node, crash=True)
                else:
                    self.cluster.start(node)
                    self.cluster.ready([node])
            elif action != 'status':
                for proxy in self.proxies:
                    proxy.control(action)
            state = self.snapshot()
            if action != 'status':
                self.events.append(dict(action=action, timestamp=time.time(), state=state))
            return dict(ok=True, action=action, **state, events=list(self.events))


class BoundedFileControl(FileControl):
    def shutdown(self):
        self.stopping.set()
        # start's readiness budget is 75s; include the process-start/IPC margin.
        self.thread.join(timeout=90)
        if self.thread.is_alive():
            raise RuntimeError('Combined control did not stop')


class ResourceGuard:
    """Fixed host limits every 5 s and owned test RSS every 100 ms; fail closed."""
    MAX_RSS = 768 * 1024 ** 2

    def __init__(self, path, process, notify=None):
        self.host = Guard(path)
        self.process = process
        self.notify = notify or (lambda: os.kill(os.getpid(), signal.SIGTERM))
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.watch, name='combined-resource-guard')
        self.reason = None
        self.peak_rss = 0

    def rss_check(self):
        process = self.process()
        if process is None or process.poll() is not None:
            return
        try:
            fields = dict(line.split(':', 1) for line in
                          pathlib.Path(f'/proc/{process.pid}/status').read_text().splitlines()
                          if ':' in line)
            rss = int(fields['VmRSS'].split()[0]) * 1024
        except (OSError, KeyError, ValueError):
            if process.poll() is not None:
                return
            raise RuntimeError('Owned test RSS sampling failed')
        self.peak_rss = max(self.peak_rss, rss)
        if rss > self.MAX_RSS:
            raise RuntimeError('Owned test RSS exceeded 768 MiB')

    def watch(self):
        next_host = time.monotonic() + 5
        while not self.stopping.wait(.1):
            try:
                if time.monotonic() >= next_host:
                    next_host = time.monotonic() + 5
                    if not self.host.check():
                        raise RuntimeError(self.host.reason)
                self.rss_check()
            except Exception as error:
                self.reason = str(error)
                self.notify()
                return

    def shutdown(self):
        self.stopping.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=6)
        if self.thread.is_alive():
            raise RuntimeError('Resource guard did not stop')


def selected_modes(value):
    """Explicit isolation only; never silently skip, reorder or retry a mode."""
    if value not in ('tls', 'sasl', 'tls,sasl'):
        raise ValueError('KAFKA_COMBINED_MODES must be tls, sasl, or tls,sasl')
    return value.split(',')


def resolved_command(arguments):
    """Default is resolved source, not a snapshot; pass a cached VM command explicitly."""
    if arguments and arguments[0] == '--':
        arguments = arguments[1:]
    if arguments:
        return arguments
    config = ROOT / '.dart_tool/package_config.json'
    package = next(p for p in json.loads(config.read_text())['packages'] if p['name'] == 'test')
    parsed = urllib.parse.urlparse(urllib.parse.urljoin(config.as_uri(), package['rootUri']))
    if parsed.scheme != 'file' or parsed.netloc not in ('', 'localhost'):
        raise RuntimeError('Test package must already resolve to a local file')
    entry = pathlib.Path(urllib.parse.unquote(parsed.path)) / 'bin/test.dart'
    if not entry.is_file():
        raise RuntimeError('Resolve test dependencies offline before running')
    return [os.environ.get('DART', 'dart'), '--disable-dart-dev', f'--packages={config}',
            str(entry), 'test/combined_secure_fault_integration_test.dart', '--reporter', 'expanded']


def scrub(value, private_values):
    for secret in private_values:
        value = value.replace(secret, '[REDACTED]')
    return value


def reusable_ports(ports):
    states = {}
    for port in ports:
        with socket.socket() as probe:
            try:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(('127.0.0.1', port))
                states[str(port)] = True
            except OSError:
                states[str(port)] = False
    return states


def main():
    os.umask(0o077)
    home, kafka_version = selected_distribution()
    root = pathlib.Path(os.environ.get('KAFKA_COMBINED_RUN_ROOT',
                                     str(ROOT / '.dart_tool/kafka-combined-secure-fault'))).resolve()
    root.mkdir(parents=True, exist_ok=True)
    run = pathlib.Path(tempfile.mkdtemp(prefix='private.', dir=root))
    result_path = root / f'result-{run.name}.json'
    username, password, store_password = 'u' + secrets.token_hex(8), secrets.token_hex(24), secrets.token_hex(24)
    private_values = [username, password, store_password, str(run)]
    cluster = process = None
    probes, ports, proxies, controls, adapters = [], [], [], [], []
    mode_results = {}
    requested_modes = []
    status, stage, failure = 1, 'initializing', None
    guard = ResourceGuard(root, lambda: process)
    prior_signals = {}

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'Signal {signum}')

    for sig in (signal.SIGINT, signal.SIGTERM):
        prior_signals[sig] = signal.signal(sig, interrupted)
    try:
        stage = 'mode-selection'
        requested_modes = selected_modes(os.environ.get('KAFKA_COMBINED_MODES', 'tls,sasl'))
        stage = 'resource-preflight'
        if not guard.host.check():
            guard.reason = guard.host.reason
            raise RuntimeError(guard.reason)
        guard.thread.start()
        command = resolved_command(sys.argv[1:])
        # direct TLS[0:3], SASL[3:6], admin[6:9], controller[9:12], proxies[12:18].
        for _ in range(18):
            probe = socket.socket()
            probes.append(probe)
            probe.bind(('127.0.0.1', 0))
            ports.append(probe.getsockname()[1])
        stage = 'certificates'
        certificates(run, store_password)
        cluster = ProxiedCluster(home, run, ports, username, password, store_password)
        for probe in probes:
            probe.close()
        shared_lock = threading.RLock()
        for offset, mode in ((0, 'tls'), (3, 'sasl')):
            for node in range(3):
                proxy = OpaqueFaultProxy(ports[12 + offset + node], ports[offset + node])
                proxies.append(proxy)
                proxy.thread.start()
            directory = run / mode
            directory.mkdir(mode=0o700)
            adapter = CombinedControl(cluster, proxies[offset:offset + 3], shared_lock)
            adapters.append(adapter)
            control = BoundedFileControl(directory, adapter)
            controls.append(control)
            control.thread.start()
        stage = 'startup'
        cluster.format()
        for node in range(1, 4):
            cluster.start(node)
        cluster.ready([1, 2, 3])
        with (run / 'tests.log').open('w') as output:
            for mode in requested_modes:
                offset = 0 if mode == 'tls' else 3
                stage = f'{mode}-restore'
                for proxy in proxies:
                    proxy.control('pass')
                    proxy.control('disconnect')
                for node in range(1, 4):
                    if cluster.processes[node].poll() is not None:
                        cluster.start(node)
                cluster.ready([1, 2, 3])
                topic = 'combined-' + run.name.replace('.', '-') + '-' + mode
                stage = f'{mode}-topic'
                cluster.command('kafka-topics.sh', '--bootstrap-server', f'127.0.0.1:{ports[6]}',
                    '--create', '--topic', topic, '--replica-assignment', '1:2:3,2:3:1,3:1:2',
                    '--config', 'min.insync.replicas=2')
                env = dict(os.environ, KAFKA_COMBINED_MODE=mode,
                    KAFKA_COMBINED_PORTS=','.join(map(str, ports[12 + offset:15 + offset])),
                    KAFKA_COMBINED_TOPIC=topic, KAFKA_COMBINED_CONTROL=str(run / mode),
                    KAFKA_COMBINED_CA=str(run / 'ca.pem'),
                    KAFKA_COMBINED_USERNAME=username, KAFKA_COMBINED_PASSWORD=password)
                stage = f'{mode}-tests'
                mode_results[mode] = dict(exit_code=None, completed=False)
                process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=output,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                status = process.wait(timeout=240)
                mode_results[mode] = dict(exit_code=status, completed=True)
                if status:
                    break
        stage = 'complete' if status == 0 else 'test-failure'
    except BaseException as error:
        # Preserve a nonzero test result rather than allowing later success to mask it.
        status = status if status else 1
        failure = scrub(f'{type(error).__name__}: {error}', private_values)
    finally:
        for sig in prior_signals:
            signal.signal(sig, signal.SIG_IGN)
        errors = []

        def attempt(label, operation):
            try:
                return operation()
            except BaseException as error:
                errors.append(scrub(f'{label}: {type(error).__name__}: {error}', private_values))
                return None

        # Stop the signal source before any final cleanup begins.
        attempt('resource guard', guard.shutdown)
        if guard.reason and failure is None:
            failure = scrub('Resource guard: ' + guard.reason, private_values)
            stage = 'resource-guard-failure'
        attempt('test process', lambda: terminate(process))
        # Prevent queued actions from starting after a readiness wait releases the lock.
        for adapter in adapters:
            adapter.closing.set()
        for control in controls:
            control.stopping.set()
        for control in controls:
            attempt('control', control.shutdown)
        for proxy in proxies:
            attempt('proxy', proxy.shutdown)
        if cluster:
            for node in range(1, 4):
                attempt(f'node {node}', lambda node=node: cluster.stop(node))
        for probe in probes:
            attempt('reserved socket', probe.close)
        output = attempt('test diagnostics', lambda: scrub(read_diagnostics(run / 'tests.log'), private_values)) or ''
        states = attempt('proxy snapshots', lambda: [dict(node=i % 3 + 1,
            mode='tls' if i < 3 else 'sasl', state=p.snapshot()) for i, p in enumerate(proxies)])
        events = attempt('control snapshots', lambda: [dict(mode=mode, events=list(adapter.events),
            closing=adapter.closing.is_set(), stopped=not control.thread.is_alive())
            for mode, adapter, control in zip(('tls', 'sasl'), adapters, controls)])
        # No diagnostic, snapshot or serialization failure can bypass key removal.
        attempt('private fixture removal', lambda: shutil.rmtree(run))
        stopped = cluster is None or all(p.poll() is not None for p in cluster.processes.values())
        test_stopped = process is None or process.poll() is not None
        proxy_stopped = all(not p.thread.is_alive() and not p.connections and not p.workers for p in proxies)
        controls_stopped = all(not c.thread.is_alive() for c in controls)
        port_states = attempt('port reuse', lambda: reusable_ports(ports)) or {}
        cleanup_ok = (stopped and test_stopped and proxy_stopped and controls_stopped and
                      not guard.thread.is_alive() and not run.exists() and len(port_states) == len(ports)
                      and all(port_states.values()) and not errors)
        exit_code = status if status else (1 if failure or guard.reason or not cleanup_ok else 0)
        result = dict(kafka_version=kafka_version, exit_code=exit_code, test_exit_code=status, stage=stage, failure=failure,
            requested_modes=requested_modes, mode_results=mode_results, generations=cluster.generations if cluster else {},
            nodes={str(node): dict(running=cluster is not None and node in cluster.processes
                and cluster.processes[node].poll() is None,
                generation=cluster.generations.get(node, 0) if cluster else 0)
                for node in range(1, 4)},
            owned_processes_stopped=stopped, test_process_stopped=test_stopped,
            proxy_stopped=proxy_stopped, controls_stopped=controls_stopped,
            private_fixture_removed=not run.exists(), ports=ports, port_reuse=port_states,
            ports_reusable=len(port_states) == len(ports) and all(port_states.values()),
            proxies=states, controls=events, cleanup_errors=errors,
            guard=dict(reason=guard.reason, host_reason=guard.host.reason,
                       last_metrics=guard.host.latest, peak_test_rss_bytes=guard.peak_rss,
                       stopped=not guard.thread.is_alive()))
        try:
            retained = json.dumps(result) + output
            if any(value in retained for value in private_values):
                raise RuntimeError('Sanitization failed; no result retained')
            result['retained_output_secret_scan_passed'] = True
            result_path.write_text(json.dumps(result, indent=2) + '\n')
            result_path.with_suffix('.txt').write_text(output)
            print(output, end='', flush=True)
            print(f'Sanitized result: {result_path}', flush=True)
        finally:
            for sig, previous in prior_signals.items():
                signal.signal(sig, previous)
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
