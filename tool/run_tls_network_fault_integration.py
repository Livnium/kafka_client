#!/usr/bin/env python3
"""Owned Kafka TLS/SASL_TLS fault harness with a bounded opaque TCP proxy.

The proxy never decrypts TLS or counts Kafka frames. Direct secured bootstrap
and proxied secured advertised leaders isolate lost Produce acknowledgements.
One-day certificates and random credentials are synthetic local test fixtures.
Loopback plaintext admin/controller plumbing is not deployment guidance.
Private configs, logs, certificates, credentials and data are always removed.
Only sanitized test output and cleanup flags survive. No package resolution.
"""
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
import urllib.parse

from run_operational_integration import BrokerFixture, FileControl, ROOT
from run_secure_cluster_integration import certificates
from run_cluster_integration import terminate


class OpaqueFaultProxy:
    def __init__(self, port, upstream):
        self.upstream = upstream
        self.lock = threading.RLock()
        self.stopping = threading.Event()
        self.mode = 'pass'
        self.connections = set()
        self.workers = set()
        self.stats = dict(accepted=0, rejected=0, upstream_bytes=0, downstream_bytes=0,
                          dropped_upstream=0, dropped_downstream=0,
                          fragments=0, disconnects=0, peak_connections=0, peak_workers=0)
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', port))
        self.listener.listen(16)
        self.listener.settimeout(.2)
        self.thread = threading.Thread(target=self.accept, name='fault-accept')

    def snapshot(self):
        with self.lock:
            return dict(ok=True, mode=self.mode, active=len(self.connections),
                        workers=len(self.workers), **self.stats)

    @staticmethod
    def close_pair(pair):
        for connection in pair:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()

    def control(self, action):
        with self.lock:
            if action == 'disconnect':
                self.stats['disconnects'] += len(self.connections)
                for pair in list(self.connections):
                    self.close_pair(pair)
            elif action in ('pass', 'drop_down', 'blackhole', 'fragment'):
                self.mode = action
            elif action != 'status':
                raise ValueError('Unsupported proxy action')
            return self.snapshot()

    def accept(self):
        while not self.stopping.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            upstream = None
            try:
                with self.lock:
                    if len(self.connections) >= 16 or len(self.workers) > 30:
                        self.stats['rejected'] += 1
                        client.close()
                        continue
                upstream = socket.create_connection(('127.0.0.1', self.upstream), 1)
                client.settimeout(.5)
                upstream.settimeout(.5)
                pair = (client, upstream)
                with self.lock:
                    self.connections.add(pair)
                    self.stats['accepted'] += 1
                    self.stats['peak_connections'] = max(self.stats['peak_connections'], len(self.connections))
                    for downstream in (False, True):
                        worker = threading.Thread(target=self.pump,
                                                  args=(pair, downstream))
                        self.workers.add(worker)
                        self.stats['peak_workers'] = max(self.stats['peak_workers'], len(self.workers))
                        worker.start()
            except OSError:
                client.close()
                if upstream:
                    upstream.close()

    def pump(self, pair, downstream):
        source, target = pair[::-1] if downstream else pair
        try:
            while not self.stopping.is_set():
                try:
                    data = source.recv(16384)
                except socket.timeout:
                    continue
                if not data:
                    return
                with self.lock:
                    mode = self.mode
                    drop = mode == 'blackhole' or (downstream and mode == 'drop_down')
                    direction = 'downstream' if downstream else 'upstream'
                    self.stats[('dropped_' if drop else '') + direction +
                               ('' if drop else '_bytes')] += len(data)
                if drop:
                    continue
                if downstream and mode == 'fragment':
                    # Opaque ciphertext fragmentation; never inspect TLS or Kafka headers.
                    chunks = [data[i:i + 1] for i in range(min(4, len(data)))]
                    chunks += [data[i:i + 137] for i in range(4, len(data), 137)]
                    for chunk in chunks:
                        if self.stopping.wait(.002):
                            return
                        target.sendall(chunk)
                        with self.lock:
                            self.stats['fragments'] += 1
                else:
                    target.sendall(data)
        except (OSError, ValueError):
            pass
        finally:
            self.close_pair(pair)
            with self.lock:
                self.connections.discard(pair)
                self.workers.discard(threading.current_thread())

    def shutdown(self):
        self.stopping.set()
        self.listener.close()
        self.thread.join(timeout=3)
        with self.lock:
            pairs, workers = list(self.connections), list(self.workers)
        for pair in pairs:
            self.close_pair(pair)
        for worker in workers:
            worker.join(timeout=2)
        if self.thread.is_alive() or any(worker.is_alive() for worker in workers):
            raise RuntimeError('Proxy threads did not stop')


def read_diagnostics(path):
    if not path.exists():
        return ''
    with path.open('rb') as stream:
        stream.seek(0, os.SEEK_END)
        stream.seek(max(0, stream.tell() - 131072))
        return stream.read(131072).decode(errors='replace')


def main():
    os.umask(0o077)
    home = pathlib.Path(os.environ.get('KAFKA_HOME', '')).resolve()
    if not (home / 'libs/kafka-clients-4.0.2.jar').is_file():
        raise SystemExit('KAFKA_HOME must be a verified Kafka 4.0.2 distribution')
    root = pathlib.Path(os.environ.get('KAFKA_TLS_FAULT_RUN_ROOT',
                                     str(ROOT / '.dart_tool/kafka-tls-fault'))).resolve()
    root.mkdir(parents=True, exist_ok=True)
    run = pathlib.Path(tempfile.mkdtemp(prefix='private.', dir=root))
    result_path = root / f'result-{run.name}.json'
    log_path = result_path.with_suffix('.txt')
    username = 'u' + secrets.token_hex(8)
    password, store_password = secrets.token_hex(24), secrets.token_hex(24)
    private_values = [username, password, store_password, str(run)]
    broker = process = None
    proxies, controls, probes, ports = [], [], [], []
    status, stage, failure = 1, 'initializing', None

    def scrub(value):
        for secret in private_values:
            value = value.replace(secret, '[REDACTED]')
        return value

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'Signal {signum}')

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        # admin, controller, TLS direct/proxy, SASL_TLS direct/proxy
        for _ in range(6):
            probe = socket.socket()
            probes.append(probe)
            probe.bind(('127.0.0.1', 0))
            ports.append(probe.getsockname()[1])
        stage = 'certificates'
        certificates(run, store_password)
        broker = BrokerFixture(home, run, ports[0], ports[1])
        config = broker.config.read_text().replace(
            f'listeners=PLAINTEXT://127.0.0.1:{ports[0]},',
            f'listeners=SSL://127.0.0.1:{ports[2]},SASL_SSL://127.0.0.1:{ports[4]},'
            f'PLAINTEXT://127.0.0.1:{ports[0]},', 1)
        config = config.replace(
            f'advertised.listeners=PLAINTEXT://127.0.0.1:{ports[0]},',
            f'advertised.listeners=SSL://localhost:{ports[3]},SASL_SSL://localhost:{ports[5]},'
            f'PLAINTEXT://127.0.0.1:{ports[0]},')
        config = config.replace('listener.security.protocol.map=',
                                'listener.security.protocol.map=SSL:SSL,SASL_SSL:SASL_SSL,')
        config += f'''ssl.keystore.type=PKCS12
ssl.keystore.location={run / 'server.p12'}
ssl.keystore.password={store_password}
ssl.key.password={store_password}
ssl.client.auth=none
sasl.enabled.mechanisms=PLAIN
listener.name.sasl_ssl.sasl.enabled.mechanisms=PLAIN
listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_{username}="{password}";
'''
        broker.config.write_text(config)
        for probe in probes:
            probe.close()
        for mode, direct, proxied in [('tls', ports[2], ports[3]), ('sasl', ports[4], ports[5])]:
            proxy = OpaqueFaultProxy(proxied, direct)
            proxies.append(proxy)
            proxy.thread.start()
            directory = run / mode
            directory.mkdir(mode=0o700)
            control = FileControl(directory, proxy)
            controls.append(control)
            control.thread.start()
        stage = 'startup'
        broker.format()
        broker.start()
        package_config = ROOT / '.dart_tool/package_config.json'
        packages = json.loads(package_config.read_text())['packages']
        test_package = next(package for package in packages if package['name'] == 'test')
        parsed = urllib.parse.urlparse(urllib.parse.urljoin(package_config.as_uri(), test_package['rootUri']))
        if parsed.scheme != 'file' or parsed.netloc not in ('', 'localhost'):
            raise RuntimeError('Test runner must already be resolved to a local file')
        entry = pathlib.Path(urllib.parse.unquote(parsed.path)) / 'bin/test.dart'
        if not entry.is_file():
            raise RuntimeError('Resolve test dependencies offline before running')
        arguments = sys.argv[1:]
        if arguments and arguments[0] == '--':
            arguments = arguments[1:]
        command = arguments or [os.environ.get('DART', 'dart'), '--disable-dart-dev',
                                f'--packages={package_config}', str(entry),
                                'test/tls_network_fault_integration_test.dart', '--reporter', 'expanded']
        stage = 'tests'
        with (run / 'tests.log').open('w') as output:
            for mode, direct, proxied in [('tls', ports[2], ports[3]), ('sasl', ports[4], ports[5])]:
                topic = 'tls-fault-' + run.name.replace('.', '-') + '-' + mode
                broker.command('kafka-topics.sh', '--bootstrap-server', f'127.0.0.1:{ports[0]}',
                               '--create', '--topic', topic, '--partitions', '1', '--replication-factor', '1')
                env = dict(os.environ, KAFKA_TLS_FAULT_DIRECT_PORT=str(direct),
                           KAFKA_TLS_FAULT_PROXY_PORT=str(proxied), KAFKA_TLS_FAULT_TOPIC=topic,
                           KAFKA_TLS_FAULT_CONTROL=str(run / mode), KAFKA_TLS_FAULT_MODE=mode,
                           KAFKA_TLS_FAULT_CA=str(run / 'ca.pem'),
                           KAFKA_TLS_FAULT_USERNAME=username, KAFKA_TLS_FAULT_PASSWORD=password)
                process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=output,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                status = process.wait(timeout=180)
                if status:
                    break
        stage = 'complete' if status == 0 else 'test-failure'
    except BaseException as error:
        status = 1
        failure = scrub(f'{type(error).__name__}: {error}')
    finally:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        errors = []

        def attempt(label, operation):
            try:
                operation()
            except Exception as error:
                errors.append(scrub(f'{label}: {type(error).__name__}: {error}'))

        attempt('test', lambda: terminate(process))
        for control in controls:
            attempt('control', control.shutdown)
        for proxy in proxies:
            attempt('proxy', proxy.shutdown)
        if broker:
            attempt('broker', broker.stop)
        for probe in probes:
            attempt('reserved socket', probe.close)
        output = ''

        def diagnostics():
            nonlocal output
            output = scrub(read_diagnostics(run / 'tests.log'))

        # Diagnostics failure must never bypass removal of private key material.
        attempt('test diagnostics', diagnostics)
        stopped = broker is None or broker.process is None or broker.process.poll() is not None
        test_stopped = process is None or process.poll() is not None
        proxy_states = [proxy.snapshot() for proxy in proxies]
        proxy_stopped = all(not proxy.thread.is_alive() and state['active'] == 0 and state['workers'] == 0
                            for proxy, state in zip(proxies, proxy_states))
        controls_stopped = all(not control.thread.is_alive() for control in controls)
        attempt('private fixture', lambda: shutil.rmtree(run))
        removed = not run.exists()
        reusable = True
        for port in ports:
            probe = socket.socket()
            try:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(('127.0.0.1', port))
            except OSError:
                reusable = False
            finally:
                probe.close()
        cleanup_ok = stopped and test_stopped and proxy_stopped and controls_stopped and removed and reusable and not errors
        result = dict(exit_code=status if cleanup_ok else 1, test_exit_code=status, stage=stage,
                      failure=failure, owned_process_stopped=stopped, test_process_stopped=test_stopped,
                      proxy_stopped=proxy_stopped, controls_stopped=controls_stopped,
                      private_fixture_removed=removed, ports=ports, ports_reusable=reusable,
                      proxies=proxy_states, cleanup_errors=errors)
        retained = json.dumps(result) + output
        if any(value in retained for value in private_values):
            raise RuntimeError('Sanitization failed; no result retained')
        result['retained_output_secret_scan_passed'] = True
        result_path.write_text(json.dumps(result, indent=2) + '\n')
        log_path.write_text(output)
        print(output, end='', flush=True)
        print(f'Sanitized result: {result_path}', flush=True)
        if failure:
            print(f'Fixture failed at {stage}: {failure}', file=sys.stderr)
        if not cleanup_ok:
            raise RuntimeError('Incomplete fixture cleanup; inspect sanitized result')
    return status


if __name__ == '__main__':
    raise SystemExit(main())
