#!/usr/bin/env python3
"""Owned Kafka 4.0.2 + loopback streaming fault proxy; no external services.

Run with KAFKA_HOME and DART set. Optional -- <command> overrides the test.
Private atomic file IPC controls only the owned proxy. Each connection uses two
16 KiB receive chunks (plus bounded fragmentation copies), at most 16
connections / 32 pump threads, and bounded socket deadlines. Tests use the
already-resolved local package configuration, never invoke a package resolver.
"""
import json
import os
import pathlib
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse

from run_operational_integration import BrokerFixture, FileControl, ROOT


class FaultProxy:
    def __init__(self, port, upstream):
        self.upstream = upstream
        self.lock = threading.RLock()
        self.stopping = threading.Event()
        self.mode = 'pass'
        self.connections = set()
        self.workers = set()
        self.stats = dict(accepted=0, rejected=0, produce_frames=0,
                          upstream_bytes=0, downstream_bytes=0,
                          dropped_upstream=0, dropped_downstream=0,
                          fragments=0, disconnects=0, armed_triggers=0)
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
            elif action in ('pass', 'arm_produce', 'blackhole', 'fragment'):
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
                    # Each accepted connection starts two workers atomically.
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
                    for downstream in (False, True):
                        worker = threading.Thread(target=self.pump,
                                                  args=(pair, downstream))
                        self.workers.add(worker)
                        worker.start()
            except OSError:
                client.close()
                if upstream:
                    upstream.close()

    def pump(self, pair, downstream):
        source, target = pair[::-1] if downstream else pair
        # Keep only six header bytes while counting complete streamed frame sizes.
        header = bytearray()
        remaining = 0
        try:
            while not self.stopping.is_set():
                try:
                    data = source.recv(16384)
                except socket.timeout:
                    continue
                if not data:
                    return
                if not downstream:
                    position = 0
                    while position < len(data):
                        if remaining:
                            count = min(remaining, len(data) - position)
                            remaining -= count
                            position += count
                            continue
                        count = min(6 - len(header), len(data) - position)
                        header.extend(data[position:position + count])
                        position += count
                        if len(header) == 6:
                            size, api = struct.unpack('>ih', header)
                            if size < 2 or size > 1048576:
                                raise ValueError('Proxy request exceeds 1 MiB bound')
                            if api == 0:
                                with self.lock:
                                    self.stats['produce_frames'] += 1
                                    if self.mode == 'arm_produce':
                                        self.mode = 'drop_down'
                                        self.stats['armed_triggers'] += 1
                            remaining = size - 2
                            header.clear()
                with self.lock:
                    mode = self.mode
                    drop = mode == 'blackhole' or (downstream and mode == 'drop_down')
                    direction = 'downstream' if downstream else 'upstream'
                    self.stats[('dropped_' if drop else '') + direction +
                               ('' if drop else '_bytes')] += len(data)
                if drop:
                    continue
                if downstream and mode == 'fragment':
                    # Split the length prefix itself, then payload, with bounded delay.
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


def main():
    os.umask(0o077)
    home = pathlib.Path(os.environ.get('KAFKA_HOME', '')).resolve()
    if not (home / 'libs/kafka-clients-4.0.2.jar').is_file():
        raise SystemExit('KAFKA_HOME must be a verified Kafka 4.0.2 distribution')
    ports = [int(os.environ.get(key, default)) for key, default in (
        ('KAFKA_FAULT_DIRECT_PORT', '19592'), ('KAFKA_FAULT_CONTROLLER_PORT', '19593'),
        ('KAFKA_FAULT_PROXY_PORT', '19594'))]
    if len(set(ports)) != 3 or any(port < 1024 or port > 65535 for port in ports):
        raise SystemExit('Three distinct unprivileged ports are required')
    probes = []
    try:
        for port in ports:
            probe = socket.socket()
            probes.append(probe)
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(('127.0.0.1', port))
    finally:
        for probe in probes:
            probe.close()
    root = pathlib.Path(os.environ.get('KAFKA_NETWORK_FAULT_RUN_ROOT',
                                     str(ROOT / '.dart_tool/kafka-network-fault'))).resolve()
    root.mkdir(parents=True, exist_ok=True)
    run = pathlib.Path(tempfile.mkdtemp(prefix='run.', dir=root))
    directory = run / 'control'
    directory.mkdir(mode=0o700)
    broker = BrokerFixture(home, run, ports[0], ports[1])
    # Discovery must advertise the proxy, never the bypass used by the oracle.
    config = broker.config.read_text().replace(
        f'advertised.listeners=PLAINTEXT://127.0.0.1:{ports[0]},',
        f'advertised.listeners=PLAINTEXT://127.0.0.1:{ports[2]},')
    broker.config.write_text(config)
    proxy = control = process = None
    status = 1

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'Signal {signum}')

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        proxy = FaultProxy(ports[2], ports[0])
        proxy.thread.start()
        broker.format()
        broker.start()
        topic = 'network-fault-' + run.name.replace('.', '-')
        (run / 'topic.log').write_text(broker.command(
            'kafka-topics.sh', '--bootstrap-server', f'127.0.0.1:{ports[0]}',
            '--create', '--topic', topic, '--partitions', '1', '--replication-factor', '1'))
        control = FileControl(directory, proxy)
        control.thread.start()
        env = dict(os.environ, KAFKA_FAULT_DIRECT_PORT=str(ports[0]),
                   KAFKA_FAULT_PROXY_PORT=str(ports[2]), KAFKA_FAULT_TOPIC=topic,
                   KAFKA_FAULT_CONTROL=str(directory))
        arguments = sys.argv[1:]
        if arguments and arguments[0] == '--':
            arguments = arguments[1:]
        package_config = ROOT / '.dart_tool/package_config.json'
        packages = json.loads(package_config.read_text())['packages']
        test_package = next(package for package in packages if package['name'] == 'test')
        test_uri = urllib.parse.urljoin(package_config.as_uri(), test_package['rootUri'])
        parsed = urllib.parse.urlparse(test_uri)
        if parsed.scheme != 'file' or parsed.netloc not in ('', 'localhost'):
            raise RuntimeError('Test runner must already be resolved to a local file')
        entry = pathlib.Path(urllib.parse.unquote(parsed.path)) / 'bin/test.dart'
        if not entry.is_file():
            raise RuntimeError('Resolve test dependencies offline before running')
        command = arguments or [os.environ.get('DART', 'dart'), '--disable-dart-dev',
                                f'--packages={package_config}', str(entry),
                                'test/network_fault_integration_test.dart',
                                '--reporter', 'expanded']
        print(f'Network fault fixture ready: {run}', flush=True)
        with (run / 'tests.log').open('w') as output:
            process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=output,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            # A hung Dart process cannot keep the fixture alive indefinitely.
            status = process.wait(timeout=180)
        with (run / 'tests.log').open('rb') as output:
            output.seek(0, os.SEEK_END)
            output.seek(max(0, output.tell() - 65536))
            print(output.read(65536).decode(errors='replace'), flush=True)
        return status
    finally:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        errors = []

        def attempt(label, action):
            try:
                action()
            except Exception as error:
                errors.append(f'{label}: {error}')

        def stop_test():
            if process is not None and process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=5)

        attempt('test', stop_test)
        if control:
            attempt('control', control.shutdown)
        if proxy:
            attempt('proxy', proxy.shutdown)
        attempt('broker', broker.stop)
        stopped = broker.process is None or broker.process.poll() is not None
        if stopped and (run / 'data').exists():
            attempt('data', lambda: shutil.rmtree(run / 'data'))
        attempt('control directory', lambda: shutil.rmtree(directory))
        ports_reusable = True
        for port in ports:
            probe = socket.socket()
            try:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(('127.0.0.1', port))
            except OSError as error:
                ports_reusable = False
                errors.append(f'Port {port} is not reusable: {error}')
            finally:
                probe.close()
        proxy_state = proxy.snapshot() if proxy else None
        test_stopped = process is None or process.poll() is not None
        proxy_stopped = proxy is None or (not proxy.thread.is_alive() and
                        proxy_state['active'] == 0 and proxy_state['workers'] == 0)
        data_removed = not (run / 'data').exists()
        control_removed = not directory.exists()
        cleanup_ok = (stopped and test_stopped and proxy_stopped and data_removed
                      and control_removed and ports_reusable and not errors)
        result = dict(exit_code=status if cleanup_ok else 1, test_exit_code=status,
                      owned_process_stopped=stopped, test_process_stopped=test_stopped,
                      proxy_stopped=proxy_stopped, ports=ports, ports_reusable=ports_reusable,
                      data_removed=data_removed, control_removed=control_removed,
                      cleanup_errors=errors, proxy=proxy_state)
        (run / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(f'Network fault cleanup: {json.dumps(result)}; diagnostics: {run}', flush=True)
        if not cleanup_ok and not errors:
            errors.append('Incomplete teardown')
        if errors:
            raise RuntimeError('; '.join(errors))


if __name__ == '__main__':
    raise SystemExit(main())
