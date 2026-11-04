#!/usr/bin/env python3
"""Disposable Kafka recovery/soak qualification fixture (Linux; Python 3, Java 17+, Dart).

Only controls its own broker process. The private file-control directory supports
start/stop/crash/status, not arbitrary commands. No external cluster is touched.
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

from kafka_distribution import selected_distribution

ROOT = pathlib.Path(__file__).resolve().parent.parent


class _PartitionNotReady(Exception):
    """A read-only readiness request received a retryable partition error."""
    def __init__(self, message, error_code=None):
        super().__init__(message)
        self.error_code = error_code


class _ReadinessResponse:
    """Strict, bounded response envelope reader; not a record decoder."""
    def __init__(self, data):
        self.data, self.position = data, 0

    def take(self, size):
        if size < 0 or size > len(self.data) - self.position:
            raise RuntimeError('Invalid readiness response length')
        start = self.position
        self.position += size
        return self.data[start:self.position]

    def number(self, kind):
        return struct.unpack('>' + kind, self.take(struct.calcsize('>' + kind)))[0]

    def partition(self, topic):
        if (self.number('i') != 1 or
                self.take(self.number('h')) != topic or
                self.number('i') != 1 or self.number('i') != 0):
            raise RuntimeError('Unexpected readiness topic or partition')
        return self.number('h')

    def finish(self, error, api):
        if self.position != len(self.data):
            raise RuntimeError('Trailing readiness response bytes')
        if error:
            message = f'{api} readiness partition error {error}'
            # Offset movement and ordinary startup/leadership errors never
            # establish readiness. All other errors fail immediately.
            if error in (1, 3, 5, 6, 7, 9, 78):
                raise _PartitionNotReady(message, error)
            raise RuntimeError(message)


class BrokerFixture:
    def __init__(self, home, run, port, controller):
        self.home, self.run, self.port = home, run, port
        self.process = None
        self.log_handle = None
        self.lock = threading.Lock()
        self.generation = 0
        # Set only after this runner has created its own dedicated topic.
        self.readiness_topic = None
        self.env = dict(os.environ, LOG_DIR=str(run / 'logs'),
                        KAFKA_HEAP_OPTS='-Xms256m -Xmx512m',
                        KAFKA_OPTS=f'-Djava.io.tmpdir={run}')
        # Never inherit optional remote management listeners or JVM agents.
        for key in ('JMX_PORT', 'JMX_HOSTNAME', 'KAFKA_JMX_OPTS',
                    'JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS',
                    'KAFKA_DEBUG', 'JAVA_DEBUG_OPTS', 'JAVA_DEBUG_PORT',
                    'DEBUG_SUSPEND_FLAG', 'KAFKA_JVM_PERFORMANCE_OPTS',
                    'KAFKA_GC_LOG_OPTS', 'KAFKA_LOG4J_OPTS', 'CLASSPATH'):
            self.env.pop(key, None)
        self.config = run / 'server.properties'
        self.config.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:{controller}
listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={run / 'data'}
num.partitions=1
num.network.threads=2
num.io.threads=2
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.segment.bytes=10485760
log.retention.bytes=134217728
auto.create.topics.enable=false
''')

    def command(self, name, *args, timeout=60):
        return subprocess.run([str(self.home / 'bin' / name), *args],
                              env=self.env, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True,
                              timeout=timeout, check=True).stdout

    def format(self):
        cluster = self.command('kafka-storage.sh', 'random-uuid').strip()
        result = self.command('kafka-storage.sh', 'format', '--standalone',
                              '-t', cluster, '-c', str(self.config))
        (self.run / 'format.log').write_text(result)

    def _partition_ready(self, deadline):
        """Verify serving, not just a startup marker, without writes/commits.

        Wire layouts match Kafka 4.0.2's official ListOffsets v1 and Fetch v4
        schemas. Latest avoids assuming offset zero survives fixture retention.
        No metadata/advertised endpoint is followed: both reads use the owned
        loopback broker. Each attempt, including fragmented reads, shares one
        deadline capped at two seconds and at the original startup deadline.
        """
        topic = self.readiness_topic.encode('utf-8')
        if not topic or len(topic) > 249:
            raise RuntimeError('Invalid owned readiness topic')
        selected = struct.pack('>ih', 1, len(topic)) + topic + struct.pack('>ii', 1, 0)
        deadline = min(deadline, time.monotonic() + 2)

        def remaining():
            value = deadline - time.monotonic()
            if value <= 0:
                raise TimeoutError('Partition readiness attempt timed out')
            return value

        with socket.create_connection(('127.0.0.1', self.port), remaining()) as connection:
            def receive(size):
                data = bytearray()
                while len(data) < size:
                    connection.settimeout(remaining())
                    chunk = connection.recv(size - len(data))
                    if not chunk:
                        raise ConnectionError('Readiness connection closed')
                    data.extend(chunk)
                return data

            def exchange(api, version, correlation, body):
                # Request header v1, with a null client ID.
                request = struct.pack('>hhih', api, version, correlation, -1) + body
                connection.settimeout(remaining())
                connection.sendall(struct.pack('>i', len(request)) + request)
                size = struct.unpack('>i', receive(4))[0]
                # maxBytes is advisory; still enforce our own hard frame bound.
                if not 4 <= size <= 1024 * 1024:
                    raise RuntimeError('Invalid readiness response frame size')
                response = _ReadinessResponse(receive(size))
                if response.number('i') != correlation:
                    raise RuntimeError('Unexpected readiness correlation ID')
                return response

            offsets = exchange(2, 1, 1, struct.pack('>i', -1) + selected +
                               struct.pack('>q', -1))
            error = offsets.partition(topic)
            timestamp, offset = offsets.number('q'), offsets.number('q')
            offsets.finish(error, 'ListOffsets')
            if timestamp < -1 or offset < 0:
                raise RuntimeError('Invalid readiness latest offset')

            fetched = exchange(1, 4, 2, struct.pack('>iiiib', -1, 0, 1, 1, 0) +
                               selected + struct.pack('>qi', offset, 1))
            throttle = fetched.number('i')
            error = fetched.partition(topic)
            watermark, stable = fetched.number('q'), fetched.number('q')
            aborted = fetched.number('i')
            if aborted < -1:
                raise RuntimeError('Invalid readiness aborted transaction count')
            fetched.take(max(0, aborted) * 16)
            records = fetched.number('i')
            if records < -1:
                raise RuntimeError('Invalid readiness records length')
            fetched.take(max(0, records))
            fetched.finish(error, 'Fetch')
            if throttle < 0 or watermark < offset or stable < -1:
                raise RuntimeError('Invalid readiness fetch watermark or throttle')
            remaining()
            return offset

    def start(self):
        if self.process is not None and self.process.poll() is None:
            raise RuntimeError('Broker is already running')
        started = time.monotonic()
        deadline = started + 60
        self.generation += 1
        log = self.run / f'broker-{self.generation}.log'
        self.log_handle = log.open('w')
        receipt = dict(generation=self.generation,
                       topic_probe_required=self.readiness_topic is not None,
                       attempts=0, transient_error_codes=[], verified_offset=None,
                       ready=False)
        try:
            self.process = subprocess.Popen(
                [str(self.home / 'bin/kafka-server-start.sh'), str(self.config)],
                env=self.env, stdout=self.log_handle, stderr=subprocess.STDOUT,
                start_new_session=True)
            last_failure = 'startup marker not observed'
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError(f'Broker exited during startup; inspect {log.name}')
                if 'Kafka Server started' in log.read_text(errors='replace'):
                    if self.readiness_topic is None:
                        # First startup precedes creation of the owned topic.
                        receipt['ready'] = True
                        return
                    receipt['attempts'] += 1
                    try:
                        offset = self._partition_ready(deadline)
                    except (OSError, _PartitionNotReady) as error:
                        last_failure = str(error)
                        if isinstance(error, _PartitionNotReady) and error.error_code is not None:
                            codes = receipt['transient_error_codes']
                            if error.error_code not in codes:
                                codes.append(error.error_code)
                    else:
                        if self.process.poll() is not None:
                            raise RuntimeError(f'Broker exited during startup; inspect {log.name}')
                        if time.monotonic() < deadline:
                            receipt.update(ready=True, verified_offset=offset)
                            return
                        last_failure = 'partition readiness completed after deadline'
                # Poll pacing only: elapsed time never substitutes for a valid Fetch.
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(min(.1, remaining))
            raise RuntimeError(f'Broker startup exceeded 60 seconds: {last_failure}')
        finally:
            receipt['elapsed_seconds'] = round(time.monotonic() - started, 6)
            (self.run / f'readiness-{self.generation}.json').write_text(
                json.dumps(receipt, indent=2) + '\n')

    def stop(self, crash=False):
        if self.process is None:
            return
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGKILL if crash else signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=5)
        self.process = None
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None

    def control(self, action):
        with self.lock:
            if action == 'start':
                self.start()
            elif action in ('stop', 'crash'):
                self.stop(crash=action == 'crash')
            elif action != 'status':
                raise ValueError('Unsupported fixture action')
            return {'ok': True, 'generation': self.generation,
                    'running': self.process is not None and self.process.poll() is None}


class FileControl:
    """Atomic local files avoid requiring Unix socket support in a sandbox."""
    def __init__(self, directory, fixture):
        self.directory, self.fixture = directory, fixture
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.serve, daemon=True)

    def serve(self):
        while not self.stopping.wait(.02):
            for request in self.directory.glob('*.request'):
                try:
                    if request.stat().st_size > 1024:
                        raise ValueError('Control request too long')
                    action = json.loads(request.read_text())['action']
                    response = self.fixture.control(action)
                except Exception as error:
                    response = {'ok': False, 'error': str(error)}
                temporary = request.with_suffix('.temporary')
                temporary.write_text(json.dumps(response))
                temporary.replace(request.with_suffix('.response'))
                request.unlink()

    def shutdown(self):
        self.stopping.set()
        self.thread.join(timeout=75)
        if self.thread.is_alive():
            raise RuntimeError('Fixture control did not stop')


def main():
    os.umask(0o077)
    home, kafka_version = selected_distribution()
    port = int(os.environ.get('KAFKA_OPERATIONAL_PORT', '19292'))
    controller = int(os.environ.get('KAFKA_OPERATIONAL_CONTROLLER_PORT', '19293'))
    # Reject occupied or duplicate ports before spawning a broker.
    probes = []
    try:
        for selected in (port, controller):
            probe = socket.socket()
            probe.bind(('127.0.0.1', selected))
            probes.append(probe)
    finally:
        for probe in probes:
            probe.close()
    run_root = pathlib.Path(os.environ.get(
        'KAFKA_OPERATIONAL_RUN_ROOT', str(ROOT / '.dart_tool/kafka-operational')))
    run_root.mkdir(parents=True, exist_ok=True)
    run = pathlib.Path(tempfile.mkdtemp(prefix='run.', dir=run_root.resolve()))
    control_dir = run / 'control'
    control_dir.mkdir(mode=0o700)
    fixture = BrokerFixture(home, run, port, controller)
    server = test_process = None
    status = 1

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'Signal {signum}')

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        fixture.format()
        fixture.start()
        topic = f'operational-{run.name.replace(".", "-")}'
        output = fixture.command('kafka-topics.sh', '--bootstrap-server',
                                 f'127.0.0.1:{port}', '--create', '--topic', topic,
                                 '--partitions', '1', '--replication-factor', '1')
        (run / 'topic.log').write_text(output)
        fixture.readiness_topic = topic
        server = FileControl(control_dir, fixture)
        server.thread.start()
        env = dict(os.environ, KAFKA_OPERATIONAL_PORT=str(port),
                   KAFKA_OPERATIONAL_TOPIC=topic,
                   KAFKA_OPERATIONAL_CONTROL=str(control_dir))
        arguments = sys.argv[1:]
        if arguments and arguments[0] == '--':
            arguments = arguments[1:]
        command = arguments or [os.environ.get('DART', 'dart'), 'test',
                                'test/operational_integration_test.dart',
                                '--reporter', 'expanded']
        print(f'Kafka operational fixture ready; diagnostics: {run}', flush=True)
        with (run / 'tests.log').open('w') as output_file:
            test_process = subprocess.Popen(command, cwd=ROOT, env=env,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT, text=True,
                                            start_new_session=True)
            for line in test_process.stdout:
                print(line, end='', flush=True)
                output_file.write(line)
                output_file.flush()
            status = test_process.wait()
        return status
    finally:
        # Repeated interrupts must not abort teardown half-way through.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        cleanup_errors = []

        def attempt(label, action):
            try:
                action()
            except Exception as error:
                cleanup_errors.append(f'{label}: {type(error).__name__}: {error}')

        def stop_test():
            # Only terminate the process group started by this invocation.
            if test_process is not None and test_process.poll() is None:
                try:
                    os.killpg(test_process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    test_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(test_process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    test_process.wait(timeout=5)

        attempt('test process', stop_test)
        if server is not None:
            attempt('control worker', server.shutdown)
        with fixture.lock:
            attempt('broker process', fixture.stop)
        stopped = fixture.process is None or fixture.process.poll() is not None
        if stopped and (run / 'data').exists():
            attempt('broker data', lambda: shutil.rmtree(run / 'data'))
        if control_dir.exists():
            attempt('control directory', lambda: shutil.rmtree(control_dir))
        (run / 'result.json').write_text(json.dumps(
            {'kafka_version': kafka_version, 'exit_code': status, 'broker_generations': fixture.generation,
             'ports': [port, controller], 'owned_process_stopped': stopped,
             'data_removed': not (run / 'data').exists(),
             'control_removed': not control_dir.exists(),
             'cleanup_errors': cleanup_errors}, indent=2) + '\n')
        print(f'Operational fixture cleanup recorded; diagnostics: {run}', flush=True)
        if cleanup_errors:
            raise RuntimeError('Fixture cleanup failed: ' + '; '.join(cleanup_errors))


if __name__ == '__main__':
    raise SystemExit(main())
