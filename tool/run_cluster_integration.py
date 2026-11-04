#!/usr/bin/env python3
"""Disposable three-node Kafka 4.0.2 cluster, RF=3 and three leaders.

KAFKA_HOME must name an independently SHA512-verified official distribution.
All listeners are IPv4 loopback; only owned process groups may be terminated.
Static KRaft quorum: https://kafka.apache.org/40/operations/kraft/
Logs/configs/result.json survive; broker data and private file IPC never do.
Failover uses heartbeat=500 ms / session=3000 ms, plus the test's ten read
retries with a 1 s jitter cap. Bounded read-only polling covers the full election
window; this does not establish default-budget or single-call recovery.
Optional arguments after -- override the test command (e.g. -- /bin/false).
"""
import json
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent


def terminate(process, crash=False):
    if process is not None and process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL if crash else signal.SIGTERM)
        except ProcessLookupError:
            pass  # The owned process exited between poll and signal.
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)


class Cluster:
    def __init__(self, home, run, ports):
        self.home, self.run, self.ports = home, run, ports
        self.processes, self.handles, self.generations = {}, {}, {}
        self.env = dict(os.environ, KAFKA_HEAP_OPTS='-Xms128m -Xmx256m',
                        KAFKA_OPTS=f'-Djava.io.tmpdir={run}')
        # Do not inherit optional remote management listeners or JVM agents.
        for key in ('JMX_PORT', 'JMX_HOSTNAME', 'KAFKA_JMX_OPTS',
                    'JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS',
                    'KAFKA_DEBUG', 'JAVA_DEBUG_OPTS', 'JAVA_DEBUG_PORT',
                    'DEBUG_SUSPEND_FLAG', 'KAFKA_JVM_PERFORMANCE_OPTS',
                    'KAFKA_GC_LOG_OPTS', 'KAFKA_LOG4J_OPTS', 'CLASSPATH'):
            self.env.pop(key, None)
        voters = ','.join(f'{i+1}@127.0.0.1:{ports[3+i]}' for i in range(3))
        for node in range(1, 4):
            directory = run / f'node-{node}'
            directory.mkdir()
            (directory / 'server.properties').write_text(f'''process.roles=broker,controller
node.id={node}
controller.quorum.voters={voters}
listeners=PLAINTEXT://127.0.0.1:{ports[node-1]},CONTROLLER://127.0.0.1:{ports[node+2]}
advertised.listeners=PLAINTEXT://127.0.0.1:{ports[node-1]}
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={directory / 'data'}
num.network.threads=2
num.io.threads=2
num.replica.fetchers=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
min.insync.replicas=2
broker.heartbeat.interval.ms=500
broker.session.timeout.ms=3000
log.segment.bytes=10485760
log.retention.bytes=134217728
auto.create.topics.enable=false
unclean.leader.election.enable=false
''')

    def command(self, name, *args):
        env = dict(self.env, LOG_DIR=str(self.run / 'cli-logs'))
        result = subprocess.run([str(self.home / 'bin' / name), *args], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, timeout=60)
        with (self.run / 'commands.log').open('a') as output:
            output.write(f'{name} {args}\n{result.stdout}\n')
        result.check_returncode()
        return result.stdout

    def format(self):
        cluster_id = self.command('kafka-storage.sh', 'random-uuid').strip()
        for node in range(1, 4):
            self.command('kafka-storage.sh', 'format', '-t', cluster_id,
                         '-c', str(self.run / f'node-{node}/server.properties'))

    def start(self, node):
        if node in self.processes and self.processes[node].poll() is None:
            raise RuntimeError('Node already running')
        directory = self.run / f'node-{node}'
        generation = self.generations.get(node, 0) + 1
        self.generations[node] = generation
        self.handles[node] = (directory / f'broker-{generation}.log').open('w')
        self.processes[node] = subprocess.Popen(
            [str(self.home / 'bin/kafka-server-start.sh'),
             str(directory / 'server.properties')],
            env=dict(self.env, LOG_DIR=str(directory / 'logs')),
            stdout=self.handles[node], stderr=subprocess.STDOUT,
            start_new_session=True)

    def ready(self, nodes):
        deadline = time.monotonic() + 75
        while time.monotonic() < deadline:
            ready = True
            for node in nodes:
                if self.processes[node].poll() is not None:
                    raise RuntimeError(f'Owned node {node} exited during startup')
                log = self.run / f'node-{node}/broker-{self.generations[node]}.log'
                ready &= 'Kafka Server started' in log.read_text(errors='replace')
            if ready:
                return
            time.sleep(.1)
        raise TimeoutError('Cluster startup exceeded 75 seconds')

    def stop(self, node, crash=False):
        try:
            terminate(self.processes.get(node), crash)
        finally:
            if node in self.handles:
                self.handles.pop(node).close()


class Control:
    def __init__(self, cluster, directory):
        self.cluster, self.directory = cluster, directory
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.serve, daemon=True)

    def serve(self):
        while not self.stopping.wait(.02):
            for request in self.directory.glob('*.request'):
                try:
                    if request.stat().st_size > 1024:
                        raise ValueError('Oversized control request')
                    payload = json.loads(request.read_text())
                    node, action = payload['node'], payload['action']
                    if type(node) is not int or node not in (1, 2, 3):
                        raise ValueError('Only owned nodes 1..3 may be controlled')
                    if action == 'crash':
                        self.cluster.stop(node, crash=True)
                    elif action == 'start':
                        self.cluster.start(node)
                        self.cluster.ready([node])
                    else:
                        raise ValueError('Only crash/start supported')
                    result = {'ok': True, 'node': node, 'action': action}
                except Exception as error:
                    result = {'ok': False, 'error': str(error)}
                temporary = request.with_suffix('.temporary')
                temporary.write_text(json.dumps(result))
                temporary.replace(request.with_suffix('.response'))
                request.unlink()

    def close(self):
        self.stopping.set()
        self.thread.join(timeout=80)
        if self.thread.is_alive():
            raise RuntimeError('Control thread did not stop')


def main():
    os.umask(0o077)
    home = pathlib.Path(os.environ.get('KAFKA_HOME', '')).resolve()
    if not (home / 'libs/kafka-clients-4.0.2.jar').is_file():
        raise SystemExit('Set KAFKA_HOME to a SHA512-verified Kafka 4.0.2 distribution')
    probes = []
    try:
        for _ in range(7):
            probe = socket.socket()
            probe.bind(('127.0.0.1', 0))
            probes.append(probe)
        ports = [probe.getsockname()[1] for probe in probes]
    except BaseException:
        for probe in probes:
            probe.close()
        raise
    # Keep an unused non-listening port reserved for bootstrap-fallback testing.
    for probe in probes[:6]:
        probe.close()
    run_root = pathlib.Path(os.environ.get('KAFKA_CLUSTER_RUN_ROOT',
                                         str(ROOT / '.dart_tool/kafka-cluster')))
    run_root.mkdir(parents=True, exist_ok=True)
    run = pathlib.Path(tempfile.mkdtemp(prefix='run.', dir=run_root.resolve()))
    control_dir = run / 'control'
    control_dir.mkdir(mode=0o700)
    cluster = Cluster(home, run, ports)
    control = test_process = None
    status = 1

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'Signal {signum}')

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        cluster.format()
        for node in range(1, 4):
            cluster.start(node)
        cluster.ready([1, 2, 3])
        topic = 'cluster-' + run.name.replace('.', '-')
        cluster.command('kafka-topics.sh', '--bootstrap-server',
                        f'127.0.0.1:{ports[0]}', '--create', '--topic', topic,
                        '--replica-assignment', '1:2:3,2:3:1,3:1:2',
                        '--config', 'min.insync.replicas=2')
        control = Control(cluster, control_dir)
        control.thread.start()
        env = dict(os.environ, KAFKA_CLUSTER_PORTS=','.join(map(str, ports[:3])),
                   KAFKA_CLUSTER_DEAD_PORT=str(ports[6]), KAFKA_CLUSTER_TOPIC=topic,
                   KAFKA_CLUSTER_CONTROL=str(control_dir))
        print(f'Kafka cluster ready; diagnostics: {run}', flush=True)
        arguments = sys.argv[1:]
        if arguments and arguments[0] == '--':
            arguments = arguments[1:]
        command = arguments or [os.environ.get('DART', 'dart'), 'test',
                                'test/cluster_integration_test.dart',
                                '--reporter', 'expanded']
        with (run / 'tests.log').open('w') as output:
            test_process = subprocess.Popen(
                command,
                cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True)
            status = test_process.wait(timeout=240)
        return status
    finally:
        # A second interrupt must not prevent stopping the other owned nodes.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        cleanup_errors = []

        def attempt(label, operation):
            try:
                operation()
            except Exception as error:
                cleanup_errors.append(f'{label}: {type(error).__name__}: {error}')

        def remove_owned(directory):
            if directory.exists():
                shutil.rmtree(directory)
            if directory.exists():
                raise RuntimeError(f'Owned directory remains: {directory}')

        # Every attempt runs even when an earlier process or file cleanup fails.
        attempt('test process', lambda: terminate(test_process))
        if control is not None:
            attempt('control thread', control.close)
        for node in range(1, 4):
            attempt(f'node {node}', lambda node=node: cluster.stop(node))
        for node in range(1, 4):
            process = cluster.processes.get(node)
            if process is not None and process.poll() is None:
                cleanup_errors.append(f'Owned node {node} is still running; data retained')
            else:
                attempt(f'node {node} data',
                        lambda node=node: remove_owned(run / f'node-{node}/data'))
        attempt('control directory', lambda: remove_owned(control_dir))
        attempt('reserved socket', probes[6].close)
        if (run / 'tests.log').exists():
            attempt('print test diagnostics', lambda: print(
                (run / 'tests.log').read_text(), end='', flush=True))
        stopped = all(p.poll() is not None for p in cluster.processes.values())
        data_removed = all(not (run / f'node-{i}/data').exists() for i in range(1, 4))
        if not stopped or not data_removed or control_dir.exists():
            cleanup_errors.append('Owned process/data/control cleanup is incomplete')
        (run / 'result.json').write_text(json.dumps({
            'exit_code': status if not cleanup_errors else 1,
            'test_exit_code': status, 'generations': cluster.generations,
            'ports': ports, 'owned_processes_stopped': stopped,
            'data_removed': data_removed,
            'control_removed': not control_dir.exists(),
            'cleanup_errors': cleanup_errors}, indent=2) + '\n')
        if cleanup_errors:
            raise RuntimeError('Cluster cleanup failed: ' + '; '.join(cleanup_errors))
        print(f'Owned cluster stopped; diagnostics: {run}', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
