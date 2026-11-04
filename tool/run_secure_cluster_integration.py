#!/usr/bin/env python3
"""Owned loopback Kafka 4.0.2 TLS/SASL-PLAIN three-node recovery fixture.

One-day test certificates and random synthetic credentials are local-only.
Private configs, logs, keys and data are removed even on test failure. Only
sanitized test output and structured cleanup evidence survive. Requires an
independently SHA512-verified KAFKA_HOME, OpenSSL, Java and Dart. Plaintext
admin/inter-broker/controller listeners are loopback fixture plumbing, not a
claim of a fully encrypted Kafka deployment. See the existing cluster runner
for bounded election settings and read-only retry limitations.
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

from run_cluster_integration import Cluster, Control, ROOT, terminate


class SecureCluster(Cluster):
    def __init__(self, home, run, ports, username, password, store_password):
        super().__init__(home, run, ports[6:12])
        self.secure_ports = ports
        for node in range(1, 4):
            path = run / f'node-{node}/server.properties'
            config = path.read_text()
            config = config.replace(
                f'listeners=PLAINTEXT://127.0.0.1:{ports[node+5]},',
                f'listeners=SSL://127.0.0.1:{ports[node-1]},'
                f'SASL_SSL://127.0.0.1:{ports[node+2]},'
                f'PLAINTEXT://127.0.0.1:{ports[node+5]},', 1)
            config = config.replace(
                f'advertised.listeners=PLAINTEXT://127.0.0.1:{ports[node+5]}',
                f'advertised.listeners=SSL://localhost:{ports[node-1]},'
                f'SASL_SSL://localhost:{ports[node+2]},'
                f'PLAINTEXT://127.0.0.1:{ports[node+5]}')
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
            path.write_text(config)


def certificates(run, store_password):
    def openssl(*args, succeeds=True):
        with (run / 'openssl.log').open('a') as output:
            result = subprocess.run(['openssl', *args], cwd=run,
                                    env=dict(os.environ, FIXTURE_STORE_PASSWORD=store_password),
                                    stdout=output, stderr=subprocess.STDOUT, timeout=30)
        if (result.returncode == 0) != succeeds:
            raise RuntimeError('Ephemeral certificate creation/verification failed')
    openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-sha256', '-days', '1',
            '-subj', '/CN=Kafka ephemeral cluster CA', '-keyout', 'ca.key', '-out', 'ca.pem')
    openssl('req', '-newkey', 'rsa:2048', '-nodes', '-sha256', '-subj', '/CN=localhost',
            '-keyout', 'server.key', '-out', 'server.csr')
    (run / 'server.ext').write_text('basicConstraints=critical,CA:FALSE\n'
                                  'keyUsage=critical,digitalSignature,keyEncipherment\n'
                                  'extendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n')
    openssl('x509', '-req', '-in', 'server.csr', '-CA', 'ca.pem', '-CAkey', 'ca.key',
            '-CAcreateserial', '-days', '1', '-sha256', '-extfile', 'server.ext', '-out', 'server.pem')
    openssl('verify', '-CAfile', 'ca.pem', '-verify_hostname', 'localhost', 'server.pem')
    openssl('verify', '-CAfile', 'ca.pem', '-verify_ip', '127.0.0.1', 'server.pem', succeeds=False)
    openssl('pkcs12', '-export', '-in', 'server.pem', '-inkey', 'server.key', '-certfile',
            'ca.pem', '-name', 'kafka', '-out', 'server.p12', '-passout', 'env:FIXTURE_STORE_PASSWORD')


def main():
    os.umask(0o077)
    home = pathlib.Path(os.environ.get('KAFKA_HOME', '')).resolve()
    if not (home / 'libs/kafka-clients-4.0.2.jar').is_file():
        raise SystemExit('Set KAFKA_HOME to a SHA512-verified Kafka 4.0.2 distribution')
    run_root = pathlib.Path(os.environ.get('KAFKA_SECURE_CLUSTER_RUN_ROOT',
                                          str(ROOT / '.dart_tool/kafka-secure-cluster')))
    run_root.mkdir(parents=True, exist_ok=True)
    run = pathlib.Path(tempfile.mkdtemp(prefix='private.', dir=run_root.resolve()))
    result_path = run_root / f'result-{run.name}.json'
    log_path = run_root / f'result-{run.name}.txt'
    username, password, store_password = 'u' + secrets.token_hex(8), secrets.token_hex(24), secrets.token_hex(24)
    private_values = [username, password, store_password, str(run)]
    probes, ports = [], []
    cluster = control = test_process = None
    status, stage, failure = 1, 'initializing', None

    def scrub(text):
        for value in private_values:
            text = text.replace(value, '[REDACTED]')
        return text

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'Signal {signum}')

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        for _ in range(13):
            probe = socket.socket()
            probes.append(probe)
            probe.bind(('127.0.0.1', 0))
            ports.append(probe.getsockname()[1])
        for probe in probes[:12]:
            probe.close()
        stage = 'certificates'
        certificates(run, store_password)
        cluster = SecureCluster(home, run, ports, username, password, store_password)
        stage = 'startup'
        cluster.format()
        for node in range(1, 4):
            cluster.start(node)
        cluster.ready([1, 2, 3])
        topic = 'secure-cluster-' + run.name.replace('.', '-')
        directory = run / 'control'
        directory.mkdir(mode=0o700)
        control = Control(cluster, directory)
        control.thread.start()
        env = dict(os.environ, KAFKA_SECURE_CLUSTER_TLS_PORTS=','.join(map(str, ports[:3])),
                   KAFKA_SECURE_CLUSTER_SASL_PORTS=','.join(map(str, ports[3:6])),
                   KAFKA_SECURE_CLUSTER_DEAD_PORT=str(ports[12]),
                   KAFKA_SECURE_CLUSTER_TOPIC=topic, KAFKA_SECURE_CLUSTER_CONTROL=str(directory),
                   KAFKA_SECURE_CLUSTER_CA=str(run / 'ca.pem'),
                   KAFKA_SECURE_CLUSTER_USERNAME=username, KAFKA_SECURE_CLUSTER_PASSWORD=password)
        arguments = sys.argv[1:]
        if arguments and arguments[0] == '--':
            arguments = arguments[1:]
        command = arguments or [os.environ.get('DART', 'dart'), 'test',
                                'test/secure_cluster_integration_test.dart', '--reporter', 'expanded']
        stage = 'tests'
        with (run / 'tests.log').open('w') as output:
            for mode in ('tls', 'sasl'):
                cluster.command('kafka-topics.sh', '--bootstrap-server', f'127.0.0.1:{ports[6]}',
                                '--create', '--topic', f'{topic}-{mode}', '--replica-assignment',
                                '1:2:3,2:3:1,3:1:2', '--config', 'min.insync.replicas=2')
                test_process = subprocess.Popen(command, cwd=ROOT,
                                                env=dict(env, KAFKA_SECURE_CLUSTER_MODE=mode),
                                                stdout=output, stderr=subprocess.STDOUT,
                                                start_new_session=True)
                status = test_process.wait(timeout=240)
                if status != 0:
                    break
        stage = 'complete' if status == 0 else 'test-failure'
    except BaseException as error:
        status = 1
        failure = scrub(f'{type(error).__name__}: {error}')
    finally:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        cleanup_errors = []

        def attempt(label, operation):
            try:
                operation()
            except Exception as error:
                cleanup_errors.append(scrub(f'{label}: {type(error).__name__}: {error}'))

        attempt('test process', lambda: terminate(test_process))
        if control is not None:
            attempt('control thread', control.close)
        if cluster is not None:
            for node in range(1, 4):
                attempt(f'node {node}', lambda node=node: cluster.stop(node))
        for probe in probes:
            attempt('reserved socket', probe.close)
        stopped = cluster is None or all(p.poll() is not None for p in cluster.processes.values())
        test_stopped = test_process is None or test_process.poll() is not None
        output = ''
        try:
            if (run / 'tests.log').exists():
                output = scrub((run / 'tests.log').read_text(errors='replace'))
        except Exception as error:
            cleanup_errors.append(scrub(f'test diagnostics: {type(error).__name__}: {error}'))
        # Never retain broker output, OpenSSL output, configs, or certificate material.
        attempt('private fixture removal', lambda: shutil.rmtree(run))
        removed = not run.exists()
        if not stopped or not test_stopped or not removed:
            cleanup_errors.append('Owned process/private fixture cleanup incomplete')
        result = dict(exit_code=status if not cleanup_errors and failure is None else 1, test_exit_code=status,
                      stage=stage, failure=failure, ports=ports,
                      generations=cluster.generations if cluster else {},
                      owned_processes_stopped=stopped, test_process_stopped=test_stopped,
                      private_fixture_removed=removed, cleanup_errors=cleanup_errors)
        retained = json.dumps(result, indent=2) + '\n' + output
        if any(value in retained for value in private_values):
            raise RuntimeError('Sanitization failed; no result retained')
        result['retained_output_secret_scan_passed'] = True
        result_path.write_text(json.dumps(result, indent=2) + '\n')
        log_path.write_text(output)
        print(output, end='', flush=True)
        print(f'Sanitized result: {result_path}', flush=True)
        if failure:
            print(f'Fixture failed at {stage}: {failure}', file=sys.stderr)
        if cleanup_errors:
            raise RuntimeError('Fixture cleanup failed: ' + '; '.join(cleanup_errors))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
