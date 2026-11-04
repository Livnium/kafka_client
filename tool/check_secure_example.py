#!/usr/bin/env python3
"""Run source/native secure examples inside run_secure_broker_integration.sh.

Pass a JSON list of {label, cwd, command} entries, without credentials. Each
command must run the secure example on this host. The enclosing owned fixture
provides and removes ephemeral CA material and synthetic credentials. Nothing
in this driver starts a broker or configures access to an external service.
Only fixed pass/fail fields survive; child output is checked, never echoed.
"""
import json
import os
import pathlib
import re
import subprocess
import sys
import time


EXAMPLE_KEYS = ('KAFKA_HOST', 'KAFKA_PORT', 'KAFKA_TOPIC', 'KAFKA_CA_FILE',
                'KAFKA_USERNAME', 'KAFKA_PASSWORD')


def case_environment(source, case):
    env = {key: value for key, value in source.items() if key not in EXAMPLE_KEYS}
    env.update(KAFKA_HOST='localhost', KAFKA_PORT=source['KAFKA_SECURE_TLS_PORT'],
               KAFKA_TOPIC=source['KAFKA_SECURE_TOPIC'],
               KAFKA_CA_FILE=source['KAFKA_SECURE_CA'])
    if case in ('sasl_tls', 'bad_password'):
        env.update(KAFKA_PORT=source['KAFKA_SECURE_SASL_PORT'],
                   KAFKA_USERNAME=source['KAFKA_SECURE_USERNAME'],
                   KAFKA_PASSWORD=source['KAFKA_SECURE_PASSWORD'])
    if case == 'bad_password':
        env['KAFKA_PASSWORD'] = 'deliberately-wrong-' + env['KAFKA_PASSWORD']
    elif case == 'wrong_ca':
        env['KAFKA_CA_FILE'] = source['KAFKA_SECURE_WRONG_CA']
    elif case == 'wrong_hostname':
        env['KAFKA_HOST'] = '127.0.0.1'
    elif case not in ('tls', 'sasl_tls'):
        raise ValueError('Unknown test case')
    return env


def output_matches(case, status, stdout, stderr):
    if case in ('tls', 'sasl_tls'):
        return (status == 0 and stderr == '' and re.fullmatch(
            rf'SECURE_EXAMPLE_OK offset=[0-9]+ mode={case}\n', stdout) is not None)
    category = 'authentication' if case == 'bad_password' else 'tls'
    return status == 1 and stdout == '' and stderr == (
        f'SECURE_EXAMPLE_ERROR {category}\n'
        'A write may already be stored; do not blindly rerun it.\n')


def main():
    if len(sys.argv) != 2:
        raise SystemExit('Usage: check_secure_example.py manifest.json (inside owned fixture)')
    entries = json.loads(pathlib.Path(sys.argv[1]).read_text())
    if not isinstance(entries, list) or not 1 <= len(entries) <= 8:
        raise ValueError('Expected one to eight local runtime entries')
    # Require the complete fixture contract before attempting any child command.
    required = ('CA', 'WRONG_CA', 'TLS_PORT', 'SASL_PORT', 'TOPIC', 'USERNAME', 'PASSWORD')
    for suffix in required:
        if not os.environ.get('KAFKA_SECURE_' + suffix):
            raise ValueError('Owned secure fixture environment is incomplete')
    secrets = [os.environ['KAFKA_SECURE_' + key] for key in ('USERNAME', 'PASSWORD')]
    results = []
    for entry in entries:
        label, cwd, command = entry['label'], entry['cwd'], entry['command']
        if not re.fullmatch(r'[a-zA-Z0-9_.-]{1,80}', label):
            raise ValueError('Invalid runtime label')
        if not isinstance(command, list) or not command or not all(isinstance(x, str) for x in command):
            raise ValueError('Expected argument list')
        for case in ('tls', 'sasl_tls', 'wrong_ca', 'wrong_hostname', 'bad_password'):
            started = time.monotonic()
            try:
                completed = subprocess.run(command, cwd=cwd, env=case_environment(os.environ, case),
                                           capture_output=True, text=True, timeout=60)
                leak = any(secret in completed.stdout + completed.stderr for secret in secrets)
                passed = not leak and output_matches(case, completed.returncode,
                                                    completed.stdout, completed.stderr)
                result = dict(runtime=label, case=case, exit=completed.returncode,
                              output_contract=passed, secret_detected=leak, timeout=False)
            except subprocess.TimeoutExpired:
                result = dict(runtime=label, case=case, exit=None, output_contract=False,
                              secret_detected=None, timeout=True)
            result['seconds'] = round(time.monotonic() - started, 3)
            results.append(result)
            print(json.dumps(result, sort_keys=True), flush=True)
    passed = all(item['output_contract'] for item in results)
    print(json.dumps(dict(secure_example_matrix=True, cases=len(results), passed=passed)), flush=True)
    return 0 if passed else 1


if __name__ == '__main__':
    sys.exit(main())
