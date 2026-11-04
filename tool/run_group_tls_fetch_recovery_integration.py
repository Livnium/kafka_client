#!/usr/bin/env python3
"""Strict disposable TLS Fetch recovery matrix, using explicit cached commands.

The base fixture is loaded into a private module instance: its original 22-case
runner and tests remain unchanged even when both Python suites share a process.
Certificate generation/verification, fixed host/RSS guards, process ownership,
sanitized reporter handling, and key cleanup are the base runner's audited path.
No resolver, external service, real broker, or raw child log is used here.
"""
import importlib.util
import json
from pathlib import Path
import sys

_BASE_PATH = Path(__file__).with_name('run_bootstrap_tls_fallback_integration.py')
_SPEC = importlib.util.spec_from_file_location('_group_tls_fetch_recovery_fixture', _BASE_PATH)
_BASE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_BASE)

CASE_NAMES = {
    'ambiguous Join is sent once and never gains Fetch recovery replay',
    'exhaustion after rejected leader is terminal without automatic rejoin',
    'auth after rejected leader is terminal without automatic rejoin',
    'malformed after rejected leader is terminal without automatic rejoin',
    'close while recovered Fetch is gated allows no callback or commit',
    'heartbeat revoke while recovered Fetch is gated allows no callback or commit',
    'stop after TLS rejection with recovery delay admits no later metadata or callback',
    'heartbeat 27 rejoin cleans old recovery data before fresh generation and positions',
    'ambiguous commit after recovered Fetch is sent once and never rejoins',
}
for api in ('session', 'consumer'):
    CASE_NAMES.add(f'{api} invalid local CA is terminal before any network')
    for fallback in ('false', 'true'):
        CASE_NAMES.add(f'{api} null policy preserves terminal leader rejection fallback={fallback}')
    for cert in ('eof', 'untrusted', 'wronghost', 'expired'):
        CASE_NAMES.add(f'{api} {cert} leader rejected once then strictly validated replacement')
CASE_NAMES = frozenset(CASE_NAMES)
REPORTER_NAMES = {'managed TLS Fetch recovery ' + name: name for name in CASE_NAMES}
_BASE.CASE_NAMES = CASE_NAMES
_BASE.REPORTER_NAMES = REPORTER_NAMES


def execute(command, **kwargs):
    result = _BASE.execute(command, **kwargs)
    # Fail closed even if an accidental empty allowlist passes the base equality
    # checks. Zero tests, skipped cases, extras, duplicates, partial output, and
    # reporter success=false can never count as a successful matrix.
    result['ok'] = (result['ok'] and bool(CASE_NAMES)
                    and result.get('expected_tests') == len(CASE_NAMES)
                    and result.get('complete_test_matrix') is True)
    return result


def main():
    args = sys.argv[1:]
    if len(args) < 2 or args[0] != '--':
        print('Usage: runner -- <explicit cached Dart test command>')
        return 2
    result = execute(args[1:])
    print('GROUP_TLS_FETCH_RECOVERY_FIXTURE ' + json.dumps(result, sort_keys=True))
    return 0 if result['ok'] and result['cleaned'] else 1


if __name__ == '__main__':
    sys.exit(main())
