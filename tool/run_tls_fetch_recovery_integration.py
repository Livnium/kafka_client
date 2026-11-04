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
_SPEC = importlib.util.spec_from_file_location('_tls_fetch_recovery_fixture', _BASE_PATH)
_BASE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_BASE)

CASE_NAMES = {
    'concurrent calls share rejected handshake but retain independent recovery',
    'shared versions future preserves each Fetch admission window',
    'admitted SASL may finish after window but admits no version or data',
    'invalid security combinations fail before network',
    'later metadata TLS rejection joins the same exclusion set',
    'expired rejected handshake cannot start next metadata endpoint',
    'rejected bootstrap later advertised as leader is not retried',
    'repeated stale discovered leader is skipped until replacement',
    'all invalid bootstraps terminate without outer retries',
    'TLS rejection plus transport exhaustion is terminal',
    'earlier leader rejection survives later metadata transport exhaustion',
    'alternating rejected leaders share exclusion set and round budget',
    'transient Fetch after TLS recovery can succeed within shared budget',
    'window checked after leader versions before Fetch enqueue',
    'expired metadata response cannot start leader connection',
    'retry delay cannot admit another round after window',
    'admitted Fetch may finish after polling window',
    'close interrupts recovery delay without another round',
    'close interrupts connecting recovery peer without retry',
    'invalid local CA remains terminal before any network',
    'new calls get independent rejection sets',
    'concurrent calls each retain their own round budget',
    'metadata ignores Fetch recovery budget and window',
    'Produce ignores Fetch recovery window',
    'ambiguous Produce is sent once with recovery configured',
    'Produce leader rejection never redirects with recovery configured',
    'low-level Fetch remains single attempt without discovery',
}
for enabled in ('false', 'true'):
    CASE_NAMES.add(f'null recovery preserves leader failure, fallback={enabled}')
    CASE_NAMES.add(f'ordinary transient shares total budget, earlier TLS={enabled}')
    CASE_NAMES.add(f'window checked after metadata versions, earlier TLS={enabled}')
for cert in ('eof', 'untrusted', 'wronghost', 'expired'):
    CASE_NAMES.add(f'{cert} leader rejected once then validated replacement')
for attempts in (1, 3):
    CASE_NAMES.add(f'forever stale consumes exactly {attempts} total rounds')
for stage in ('bootstrap', 'leader'):
    for mode in ('auth', 'mechanism', 'unsupported', 'malformed', 'resource'):
        CASE_NAMES.add(f'{mode} terminal after TLS rejection at {stage}')
CASE_NAMES = frozenset(CASE_NAMES)
REPORTER_NAMES = {'bounded TLS Fetch recovery ' + name: name for name in CASE_NAMES}
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
    print('TLS_FETCH_RECOVERY_FIXTURE ' + json.dumps(result, sort_keys=True))
    return 0 if result['ok'] and result['cleaned'] else 1


if __name__ == '__main__':
    sys.exit(main())
