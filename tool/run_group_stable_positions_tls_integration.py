#!/usr/bin/env python3
"""Strict disposable stable-position TLS matrix, using explicit cached commands.

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
_SPEC = importlib.util.spec_from_file_location('_group_stable_positions_tls_fixture', _BASE_PATH)
_BASE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_BASE)

CASE_NAMES = {
    'omitted stable option preserves default v5',
    'ambiguous Join is sent once without retry',
    'ambiguous commit is sent once without retry',
}
for fault in ('code15', 'code16', 'disconnect'):
    for enabled in ('false', 'true'):
        CASE_NAMES.add(f'{fault} stable heartbeat optIn={enabled} drains before fresh generation')
for defect in ('untrusted', 'wronghost', 'expired', 'auth', 'malformed'):
    CASE_NAMES.add(f'{defect} replacement startup is terminal without retry')
for fault in ('code16', 'disconnect'):
    CASE_NAMES.add(f'{fault} initial heartbeat is terminal without retry')
for enabled in ('true', 'false'):
    for fresh in ((-1, 9, 10, 11) if enabled == 'true' else (-1, 9)):
        CASE_NAMES.add(f'replacement position {fresh} after stored10 optIn={enabled} uses only fresh authority')
for enabled in ('false', 'true'):
    CASE_NAMES.add(f'position optIn={enabled} selects exact wire version')
for fault in ('unsupported', 'partition88', 'group88', 'malformed', 'empty', 'null'):
    CASE_NAMES.add(f'{fault} stable position is terminal without retry or downgrade')
for ending in ('revoke', 'stop', 'expiry'):
    CASE_NAMES.add(f'{ending} during delayed stable lookup closes positions without admission')
CASE_NAMES = frozenset(CASE_NAMES)
REPORTER_NAMES = {'managed stable positions TLS ' + name: name for name in CASE_NAMES}
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
    print('GROUP_STABLE_POSITIONS_TLS_FIXTURE ' + json.dumps(result, sort_keys=True))
    return 0 if result['ok'] and result['cleaned'] else 1


if __name__ == '__main__':
    sys.exit(main())
