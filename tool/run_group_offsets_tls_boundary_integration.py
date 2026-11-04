#!/usr/bin/env python3
"""Strict disposable manual group TLS boundary matrix, using explicit cached commands.

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
_SPEC = importlib.util.spec_from_file_location('_group_offsets_tls_boundary_fixture', _BASE_PATH)
_BASE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_BASE)

CASE_NAMES = {'ambiguous commit is never replayed with both recovery flags'}
for op in ('fetch', 'commit'):
    CASE_NAMES.add(f'{op} validated distinct coordinator succeeds')
    for stage in ('bootstrap', 'coordinator'):
        for defect in ('eof', 'untrusted', 'wronghost', 'expired', 'auth', 'mechanism'):
            CASE_NAMES.add(f'{op} {stage} {defect} terminal with both recovery flags')
CASE_NAMES = frozenset(CASE_NAMES)
REPORTER_NAMES = {'manual group TLS boundaries ' + name: name for name in CASE_NAMES}
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
    print('GROUP_OFFSETS_TLS_BOUNDARY_FIXTURE ' + json.dumps(result, sort_keys=True))
    return 0 if result['ok'] and result['cleaned'] else 1


if __name__ == '__main__':
    sys.exit(main())
