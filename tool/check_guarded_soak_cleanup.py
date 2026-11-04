#!/usr/bin/env python3
"""Explicit opt-in check of active host-guard interruption and owned cleanup.

Starts only the ordinary owned operational fixture. Once a progress record proves
that sustained work is active, its guard sampler reports a synthetic low-memory
value. The real guard then signals this process and the real fixture tears down.
No system memory is allocated or exhausted to induce the condition. Pass an
explicit local test command after --, preferably restricted to the sustained test.
"""
import json
import os
import pathlib
import signal
import socket
import tempfile

import run_guarded_soak as guarded


def active_progress(path):
    """Return measured positive-record progress, never the initial empty sample."""
    for log in sorted(path.glob('run.*/tests.log')):
        for line in log.read_text(errors='replace').splitlines():
            if line.startswith('SOAK_PROGRESS '):
                try:
                    progress = json.loads(line.partition(' ')[2])
                except json.JSONDecodeError:
                    # A reader may see the final line between pipe writes.
                    continue
                if progress['records'] > 0 and progress['health_failure'] is None:
                    return progress
    return None


def reusable(ports):
    sockets = []
    try:
        for port in ports:
            probe = socket.socket()
            sockets.append(probe)
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(('127.0.0.1', port))
        return True
    except OSError:
        return False
    finally:
        for probe in sockets:
            probe.close()


def check_passed(status, injection, receipt, ports_ok):
    return bool(status == 2 and injection['progress'] and
                injection['termination_called'] and injection['interruption_observed'] and
                receipt and receipt['exit_code'] != 0 and
                receipt['owned_process_stopped'] and receipt['data_removed'] and
                receipt['control_removed'] and not receipt['cleanup_errors'] and ports_ok)


def main():
    os.umask(0o077)
    base = pathlib.Path(os.environ.get('KAFKA_GUARD_CHECK_ROOT', str(
        guarded.run_operational_integration.ROOT / '.dart_tool/kafka-guard-check')))
    base.mkdir(parents=True, exist_ok=True)
    run = pathlib.Path(tempfile.mkdtemp(prefix='check.', dir=base.resolve()))
    os.environ['KAFKA_OPERATIONAL_RUN_ROOT'] = str(run)
    # Enough time for a positive minute sample; interruption must occur before
    # natural completion. A run with no positive progress is a failed check.
    os.environ['KAFKA_SOAK_SECONDS'] = '180'
    original_guard = guarded.Guard
    original_signal = signal.signal
    injection = {'progress': None, 'termination_called': False,
                 'interruption_observed': False}

    def trace_handler_registration(selected, handler):
        if selected == signal.SIGTERM and callable(handler):
            def traced(signum, frame):
                try:
                    return handler(signum, frame)
                except KeyboardInterrupt:
                    if injection['progress'] and injection['termination_called']:
                        injection['interruption_observed'] = True
                    raise
            return original_signal(selected, traced)
        return original_signal(selected, handler)

    class InjectedGuard(original_guard):
        def __init__(self, path):
            def sample(selected):
                actual = guarded.read_resources(selected)
                if guarded.violation(actual) is None:
                    progress = active_progress(selected)
                    if progress is not None:
                        injection['progress'] = progress
                        print('INJECTED_HOST_GUARD ' + json.dumps({
                            'observed_progress': progress,
                            'actual_resources': actual,
                            'synthetic_mem_available_bytes': 0}), flush=True)
                        return dict(actual, mem_available_bytes=0)
                return actual
            super().__init__(path, sample=sample)
            terminate = self.terminate

            def traced_termination():
                injection['termination_called'] = True
                terminate()
            self.terminate = traced_termination

    guarded.Guard = InjectedGuard
    signal.signal = trace_handler_registration
    try:
        status = guarded.main()
    finally:
        guarded.Guard = original_guard
        signal.signal = original_signal
    receipts = list(run.glob('run.*/result.json'))
    receipt = json.loads(receipts[0].read_text()) if len(receipts) == 1 else None
    ports_ok = bool(receipt and reusable(receipt['ports']))
    # A stale positive progress line plus a natural test failure is insufficient.
    # Cleanup ignores SIGTERM; only a delivered signal whose real handler raised
    # KeyboardInterrupt proves this guard actually interrupted the fixture.
    passed = check_passed(status, injection, receipt, ports_ok)
    result = {'passed': passed, 'guard_exit_code': status,
              'injected_after_progress': injection['progress'],
              'guard_termination_called': injection['termination_called'],
              'guard_interruption_observed': injection['interruption_observed'],
              'fixture_result': receipt, 'ports_reusable': ports_ok}
    (run / 'guard-cleanup-result.json').write_text(json.dumps(result, indent=2) + '\n')
    print('GUARD_CLEANUP_RESULT ' + json.dumps(result), flush=True)
    print(f'Guard cleanup diagnostics: {run}', flush=True)
    return 0 if passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
