#!/usr/bin/env python3
"""Guard a standalone retry probe. No broker, resolver, or VM service is started.

Usage: run_retry_waiter_probe.py RUN_DIRECTORY -- DART [cached/source arguments] MODE ITERATIONS
Fixed guards apply to the owned child process group: 120s, 768MiB RSS,
2GiB free disk, 1GiB available RAM, sampled every 100ms. Never changes limits.
The intended Dart probe launches no descendants. Cleanup records owned leader
exit, not an independently verified empty process group; no arbitrary worker
command or descendant-lifecycle guarantee is implied by this diagnostic.
"""
import hashlib
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

from run_bootstrap_tls_fallback_integration import group_rss
from run_guarded_soak import read_resources, violation

RSS_LIMIT = 768 * 1024 ** 2
WALL_LIMIT = 120


def expected_checkpoints(iterations):
    return sorted({0, iterations, *(n for n in (10, 100, 1000) if n <= iterations)})


def bounded_int(value, minimum, maximum):
    return type(value) is int and minimum <= value <= maximum


def check_report(text, return_code, guard_failure, expected_mode, expected_iterations):
    rows = [line[len('RETRY_WAITER_PROBE '):] for line in text.splitlines()
            if line.startswith('RETRY_WAITER_PROBE ')]
    if (return_code != 0 or guard_failure is not None or len(rows) != 1
            or expected_mode not in ('legacy', 'removable')
            or not bounded_int(expected_iterations, 1, 10000)):
        return False
    try:
        row = json.loads(rows[0])
        if not isinstance(row, dict):
            return False
        required = {'passed', 'guard_failure', 'failure', 'after_close_waiters',
                    'network_used', 'iterations', 'retries', 'operations', 'mode',
                    'source_method', 'synthetic_operation', 'sdk_private_mirrors',
                    'dart_version', 'samples', 'rss_limit_bytes',
                    'rss_sample_interval_ms', 'wall_limit_seconds', 'elapsed_ms',
                    'initial_rss_bytes', 'sampled_peak_rss_bytes', 'final_rss_bytes',
                    'close_elapsed_us'}
        if not required.issubset(row):
            return False
        if not (row['passed'] is True and row['guard_failure'] is None
                and row['failure'] is None and row['network_used'] is False
                and row['mode'] == expected_mode
                and row['source_method'] == '_safeRetry'
                and row['synthetic_operation'] is True
                and row['sdk_private_mirrors'] is True
                and isinstance(row['dart_version'], str) and row['dart_version'].strip()):
            return False
        for key, expected in (
                ('iterations', expected_iterations), ('retries', expected_iterations),
                ('operations', 2 * expected_iterations), ('after_close_waiters', 0),
                ('rss_limit_bytes', RSS_LIMIT), ('rss_sample_interval_ms', 100),
                ('wall_limit_seconds', WALL_LIMIT)):
            if type(row[key]) is not int or row[key] != expected:
                return False
        if not bounded_int(row['elapsed_ms'], 0, WALL_LIMIT * 1000):
            return False
        if not bounded_int(row['close_elapsed_us'], 0, WALL_LIMIT * 1000000):
            return False
        for key in ('initial_rss_bytes', 'sampled_peak_rss_bytes', 'final_rss_bytes'):
            if not bounded_int(row[key], 1, RSS_LIMIT):
                return False
        if row['sampled_peak_rss_bytes'] < max(row['initial_rss_bytes'], row['final_rss_bytes']):
            return False
        samples = row['samples']
        checkpoints = expected_checkpoints(expected_iterations)
        if not isinstance(samples, list) or len(samples) != len(checkpoints):
            return False
        previous_ms = 0
        for sample, checkpoint in zip(samples, checkpoints):
            if not isinstance(sample, dict):
                return False
            for key, expected in (
                    ('completed_retries', checkpoint),
                    ('retained_close_listeners', checkpoint if expected_mode == 'legacy' else 0),
                    ('active_removable_waiters', 0)):
                if type(sample[key]) is not int or sample[key] != expected:
                    return False
            if not bounded_int(sample['rss_bytes'], 1, row['sampled_peak_rss_bytes']):
                return False
            if not bounded_int(sample['elapsed_ms'], previous_ms, row['elapsed_ms']):
                return False
            previous_ms = sample['elapsed_ms']
        return True
    except (ValueError, TypeError, KeyError):
        return False


def stop_owned_process(process):
    """Reap the owned leader; tolerate a verified exit racing killpg."""
    if process.poll() is not None:
        return
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            if process.poll() is None:
                raise
            return
        try:
            process.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            if sig == signal.SIGKILL:
                raise


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) < 6 or args[1] != '--':
        print(__doc__, file=sys.stderr)
        return 64
    command = args[2:]
    try:
        expected_mode, expected_iterations = command[-2], int(command[-1])
        if expected_mode not in ('legacy', 'removable') or not 1 <= expected_iterations <= 10000:
            raise ValueError('Invalid probe mode or iteration count')
    except ValueError:
        print('Command must end with legacy|removable and 1..10000 iterations', file=sys.stderr)
        return 64
    target = pathlib.Path(args[0])
    target.mkdir(parents=True, exist_ok=False)
    provenance = {}
    for name in ('lib/src/kafka_client_base.dart', 'lib/src/close_wait_group.dart',
                 'tool/measure_retry_waiters.dart', '.dart_tool/package_config.json', command[0]):
        path = pathlib.Path(name)
        if path.is_file():
            provenance[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    log = target / 'probe.log'
    started = time.monotonic()
    process = None
    reason = None
    peak_rss = 0
    resource_samples = []
    return_code = None
    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'Signal {signum}')
    previous = {s: signal.signal(s, interrupted) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        preflight = read_resources(target)
        resource_samples.append(preflight)
        reason = violation(preflight)
        if reason is None:
            with log.open('w') as output:
                process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                           start_new_session=True)
                while True:
                    return_code = process.poll()
                    if return_code is not None:
                        break
                    if time.monotonic() - started > WALL_LIMIT:
                        reason = 'Fixed wall deadline exceeded'
                        break
                    resources = read_resources(target)
                    if len(resource_samples) < 2:
                        resource_samples.append(resources)
                    else:
                        resource_samples[-1] = resources
                    reason = violation(resources)
                    if reason is not None:
                        break
                    try:
                        rss = group_rss(process.pid)
                    except Exception:
                        # A disappearing owned child is not a sampling failure
                        # only when a fresh poll confirms its actual exit.
                        return_code = process.poll()
                        if return_code is None:
                            raise
                        break
                    peak_rss = max(peak_rss, rss)
                    if rss > RSS_LIMIT:
                        reason = 'Fixed 768 MiB RSS limit exceeded'
                        break
                    time.sleep(0.1)
    except (Exception, KeyboardInterrupt) as error:
        reason = f'{type(error).__name__}: {error}'
    finally:
        try:
            if process is not None:
                stop_owned_process(process)
                return_code = process.poll()
        except (Exception, KeyboardInterrupt) as error:
            reason = reason or f'Cleanup failed: {type(error).__name__}: {error}'
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
        try:
            final_resources = read_resources(target)
            if len(resource_samples) < 2:
                resource_samples.append(final_resources)
            else:
                resource_samples[-1] = final_resources
            reason = reason or violation(final_resources)
        except Exception as error:
            reason = reason or f'Final resource sampling failed: {type(error).__name__}: {error}'
        if time.monotonic() - started > WALL_LIMIT:
            reason = reason or 'Fixed wall deadline exceeded'
    content = log.read_text() if log.exists() else ''
    result = {
        'passed': check_report(content, return_code, reason, expected_mode, expected_iterations), 'command': command,
        'cwd': str(pathlib.Path.cwd()), 'file_sha256_before_run': provenance, 'exit_code': return_code,
        'seconds': round(time.monotonic() - started, 3), 'guard_failure': reason,
        'sampled_peak_rss_bytes': peak_rss, 'rss_limit_bytes': RSS_LIMIT,
        'wall_limit_seconds': WALL_LIMIT, 'sample_interval_ms': 100,
        'host_resources_first_last': resource_samples,
        'owned_process_stopped': process is None or process.poll() is not None,
        'probe_log_sha256': hashlib.sha256(content.encode()).hexdigest(),
    }
    (target / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(content, end='')
    print('RETRY_WAITER_RUNNER ' + json.dumps(result), flush=True)
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
