#!/usr/bin/env python3
"""Bounded sustained observation; resource guards are not capacity certification.

Runs the existing operational runner in this process so its signal handler owns
cleanup. Signals only this wrapper PID, never searches for or kills other PIDs.
Use -- followed by an explicit cached Dart test command to avoid pub resolution.
"""
import json
import os
import pathlib
import shutil
import signal
import threading
import time

import run_operational_integration

GIB = 1024 ** 3


def read_resources(path):
    fields = {}
    for line in pathlib.Path('/proc/meminfo').read_text().splitlines():
        name, value = line.split(':', 1)
        fields[name] = int(value.split()[0]) * 1024
    return {'disk_free_bytes': shutil.disk_usage(path).free,
            'mem_available_bytes': fields['MemAvailable']}


def violation(sample):
    if sample['disk_free_bytes'] < 2 * GIB:
        return 'free disk below 2 GiB'
    if sample['mem_available_bytes'] < GIB:
        return 'MemAvailable below 1 GiB'
    return None


class Guard:
    def __init__(self, path, sample=read_resources, terminate=None):
        self.path, self.sample = path, sample
        self.terminate = terminate or (lambda: os.kill(os.getpid(), signal.SIGTERM))
        self.stopping = threading.Event()
        self.reason = None
        self.started = time.monotonic()
        self.last_report = -60.0
        self.latest = None

    def check(self):
        try:
            self.latest = self.sample(self.path)
            reason = violation(self.latest)
        except Exception as error:
            reason = f'resource sampling failed: {type(error).__name__}: {error}'
        elapsed = time.monotonic() - self.started
        if reason or elapsed - self.last_report >= 60:
            print('HOST_RESOURCES ' + json.dumps({
                'seconds': round(elapsed, 3), 'metrics': self.latest,
                'guard_failure': reason}), flush=True)
            self.last_report = elapsed
        if reason:
            self.reason = reason
        return reason is None

    def watch(self):
        while not self.stopping.wait(5):
            if not self.check():
                self.terminate()
                return


def main(runner=run_operational_integration.main):
    os.environ.setdefault('KAFKA_SOAK_SECONDS', '1800')
    os.environ.setdefault('KAFKA_SOAK_MAX_RSS_BYTES', str(768 * 1024 ** 2))
    path = pathlib.Path(os.environ.get('KAFKA_OPERATIONAL_RUN_ROOT', str(
        run_operational_integration.ROOT / '.dart_tool/kafka-operational')))
    path.mkdir(parents=True, exist_ok=True)
    guard = Guard(path)
    if not guard.check():
        print('SOAK_GUARD_RESULT ' + json.dumps({'exit_code': 2, 'reason': guard.reason}), flush=True)
        return 2
    thread = threading.Thread(target=guard.watch, daemon=True)
    # Cover the small interval before the operational runner installs its own
    # handler. No broker has been started at that point.
    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'Signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    status = 1
    try:
        thread.start()
        status = runner()
    except KeyboardInterrupt:
        status = 130
    finally:
        guard.stopping.set()
        thread.join(timeout=6)
        if guard.reason:
            status = 2
        print('SOAK_GUARD_RESULT ' + json.dumps({
            'exit_code': status, 'reason': guard.reason,
            'last_metrics': guard.latest}), flush=True)
    return status


if __name__ == '__main__':
    raise SystemExit(main())
