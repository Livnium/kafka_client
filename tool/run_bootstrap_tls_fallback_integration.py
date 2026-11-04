#!/usr/bin/env python3
"""Disposable strict TLS fixture. Usage: runner -- <cached Dart test command>.

No resolver, broker, external service, persistent credentials, or raw child logs.
Child output is drained without persistence. Only numeric JSON test statuses
and fixed result fields are emitted. Certificates are verified for their exact intended defect.
"""
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import threading

ROOT = Path(__file__).resolve().parent.parent
PREFIX = 'KAFKA_BOOTSTRAP_TLS_'

# Exact owned test names only: arbitrary reporter strings are not safe output.
CASE_NAMES = set()
for bad in ('eof', 'untrusted', 'wronghost', 'expired'):
    CASE_NAMES.add(f'{bad} then valid strictly validates before SASL or Kafka')
    CASE_NAMES.add(f'{bad} default-off never reaches next valid peer')
CASE_NAMES.add('all invalid bounded once, sanitized, no outer read retries')
for earlier in ('false', 'true'):
    for mode in ('auth', 'mechanism', 'unsupported', 'malformed', 'resource'):
        CASE_NAMES.add(f'{mode} terminal, earlier TLS failure={earlier}')
for ambiguous in ('false', 'true'):
    CASE_NAMES.add(f'pre-send metadata fallback Produce once, ambiguous={ambiguous}')
CASE_NAMES.add('discovered leader TLS failure never uses another bootstrap for Produce')
REPORTER_NAMES = {'validated bootstrap TLS fallback '+name: name for name in CASE_NAMES}

def guard():
    mem = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    if int(mem['MemAvailable'].split()[0]) * 1024 < 1024**3:
        raise RuntimeError('memory guard')
    if shutil.disk_usage(ROOT).free < 2 * 1024**3:
        raise RuntimeError('disk guard')

def certificates(run):
    def openssl(*args, expected=None):
        result = subprocess.run(['openssl', *args], cwd=run, capture_output=True, timeout=15)
        # Never propagate OpenSSL output or paths to public diagnostics.
        if expected is None and result.returncode:
            raise RuntimeError('certificate generation failed')
        if expected is not None and (result.returncode == 0 or expected not in result.stderr.decode(errors='replace')):
            raise RuntimeError('certificate rejection verification failed')
    for ca in ['ca', 'untrusted-ca']:
        openssl('req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes', '-days', '2', '-subj', '/CN=DisposableFixture-'+ca, '-keyout', ca+'.key', '-out', ca+'.pem')
    for name, ca, host in [('valid','ca','localhost'), ('untrusted','untrusted-ca','localhost'), ('wronghost','ca','not-localhost.invalid'), ('expired','ca','localhost')]:
        openssl('req', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes', '-subj', '/CN='+host, '-keyout', name+'.key', '-out', name+'.csr')
        (run / 'extensions.cnf').write_text('subjectAltName=DNS:'+host+'\nextendedKeyUsage=serverAuth\nbasicConstraints=critical,CA:FALSE\n')
        if name == 'expired':
            (run / 'index').touch()
            (run / 'serial').write_text('1000\n')
            (run / 'ca.cnf').write_text('[ca]\ndefault_ca=local\n[local]\ndatabase=index\nserial=serial\nnew_certs_dir=.\ncertificate=ca.pem\nprivate_key=ca.key\ndefault_md=sha256\npolicy=policy\n[policy]\ncommonName=supplied\n')
            openssl('ca','-batch','-config','ca.cnf','-in',name+'.csr','-out',name+'.pem','-startdate','20000101000000Z','-enddate','20000102000000Z','-extfile','extensions.cnf')
        else:
            openssl('x509','-req','-in',name+'.csr','-CA',ca+'.pem','-CAkey',ca+'.key','-CAcreateserial','-days','1','-extfile','extensions.cnf','-out',name+'.pem')
    openssl('verify','-CAfile','ca.pem','-verify_hostname','localhost','valid.pem')
    openssl('verify','-CAfile','untrusted-ca.pem','-verify_hostname','localhost','untrusted.pem')
    openssl('verify','-CAfile','ca.pem','-verify_hostname','localhost','untrusted.pem',expected='error 20 ')
    openssl('verify','-CAfile','ca.pem','-verify_hostname','not-localhost.invalid','wronghost.pem')
    openssl('verify','-CAfile','ca.pem','-verify_hostname','localhost','wronghost.pem',expected='error 62 ')
    openssl('verify','-CAfile','ca.pem','-verify_hostname','localhost','expired.pem',expected='error 10 ')
    openssl('verify','-no_check_time','-CAfile','ca.pem','-verify_hostname','localhost','expired.pem')

class FixtureInterrupted(BaseException):
    pass


def process_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def group_rss(pgid):
    def rss(pid):
        try:
            status = (Path('/proc') / str(pid) / 'status').read_text()
            fields = dict(line.split(':', 1) for line in status.splitlines() if ':' in line)
            if fields.get('State', '').strip().startswith(('Z', 'X')):
                return 0  # A genuinely exited, not-yet-reaped process.
            value = fields.get('VmRSS', '').split()
            if len(value) != 2 or value[1] != 'kB' or int(value[0]) < 0:
                raise RuntimeError('rss unavailable')
            return int(value[0]) * 1024
        except (OSError, ValueError, RuntimeError):
            if process_exists(pid):
                raise RuntimeError('rss unavailable') from None
            return 0
    # Root is mandatory, even if it disappears from a racing /proc listing.
    total = rss(pgid)
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit() or int(entry.name) == pgid:
            continue
        pid = int(entry.name)
        try:
            owned = os.getpgid(pid) == pgid
        except OSError:
            continue  # Unrelated process churn or an exited descendant.
        if owned:
            total += rss(pid)
    return total


def stop(process):
    if getattr(process, '_fixture_stopped', False):
        return True
    # Signal the owned group once, including children after root exit.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=10)
    process._fixture_stopped = True
    return process.returncode is not None

def execute(command, fixture_builder=certificates, timeout=180, guard_fn=guard):
    run = None
    process = None
    reader = None
    reader_failed = threading.Event()
    previous_handlers = {}
    result = {'ok': False, 'stage': 'guard', 'cleaned': False, 'process_finished': True, 'reader_finished': True, 'reader_ok': True, 'peak_rss_bytes': 0}
    def interrupt(signum, frame):
        raise FixtureInterrupted()
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[sig] = signal.signal(sig, interrupt)
        guard_fn()
        run = Path(tempfile.mkdtemp(prefix='bootstrap-tls-'))
        run.chmod(0o700)
        result['stage'] = 'certificates'
        fixture_builder(run)
        env = dict(os.environ)
        env.update({PREFIX+'FIXTURE_DIR': str(run), PREFIX+'USERNAME': secrets.token_urlsafe(20), PREFIX+'PASSWORD': secrets.token_urlsafe(32)})
        result['stage'] = 'child'
        # Drain without storing arbitrary child output. JSON test reporter
        # numbers/statuses are the only child-derived diagnostics retained.
        summary = {'passed': 0, 'failed': 0, 'skipped': 0, 'failed_test_ids': [], 'cases': [], 'reporter_success': False}
        test_names = {}
        def drain(stream):
            while True:
                line = stream.readline(65537)
                if not line:
                    return
                if len(line) > 65536:
                    while line and not line.endswith(b'\n'):
                        line = stream.readline(65537)
                    continue
                try:
                    event = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    continue
                if not isinstance(event, dict):
                    continue
                if event.get('type') == 'done':
                    summary['reporter_success'] = event.get('success') is True
                    continue
                if event.get('type') == 'testStart':
                    test = event.get('test')
                    if isinstance(test, dict) and type(test.get('id')) is int and test.get('name') in REPORTER_NAMES and len(test_names) < 64:
                        test_names[test['id']] = REPORTER_NAMES[test['name']]
                    continue
                if event.get('type') != 'testDone':
                    continue
                if event.get('hidden') is True:
                    continue
                status = 'skipped' if event.get('skipped') is True else ('passed' if event.get('result') == 'success' else 'failed')
                summary[status] += 1
                test_id = event.get('testID')
                if type(test_id) is int and test_id in test_names and len(summary['cases']) < 64:
                    case = {'name': test_names[test_id], 'status': status}
                    if status == 'failed':
                        case['error_category'] = 'test_failure'
                    summary['cases'].append(case)
                if status == 'failed' and type(test_id) is int and len(summary['failed_test_ids']) < 64:
                    summary['failed_test_ids'].append(test_id)
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        def safe_drain():
            try:
                drain(process.stdout)
            except Exception:
                reader_failed.set()
        reader = threading.Thread(target=safe_drain, daemon=True)
        reader.start()
        deadline = time.monotonic() + timeout
        next_guard = time.monotonic() + 1
        while process.poll() is None:
            if time.monotonic() >= next_guard:
                try:
                    guard_fn()
                except Exception:
                    result['stage'] = 'host_guard'
                    break
                next_guard = time.monotonic() + 1
            if time.monotonic() >= deadline:
                result['stage'] = 'timeout'
                break
            try:
                sampled_rss = group_rss(process.pid)
            except Exception:
                result['stage'] = 'rss_unavailable'
                # Linux may discard mm/VmRSS before waitpid exposes exit.
                # Confirm exit within one sampling interval and the existing
                # deadline; a live/unobservable child still fails closed.
                exited_code = None
                try:
                    exited_code = process.poll()
                    if exited_code is None:
                        exited_code = process.wait(timeout=min(
                            .1, max(0, deadline - time.monotonic())))
                except Exception:
                    pass
                if exited_code is not None:
                    result['stage'] = 'child'
                    result['exit_code'] = exited_code
                    result['ok'] = exited_code == 0
                break
            result['peak_rss_bytes'] = max(result['peak_rss_bytes'], sampled_rss)
            if sampled_rss > 768 * 1024**2:
                result['stage'] = 'rss_guard'
                break
            time.sleep(.1)
        else:
            result['exit_code'] = process.returncode
            result['ok'] = process.returncode == 0
        stop(process)
        reader.join(timeout=5)
        result['tests'] = summary
        result['expected_tests'] = len(CASE_NAMES)
        complete = (summary['reporter_success'] and summary['passed'] == len(CASE_NAMES) and summary['failed'] == 0 and summary['skipped'] == 0 and len(summary['cases']) == len(CASE_NAMES) and {case['name'] for case in summary['cases']} == CASE_NAMES)
        result['complete_test_matrix'] = complete
        result['ok'] = result['ok'] and complete
    except (FixtureInterrupted, KeyboardInterrupt):
        result['ok'] = False
        result['stage'] = 'interrupted'
    except Exception:
        # Exception text can contain private file paths or synthetic credentials.
        result['ok'] = False
    finally:
        # Repeated termination must not interrupt owned process/key cleanup.
        for sig in previous_handlers:
            signal.signal(sig, signal.SIG_IGN)
        try:
            try:
                if process is not None:
                    result['process_finished'] = stop(process)
            except Exception:
                result['process_finished'] = False
                result['stage'] = 'process_cleanup_failed'
            if reader is not None:
                reader.join(timeout=5)
                result['reader_finished'] = not reader.is_alive()
                result['reader_ok'] = not reader_failed.is_set()
                if not result['reader_finished'] or not result['reader_ok']:
                    result['stage'] = 'reader_cleanup_failed'
            if process is not None and process.stdout is not None and result['reader_finished']:
                try:
                    process.stdout.close()
                except Exception:
                    result['reader_ok'] = False
                    result['stage'] = 'reader_cleanup_failed'
            try:
                if run is not None:
                    shutil.rmtree(run)
                result['cleaned'] = run is None or not run.exists()
            except Exception:
                result['cleaned'] = False
                result['stage'] = 'fixture_cleanup_failed'
            result['ok'] = result['ok'] and all(result[key] for key in ('cleaned', 'process_finished', 'reader_finished', 'reader_ok'))
        finally:
            for sig, previous in previous_handlers.items():
                signal.signal(sig, previous)
    return result

def main():
    args = sys.argv[1:]
    if len(args) < 2 or args[0] != '--':
        print('Usage: runner -- <explicit cached Dart test command>')
        return 2
    result = execute(args[1:])
    print('BOOTSTRAP_TLS_FIXTURE '+json.dumps(result, sort_keys=True))
    return 0 if result['ok'] and result['cleaned'] else 1

if __name__ == '__main__':
    sys.exit(main())
