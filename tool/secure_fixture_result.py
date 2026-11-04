"""Fail-closed, fixed-field summary for fixtures holding client private keys.

No raw test text, PEM, byte lists, encodings, paths or native diagnostics survive
into the mTLS result. Older server-only fixture output retains its prior contract.
"""
import re


def mtls_summary(test_text, status, stage, version):
    """Return (safe summary, effective status), discarding every arbitrary byte."""
    # Version/stage values are normally internal, but keep this helper's output
    # allowlisted even if its direct caller supplies an untrusted value.
    if not re.fullmatch(r'4\.[0-9]+\.[0-9]+', version):
        version = 'unknown'
    stages = {'initializing', 'certificates', 'startup', 'java-security-smoke',
              'tests', 'java-oracle', 'complete'}
    if stage not in stages:
        stage = 'unknown'
    plain_text = re.sub(r'\x1b\[[0-9;]*m', '', test_text)
    match = re.search(r'^\d+:\d+ \+([0-9]+): All tests passed!$',
                      plain_text, flags=re.MULTILINE)
    success = status == 0 and stage == 'complete' and match is not None
    if status == 0 and not success:
        status = 1
    count = int(match.group(1)) if success else 0
    return (f'Kafka {version} mTLS fixture: stage={stage}, exit={int(status)}\n'
            'client_auth=required (SSL and SASL_SSL listener overrides)\n'
            f'tests={"passed" if success else "not-passed"}; passed_cases={count}\n'
            'Raw output and identity material are excluded from this summary.\n',
            status)
