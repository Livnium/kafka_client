"""Select an exact Kafka distribution for an owned qualification fixture.

The historical default remains 4.0.2. Selecting another stable 4.x release only
enables a test attempt; it is not evidence that the client supports that release.
The caller must independently verify the official distribution's SHA512 before
running a fixture. A matching jar filename is not an authenticity check.
"""
import os
import pathlib
import re


def selected_distribution(environ=None):
    """Return (resolved home, exact version), or fail before starting a broker."""
    env = os.environ if environ is None else environ
    version = env.get('KAFKA_VERSION', '4.0.2')
    if not re.fullmatch(r'4\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', version):
        raise SystemExit('KAFKA_VERSION must be an exact stable 4.x release '
                         '(for example 4.0.2 or 4.4.0); prereleases are not accepted')
    if not env.get('KAFKA_HOME'):
        raise SystemExit('Set KAFKA_HOME to an independently SHA512-verified '
                         f'official Kafka {version} distribution')
    home = pathlib.Path(env['KAFKA_HOME']).resolve()
    expected = home / 'libs' / f'kafka-clients-{version}.jar'
    if not expected.is_file():
        raise SystemExit(f'Expected Kafka {version} client jar: {expected}. '
                         'Set KAFKA_VERSION explicitly for another 4.x release')
    jars = sorted(home.joinpath('libs').glob('kafka-clients-*.jar'))
    if jars != [expected]:
        raise SystemExit('KAFKA_HOME contains multiple kafka-clients jars; '
                         'use one unmodified, independently verified distribution')
    return home, version


if __name__ == '__main__':
    print(selected_distribution()[1])
