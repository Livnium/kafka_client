#!/usr/bin/env python3
"""No-broker checks for explicit release selection and launcher preflight."""
import importlib
import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from kafka_distribution import selected_distribution


class DistributionSelectionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.home = pathlib.Path(self.temporary.name) / 'kafka'
        self.libs = self.home / 'libs'
        self.libs.mkdir(parents=True)

    def jar(self, version):
        (self.libs / f'kafka-clients-{version}.jar').touch()

    def test_default_retains_historical_release(self):
        self.jar('4.0.2')
        self.assertEqual(selected_distribution({'KAFKA_HOME': str(self.home)}),
                         (self.home.resolve(), '4.0.2'))

    def test_explicit_stable_four_x_versions(self):
        for version in ('4.0.0', '4.1.2', '4.2.2', '4.3.2', '4.4.0'):
            with self.subTest(version=version):
                self.jar(version)
                self.assertEqual(selected_distribution({
                    'KAFKA_HOME': str(self.home), 'KAFKA_VERSION': version}),
                    (self.home.resolve(), version))
                (self.libs / f'kafka-clients-{version}.jar').unlink()

    def test_other_release_requires_explicit_selection(self):
        self.jar('4.4.0')
        with self.assertRaisesRegex(SystemExit, 'Expected Kafka 4.0.2'):
            selected_distribution({'KAFKA_HOME': str(self.home)})

    def test_mismatch_and_missing_home_fail_closed(self):
        self.jar('4.0.2')
        for env in ({}, {'KAFKA_HOME': ''},
                    {'KAFKA_HOME': str(self.home), 'KAFKA_VERSION': '4.4.0'}):
            with self.subTest(env=env), self.assertRaises(SystemExit):
                selected_distribution(env)

    def test_non_four_x_prerelease_malformed_and_empty_are_rejected(self):
        self.jar('4.0.2')
        for version in ('', '3.9.1', '5.0.0', '4.4', '4.4.0-RC1',
                        '4.4.0-SNAPSHOT', '4.04.0', '4.4.00',
                        '4.4.0\n', '../../4.4.0', '4.4.0;echo unsafe'):
            with self.subTest(version=version), self.assertRaisesRegex(
                    SystemExit, 'exact stable 4.x'):
                selected_distribution({'KAFKA_HOME': str(self.home),
                                       'KAFKA_VERSION': version})

    def test_mixed_distribution_is_rejected(self):
        self.jar('4.0.2')
        self.jar('4.4.0')
        with self.assertRaisesRegex(SystemExit, 'multiple kafka-clients jars'):
            selected_distribution({'KAFKA_HOME': str(self.home),
                                   'KAFKA_VERSION': '4.4.0'})

    def test_directory_named_jar_is_rejected(self):
        (self.libs / 'kafka-clients-4.0.2.jar').mkdir()
        with self.assertRaisesRegex(SystemExit, 'Expected Kafka 4.0.2'):
            selected_distribution({'KAFKA_HOME': str(self.home)})

    def test_shell_launchers_stop_before_creating_run_data(self):
        tool = pathlib.Path(__file__).resolve().parent
        output = pathlib.Path(self.temporary.name) / 'not-created'
        env = dict(os.environ, KAFKA_HOME=str(self.home), KAFKA_VERSION='4.4.0',
                   KAFKA_TEST_RUN_ROOT=str(output), KAFKA_SECURE_RUN_ROOT=str(output))
        for name in ('run_broker_integration.sh', 'run_secure_broker_integration.sh'):
            with self.subTest(name=name):
                result = subprocess.run(['bash', str(tool / name)], env=env,
                                        capture_output=True, text=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Expected Kafka 4.4.0', result.stderr)
                self.assertFalse(output.exists())

    def test_python_launchers_validate_before_external_actions(self):
        names = ('run_operational_integration', 'run_cluster_integration',
                 'run_secure_cluster_integration', 'run_network_fault_integration',
                 'run_tls_network_fault_integration',
                 'run_combined_secure_fault_integration',
                 'run_stable_offsets_migration_integration')
        for name in names:
            with self.subTest(name=name), patch.dict(os.environ,
                    KAFKA_HOME=str(self.home), KAFKA_VERSION='4.4.0'):
                runner = importlib.import_module(name)
                with patch.object(runner.subprocess, 'Popen') as popen, \
                        self.assertRaisesRegex(SystemExit, 'Expected Kafka 4.4.0'):
                    runner.main()
                popen.assert_not_called()


class ConsoleConsumerOptionTest(unittest.TestCase):
    """Execute the actual oracle branch against a fake CLI, never a broker."""
    def exercise(self, help_text, expected_option=None, help_status=0, expected_status=0):
        source = pathlib.Path(__file__).with_name('run_secure_broker_integration.sh').read_text()
        branch = source.split('  STAGE=java-oracle\n', 1)[1].split('\nfi\nSTAGE=complete', 1)[0]
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            binary = root / 'bin'
            binary.mkdir()
            run = root / 'run'
            run.mkdir()
            cli = binary / 'kafka-console-consumer.sh'
            cli.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
if sys.argv[1:] == ['--help']:
    print(os.environ['FIXTURE_HELP'])
    sys.exit(int(os.environ['FIXTURE_HELP_STATUS']))
args = sys.argv[1:]
pathlib.Path(os.environ['RUN_DIR'], 'selected-args.json').write_text(json.dumps(args))
if '--command-config' in args and '--command-config' not in os.environ['FIXTURE_HELP']:
    sys.exit(78)
if '--consumer.config' in args and '--command-config' in os.environ['FIXTURE_HELP']:
    print('Option --consumer.config is deprecated. Use --command-config instead.')
print('secure-java-to-dart-नमस्ते')
print('secure-dart-to-java-नमस्ते')
''')
            cli.chmod(0o700)
            if expected_option:
                help_text = 'Option                                   Description\n' + help_text
            env = dict(os.environ, KAFKA_HOME=str(root), RUN_DIR=str(run),
                       KAFKA_SECURE_SASL_PORT='1', KAFKA_SECURE_ORACLE_TOPIC='synthetic',
                       FIXTURE_HELP=help_text, FIXTURE_HELP_STATUS=str(help_status))
            result = subprocess.run(['bash', '-c', 'set -euo pipefail\n' + branch],
                                    env=env, capture_output=True, text=True, timeout=10)
            if expected_status:
                self.assertEqual(result.returncode, expected_status)
                self.assertFalse((run / 'selected-args.json').exists())
            else:
                self.assertEqual(result.returncode, 0, result.stderr)
                args = json.loads((run / 'selected-args.json').read_text())
                self.assertIn(expected_option, args)
                self.assertIn('Official Java SASL_SSL consumer verified',
                              (run / 'tests.log').read_text())

    def test_old_cli_keeps_legacy_option(self):
        for status in (0, 1):
            with self.subTest(help_status=status):
                self.exercise('--consumer.config <String: config file>',
                              '--consumer.config', help_status=status)

    def test_new_cli_avoids_deprecated_stdout_warning(self):
        for status in (0, 1):
            with self.subTest(help_status=status):
                self.exercise('--command-config <String: config file>\n'
                              '--consumer.config <String: deprecated>',
                              '--command-config', help_status=status)

    def test_help_failure_does_not_run_oracle(self):
        self.exercise('injected CLI failure', help_status=19, expected_status=19)

    def test_unrecognized_help_is_terminal_even_with_normal_help_status(self):
        for status in (0, 1):
            with self.subTest(help_status=status):
                self.exercise('injected CLI failure', help_status=status, expected_status=1)

    def test_usage_table_without_config_option_is_terminal(self):
        self.exercise('Option     Description\n--bootstrap-server <String: server>',
                      help_status=1, expected_status=1)


if __name__ == '__main__':
    unittest.main()
