import unittest

from check_secure_example import case_environment, output_matches


class SecureExampleRunnerTests(unittest.TestCase):
    def test_fixture_environment_replaces_stale_inputs(self):
        source = dict(KAFKA_SECURE_TLS_PORT='1', KAFKA_SECURE_SASL_PORT='2',
                      KAFKA_SECURE_TOPIC='owned', KAFKA_SECURE_CA='ca',
                      KAFKA_SECURE_WRONG_CA='wrong', KAFKA_SECURE_USERNAME='synthetic-user',
                      KAFKA_SECURE_PASSWORD='synthetic-password', KAFKA_HOST='external',
                      KAFKA_USERNAME='stale', KAFKA_PASSWORD='stale', KEEP='yes')
        tls = case_environment(source, 'tls')
        self.assertEqual(tls['KAFKA_HOST'], 'localhost')
        self.assertNotIn('KAFKA_USERNAME', tls)
        self.assertNotIn('KAFKA_PASSWORD', tls)
        self.assertEqual(tls['KEEP'], 'yes')
        sasl = case_environment(source, 'sasl_tls')
        self.assertEqual(sasl['KAFKA_PORT'], '2')
        self.assertEqual(sasl['KAFKA_PASSWORD'], 'synthetic-password')
        self.assertEqual(case_environment(source, 'wrong_hostname')['KAFKA_HOST'], '127.0.0.1')
        self.assertEqual(case_environment(source, 'wrong_ca')['KAFKA_CA_FILE'], 'wrong')
        self.assertNotEqual(case_environment(source, 'bad_password')['KAFKA_PASSWORD'], sasl['KAFKA_PASSWORD'])
        with self.assertRaises(ValueError):
            case_environment(source, 'unknown')

    def test_result_contract_rejects_false_success_and_extra_output(self):
        self.assertTrue(output_matches('tls', 0, 'SECURE_EXAMPLE_OK offset=2 mode=tls\n', ''))
        self.assertTrue(output_matches('sasl_tls', 0, 'SECURE_EXAMPLE_OK offset=2 mode=sasl_tls\n', ''))
        self.assertFalse(output_matches('tls', 1, 'SECURE_EXAMPLE_OK offset=2 mode=tls\n', ''))
        self.assertFalse(output_matches('tls', 0, 'SECURE_EXAMPLE_OK offset=2 mode=tls\nextra', ''))
        self.assertFalse(output_matches('tls', 0, 'SECURE_EXAMPLE_OK offset=2 mode=tls\n', 'extra'))
        self.assertTrue(output_matches('wrong_ca', 1, '', 'SECURE_EXAMPLE_ERROR tls\nA write may already be stored; do not blindly rerun it.\n'))
        self.assertTrue(output_matches('wrong_hostname', 1, '', 'SECURE_EXAMPLE_ERROR tls\nA write may already be stored; do not blindly rerun it.\n'))
        self.assertTrue(output_matches('bad_password', 1, '', 'SECURE_EXAMPLE_ERROR authentication\nA write may already be stored; do not blindly rerun it.\n'))
        self.assertFalse(output_matches('bad_password', 0, '', 'SECURE_EXAMPLE_ERROR authentication\nA write may already be stored; do not blindly rerun it.\n'))
        self.assertFalse(output_matches('bad_password', 1, '', 'SECURE_EXAMPLE_ERROR tls\nA write may already be stored; do not blindly rerun it.\n'))


if __name__ == '__main__':
    unittest.main()
