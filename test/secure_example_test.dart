import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

import '../example/kafka_client_secure_example.dart' as example;

void main() {
  Map<String, String> environment() => {
        'KAFKA_HOST': 'localhost',
        'KAFKA_TOPIC': 'secure-example',
      };

  group('secure example configuration, without network access', () {
    test('minimum configuration uses TLS port and no SASL or custom CA', () {
      final config = example.SecureExampleConfig.fromEnvironment(environment());
      expect(config.host, 'localhost');
      expect(config.port, 9093);
      expect(config.topic, 'secure-example');
      expect(config.caFile, isNull);
      expect(config.sasl, isNull);
    });

    test('accepts explicit CA path and port without reading the file', () {
      final config = example.SecureExampleConfig.fromEnvironment(environment()
        ..['KAFKA_PORT'] = '19194'
        ..['KAFKA_CA_FILE'] = '/nonexistent/example-ca.pem');
      expect(config.port, 19194);
      expect(config.caFile, '/nonexistent/example-ca.pem');
    });

    test('requires host and topic', () {
      for (final key in ['KAFKA_HOST', 'KAFKA_TOPIC']) {
        expect(
            () => example.SecureExampleConfig.fromEnvironment(
                environment()..remove(key)),
            throwsFormatException);
        expect(
            () => example.SecureExampleConfig.fromEnvironment(
                environment()..[key] = ''),
            throwsFormatException);
      }
    });

    test('rejects malformed endpoints and invalid ports', () {
      for (final host in [
        ' localhost',
        'local host',
        'tls://localhost',
        'a\u0000b',
        'localhost:9093',
        '127.0.0.1:9093',
        '[::1]:9093'
      ]) {
        expect(
            () => example.SecureExampleConfig.fromEnvironment(
                environment()..['KAFKA_HOST'] = host),
            throwsFormatException);
      }
      for (final port in [
        '',
        '0',
        '-1',
        '65536',
        '9093x',
        ' 9093',
        '+9093',
        '0x2385'
      ]) {
        expect(
            () => example.SecureExampleConfig.fromEnvironment(
                environment()..['KAFKA_PORT'] = port),
            throwsFormatException);
      }
      for (final port in ['1', '65535']) {
        expect(
            example.SecureExampleConfig.fromEnvironment(
                    environment()..['KAFKA_PORT'] = port)
                .port,
            int.parse(port));
      }
      expect(
          example.SecureExampleConfig.fromEnvironment(
                  environment()..['KAFKA_HOST'] = '::1')
              .host,
          '::1');
    });

    test('rejects invalid topic names and empty or NUL-containing CA paths',
        () {
      for (final topic in ['.', '..', 'bad topic', 'a/b', 'x' * 250]) {
        expect(
            () => example.SecureExampleConfig.fromEnvironment(
                environment()..['KAFKA_TOPIC'] = topic),
            throwsFormatException);
      }
      for (final path in ['', '   ', 'a\u0000b']) {
        expect(
            () => example.SecureExampleConfig.fromEnvironment(
                environment()..['KAFKA_CA_FILE'] = path),
            throwsFormatException);
      }
    });

    test('requires paired, nonempty SASL credentials', () {
      for (final key in ['KAFKA_USERNAME', 'KAFKA_PASSWORD']) {
        expect(
            () => example.SecureExampleConfig.fromEnvironment(
                environment()..[key] = 'synthetic'),
            throwsFormatException);
      }
      for (final credentials in [
        {'KAFKA_USERNAME': '', 'KAFKA_PASSWORD': 'synthetic'},
        {'KAFKA_USERNAME': 'synthetic', 'KAFKA_PASSWORD': ''},
        {'KAFKA_USERNAME': 'bad\u0000user', 'KAFKA_PASSWORD': 'synthetic'},
        {'KAFKA_USERNAME': 'synthetic', 'KAFKA_PASSWORD': 'bad\u0000pass'},
        {'KAFKA_USERNAME': 'synthetic', 'KAFKA_PASSWORD': 'x' * 16385},
        {'KAFKA_USERNAME': 'x' * 16385, 'KAFKA_PASSWORD': 'synthetic'},
        {'KAFKA_USERNAME': 'synthetic', 'KAFKA_PASSWORD': '\ud800'},
        {'KAFKA_USERNAME': 'synthetic', 'KAFKA_PASSWORD': 'é' * 8193},
      ]) {
        expect(
            () => example.SecureExampleConfig.fromEnvironment(
                environment()..addAll(credentials)),
            throwsArgumentError);
      }
    });

    test('preserves credential whitespace and does not include it in errors',
        () {
      final config = example.SecureExampleConfig.fromEnvironment(environment()
        ..['KAFKA_USERNAME'] = ' user '
        ..['KAFKA_PASSWORD'] = ' password ');
      expect(
          config.sasl!.authBytes, utf8.encode('\u0000 user \u0000 password '));
      expect(config.toString(), isNot(contains('password')));
      expect(config.sasl.toString(), isNot(contains('password')));
      const secret = 'synthetic-should-never-be-echoed';
      try {
        example.SecureExampleConfig.fromEnvironment(
            environment()..['KAFKA_USERNAME'] = secret);
        fail('Expected incomplete credentials to fail');
      } on FormatException catch (error) {
        expect(error.toString(), isNot(contains(secret)));
      }
    });
  });

  group('secure example exact-offset verification', () {
    final expected = KafkaRecord(key: [
      1,
      2
    ], value: [
      3,
      4
    ], headers: [
      KafkaRecordHeader('example', [5])
    ]);
    KafkaRecord matching({int offset = 42}) => KafkaRecord(
        key: expected.key,
        value: expected.value,
        headers: expected.headers,
        timestamp: 1,
        offset: offset);
    bool verify(List<KafkaRecord> records) => example
        .verifyAcknowledgedRecord(records, offset: 42, expected: expected);

    test('requires exact offset and ignores unrelated record contents', () {
      expect(verify([]), isFalse);
      expect(verify([matching(offset: 41)]), isFalse);
      expect(
          verify([
            KafkaRecord(offset: 41, value: [255]),
            matching(),
            KafkaRecord(offset: 43)
          ]),
          isTrue);
    });

    test('rejects duplicate offsets and wrong key/value/header bytes', () {
      expect(() => verify([matching(), matching()]), throwsStateError);
      for (final wrong in [
        KafkaRecord(
            offset: 42,
            key: [9],
            value: expected.value,
            headers: expected.headers),
        KafkaRecord(
            offset: 42,
            key: expected.key,
            value: [9],
            headers: expected.headers),
        KafkaRecord(offset: 42, key: expected.key, value: expected.value),
        KafkaRecord(
            offset: 42,
            key: expected.key,
            value: expected.value,
            headers: [
              KafkaRecordHeader('wrong', [5])
            ]),
        KafkaRecord(
            offset: 42,
            key: expected.key,
            value: expected.value,
            headers: [
              KafkaRecordHeader('example', [9])
            ]),
      ]) {
        expect(() => verify([wrong]), throwsStateError);
      }
    });

    test('distinguishes null bytes from empty bytes', () {
      final tombstone = KafkaRecord(key: null, value: null);
      expect(
          example.verifyAcknowledgedRecord([KafkaRecord(offset: 42)],
              offset: 42, expected: tombstone),
          isTrue);
      for (final wrong in [
        KafkaRecord(offset: 42, key: []),
        KafkaRecord(offset: 42, value: [])
      ]) {
        expect(
            () => example.verifyAcknowledgedRecord([wrong],
                offset: 42, expected: tombstone),
            throwsStateError);
      }
    });

    test('rejects negative expected offsets', () {
      expect(
          () => example
              .verifyAcknowledgedRecord([], offset: -1, expected: expected),
          throwsArgumentError);
    });
  });

  test('verification category includes client state and discovery failures',
      () {
    // Fixed synthetic messages only; no broker request or real error output.
    for (final message in [
      'ApiVersions error 35',
      'No leader for synthetic-topic partition 0',
      'KafkaClient is closed',
      'KafkaClient closed',
    ]) {
      expect(example.secureExampleFailureCategory(StateError(message)),
          'verification');
    }
  });

  test('failure diagnostics are fixed categories and never stringify errors',
      () {
    const secret = 'synthetic-secret-that-must-not-be-logged';
    final errors = <Object, String>{
      const KafkaAuthenticationException(errorCode: 58): 'authentication',
      const HandshakeException(secret): 'tls',
      TimeoutException(secret): 'timeout',
      KafkaBrokerException(29, secret): 'broker',
      KafkaResourceLimitException(secret): 'resource',
      KafkaConnectionException(): 'connection',
      const SocketException(secret): 'connection',
      const FileSystemException(secret, secret): 'configuration',
      const FormatException(secret): 'format',
      ArgumentError(secret): 'configuration',
      StateError(secret): 'verification',
      _UnprintableError(): 'unexpected',
    };
    for (final entry in errors.entries) {
      expect(example.secureExampleFailureCategory(entry.key), entry.value);
    }
  });
}

class _UnprintableError {
  @override
  String toString() =>
      throw StateError('Do not stringify untrusted exceptions');
}
