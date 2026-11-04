import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

void main() {
  final env = Platform.environment;
  final tlsPort = int.tryParse(env['KAFKA_SECURE_TLS_PORT'] ?? '');
  final saslPort = int.tryParse(env['KAFKA_SECURE_SASL_PORT'] ?? '');
  final scramPort = int.tryParse(env['KAFKA_SECURE_SCRAM_PORT'] ?? '');
  final caPath = env['KAFKA_SECURE_CA'];
  final wrongCaPath = env['KAFKA_SECURE_WRONG_CA'];
  final topic = env['KAFKA_SECURE_TOPIC'];
  final oracleTopic = env['KAFKA_SECURE_ORACLE_TOPIC'];
  final username = env['KAFKA_SECURE_USERNAME'];
  final password = env['KAFKA_SECURE_PASSWORD'];
  final configured = [
    tlsPort,
    saslPort,
    scramPort,
    caPath,
    wrongCaPath,
    topic,
    oracleTopic,
    username,
    password
  ].every((v) => v != null);

  group('real Kafka 4.0.2 TLS and SASL/PLAIN', () {
    KafkaTlsConfig trustedTls() => KafkaTlsConfig(
          trustedCertificates: File(caPath!).readAsBytesSync(),
          withTrustedRoots: false,
        );
    KafkaClient connect(
        {required int port,
        String host = 'localhost',
        KafkaTlsConfig? tls,
        KafkaSaslPlain? sasl}) {
      final client = KafkaClient(
        brokers: [Broker(host, port)],
        tls: tls ?? trustedTls(),
        sasl: sasl,
        requestTimeout: const Duration(seconds: 5),
        connectionTimeout: const Duration(seconds: 5),
      );
      addTearDown(client.close);
      return client;
    }

    KafkaSaslPlain credentials() =>
        KafkaSaslPlain(username: username!, password: password!);

    Future<void> roundTrip(KafkaClient client, String marker) async {
      final records = [
        KafkaRecord(
            key: [0, 255],
            value: utf8.encode(marker),
            timestamp: DateTime.now().millisecondsSinceEpoch,
            headers: [
              KafkaRecordHeader('secure', [1])
            ]),
        KafkaRecord(
            key: utf8.encode('tombstone'),
            value: null,
            timestamp: DateTime.now().millisecondsSinceEpoch),
      ];
      final produced = await client.produce(topic!, records, acks: -1);
      final offset =
          produced!.responses.single.partitionResponses.single.baseOffset;
      final fetched = (await client.fetch(topic, offset: offset))
          .topics
          .single
          .partitions
          .single
          .records;
      expect(fetched, hasLength(2));
      expect(fetched.first.value, records.first.value);
      expect(fetched.first.key, records.first.key);
      expect(fetched.first.headers.single.key, 'secure');
      expect(fetched.first.headers.single.value, [1]);
      expect(fetched.last.value, isNull);
      expect(fetched.last.offset, offset + 1);
    }

    Future<void> expectTlsRejected(KafkaClient client) async {
      await expectLater(
          client.metadata({topic!}), throwsA(isA<HandshakeException>()));
    }

    test('trusted standalone TLS supports produce/fetch', () async {
      await roundTrip(connect(port: tlsPort!), 'tls-नमस्ते');
    });
    test('trusted SASL_SSL PLAIN supports produce/fetch', () async {
      await roundTrip(
          connect(port: saslPort!, sasl: credentials()), 'sasl-tls-नमस्ते');
    });
    test('wrong certificate authority is rejected', () async {
      await expectTlsRejected(connect(
          port: tlsPort!,
          tls: KafkaTlsConfig(
              trustedCertificates: File(wrongCaPath!).readAsBytesSync(),
              withTrustedRoots: false)));
    });
    test('untrusted private CA is rejected by default trust roots', () async {
      await expectTlsRejected(connect(port: tlsPort!, tls: KafkaTlsConfig()));
    });
    test('valid CA does not bypass hostname verification', () async {
      await expectTlsRejected(connect(port: tlsPort!, host: '127.0.0.1'));
    });
    test('wrong SASL password fails promptly and does not leak credentials',
        () async {
      final wrongPassword = '${password!}-wrong';
      final client = connect(
          port: saslPort!,
          sasl: KafkaSaslPlain(username: username!, password: wrongPassword));
      Object? failure;
      try {
        await client.metadata({topic!});
      } catch (error) {
        failure = error;
      }
      expect(failure, isA<KafkaAuthenticationException>());
      expect(failure, isNot(isA<TimeoutException>()));
      expect(failure.toString(), isNot(contains(username)));
      expect(failure.toString(), isNot(contains(password)));
      expect(failure.toString(), isNot(contains(wrongPassword)));
    });
    test('SCRAM-only listener rejects PLAIN before credential payload is read',
        () async {
      final observed = _ObservedCredentials(credentials());
      final client = connect(port: scramPort!, sasl: observed);
      Object? failure;
      try {
        await client.metadata({topic!});
      } catch (error) {
        failure = error;
      }
      expect(failure, isA<KafkaAuthenticationException>());
      expect((failure as KafkaAuthenticationException).errorCode, 33,
          reason: 'Kafka UNSUPPORTED_SASL_MECHANISM handshake error');
      expect(observed.payloadReads, 0,
          reason: 'Credentials must not be requested after rejected handshake');
      expect(failure.toString(), isNot(contains(username)));
      expect(failure.toString(), isNot(contains(password)));
    });
    test('missing SASL credentials cannot use the authenticated listener',
        () async {
      final client = connect(port: saslPort!);
      await expectLater(
          client.metadata({topic!}), throwsA(isNot(isA<TimeoutException>())));
    });
    test('secure bidirectional interoperability with official Java clients',
        () async {
      final client = connect(port: saslPort!, sasl: credentials());
      final fetched = (await client.fetch(oracleTopic!))
          .topics
          .single
          .partitions
          .single
          .records;
      expect(utf8.decode(fetched.single.value!), 'secure-java-to-dart-नमस्ते');
      await client.produce(oracleTopic, [
        KafkaRecord(
            value: utf8.encode('secure-dart-to-java-नमस्ते'),
            timestamp: DateTime.now().millisecondsSinceEpoch)
      ]);
    });
  },
      skip: configured ? false : 'Run tool/run_secure_broker_integration.sh',
      timeout: const Timeout(Duration(seconds: 30)));
}

// Observes whether transport obtains a credential token after a failed handshake.
class _ObservedCredentials implements KafkaSaslPlain {
  final KafkaSaslPlain _delegate;
  int payloadReads = 0;
  _ObservedCredentials(this._delegate);
  @override
  Uint8List get authBytes {
    payloadReads++;
    return _delegate.authBytes;
  }

  @override
  String toString() => 'ObservedCredentials(<redacted>)';
}
