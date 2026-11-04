import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

// Real Kafka qualification is deliberately separate from loopback unit tests.
// This test only runs inside the owned ssl.client.auth=required fixture.
void main() {
  final env = Platform.environment;
  final configured = env['KAFKA_SECURE_MTLS'] == '1' &&
      [
        'KAFKA_SECURE_CA',
        'KAFKA_SECURE_WRONG_CA',
        'KAFKA_SECURE_CLIENT_CERT',
        'KAFKA_SECURE_CLIENT_KEY',
        'KAFKA_SECURE_CLIENT_PKCS12',
        'KAFKA_SECURE_UNTRUSTED_CLIENT_CERT',
        'KAFKA_SECURE_UNTRUSTED_CLIENT_KEY',
        'KAFKA_SECURE_STORE_PASSWORD',
        'KAFKA_SECURE_USERNAME',
        'KAFKA_SECURE_PASSWORD',
        'KAFKA_SECURE_TLS_PORT',
        'KAFKA_SECURE_SASL_PORT',
        'KAFKA_SECURE_TOPIC',
      ].every((key) => env[key] != null);
  List<int> material(String key) => File(env[key]!).readAsBytesSync();
  KafkaTlsClientIdentity pem() => KafkaTlsClientIdentity.pem(
      certificateChain: material('KAFKA_SECURE_CLIENT_CERT'),
      privateKey: material('KAFKA_SECURE_CLIENT_KEY'));
  KafkaTlsConfig tls(KafkaTlsClientIdentity? identity,
          {bool wrongCa = false}) =>
      KafkaTlsConfig(
          trustedCertificates:
              material(wrongCa ? 'KAFKA_SECURE_WRONG_CA' : 'KAFKA_SECURE_CA'),
          withTrustedRoots: false,
          clientIdentity: identity);
  KafkaSaslPlain sasl({bool wrongPassword = false}) => KafkaSaslPlain(
      username: env['KAFKA_SECURE_USERNAME']!,
      password:
          '${env['KAFKA_SECURE_PASSWORD']!}${wrongPassword ? '-wrong' : ''}');
  KafkaClient connect(KafkaTlsConfig config,
      {bool withSasl = false,
      bool wrongPassword = false,
      String host = 'localhost'}) {
    final client = KafkaClient(
        brokers: [
          Broker(
              host,
              int.parse(env[withSasl
                  ? 'KAFKA_SECURE_SASL_PORT'
                  : 'KAFKA_SECURE_TLS_PORT']!))
        ],
        tls: config,
        sasl: withSasl ? sasl(wrongPassword: wrongPassword) : null,
        requestTimeout: const Duration(seconds: 5),
        connectionTimeout: const Duration(seconds: 5));
    addTearDown(() async {
      await client.close();
      expect(client.connectionCount, 0);
      expect(client.admittedRequestCount, 0);
      expect(client.pendingRequestCount, 0);
      expect(client.queuedRequestBytes, 0);
      expect(client.bufferedResponseBytes, 0);
    });
    return client;
  }

  Future<void> roundTrip(KafkaClient client, String marker) async {
    final topic = env['KAFKA_SECURE_TOPIC']!;
    final value = utf8.encode(marker);
    final produced = await client.produce(topic, [KafkaRecord(value: value)]);
    final offset =
        produced!.responses.single.partitionResponses.single.baseOffset;
    final fetched = await client.fetch(topic, offset: offset);
    expect(fetched.topics.single.partitions.single.records.first.value, value);
  }

  Future<void> rejected(KafkaClient client) async {
    Object? failure;
    try {
      await client.metadata({env['KAFKA_SECURE_TOPIC']!});
    } catch (error) {
      failure = error;
    }
    expect(failure, isNotNull);
    expect(failure, isNot(isA<TimeoutException>()));
    // Never print native TLS details or supplied material. Only test that our
    // configuration and failure text do not expose synthetic passwords.
    expect(failure.toString(),
        isNot(contains(env['KAFKA_SECURE_STORE_PASSWORD']!)));
    expect(failure.toString(), isNot(contains(env['KAFKA_SECURE_PASSWORD']!)));
  }

  group('real owned Kafka requiring mutual TLS', () {
    test('PEM client identity supports SSL produce/fetch', () async {
      await roundTrip(connect(tls(pem())), 'mtls-pem-नमस्ते');
    });
    test('PKCS12 client identity supports SSL produce/fetch', () async {
      final identity = KafkaTlsClientIdentity.pkcs12(
          bytes: material('KAFKA_SECURE_CLIENT_PKCS12'),
          password: env['KAFKA_SECURE_STORE_PASSWORD']);
      await roundTrip(connect(tls(identity)), 'mtls-pkcs12-नमस्ते');
    });
    test('client identity and PLAIN support SASL_SSL produce/fetch', () async {
      await roundTrip(connect(tls(pem()), withSasl: true), 'mtls-plain-नमस्ते');
    });
    test('missing identity cannot use SSL', () async {
      await rejected(connect(tls(null)));
    });
    test('PLAIN cannot replace a missing TLS client identity', () async {
      await rejected(connect(tls(null), withSasl: true));
    });
    test('untrusted client identity is rejected', () async {
      final identity = KafkaTlsClientIdentity.pem(
          certificateChain: material('KAFKA_SECURE_UNTRUSTED_CLIENT_CERT'),
          privateKey: material('KAFKA_SECURE_UNTRUSTED_CLIENT_KEY'));
      await rejected(connect(tls(identity)));
    });
    test('client identity does not bypass server trust', () async {
      await rejected(connect(tls(pem(), wrongCa: true)));
    });
    test('client identity does not bypass server hostname verification',
        () async {
      await rejected(connect(tls(pem()), host: '127.0.0.1'));
    });
    test('client identity does not replace PLAIN authentication', () async {
      final client = connect(tls(pem()), withSasl: true, wrongPassword: true);
      await expectLater(client.metadata({env['KAFKA_SECURE_TOPIC']!}),
          throwsA(isA<KafkaAuthenticationException>()));
    });
    test('operation-scoped stable offset reads also use mutual TLS', () async {
      final client = connect(tls(pem()));
      final offsets = await client.committedOffsets(
          'synthetic-mtls-group', env['KAFKA_SECURE_TOPIC']!,
          partitions: {0},
          requireStable: true,
          stableOffsetRetry: KafkaStableOffsetRetry());
      expect(offsets[0], isNull);
    });
  },
      skip: configured ? false : 'Run tool/run_mtls_broker_integration.sh',
      timeout: const Timeout(Duration(seconds: 30)));
}
