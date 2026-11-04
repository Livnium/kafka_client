import 'dart:io';
import 'dart:typed_data';

import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

import 'operational_integration_test.dart' as fixture;

String _group() =>
    'secure-manual-$pid-${DateTime.now().microsecondsSinceEpoch}';

void main() {
  final env = Platform.environment;
  final tlsPort = int.tryParse(env['KAFKA_SECURE_TLS_PORT'] ?? '');
  final saslPort = int.tryParse(env['KAFKA_SECURE_SASL_PORT'] ?? '');
  final ca = env['KAFKA_SECURE_CA'];
  final wrongCa = env['KAFKA_SECURE_WRONG_CA'];
  final topic = env['KAFKA_SECURE_TOPIC'];
  final username = env['KAFKA_SECURE_USERNAME'];
  final password = env['KAFKA_SECURE_PASSWORD'];
  final configured = [tlsPort, saslPort, ca, wrongCa, topic, username, password]
      .every((value) => value != null && value != '');

  KafkaTlsConfig trust(String path) => KafkaTlsConfig(
      trustedCertificates: File(path).readAsBytesSync(),
      withTrustedRoots: false);
  KafkaSaslPlain credentials() =>
      KafkaSaslPlain(username: username!, password: password!);
  KafkaClient connect(
      {required int port,
      String host = 'localhost',
      KafkaTlsConfig? tls,
      KafkaSaslPlain? sasl}) {
    final client = KafkaClient(
      brokers: [Broker(host, port)],
      tls: tls ?? trust(ca!),
      sasl: sasl,
      requestTimeout: const Duration(seconds: 5),
      connectionTimeout: const Duration(seconds: 5),
      maxReadRetries: 10,
      retryBaseDelay: const Duration(milliseconds: 100),
    );
    expect(client.allowTlsBootstrapFallback, isFalse);
    expect(client.tlsFetchRecovery, isNull);
    addTearDown(() async {
      await client.close().timeout(const Duration(seconds: 10));
      await fixture.idle(client);
      expect(client.activeSubscriptionCount, 0);
      expect(client.connectionCount, 0);
      expect(client.bootstrapTlsFallbackCount, 0);
    });
    return client;
  }

  group('real Kafka strict secure manual committed offsets', () {
    for (final authenticated in [false, true]) {
      final mode = authenticated ? 'SASL_SSL PLAIN' : 'TLS';
      test('$mode acknowledged commit is read by a fresh client', () async {
        final port = authenticated ? saslPort! : tlsPort!;
        final group = _group();
        final writer =
            connect(port: port, sasl: authenticated ? credentials() : null);
        final absent =
            await writer.committedOffsets(group, topic!, partitions: {0});
        expect(absent[0], isNull);
        await writer.commitOffsets(group, topic,
            nextOffsets: {0: 42}, metadata: 'secure-नमस्ते');
        await fixture.idle(writer);
        await writer.close();
        final observed =
            authenticated ? _ObservedCredentials(credentials()) : null;
        final reader = connect(port: port, sasl: observed);
        final value =
            (await reader.committedOffsets(group, topic, partitions: {0}))[0]!;
        expect(value.nextOffset, 42);
        expect(value.leaderEpoch, -1);
        expect(value.metadata, 'secure-नमस्ते');
        if (observed != null) expect(observed.payloadReads, greaterThan(0));
        await fixture.idle(reader);
      });

      for (final operation in ['read', 'commit']) {
        for (final badHostname in [false, true]) {
          test(
              '$mode $operation rejects ${badHostname ? 'hostname' : 'wrong CA'} before credentials',
              () async {
            final observed =
                authenticated ? _ObservedCredentials(credentials()) : null;
            final client = connect(
              port: authenticated ? saslPort! : tlsPort!,
              host: badHostname ? '127.0.0.1' : 'localhost',
              tls: badHostname ? null : trust(wrongCa!),
              sasl: observed,
            );
            final group = _group();
            final Future<Object?> pending = operation == 'read'
                ? client.committedOffsets(group, topic!, partitions: {0})
                : client.commitOffsets(group, topic!, nextOffsets: {0: 1});
            await expectLater(pending, throwsA(isA<HandshakeException>()));
            if (observed != null) expect(observed.payloadReads, 0);
            await fixture.idle(client);
            expect(client.connectionCount, 0);
          });
        }
        if (authenticated) {
          test(
              'SASL_SSL $operation wrong password is sanitized and not retried',
              () async {
            final wrongPassword = '${password!}-wrong';
            final observed = _ObservedCredentials(
                KafkaSaslPlain(username: username!, password: wrongPassword));
            final client = connect(port: saslPort!, sasl: observed);
            Object? failure;
            try {
              if (operation == 'read') {
                await client
                    .committedOffsets(_group(), topic!, partitions: {0});
              } else {
                await client
                    .commitOffsets(_group(), topic!, nextOffsets: {0: 1});
              }
            } catch (error) {
              failure = error;
            }
            expect(failure, isA<KafkaAuthenticationException>());
            expect(observed.payloadReads, 1);
            expect(failure.toString(), isNot(contains(username)));
            expect(failure.toString(), isNot(contains(password)));
            expect(failure.toString(), isNot(contains(wrongPassword)));
            await fixture.idle(client);
            expect(client.connectionCount, 0);
          });
        }
      }
    }
  },
      skip: configured
          ? false
          : 'Run tool/run_secure_broker_integration.sh dart test '
              'test/group_offsets_security_integration_test.dart --concurrency=1',
      timeout: const Timeout(Duration(seconds: 90)));
}

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
