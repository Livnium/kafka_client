// Raw classic membership over the existing strict TLS/SASL fixture.
import 'dart:io';
import 'dart:typed_data';
import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';
import 'group_membership_integration_test.dart' as membership;
import 'operational_integration_test.dart' as fixture;

void main() {
  final env = Platform.environment;
  final tlsPort = int.tryParse(env['KAFKA_SECURE_TLS_PORT'] ?? '');
  final saslPort = int.tryParse(env['KAFKA_SECURE_SASL_PORT'] ?? '');
  final ca = env['KAFKA_SECURE_CA'],
      wrongCa = env['KAFKA_SECURE_WRONG_CA'],
      topic = env['KAFKA_SECURE_TOPIC'];
  final username = env['KAFKA_SECURE_USERNAME'],
      password = env['KAFKA_SECURE_PASSWORD'];
  final configured = [tlsPort, saslPort, ca, wrongCa, topic, username, password]
      .every((v) => v != null && v != '');
  KafkaTlsConfig trust(String path) => KafkaTlsConfig(
      trustedCertificates: File(path).readAsBytesSync(),
      withTrustedRoots: false);
  KafkaClient make(bool sasl,
      {bool badHost = false,
      bool badTrust = false,
      _ObservedCredentials? observed}) {
    final c = KafkaClient(
        brokers: [
          Broker(
              badHost ? '127.0.0.1' : 'localhost', sasl ? saslPort! : tlsPort!)
        ],
        tls: trust(badTrust ? wrongCa! : ca!),
        sasl: sasl
            ? (observed ??
                KafkaSaslPlain(username: username!, password: password!))
            : null,
        requestTimeout: const Duration(seconds: 35),
        connectionTimeout: const Duration(seconds: 3),
        maxReadRetries: 10,
        retryBaseDelay: const Duration(milliseconds: 100));
    expect(c.allowTlsBootstrapFallback, isFalse);
    expect(c.tlsFetchRecovery, isNull);
    addTearDown(() async {
      await c.close();
      await fixture.idle(c);
      expect(c.connectionCount, 0);
      expect(c.activeSubscriptionCount, 0);
      expect(c.bootstrapTlsFallbackCount, 0);
    });
    return c;
  }

  group('real Kafka strict secure classic membership', () {
    for (final sasl in [false, true]) {
      final mode = sasl ? 'SASL_SSL PLAIN' : 'TLS';
      test('$mode join sync heartbeat commit independent read leave', () async {
        final c = make(sasl);
        final g = membership.uniqueGroup('secure');
        final b = await membership.checkedCoordinator(c, g, topic!);
        final j = await membership.joinMember(c, b, g, topic);
        await membership.syncMember(c, b, g, topic, j);
        expect(await membership.heartbeat(c, b, g, j), 0);
        expect(await membership.memberCommit(c, b, g, topic, j, 42), 0);
        expect(
            (await make(sasl).committedOffsets(g, topic, partitions: {0}))[0]!
                .nextOffset,
            42);
        expect(
            await membership.memberCommit(c, b, g, topic, j, 999,
                generation: j.generationId + 1),
            22);
        expect(
            (await make(sasl).committedOffsets(g, topic, partitions: {0}))[0]!
                .nextOffset,
            42);
        await membership.leave(c, b, g, j);
      });
      for (final badHost in [false, true]) {
        test(
            '$mode JoinGroup rejects ${badHost ? 'hostname' : 'wrong CA'} before credentials',
            () async {
          final observed = _ObservedCredentials(
              KafkaSaslPlain(username: username!, password: password!));
          final c = make(sasl,
              badHost: badHost, badTrust: !badHost, observed: observed);
          // Deliberately raw bootstrap send: failure must precede any group bytes.
          await expectLater(
              c.send(JoinGroupRequest(membership.uniqueGroup('untrusted'),
                  sessionTimeoutMs: 10000,
                  rebalanceTimeoutMs: 20000,
                  protocols: [
                    GroupJoinProtocol(
                        'range', ConsumerGroupSubscription([topic!]).toBytes())
                  ])),
              throwsA(isA<HandshakeException>()));
          expect(observed.payloadReads, 0);
        });
      }
    }
  },
      skip: configured ? false : 'Run owned secure fixture',
      timeout: const Timeout(Duration(minutes: 2)));
}

class _ObservedCredentials implements KafkaSaslPlain {
  final KafkaSaslPlain delegate;
  int payloadReads = 0;
  _ObservedCredentials(this.delegate);
  @override
  Uint8List get authBytes {
    payloadReads++;
    return delegate.authBytes;
  }
}
