// Opt-in strict TLS/SASL_SSL lifecycle tests using only the owned secure broker.
import 'dart:io';
import 'dart:typed_data';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/kafka_protocol.dart'
    show HeartbeatRequest, HeartbeatResponse;
import 'package:test/test.dart';
import 'group_membership_integration_test.dart' as membership;

void main() {
  final env = Platform.environment;
  final tlsPort = int.tryParse(env['KAFKA_SECURE_TLS_PORT'] ?? '');
  final saslPort = int.tryParse(env['KAFKA_SECURE_SASL_PORT'] ?? '');
  final ca = env['KAFKA_SECURE_CA'], wrongCa = env['KAFKA_SECURE_WRONG_CA'];
  final topic = env['KAFKA_SECURE_TOPIC'];
  final username = env['KAFKA_SECURE_USERNAME'],
      password = env['KAFKA_SECURE_PASSWORD'];
  final configured = [tlsPort, saslPort, ca, wrongCa, topic, username, password]
      .every((v) => v != null && v != '');
  KafkaTlsConfig trust(String path) => KafkaTlsConfig(
      trustedCertificates: File(path).readAsBytesSync(),
      withTrustedRoots: false);
  KafkaSaslPlain credentials() =>
      KafkaSaslPlain(username: username!, password: password!);
  KafkaClient reader(bool sasl) {
    final c = KafkaClient(
        brokers: [Broker('localhost', sasl ? saslPort! : tlsPort!)],
        tls: trust(ca!),
        sasl: sasl ? credentials() : null,
        maxReadRetries: 10,
        retryBaseDelay: const Duration(milliseconds: 100));
    addTearDown(c.close);
    return c;
  }

  KafkaGroupSession session(String group, bool sasl,
      {bool badHost = false, bool badTrust = false, KafkaSaslPlain? observed}) {
    final s = KafkaGroupSession(
        brokers: [
          Broker(
              badHost ? '127.0.0.1' : 'localhost', sasl ? saslPort! : tlsPort!)
        ],
        groupId: group,
        topics: {topic!},
        tls: trust(badTrust ? wrongCa! : ca!),
        sasl: sasl ? (observed ?? credentials()) : null,
        // Keep fallback disabled even for the negative trust tests.
        allowTlsBootstrapFallback: false,
        sessionTimeout: const Duration(seconds: 6),
        heartbeatInterval: const Duration(seconds: 1),
        localExpiry: const Duration(seconds: 4));
    addTearDown(s.close);
    return s;
  }

  group('real Kafka strict secure managed sessions', () {
    for (final sasl in [false, true]) {
      final mode = sasl ? 'SASL_SSL PLAIN' : 'TLS';
      test(
          '$mode stays locally current then revokes on eager rebalance without commits',
          () async {
        final c = reader(sasl),
            group = membership.uniqueGroup('managed-secure');
        await membership.checkedCoordinator(c, group, topic!);
        expect(
            (await c.committedOffsets(group, topic, partitions: {0}))[0]
                ?.nextOffset,
            isNull);
        final first = session(group, sasl), second = session(group, sasl);
        final old = await first.start();
        await Future<void>.delayed(const Duration(seconds: 13));
        expect(first.isCurrent(old), isTrue);
        expect(first.assignment, same(old));
        final broker = await c.findGroupCoordinator(group);
        expect(
            (await c.send(
                    HeartbeatRequest(group,
                        generationId: old.generationId, memberId: old.memberId),
                    broker: broker) as HeartbeatResponse)
                .errorCode,
            0);
        final joining = second.start();
        final revoked =
            await first.revoked.timeout(const Duration(seconds: 10));
        expect(revoked.reason, KafkaGroupStopReason.brokerError);
        expect(
            revoked.error,
            isA<KafkaBrokerException>()
                .having((e) => e.errorCode, 'rebalance error', 27));
        expect(first.isCurrent(old), isFalse);
        expect(() => first.assertCurrent(old), throwsStateError);
        final current = await joining;
        await first.closed;
        await Future<void>.delayed(const Duration(seconds: 7));
        expect(first.state, KafkaGroupSessionState.closed);
        expect(second.isCurrent(current), isTrue);
        expect(current.generationId, greaterThan(old.generationId));
        expect(
            (await c.committedOffsets(group, topic, partitions: {0}))[0]
                ?.nextOffset,
            isNull);
        await Future<void>.delayed(const Duration(milliseconds: 250));
        final ending = second.close();
        expect(second.isCurrent(current), isFalse);
        expect((await ending).leaveOutcome, KafkaGroupLeaveOutcome.confirmed);
      });
      for (final badHost in [false, true]) {
        test(
            '$mode rejects ${badHost ? 'hostname' : 'wrong CA'} before credentials; never becomes current',
            () async {
          final observed = _ObservedCredentials(credentials());
          final s = session(membership.uniqueGroup('managed-untrusted'), sasl,
              badHost: badHost, badTrust: !badHost, observed: observed);
          await expectLater(s.start(), throwsA(isA<HandshakeException>()));
          expect(observed.payloadReads, 0);
          expect(s.assignment, isNull);
          final end = await s.closed;
          expect(end.reason, KafkaGroupStopReason.failure);
          expect(end.leaveOutcome, KafkaGroupLeaveOutcome.notAttempted);
          expect(s.state, KafkaGroupSessionState.closed);
          await expectLater(s.start(), throwsStateError);
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
