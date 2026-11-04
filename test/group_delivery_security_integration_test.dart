// Opt-in strict TLS/SASL_SSL tests against the existing owned secure fixture.
// All credential values are disposable fixture inputs, never diagnostic output.
import 'dart:io';
import 'dart:typed_data';

import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';

import 'group_delivery_fixture.dart';
import 'group_membership_integration_test.dart' as membership;
import 'operational_integration_test.dart' as fixture;

void main() {
  final env = Platform.environment;
  final tlsPort = int.tryParse(env['KAFKA_SECURE_TLS_PORT'] ?? '');
  final saslPort = int.tryParse(env['KAFKA_SECURE_SASL_PORT'] ?? '');
  final ca = env['KAFKA_SECURE_CA'], wrongCa = env['KAFKA_SECURE_WRONG_CA'];
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

  KafkaClient reader(bool sasl) {
    final client = KafkaClient(
        brokers: [Broker('localhost', sasl ? saslPort! : tlsPort!)],
        tls: trust(ca!),
        sasl: sasl ? credentials() : null,
        allowTlsBootstrapFallback: false,
        maxReadRetries: 10,
        retryBaseDelay: const Duration(milliseconds: 100));
    expect(client.allowTlsBootstrapFallback, isFalse);
    expect(client.tlsFetchRecovery, isNull);
    addTearDown(() async {
      await client.close().timeout(deliveryDeadline);
      await fixture.idle(client);
      expect(client.connectionCount, 0);
      expect(client.bootstrapTlsFallbackCount, 0);
    });
    return client;
  }

  KafkaGroupSession session(String group, bool sasl,
      {bool badHost = false,
      bool badTrust = false,
      KafkaTlsConfig? tls,
      KafkaSaslPlain? observed}) {
    final owner = KafkaGroupSession(
        brokers: [
          Broker(
              badHost ? '127.0.0.1' : 'localhost', sasl ? saslPort! : tlsPort!)
        ],
        groupId: group,
        topics: {topic!},
        tls: tls ?? trust(badTrust ? wrongCa! : ca!),
        sasl: sasl ? (observed ?? credentials()) : null,
        // These tests never opt into bootstrap fallback or TLS recovery.
        allowTlsBootstrapFallback: false,
        sessionTimeout: const Duration(seconds: 6),
        heartbeatInterval: const Duration(seconds: 1),
        localExpiry: const Duration(seconds: 4));
    addTearDown(() => owner.close().timeout(deliveryDeadline));
    return owner;
  }

  KafkaGroupDelivery subscribe(
          KafkaGroupSession owner, KafkaGroupAssignment token, int offset) =>
      owner.subscribe(
          assignment: token,
          startOffsets: {
            topic!: {0: offset}
          },
          pollInterval: const Duration(milliseconds: 20),
          maxWaitMs: 100,
          maxBytes: 64 * 1024);

  Future<Object> expectDataFailure(KafkaGroupSession owner,
      KafkaGroupAssignment token, Matcher expected) async {
    final delivery = subscribe(owner, token, 0);
    final probe = GroupDeliveryProbe(delivery, (_) {
      fail('An untrusted or unauthenticated data client must deliver nothing');
    });
    final stopped = await delivery.stopped.timeout(deliveryDeadline);
    expect(stopped.reason, KafkaGroupDeliveryStopReason.failure);
    expect(stopped.error, expected);
    expect(stopped.cleanupError, isNull);
    await probe.done.future.timeout(deliveryDeadline);
    probe.checkCallbacks();
    expect(probe.records, isEmpty);
    expect(probe.errors, hasLength(1));
    expect(probe.errors.single, same(stopped.error));
    expect(owner.isCurrent(token), isFalse);
    expect(owner.assignment, isNull);
    expect((await owner.closed).cleanupError, isNull);
    expect(owner.state, KafkaGroupSessionState.closed);
    await expectLater(owner.start(), throwsStateError);
    return stopped.error!;
  }

  group('real Kafka strict secure group delivery', () {
    for (final sasl in [false, true]) {
      final mode = sasl ? 'SASL_SSL PLAIN' : 'TLS';

      test('$mode consumes exact markers and revokes paused data on rebalance',
          () async {
        final client = reader(sasl);
        final group = membership.uniqueGroup('delivery-secure');
        await membership.checkedCoordinator(client, group, topic!);
        await expectNoDeliveryCommit(client, group, topic);
        final records = [
          KafkaRecord(key: [
            0,
            255
          ], value: [
            255,
            0,
            1
          ], headers: [
            KafkaRecordHeader('duplicate', [1, 0, 255]),
            KafkaRecordHeader('duplicate', null),
            KafkaRecordHeader('empty', []),
          ]),
          KafkaRecord(key: [1], value: null),
          KafkaRecord(key: null, value: []),
        ];
        final base = await produceDeliveryRecords(client, topic, records);
        final observed = _ObservedCredentials(credentials());
        final first = session(group, sasl, observed: observed);
        final second = session(group, sasl);
        final old = await first.start();
        final controlAuthenticationReads = observed.payloadReads;
        if (sasl) expect(controlAuthenticationReads, greaterThan(0));
        final delivery = subscribe(first, old, base);
        expect(observed.payloadReads, controlAuthenticationReads,
            reason: 'Creating delivery does not start a data connection');
        late GroupDeliveryProbe probe;
        probe = GroupDeliveryProbe(delivery, (record) {
          expectDeliveryRecord(record, first, old, topic, base, records.first);
          probe.subscription.pause();
        });
        await probe.received(1);
        if (sasl) {
          expect(observed.payloadReads, greaterThan(controlAuthenticationReads),
              reason: 'Owned data transport authenticates separately');
        }
        // A paused data path spans >2 full broker session windows. No raw
        // application heartbeat is sent, so only the owned control client can
        // keep the assignment locally and broker-side alive.
        for (var second = 0; second < 13; second++) {
          await Future<void>.delayed(const Duration(seconds: 1));
          probe.checkHealthy();
          expect(probe.records, hasLength(1));
          expect(first.isCurrent(old), isTrue);
        }
        final joining = second.start();
        final revoked =
            await first.revoked.timeout(const Duration(seconds: 10));
        expect(revoked.reason, KafkaGroupStopReason.brokerError);
        expect(
            revoked.error,
            isA<KafkaBrokerException>()
                .having((error) => error.errorCode, 'rebalance error', 27));
        expectCleanDeliveryStop(
            await delivery.stopped.timeout(deliveryDeadline),
            KafkaGroupDeliveryStopReason.revoked);
        expect(probe.subscription.isPaused, isTrue);
        expect(probe.done.isCompleted, isFalse);
        probe.subscription.resume();
        await probe.done.future.timeout(deliveryDeadline);
        probe.checkHealthy();
        expect(probe.records, hasLength(1),
            reason: 'Secure buffered records are discarded on revocation');
        expect(first.isCurrent(old), isFalse);
        expect((await first.closed).cleanupError, isNull);
        final current = await joining;
        expect(current.generationId, greaterThan(old.generationId));
        final nextDelivery = subscribe(second, current, base + 1);
        var next = 1;
        final nextProbe = GroupDeliveryProbe(nextDelivery, (record) {
          expect(next, lessThan(records.length));
          expectDeliveryRecord(
              record, second, current, topic, base + next, records[next]);
          next++;
        });
        await nextProbe.received(2);
        expectCleanDeliveryStop(await nextDelivery.cancel(),
            KafkaGroupDeliveryStopReason.cancelled);
        await nextProbe.done.future.timeout(deliveryDeadline);
        nextProbe.checkHealthy();
        expect(nextProbe.records, hasLength(2));
        expect(next, records.length);
        expect((await second.closed).cleanupError, isNull);
        await expectNoDeliveryCommit(client, group, topic);
        expect(first.assignment, isNull);
        await expectLater(first.start(), throwsStateError);
      });

      for (final badHost in [false, true]) {
        test(
            '$mode rejects ${badHost ? 'hostname' : 'wrong CA'} before a token',
            () async {
          final observed = _ObservedCredentials(credentials());
          final owner = session(
              membership.uniqueGroup('delivery-untrusted-start'), sasl,
              badHost: badHost, badTrust: !badHost, observed: observed);
          await expectLater(owner.start(), throwsA(isA<HandshakeException>()));
          expect(observed.payloadReads, 0,
              reason: 'Trust validation must precede SASL payload access');
          expect(owner.assignment, isNull);
          final stopped = await owner.closed;
          expect(stopped.reason, KafkaGroupStopReason.failure);
          expect(stopped.leaveOutcome, KafkaGroupLeaveOutcome.notAttempted);
          expect(stopped.cleanupError, isNull);
          expect(owner.state, KafkaGroupSessionState.closed);
          await expectLater(owner.start(), throwsStateError);
        });
      }

      test('$mode owned data connection rejects a newly untrusted CA',
          () async {
        final client = reader(sasl);
        final group = membership.uniqueGroup('delivery-untrusted-data');
        await membership.checkedCoordinator(client, group, topic!);
        final observed = _ObservedCredentials(credentials());
        final trustBoundary = _NewConnectionTrust(trust(ca!), trust(wrongCa!));
        final owner =
            session(group, sasl, tls: trustBoundary, observed: observed);
        final token = await owner.start();
        final readsBeforeData = observed.payloadReads;
        // Fault injection changes only future TLS contexts. The already-secure
        // control socket stays connected; the new owned data socket must still
        // validate trust rather than borrow membership's authenticated channel.
        trustBoundary.rejectNewConnections = true;
        await expectDataFailure(owner, token, isA<HandshakeException>());
        expect(observed.payloadReads, readsBeforeData,
            reason: 'Untrusted data TLS must fail before SASL payload access');
        await expectNoDeliveryCommit(client, group, topic);
      });
    }

    test('wrong SASL password at startup is terminal and sanitized', () async {
      final wrongPassword = '${password!}-wrong';
      final observed = _ObservedCredentials(
          KafkaSaslPlain(username: username!, password: wrongPassword));
      final owner = session(membership.uniqueGroup('delivery-bad-auth'), true,
          observed: observed);
      Object? failure;
      try {
        await owner.start();
      } catch (error) {
        failure = error;
      }
      expect(failure, isA<KafkaAuthenticationException>());
      expect(observed.payloadReads, 1,
          reason: 'A rejected authentication is not replayed');
      expect(owner.assignment, isNull);
      final stopped = await owner.closed;
      expect(stopped.reason, KafkaGroupStopReason.failure);
      expect(stopped.leaveOutcome, KafkaGroupLeaveOutcome.notAttempted);
      expect(stopped.cleanupError, isNull);
      for (final secret in [username, password, wrongPassword]) {
        expect(failure.toString(), isNot(contains(secret)));
      }
    });

    test('owned SASL data authentication failure stops delivery and membership',
        () async {
      final client = reader(true);
      final group = membership.uniqueGroup('delivery-bad-data-auth');
      await membership.checkedCoordinator(client, group, topic!);
      final observed = _ObservedCredentials(credentials());
      final owner = session(group, true, observed: observed);
      final token = await owner.start();
      final readsBeforeData = observed.payloadReads;
      expect(readsBeforeData, greaterThan(0));
      final wrongPassword = '${password!}-wrong';
      // Disposable-fixture fault injection affects the next authentication,
      // never the existing control connection or the broker's credentials.
      observed.delegate =
          KafkaSaslPlain(username: username!, password: wrongPassword);
      final error = await expectDataFailure(
          owner, token, isA<KafkaAuthenticationException>());
      expect(observed.payloadReads, readsBeforeData + 1,
          reason: 'Rejected data credentials are not replayed');
      for (final secret in [username, password, wrongPassword]) {
        expect(error.toString(), isNot(contains(secret)));
      }
      await expectNoDeliveryCommit(client, group, topic);
    });
  },
      skip:
          configured ? false : 'Run owned secure fixture with --concurrency=1',
      timeout: const Timeout(Duration(minutes: 2)));
}

// Count accesses without retaining or exposing returned authentication bytes.
class _ObservedCredentials implements KafkaSaslPlain {
  KafkaSaslPlain delegate;
  int payloadReads = 0;
  _ObservedCredentials(this.delegate);

  @override
  Uint8List get authBytes {
    payloadReads++;
    return delegate.authBytes;
  }

  @override
  String toString() => 'ObservedCredentials(<redacted>)';
}

// Supported configuration interface used solely to inject wrong trust into a
// later, independently created socket. Both branches enforce certificate and
// hostname checks. There is no validation bypass, server change or fallback.
class _NewConnectionTrust implements KafkaTlsConfig {
  final KafkaTlsConfig trusted;
  final KafkaTlsConfig untrusted;
  bool rejectNewConnections = false;
  _NewConnectionTrust(this.trusted, this.untrusted);

  @override
  bool get withTrustedRoots => false;

  @override
  SecurityContext createSecurityContext() =>
      (rejectNewConnections ? untrusted : trusted).createSecurityContext();
}
