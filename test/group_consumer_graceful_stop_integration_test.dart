// Opt-in owned single-partition fixtures only. Run serially with either
// run_operational_integration.py or run_secure_broker_integration.sh.
// A drain report is not cleanup evidence; every test also awaits closed.
import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';

import 'group_consumer_integration_test.dart'
    show expectCleanConsumerStop, waitForConsumerCondition;
import 'group_delivery_fixture.dart';
import 'group_membership_integration_test.dart' as membership;
import 'operational_integration_test.dart' as operational;

void main() {
  final env = Platform.environment;
  final plain = _Fixture(
      'PLAINTEXT',
      int.tryParse(env['KAFKA_OPERATIONAL_PORT'] ?? ''),
      env['KAFKA_OPERATIONAL_TOPIC']);
  final secure = <_Fixture>[
    for (final sasl in [false, true])
      _Fixture(
          sasl ? 'SASL_SSL PLAIN' : 'TLS',
          int.tryParse(
              env[sasl ? 'KAFKA_SECURE_SASL_PORT' : 'KAFKA_SECURE_TLS_PORT'] ??
                  ''),
          env['KAFKA_SECURE_TOPIC'],
          secure: true,
          ca: env['KAFKA_SECURE_CA'],
          sasl: sasl,
          username: env['KAFKA_SECURE_USERNAME'],
          password: env['KAFKA_SECURE_PASSWORD'])
  ];

  for (final fixture in [plain, ...secure]) {
    group('real Kafka ${fixture.label} graceful consumer stop', () {
      test(
          'reentrant request retains heartbeats then confirms only the admitted record commit',
          () async {
        final client = fixture.reader();
        final group = membership.uniqueGroup('grace-commit');
        final topic = fixture.topic!;
        await membership.checkedCoordinator(client, group, topic);
        final markers = [
          deliveryMarker('grace/skip-stored'),
          deliveryMarker('grace/process'),
          deliveryMarker('grace/not-admitted')
        ];
        final base = await produceDeliveryRecords(client, topic, markers);
        await client.commitOffsets(group, topic, nextOffsets: {0: base + 1});
        final owner = fixture.consumer(group, fallback: base, commit: true);
        final release = _releaseOnTearDown();
        final records = <KafkaGroupRecord>[];
        Future<KafkaGroupConsumerDrainResult>? draining;
        var reported = false;
        final running = owner.run((record) {
          records.add(record);
          expect(record.offset, base + 1);
          expect(record.record.value, markers[1].value);
          expect(owner.isCurrent(record.assignment), isTrue);
          // Request in the original synchronous callback, before returning the
          // exact Future that remains owned. Never await grace in this callback.
          draining = owner.drainAndStop(timeout: const Duration(seconds: 20));
          unawaited(draining!.then((_) => reported = true));
          return release.future;
        });
        await waitForConsumerCondition(() => draining != null);
        // Longer than both local expiry (4s) and broker session timeout (6s).
        // A successful current-generation commit afterward also checks the
        // broker still recognizes membership, not just a local token flag.
        await Future<void>.delayed(const Duration(seconds: 7));
        expect(reported, isFalse);
        expect(owner.isCurrent(records.single.assignment), isTrue);
        expect(owner.generationsStarted, 1);
        expect(records, hasLength(1));
        expect(await fixture.storedOffset(group), base + 1,
            reason: 'The owned application Future has not settled yet');
        release.complete();
        final report = await draining!.timeout(deliveryDeadline);
        _expectDrained(report);
        final commit = report.lastCommit!;
        expect(commit.assignment, same(records.single.assignment));
        expect(commit.error, isNull);
        expect(commit.locallyCurrent, isTrue);
        expect(
            commit.partitions[0]!.outcome, KafkaGroupCommitOutcome.confirmed);
        final stopped = await running.timeout(deliveryDeadline);
        expectCleanConsumerStop(
            stopped, KafkaGroupConsumerStopReason.cancelled, 1);
        expect(stopped.lastCommit, same(commit));
        expect(records, hasLength(1));
        expect(await fixture.storedOffset(group), base + 2);
        expect(owner.state, KafkaGroupConsumerState.closed);
        expect(owner.assignment, isNull);
      });

      test('disabled commit drains the callback without creating an offset',
          () async {
        final client = fixture.reader();
        final group = membership.uniqueGroup('grace-no-commit');
        final topic = fixture.topic!;
        await membership.checkedCoordinator(client, group, topic);
        final base = await produceDeliveryRecords(client, topic, [
          deliveryMarker('grace/no-commit'),
          deliveryMarker('grace/no-commit-not-admitted')
        ]);
        final owner = fixture.consumer(group, fallback: base, commit: false);
        final release = _releaseOnTearDown();
        Future<KafkaGroupConsumerDrainResult>? draining;
        var calls = 0;
        final running = owner.run((record) {
          calls++;
          expect(record.offset, base);
          draining = owner.drainAndStop(timeout: const Duration(seconds: 10));
          return release.future;
        });
        await waitForConsumerCondition(() => draining != null);
        release.complete();
        final report = await draining!.timeout(deliveryDeadline);
        _expectDrained(report);
        expect(report.lastCommit, isNull);
        final stopped = await running.timeout(deliveryDeadline);
        expectCleanConsumerStop(
            stopped, KafkaGroupConsumerStopReason.cancelled, 1);
        expect(stopped.lastCommit, isNull);
        expect(calls, 1);
        expect(await fixture.storedOffset(group), isNull);
      });

      test(
          'expired grace reports pending callback and prevents its late commit',
          () async {
        final client = fixture.reader();
        final group = membership.uniqueGroup('grace-expired');
        final topic = fixture.topic!;
        await membership.checkedCoordinator(client, group, topic);
        final base = await produceDeliveryRecords(client, topic, [
          deliveryMarker('grace/expired'),
          deliveryMarker('grace/expired-not-admitted')
        ]);
        final owner = fixture.consumer(group, fallback: base, commit: true);
        final release = _releaseOnTearDown();
        final records = <KafkaGroupRecord>[];
        Future<KafkaGroupConsumerDrainResult>? draining;
        var closed = false;
        final running = owner.run((record) {
          records.add(record);
          expect(record.offset, base);
          draining =
              owner.drainAndStop(timeout: const Duration(milliseconds: 250));
          return release.future;
        });
        unawaited(running.then((_) => closed = true));
        await waitForConsumerCondition(() => draining != null);
        final report = await draining!.timeout(deliveryDeadline);
        expect(report.outcome, KafkaGroupConsumerDrainOutcome.deadlineExpired);
        expect(report.callbackPending, isTrue);
        expect(report.commitPending, isFalse);
        expect(report.lastCommit, isNull);
        expect(report.error, isNull);
        expect(owner.isCurrent(records.single.assignment), isFalse);
        await Future<void>.delayed(const Duration(milliseconds: 100));
        expect(closed, isFalse,
            reason: 'Grace expiry cannot settle the actual callback Future');
        expect(await fixture.storedOffset(group), isNull);
        release.complete();
        final stopped = await running.timeout(deliveryDeadline);
        expectCleanConsumerStop(
            stopped, KafkaGroupConsumerStopReason.cancelled, 1);
        expect(stopped.lastCommit, isNull);
        expect(records, hasLength(1));
        expect(report.callbackPending, isTrue,
            reason: 'The report is an immutable snapshot at expiry');
        expect(await fixture.storedOffset(group), isNull);
      });

      test(
          'real competing member revokes grace without a late commit or rejoin',
          () async {
        final client = fixture.reader();
        final group = membership.uniqueGroup('grace-revoked');
        final topic = fixture.topic!;
        await membership.checkedCoordinator(client, group, topic);
        final base = await produceDeliveryRecords(client, topic, [
          deliveryMarker('grace/revoked'),
          deliveryMarker('grace/revoked-not-admitted')
        ]);
        final owner = fixture.consumer(group, fallback: base, commit: true);
        final release = _releaseOnTearDown();
        final records = <KafkaGroupRecord>[];
        Future<KafkaGroupConsumerDrainResult>? draining;
        var closed = false;
        final running = owner.run((record) {
          records.add(record);
          expect(record.offset, base);
          draining = owner.drainAndStop(timeout: const Duration(seconds: 45));
          return release.future;
        });
        unawaited(running.then((_) => closed = true));
        await waitForConsumerCondition(() => draining != null);
        final other = fixture.competitor(group);
        // Own the join error immediately while first waiting for the old member
        // to be revoked. No membership request is replayed by this test.
        final joined = other.start().then<Object>((value) => value,
            onError: (Object error, StackTrace _) => error);
        final report = await draining!.timeout(const Duration(seconds: 40));
        expect(report.outcome, KafkaGroupConsumerDrainOutcome.revoked);
        expect(report.callbackPending, isTrue);
        expect(report.commitPending, isFalse);
        expect(report.lastCommit, isNull);
        expect(report.error, isA<KafkaBrokerException>());
        final error = report.error! as KafkaBrokerException;
        expect(error.operation, 'heartbeat');
        expect(error.errorCode, 27);
        expect(owner.isCurrent(records.single.assignment), isFalse);
        final replacement = await joined.timeout(const Duration(seconds: 45));
        expect(replacement, isA<KafkaGroupAssignment>());
        expect((replacement as KafkaGroupAssignment).generationId,
            greaterThan(records.single.assignment.generationId));
        expect((await other.close()).cleanupError, isNull);
        expect(closed, isFalse);
        expect(owner.generationsStarted, 1);
        expect(await fixture.storedOffset(group), isNull);
        release.complete();
        final stopped = await running.timeout(deliveryDeadline);
        expectCleanConsumerStop(
            stopped, KafkaGroupConsumerStopReason.cancelled, 1);
        expect(stopped.lastCommit, isNull);
        expect(records, hasLength(1));
        expect(await fixture.storedOffset(group), isNull);
      });
    },
        skip: fixture.configured
            ? false
            : 'Run the owned ${fixture.secure ? 'secure' : 'operational'} '
                'fixture with --concurrency=1',
        timeout: const Timeout(Duration(minutes: 3)));
  }
}

Completer<void> _releaseOnTearDown() {
  final release = Completer<void>();
  addTearDown(() {
    if (!release.isCompleted) release.complete();
  });
  return release;
}

void _expectDrained(KafkaGroupConsumerDrainResult report) {
  expect(report.outcome, KafkaGroupConsumerDrainOutcome.drained);
  expect(report.callbackPending, isFalse);
  expect(report.commitPending, isFalse);
  expect(report.error, isNull);
}

class _Fixture {
  final String label;
  final int? port;
  final String? topic, ca, username, password;
  final bool secure, sasl;
  const _Fixture(this.label, this.port, this.topic,
      {this.secure = false,
      this.ca,
      this.sasl = false,
      this.username,
      this.password});

  bool get configured =>
      port != null &&
      topic != null &&
      (!secure || ca != null) &&
      (!sasl || (username != null && password != null));
  Broker get broker => Broker(secure ? 'localhost' : '127.0.0.1', port!);
  KafkaTlsConfig? get tls => secure
      ? KafkaTlsConfig(
          trustedCertificates: File(ca!).readAsBytesSync(),
          withTrustedRoots: false)
      : null;
  KafkaSaslPlain? get credentials =>
      sasl ? KafkaSaslPlain(username: username!, password: password!) : null;

  KafkaClient reader() {
    final client = KafkaClient(
        brokers: [broker],
        tls: tls,
        sasl: credentials,
        allowTlsBootstrapFallback: false,
        maxReadRetries: 10,
        retryBaseDelay: const Duration(milliseconds: 100));
    addTearDown(() async {
      await client.close().timeout(deliveryDeadline);
      await operational.idle(client);
      expect(client.connectionCount, 0);
      expect(client.bootstrapTlsFallbackCount, 0);
      expect(client.tlsFetchRecovery, isNull);
    });
    return client;
  }

  // Fresh transport is intentional: broker storage, not a local candidate, is
  // the readback oracle. The grace report itself proves the commit response.
  Future<int?> storedOffset(String group) async =>
      (await reader().committedOffsets(group, topic!, partitions: {0}))[0]
          ?.nextOffset;

  KafkaGroupConsumer consumer(String group,
      {required int fallback, required bool commit}) {
    final owner = KafkaGroupConsumer(
        brokers: [broker],
        groupId: group,
        topics: {topic!},
        commitAfterProcessing: commit,
        missingOffsets: {
          topic!: {0: fallback}
        },
        maxRejoins: 3,
        processingTimeout: const Duration(seconds: 90),
        tls: tls,
        sasl: credentials,
        allowTlsBootstrapFallback: false,
        sessionTimeout: const Duration(seconds: 6),
        heartbeatInterval: const Duration(milliseconds: 500),
        localExpiry: const Duration(seconds: 4),
        pollInterval: const Duration(milliseconds: 20));
    addTearDown(() async {
      owner.stop();
      await owner.closed.timeout(deliveryDeadline);
    });
    return owner;
  }

  KafkaGroupSession competitor(String group) {
    final other = KafkaGroupSession(
        brokers: [broker],
        groupId: group,
        topics: {topic!},
        tls: tls,
        sasl: credentials,
        allowTlsBootstrapFallback: false,
        sessionTimeout: const Duration(seconds: 6),
        heartbeatInterval: const Duration(milliseconds: 500),
        localExpiry: const Duration(seconds: 4));
    addTearDown(() => other.close().timeout(deliveryDeadline));
    return other;
  }
}
