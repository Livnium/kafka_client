// Opt-in, owned single-partition real broker tests. Run fixtures serially.
import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';

import 'group_delivery_fixture.dart';
import 'group_membership_integration_test.dart' as membership;
import 'operational_integration_test.dart' as fixture;

Future<void> waitForConsumerCondition(bool Function() ready) async {
  final clock = Stopwatch()..start();
  while (!ready()) {
    if (clock.elapsed >= const Duration(seconds: 45)) {
      throw TimeoutException('Consumer condition was not reached');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

void expectCleanConsumerStop(KafkaGroupConsumerTermination result,
    KafkaGroupConsumerStopReason reason, int generations) {
  expect(result.reason, reason);
  expect(result.error, isNull);
  expect(result.cleanupErrors, isEmpty);
  expect(result.generationsStarted, generations);
  expect(result.lastSession?.cleanupError, isNull);
  expect(result.lastProcessing?.cleanupError, isNull);
}

void main() {
  final env = Platform.environment;
  final port = int.tryParse(env['KAFKA_OPERATIONAL_PORT'] ?? '');
  final topic = env['KAFKA_OPERATIONAL_TOPIC'];

  KafkaClient reader() {
    final client = KafkaClient(
        brokers: [Broker('127.0.0.1', port!)],
        maxReadRetries: 10,
        retryBaseDelay: const Duration(milliseconds: 100));
    addTearDown(() async {
      await client.close().timeout(deliveryDeadline);
      await fixture.idle(client);
      expect(client.connectionCount, 0);
    });
    return client;
  }

  KafkaGroupConsumer consumer(String group,
      {int? fallback, bool commit = false, int maxRejoins = 3}) {
    final owner = KafkaGroupConsumer(
        brokers: [Broker('127.0.0.1', port!)],
        groupId: group,
        topics: {topic!},
        commitAfterProcessing: commit,
        missingOffsets: fallback == null
            ? {}
            : {
                topic: {0: fallback}
              },
        maxRejoins: maxRejoins,
        processingTimeout: const Duration(seconds: 90),
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

  // Join a genuine competing native member while the old application callback
  // is held. Let the old session leave, then close the competitor before the
  // runner can rejoin. This deliberately avoids nondeterministic ownership of
  // the fixture's one partition in a stable two-member group.
  Future<void> rebalance(
      KafkaGroupConsumer owner, KafkaGroupAssignment old, String group) async {
    final other = KafkaGroupSession(
        brokers: [Broker('127.0.0.1', port!)],
        groupId: group,
        topics: {topic!},
        sessionTimeout: const Duration(seconds: 6),
        heartbeatInterval: const Duration(milliseconds: 500),
        localExpiry: const Duration(seconds: 4));
    addTearDown(() => other.close().timeout(deliveryDeadline));
    final joined = other.start();
    // Attach error ownership before waiting for revocation.
    final joinedResult = joined.then<Object>((token) => token,
        onError: (Object error, StackTrace _) => error);
    await waitForConsumerCondition(() => !owner.isCurrent(old));
    final current = await joinedResult.timeout(const Duration(seconds: 45));
    expect(current, isA<KafkaGroupAssignment>());
    expect((current as KafkaGroupAssignment).generationId,
        greaterThan(old.generationId));
    expect((await other.close()).cleanupError, isNull);
  }

  group('real Kafka bounded consumer runner', () {
    test('stored next offset wins over fallback and successful work commits',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('consumer-resume');
      await membership.checkedCoordinator(client, group, topic!);
      final markers = [
        deliveryMarker('resume/skipped'),
        deliveryMarker('resume/read'),
        deliveryMarker('resume/commit-response-barrier')
      ];
      final base = await produceDeliveryRecords(client, topic, markers);
      await client.commitOffsets(group, topic, nextOffsets: {0: base + 1});
      final owner = consumer(group, fallback: base, commit: true);
      final records = <KafkaGroupRecord>[];
      var commitResponseBarrierReached = false;
      final running = owner.run((record) {
        expect(owner.isCurrent(record.assignment), isTrue);
        if (record.offset == base + 2) {
          // The runner admits this next callback only after the prior commit
          // response has settled. OffsetFetch visibility alone is not a barrier.
          expect(record.record.value, markers.last.value);
          commitResponseBarrierReached = true;
          owner.stop();
          return;
        }
        expect(record.offset, base + 1);
        expect(record.record.value, markers[1].value);
        records.add(record);
      });
      final result = await running.timeout(deliveryDeadline);
      expectCleanConsumerStop(
          result, KafkaGroupConsumerStopReason.cancelled, 1);
      expect(commitResponseBarrierReached, isTrue);
      expect(result.lastCommit!.partitions[0]!.outcome,
          KafkaGroupCommitOutcome.confirmed);
      expect(records, hasLength(1));
      expect(owner.state, KafkaGroupConsumerState.closed);
      expect(owner.assignment, isNull);
      expect(
          (await reader().committedOffsets(group, topic, partitions: {0}))[0]!
              .nextOffset,
          base + 2);
    });

    test(
        'missing offset uses explicit fallback and disabled commit stays absent',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('consumer-fallback');
      await membership.checkedCoordinator(client, group, topic!);
      final marker = deliveryMarker('fallback');
      final base = await produceDeliveryRecords(client, topic, [marker]);
      final owner = consumer(group, fallback: base);
      var calls = 0;
      final running = owner.run((record) {
        expect(record.offset, base);
        expect(record.record.value, marker.value);
        calls++;
      });
      await waitForConsumerCondition(() => calls == 1);
      // Let successful processing settle without stopping inside its callback.
      await Future<void>.delayed(const Duration(milliseconds: 100));
      owner.stop();
      expectCleanConsumerStop(
          await running, KafkaGroupConsumerStopReason.cancelled, 1);
      await expectNoDeliveryCommit(client, group, topic);
    });

    test('missing offset without fallback fails without invoking processing',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('consumer-no-fallback');
      await membership.checkedCoordinator(client, group, topic!);
      final owner = consumer(group);
      var calls = 0;
      final result = await owner.run((_) {
        calls++;
      });
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.error, isA<StateError>());
      expect(result.error.toString(), contains('explicit fallback'));
      expect(result.cleanupErrors, isEmpty);
      expect(result.generationsStarted, 1);
      expect(calls, 0);
      await expectNoDeliveryCommit(client, group, topic);
    });

    test('failed application work is terminal and never committed or rejoined',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('consumer-process-failure');
      await membership.checkedCoordinator(client, group, topic!);
      final base = await produceDeliveryRecords(client, topic,
          [deliveryMarker('failure/0'), deliveryMarker('failure/1')]);
      final owner = consumer(group, fallback: base, commit: true);
      final failure = StateError('owned application failure');
      var calls = 0;
      final result = await owner.run((record) async {
        calls++;
        expect(record.offset, base);
        await Future<void>.delayed(const Duration(milliseconds: 20));
        throw failure;
      }).timeout(deliveryDeadline);
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.error, same(failure));
      expect(result.cleanupErrors, isEmpty);
      expect(result.generationsStarted, 1);
      expect(calls, 1);
      await expectNoDeliveryCommit(client, group, topic);
    });

    test(
        'real rebalance drains old callback then reloads committed positions with a fresh token',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('consumer-rejoin');
      await membership.checkedCoordinator(client, group, topic!);
      final markers = List.generate(3, (i) => deliveryMarker('rejoin/$i'));
      final base = await produceDeliveryRecords(client, topic, markers);
      final owner = consumer(group, fallback: base, maxRejoins: 1);
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final records = <KafkaGroupRecord>[];
      var active = 0;
      var oldSettled = false;
      final running = owner.run((record) async {
        expect(active, 0);
        active++;
        try {
          expect(owner.isCurrent(record.assignment), isTrue);
          records.add(record);
          if (records.length == 1) {
            expect(record.offset, base);
            await release.future;
            oldSettled = true;
          } else {
            expect(oldSettled, isTrue);
            expect(record.offset, base + 2);
            expect(record.record.value, markers.last.value);
            expect(record.assignment, isNot(same(records.first.assignment)));
            expect(record.assignment.generationId,
                greaterThan(records.first.assignment.generationId));
            owner.stop();
          }
        } finally {
          active--;
        }
      });
      await waitForConsumerCondition(() => records.isNotEmpty);
      final old = records.first.assignment;
      await rebalance(owner, old, group);
      expect(owner.generationsStarted, 1);
      expect(records, hasLength(1));
      expect(active, 1);
      // Group is empty now; explicitly seed progress after revocation. A cached
      // position or replay of the fallback would deliver base rather than base+2.
      await client.commitOffsets(group, topic, nextOffsets: {0: base + 2});
      release.complete();
      expectCleanConsumerStop(
          await running.timeout(const Duration(seconds: 45)),
          KafkaGroupConsumerStopReason.cancelled,
          2);
      expect(records, hasLength(2));
      expect(owner.isCurrent(old), isFalse);
      expect(active, 0);
      expect(
          (await client.committedOffsets(group, topic, partitions: {0}))[0]!
              .nextOffset,
          base + 2);
    });

    test(
        'stop during revoked callback waits for it and never starts a late generation',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('consumer-stop-drain');
      await membership.checkedCoordinator(client, group, topic!);
      final base =
          await produceDeliveryRecords(client, topic, [deliveryMarker('stop')]);
      final owner = consumer(group, fallback: base, commit: true);
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      KafkaGroupAssignment? old;
      var calls = 0;
      var closed = false;
      final running = owner.run((record) {
        calls++;
        old = record.assignment;
        return release.future;
      });
      unawaited(running.then((_) {
        closed = true;
      }));
      await waitForConsumerCondition(() => old != null);
      await rebalance(owner, old!, group);
      owner.stop();
      await Future<void>.delayed(const Duration(milliseconds: 350));
      expect(closed, isFalse);
      expect(owner.generationsStarted, 1);
      release.complete();
      expectCleanConsumerStop(
          await running, KafkaGroupConsumerStopReason.cancelled, 1);
      expect(calls, 1);
      await expectNoDeliveryCommit(client, group, topic);
    });

    for (final budget in [0, 1]) {
      test('real heartbeat rebalance stops at finite rejoin budget $budget',
          () async {
        final client = reader();
        final group = membership.uniqueGroup('consumer-budget-$budget');
        await membership.checkedCoordinator(client, group, topic!);
        final base = await produceDeliveryRecords(
            client, topic, [deliveryMarker('budget')]);
        final owner = consumer(group, fallback: base, maxRejoins: budget);
        final gates = List.generate(budget + 1, (_) => Completer<void>());
        addTearDown(() {
          for (final gate in gates) {
            if (!gate.isCompleted) gate.complete();
          }
        });
        final records = <KafkaGroupRecord>[];
        final running = owner.run((record) {
          expect(records.length, lessThan(gates.length));
          expect(record.offset, base,
              reason: 'No commit means explicit replay on rejoin');
          records.add(record);
          return gates[records.length - 1].future;
        });
        for (var i = 0; i <= budget; i++) {
          await waitForConsumerCondition(() => records.length == i + 1);
          await rebalance(owner, records[i].assignment, group);
          expect(owner.generationsStarted, i + 1);
          gates[i].complete();
        }
        final result = await running.timeout(const Duration(seconds: 45));
        expectCleanConsumerStop(
            result, KafkaGroupConsumerStopReason.rejoinLimit, budget + 1);
        final error = result.lastSession!.error as KafkaBrokerException;
        expect(error.operation, 'heartbeat');
        expect(error.errorCode, 27);
        expect(result.lastProcessing!.reason,
            KafkaGroupDeliveryStopReason.revoked);
        expect(records, hasLength(budget + 1));
        await expectNoDeliveryCommit(client, group, topic);
      });
    }
  },
      skip: port == null || topic == null
          ? 'Run owned operational fixture with --concurrency=1'
          : false,
      timeout: const Timeout(Duration(minutes: 3)));
}
