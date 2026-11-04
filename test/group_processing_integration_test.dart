// Opt-in owned single-partition broker tests. Run serially with fixture suites.
import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/kafka_protocol.dart'
    show HeartbeatRequest, HeartbeatResponse;
import 'package:test/test.dart';

import 'group_delivery_fixture.dart';
import 'group_processing_fixture.dart';
import 'group_membership_integration_test.dart' as membership;
import 'operational_integration_test.dart' as fixture;

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

  KafkaGroupSession session(String group) {
    final owner = KafkaGroupSession(
        brokers: [Broker('127.0.0.1', port!)],
        groupId: group,
        topics: {topic!},
        sessionTimeout: const Duration(seconds: 6),
        heartbeatInterval: const Duration(seconds: 1),
        localExpiry: const Duration(seconds: 4));
    addTearDown(() => owner.close().timeout(deliveryDeadline));
    return owner;
  }

  KafkaGroupProcessing subscribe(
          KafkaGroupSession owner, KafkaGroupAssignment token, int offset,
          {int capacity = 1000,
          Duration timeout = const Duration(seconds: 30)}) =>
      owner.subscribeProcessing(
          assignment: token,
          startOffsets: {
            topic!: {0: offset}
          },
          maxPendingRecords: capacity,
          processingTimeout: timeout,
          pollInterval: const Duration(milliseconds: 20),
          maxWaitMs: 100,
          maxBytes: 64 * 1024);

  group('real Kafka bounded acknowledged processing', () {
    test(
        'delivery and out-of-order acknowledgements cannot commit past a gap; restart reads committed next offset',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('processing-gap');
      await membership.checkedCoordinator(client, group, topic!);
      final markers =
          List.generate(4, (i) => deliveryMarker('processing-gap/$i'));
      final base = await produceDeliveryRecords(client, topic, markers);
      final owner = session(group);
      final token = await owner.start();
      final processing = subscribe(owner, token, base);
      var next = 0;
      final probe = GroupProcessingProbe(processing, (record) {
        expectDeliveryRecord(
            record, owner, token, topic, base + next, markers[next]);
        next++;
      });
      await probe.received(4);
      expect(processing.pendingRecordCount, 4);
      expect(processing.acknowledgedNextOffsets, isEmpty);
      expect((await processing.commitAcknowledged(topic)).partitions, isEmpty);
      await expectNoDeliveryCommit(client, group, topic);
      expect(processing.acknowledge(probe.records[2]), isTrue);
      expect(processing.acknowledge(probe.records[1]), isTrue);
      expect(processing.acknowledge(probe.records[1]), isFalse);
      expect(processing.pendingRecordCount, 4);
      expect(processing.acknowledgedNextOffsets, isEmpty);
      expect((await processing.commitAcknowledged(topic)).partitions, isEmpty);
      await expectNoDeliveryCommit(client, group, topic);
      final first = probe.records.first;
      expect(
          () => processing.acknowledge(KafkaGroupRecord(
              first.assignment, first.topic, first.partition, first.record)),
          throwsArgumentError);
      expect(processing.acknowledge(first), isTrue);
      expect(processing.pendingRecordCount, 1);
      expect(processing.acknowledgedNextOffsets, {
        topic: {0: base + 3}
      });
      expect(processing.confirmedNextOffsets, isEmpty);
      await expectNoDeliveryCommit(client, group, topic);
      expectConfirmedProcessingCommit(
          await processing.commitAcknowledged(topic), token, topic, base + 3);
      expect(processing.confirmedNextOffsets, {
        topic: {0: base + 3}
      });
      expect((await processing.commitAcknowledged(topic)).partitions, isEmpty);
      expectCleanDeliveryStop(
          await processing.cancel(), KafkaGroupDeliveryStopReason.cancelled);
      await probe.done.future.timeout(deliveryDeadline);
      probe.checkHealthy();
      expect((await owner.closed).cleanupError, isNull);

      // New transport and new membership. Resume is explicitly chosen from
      // broker readback, never inferred from delivered records or a batch end.
      final fresh = reader();
      final saved =
          (await fresh.committedOffsets(group, topic, partitions: {0}))[0]!;
      expect(saved.nextOffset, base + 3);
      final restarted = session(group);
      final current = await restarted.start();
      final resumed = subscribe(restarted, current, saved.nextOffset);
      final resumedProbe = GroupProcessingProbe(resumed, (record) {
        expectDeliveryRecord(
            record, restarted, current, topic, base + 3, markers.last);
      });
      await resumedProbe.received(1);
      expect(resumedProbe.records, hasLength(1));
      expect(resumed.acknowledge(resumedProbe.records.single), isTrue);
      expectConfirmedProcessingCommit(
          await resumed.commitAcknowledged(topic), current, topic, base + 4);
      await resumed.cancel();
      await restarted.closed;
      expect(
          (await fresh.committedOffsets(group, topic, partitions: {0}))[0]!
              .nextOffset,
          base + 4);
    });

    test('raw subscriptions and manual commits remain explicit and independent',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('processing-raw');
      await membership.checkedCoordinator(client, group, topic!);
      final marker = deliveryMarker('processing-raw');
      final base = await produceDeliveryRecords(client, topic, [marker]);
      final record = await client
          .subscribe(topic, startOffsets: {0: base})
          .take(1)
          .single
          .timeout(deliveryDeadline);
      expect(record.offset, base);
      expect(record.record.value, marker.value);
      await expectNoDeliveryCommit(client, group, topic);
      await client.commitOffsets(group, topic,
          nextOffsets: {0: base + 1},
          metadata: 'manual-processing-compatibility');
      final saved =
          (await reader().committedOffsets(group, topic, partitions: {0}))[0]!;
      expect(saved.nextOffset, base + 1);
      expect(saved.metadata, 'manual-processing-compatibility');
    });

    test(
        'rebalance rejects stale receipts and commits without changing stored progress',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('processing-revoked');
      await membership.checkedCoordinator(client, group, topic!);
      final base = await produceDeliveryRecords(client, topic,
          [deliveryMarker('revoke/0'), deliveryMarker('revoke/1')]);
      final first = session(group), second = session(group);
      final old = await first.start();
      final processing = subscribe(first, old, base);
      final probe = GroupProcessingProbe(processing, (_) {});
      await probe.received(2);
      processing.acknowledge(probe.records.first);
      expectConfirmedProcessingCommit(
          await processing.commitAcknowledged(topic), old, topic, base + 1);
      // An uncommitted candidate must not be flushed by revocation or close.
      processing.acknowledge(probe.records.last);
      final joining = second.start();
      expect((await first.revoked.timeout(deliveryDeadline)).reason,
          KafkaGroupStopReason.brokerError);
      expectCleanDeliveryStop(
          await processing.stopped.timeout(deliveryDeadline),
          KafkaGroupDeliveryStopReason.revoked);
      expect(
          () => processing.acknowledge(probe.records.last), throwsStateError);
      await expectLater(processing.commitAcknowledged(topic), throwsStateError);
      await first.closed;
      final current = await joining;
      expect(current.generationId, greaterThan(old.generationId));
      expect(second.isCurrent(current), isTrue);
      expect(first.isCurrent(old), isFalse);
      probe.checkHealthy();
      expect(
          (await reader().committedOffsets(group, topic, partitions: {0}))[0]!
              .nextOffset,
          base + 1);
    });

    test(
        'capacity backpressure preserves independent broker heartbeats and resumes on acknowledgement',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('processing-capacity');
      await membership.checkedCoordinator(client, group, topic!);
      final base = await produceDeliveryRecords(client, topic,
          List.generate(3, (i) => deliveryMarker('capacity/$i')));
      final owner = session(group);
      final token = await owner.start();
      final processing = subscribe(owner, token, base, capacity: 1);
      final probe = GroupProcessingProbe(processing, (_) {});
      await probe.received(1);
      // No application pause and no external heartbeat during >2 session windows.
      for (var i = 0; i < 13; i++) {
        await Future<void>.delayed(const Duration(seconds: 1));
        probe.checkHealthy();
        expect(probe.records, hasLength(1));
        expect(processing.pendingRecordCount, 1);
        expect(owner.isCurrent(token), isTrue);
      }
      final coordinator = await client.findGroupCoordinator(group);
      expect(
          (await client.send(
                  HeartbeatRequest(group,
                      generationId: token.generationId,
                      memberId: token.memberId),
                  broker: coordinator) as HeartbeatResponse)
              .errorCode,
          0);
      processing.acknowledge(probe.records.first);
      await probe.received(2);
      expect(probe.records[1].offset, base + 1);
      expect(processing.pendingRecordCount, 1);
      processing.acknowledge(probe.records[1]);
      await probe.received(3);
      expect(probe.records[2].offset, base + 2);
      expectCleanDeliveryStop(
          await processing.cancel(), KafkaGroupDeliveryStopReason.cancelled);
      await owner.closed;
      await expectNoDeliveryCommit(client, group, topic);
    });

    test(
        'sustained paced processing preserves every acknowledged and committed marker',
        () async {
      // A bounded 30-second regression, not a long-duration endurance claim.
      // Only this serial fixture test writes these markers to the owned topic.
      const batchSize = 8;
      const capacity = 4;
      final client = reader();
      final observer = reader();
      final group = membership.uniqueGroup('processing-sustained');
      await membership.checkedCoordinator(client, group, topic!);
      final expected = <KafkaRecord>[
        for (var i = 0; i < batchSize; i++) deliveryMarker('$group/$i'),
      ];
      final base = await produceDeliveryRecords(client, topic, expected);
      var produced = batchSize;
      var acknowledged = 0;
      final seenOffsets = <int>{};
      final owner = session(group);
      final token = await owner.start();
      final processing = subscribe(owner, token, base, capacity: capacity);
      final probe = GroupProcessingProbe(processing, (record) {
        expect(acknowledged, lessThan(expected.length));
        expectDeliveryRecord(record, owner, token, topic, base + acknowledged,
            expected[acknowledged]);
        expect(seenOffsets.add(record.offset), isTrue,
            reason:
                'Each acknowledged marker is delivered exactly once in this run');
        expect(processing.pendingRecordCount, inInclusiveRange(1, capacity));
        expect(processing.acknowledge(record), isTrue);
        acknowledged++;
        expect(processing.pendingRecordCount, 0);
      });
      final clock = Stopwatch()..start();
      var batches = 0;
      do {
        await probe.received(produced);
        probe.checkHealthy();
        expect(acknowledged, produced);
        expect(processing.pendingRecordCount, 0);
        expect(owner.isCurrent(token), isTrue);
        final nextOffset = base + produced;
        // Await each explicit commit before the next one is admitted. Produce
        // and commit are never replayed after failure or ambiguous outcomes.
        expectConfirmedProcessingCommit(
            await processing.commitAcknowledged(topic),
            token,
            topic,
            nextOffset);
        expect(processing.acknowledgedNextOffsets, {
          topic: {0: nextOffset}
        });
        expect(processing.confirmedNextOffsets, {
          topic: {0: nextOffset}
        });
        batches++;
        if (batches % 5 == 0) {
          expect(
              (await observer
                      .committedOffsets(group, topic, partitions: {0}))[0]!
                  .nextOffset,
              nextOffset);
        }
        // Reuse the fixture's zero-outstanding-request resource guards. The
        // owned processing transport is tested separately by stopped/closed.
        await fixture.idle(client);
        await fixture.idle(observer);
        expect(client.activeSubscriptionCount, 0);
        expect(observer.activeSubscriptionCount, 0);
        await Future<void>.delayed(const Duration(seconds: 1));
        if (clock.elapsed >= const Duration(seconds: 30)) break;
        final batch = [
          for (var i = 0; i < batchSize; i++)
            deliveryMarker('$group/${produced + i}'),
        ];
        // Publish expected payloads before dispatch because a live Fetch may
        // admit the newly produced records before the Produce reply arrives.
        expected.addAll(batch);
        expect(await produceDeliveryRecords(client, topic, batch),
            base + produced);
        produced += batchSize;
      } while (true);
      expect(batches, greaterThan(1));
      expect(acknowledged, produced);
      expect(expected, hasLength(produced));
      expect(probe.records, hasLength(produced));
      expect(seenOffsets, hasLength(produced));
      expect(
          seenOffsets, orderedEquals(List.generate(produced, (i) => base + i)));
      expect(processing.acknowledgedNextOffsets, {
        topic: {0: base + produced}
      });
      expect(processing.confirmedNextOffsets, {
        topic: {0: base + produced}
      });
      expect(
          (await observer.committedOffsets(group, topic, partitions: {0}))[0]!
              .nextOffset,
          base + produced);
      expectCleanDeliveryStop(
          await processing.cancel(), KafkaGroupDeliveryStopReason.cancelled);
      await probe.done.future.timeout(deliveryDeadline);
      probe.checkHealthy();
      expect(probe.records, hasLength(produced));
      expect((await owner.closed).cleanupError, isNull);
      expect(
          (await observer.committedOffsets(group, topic, partitions: {0}))[0]!
              .nextOffset,
          base + produced);
    });

    test(
        'unacknowledged processing timeout terminates membership without committing',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('processing-stall');
      await membership.checkedCoordinator(client, group, topic!);
      final base = await produceDeliveryRecords(client, topic,
          [deliveryMarker('stall/0'), deliveryMarker('stall/1')]);
      final owner = session(group);
      final token = await owner.start();
      final processing = subscribe(owner, token, base,
          capacity: 1, timeout: const Duration(seconds: 2));
      final probe = GroupProcessingProbe(processing, (_) {});
      await probe.received(1);
      final stopped = await processing.stopped.timeout(deliveryDeadline);
      expect(stopped.reason, KafkaGroupDeliveryStopReason.failure);
      expect(stopped.error, isA<TimeoutException>());
      expect(stopped.cleanupError, isNull);
      await probe.done.future.timeout(deliveryDeadline);
      probe.checkCallbacks();
      expect(probe.errors, [same(stopped.error)]);
      expect(probe.records, hasLength(1));
      expect(processing.pendingRecordCount, 0);
      expect(owner.isCurrent(token), isFalse);
      expect(
          () => processing.acknowledge(probe.records.single), throwsStateError);
      await expectLater(processing.commitAcknowledged(topic), throwsStateError);
      expect((await owner.closed).cleanupError, isNull);
      await expectNoDeliveryCommit(client, group, topic);
    });
  },
      skip: port == null || topic == null
          ? 'Run owned operational fixture with --concurrency=1'
          : false,
      timeout: const Timeout(Duration(minutes: 2)));
}
