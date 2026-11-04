// Opt-in tests using the existing disposable operational broker and one owned
// partition. Run serially with the other owned-fixture tests.
import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/kafka_protocol.dart'
    show HeartbeatRequest, HeartbeatResponse;
import 'package:test/test.dart';

import 'group_delivery_fixture.dart';
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
    final result = KafkaGroupSession(
        brokers: [Broker('127.0.0.1', port!)],
        groupId: group,
        topics: {topic!},
        sessionTimeout: const Duration(seconds: 6),
        heartbeatInterval: const Duration(seconds: 1),
        localExpiry: const Duration(seconds: 4));
    addTearDown(() => result.close().timeout(deliveryDeadline));
    return result;
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

  group('real Kafka single-generation group delivery', () {
    test(
        'inclusive offsets preserve exact records and existing committed state',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('delivery-exact');
      await membership.checkedCoordinator(client, group, topic!);
      final records = [
        deliveryMarker('skip-prefix'),
        KafkaRecord(key: [
          0,
          255
        ], value: [
          255,
          0,
          1
        ], headers: [
          KafkaRecordHeader('duplicate', [0, 255]),
          KafkaRecordHeader('duplicate', null),
          KafkaRecordHeader('empty', []),
        ]),
        KafkaRecord(key: [1], value: null),
        KafkaRecord(key: null, value: []),
      ];
      final base = await produceDeliveryRecords(client, topic, records);
      // This explicit test setup is the only commit. Delivery must use the
      // supplied inclusive offset and must neither read nor replace this seed.
      await client.commitOffsets(group, topic,
          nextOffsets: {0: base}, metadata: 'explicit-test-seed');
      final owner = session(group);
      final token = await owner.start();
      expect(token.partitions, {
        topic: [0]
      });
      final delivery = subscribe(owner, token, base + 1);
      expect(() => subscribe(owner, token, base + 1), throwsStateError);
      var next = 1;
      final probe = GroupDeliveryProbe(delivery, (record) {
        expect(next, lessThan(records.length),
            reason: 'No duplicate or unexpected marker');
        expectDeliveryRecord(
            record, owner, token, topic, base + next, records[next]);
        next++;
      });
      await probe.received(3);
      final stopping = delivery.cancel();
      expect(owner.isCurrent(token), isFalse);
      expectCleanDeliveryStop(
          await stopping, KafkaGroupDeliveryStopReason.cancelled);
      await probe.done.future.timeout(deliveryDeadline);
      probe.checkHealthy();
      expect(probe.records, hasLength(3));
      expect(next, records.length);
      expect((await owner.closed).cleanupError, isNull);
      expect(await delivery.cancel(), same(await delivery.stopped));
      final committed =
          (await client.committedOffsets(group, topic, partitions: {0}))[0]!;
      expect(committed.nextOffset, base);
      expect(committed.metadata, 'explicit-test-seed');
      // The owned delivery closes its own clients, not the application client.
      await client.metadata({topic});
      expect(() => subscribe(owner, token, base), throwsStateError);
      await expectLater(owner.start(), throwsStateError);
    });

    test('paused delivery keeps independent heartbeats; close drops its buffer',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('delivery-paused');
      await membership.checkedCoordinator(client, group, topic!);
      await expectNoDeliveryCommit(client, group, topic);
      final records =
          List.generate(4, (index) => deliveryMarker('paused/$index'));
      final base = await produceDeliveryRecords(client, topic, records);
      final owner = session(group);
      final token = await owner.start();
      final delivery = subscribe(owner, token, base);
      late GroupDeliveryProbe probe;
      probe = GroupDeliveryProbe(delivery, (record) {
        expectDeliveryRecord(record, owner, token, topic, base, records.first);
        probe.subscription.pause();
      });
      await probe.received(1);
      expect(probe.subscription.isPaused, isTrue);
      // No outside heartbeat is sent during these >2 broker session windows.
      // Local liveness here cannot be explained by delivery progress or by the
      // final raw heartbeat used below as an independent broker observation.
      for (var second = 0; second < 13; second++) {
        await Future<void>.delayed(const Duration(seconds: 1));
        probe.checkHealthy();
        expect(probe.records, hasLength(1));
        expect(owner.isCurrent(token), isTrue);
        expect(owner.assignment, same(token));
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
      final closing = owner.close();
      expect(owner.isCurrent(token), isFalse);
      expect(owner.assignment, isNull);
      expect((await owner.revoked).reason, KafkaGroupStopReason.closed);
      // Data shutdown must settle while the listener remains paused; stream
      // done and membership cleanup are intentionally distinct barriers.
      expectCleanDeliveryStop(await delivery.stopped.timeout(deliveryDeadline),
          KafkaGroupDeliveryStopReason.revoked);
      expect(probe.subscription.isPaused, isTrue);
      expect(probe.done.isCompleted, isFalse);
      expect((await closing).cleanupError, isNull);
      final afterClose = deliveryMarker('paused/after-close');
      expect(
          await produceDeliveryRecords(client, topic, [afterClose]), base + 4);
      probe.subscription.resume();
      await probe.done.future.timeout(deliveryDeadline);
      probe.checkHealthy();
      expect(probe.records, hasLength(1),
          reason: 'Buffered and later records cannot escape revocation');
      await expectNoDeliveryCommit(client, group, topic);
    });

    test('pause and resume deliver buffered and later records exactly once',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('delivery-resume');
      await membership.checkedCoordinator(client, group, topic!);
      final records =
          List.generate(3, (index) => deliveryMarker('resume/$index'));
      final base = await produceDeliveryRecords(client, topic, records);
      final owner = session(group);
      final token = await owner.start();
      final delivery = subscribe(owner, token, base);
      var next = 0;
      late GroupDeliveryProbe probe;
      probe = GroupDeliveryProbe(delivery, (record) {
        expect(next, lessThan(records.length));
        expectDeliveryRecord(
            record, owner, token, topic, base + next, records[next]);
        next++;
        if (next == 1) probe.subscription.pause();
      });
      await probe.received(1);
      final appended = deliveryMarker('resume/appended');
      records.add(appended);
      expect(await produceDeliveryRecords(client, topic, [appended]), base + 3);
      expect(probe.records, hasLength(1));
      probe.subscription.resume();
      await probe.received(4);
      // Cancelling the listener is also cancellation of the sole owned delivery.
      await probe.subscription.cancel().timeout(deliveryDeadline);
      expectCleanDeliveryStop(
          await delivery.stopped, KafkaGroupDeliveryStopReason.cancelled);
      expect((await owner.closed).cleanupError, isNull);
      expect(owner.isCurrent(token), isFalse);
      probe.checkHealthy();
      expect(probe.records, hasLength(4));
      await expectNoDeliveryCommit(client, group, topic);
    });

    test('second member revokes buffered delivery; first never rejoins',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('delivery-rebalance');
      await membership.checkedCoordinator(client, group, topic!);
      final records =
          List.generate(3, (index) => deliveryMarker('rebalance/$index'));
      final base = await produceDeliveryRecords(client, topic, records);
      final first = session(group), second = session(group);
      final old = await first.start();
      final delivery = subscribe(first, old, base);
      late GroupDeliveryProbe probe;
      probe = GroupDeliveryProbe(delivery, (record) {
        expectDeliveryRecord(record, first, old, topic, base, records.first);
        probe.subscription.pause();
      });
      await probe.received(1);
      final joining = second.start();
      final revoked = await first.revoked.timeout(const Duration(seconds: 10));
      expect(revoked.reason, KafkaGroupStopReason.brokerError);
      expect(
          revoked.error,
          isA<KafkaBrokerException>()
              .having((error) => error.errorCode, 'rebalance error', 27));
      expect(first.isCurrent(old), isFalse);
      expectCleanDeliveryStop(await delivery.stopped.timeout(deliveryDeadline),
          KafkaGroupDeliveryStopReason.revoked);
      expect(probe.done.isCompleted, isFalse);
      probe.subscription.resume();
      await probe.done.future.timeout(deliveryDeadline);
      probe.checkHealthy();
      expect(probe.records, hasLength(1));
      final current = await joining;
      expect(current.generationId, greaterThan(old.generationId));
      expect(current.partitions, {
        topic: [0]
      });
      expect(() => subscribe(second, old, base), throwsStateError);
      final nextDelivery = subscribe(second, current, base + 1);
      var next = 1;
      final nextProbe = GroupDeliveryProbe(nextDelivery, (record) {
        expect(next, lessThan(records.length));
        expectDeliveryRecord(
            record, second, current, topic, base + next, records[next]);
        next++;
      });
      await nextProbe.received(2);
      await first.closed;
      await Future<void>.delayed(const Duration(seconds: 7));
      expect(first.state, KafkaGroupSessionState.closed);
      expect(first.assignment, isNull);
      expect(second.isCurrent(current), isTrue);
      expect(probe.records, hasLength(1));
      await expectLater(first.start(), throwsStateError);
      expectCleanDeliveryStop(
          await nextDelivery.cancel(), KafkaGroupDeliveryStopReason.cancelled);
      nextProbe.checkHealthy();
      expect(nextProbe.records, hasLength(2));
      await second.closed;
      await expectNoDeliveryCommit(client, group, topic);
    });

    test(
        'foreign live token and invalid offsets cannot consume the one-shot API',
        () async {
      final client = reader();
      final firstGroup = membership.uniqueGroup('delivery-token-a');
      final secondGroup = membership.uniqueGroup('delivery-token-b');
      await membership.checkedCoordinator(client, firstGroup, topic!);
      await membership.checkedCoordinator(client, secondGroup, topic);
      final first = session(firstGroup), second = session(secondGroup);
      final firstToken = await first.start();
      final otherToken = await second.start();
      expect(second.isCurrent(otherToken), isTrue);
      expect(() => subscribe(first, otherToken, 0), throwsStateError);
      expect(() => first.subscribe(assignment: firstToken, startOffsets: {}),
          throwsArgumentError);
      final delivery = subscribe(first, firstToken, 0);
      expect(() => subscribe(first, firstToken, 0), throwsStateError);
      // No listen, hence no data polling. Cancellation still settles and closes
      // membership instead of leaving the control heartbeat alive indefinitely.
      expectCleanDeliveryStop(
          await delivery.cancel(), KafkaGroupDeliveryStopReason.cancelled);
      expect(first.isCurrent(firstToken), isFalse);
      expect((await first.closed).cleanupError, isNull);
      expect(second.isCurrent(otherToken), isTrue);
      await expectNoDeliveryCommit(client, firstGroup, topic);
      await expectNoDeliveryCommit(client, secondGroup, topic);
    });

    test(
        'terminal Fetch failure stops membership without resetting or committing',
        () async {
      final client = reader();
      final group = membership.uniqueGroup('delivery-fetch-failure');
      await membership.checkedCoordinator(client, group, topic!);
      final owner = session(group);
      final token = await owner.start();
      final delivery = subscribe(owner, token, 9223372036854775807);
      final probe = GroupDeliveryProbe(delivery, (_) {
        fail('An out-of-range offset must not reset to retained records');
      });
      final stopped = await delivery.stopped.timeout(deliveryDeadline);
      expect(stopped.reason, KafkaGroupDeliveryStopReason.failure);
      expect(
          stopped.error,
          isA<KafkaBrokerException>()
              .having((error) => error.errorCode, 'offset range error', 1));
      expect(stopped.cleanupError, isNull);
      await probe.done.future.timeout(deliveryDeadline);
      probe.checkCallbacks();
      expect(probe.records, isEmpty);
      expect(probe.errors, hasLength(1));
      expect(probe.errors.single, same(stopped.error));
      expect(owner.isCurrent(token), isFalse);
      expect((await owner.closed).cleanupError, isNull);
      await expectNoDeliveryCommit(client, group, topic);
      await expectLater(owner.start(), throwsStateError);
    });
  },
      skip: port == null || topic == null
          ? 'Run owned operational fixture with --concurrency=1'
          : false,
      timeout: const Timeout(Duration(minutes: 2)));
}
