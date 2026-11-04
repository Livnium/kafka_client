// Opt-in bounded same-host evidence, not throughput or production certification.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';

import 'group_consumer_integration_test.dart' as consumer_checks;
import 'group_delivery_fixture.dart';
import 'group_membership_integration_test.dart' as membership;
import 'operational_integration_test.dart' as fixture;

const _deadline = Duration(seconds: 45);
const _batchSize = 5;
const _payloadBytes = 256;
const _rssLimit = 768 * 1024 * 1024;

class _Batch {
  final List<KafkaRecord> records;
  final acknowledged = Completer<int>();
  final held = Completer<KafkaGroupAssignment>();
  Completer<void> release = Completer<void>();
  Completer<KafkaGroupAssignment>? replayed;
  int received = 0;
  bool expectReplay = false;
  _Batch(this.records) {
    // Own failure even if Produce fails before any consumer callback starts.
    unawaited(acknowledged.future
        .then<void>((_) {}, onError: (Object error, StackTrace stack) {}));
  }
}

KafkaRecord _marker(int sequence) => KafkaRecord(
      key: [0, 255, ...utf8.encode('managed-endurance/$sequence')],
      value: List.generate(_payloadBytes, (i) => (sequence + i) % 256),
      headers: [
        KafkaRecordHeader('sequence', utf8.encode('$sequence')),
        KafkaRecordHeader('duplicate', [0, 255, sequence % 256]),
        KafkaRecordHeader('duplicate', null),
        KafkaRecordHeader('empty', []),
      ],
    );

void _matches(
    KafkaGroupRecord actual, String topic, int offset, KafkaRecord expected) {
  expect(actual.topic, topic);
  expect(actual.partition, 0);
  expect(actual.offset, offset);
  expect(actual.record.offset, offset);
  expect(actual.record.key, expected.key);
  expect(actual.record.value, expected.value);
  expect(actual.record.timestamp, expected.timestamp);
  expect(actual.record.headers.map((h) => h.key),
      expected.headers.map((h) => h.key));
  expect(actual.record.headers.map((h) => h.value),
      expected.headers.map((h) => h.value));
}

Future<void> _until(bool Function() ready, void Function() check) async {
  final clock = Stopwatch()..start();
  while (!ready()) {
    check();
    if (clock.elapsed >= _deadline) {
      throw TimeoutException('Managed endurance condition was not reached');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  check();
}

Future<void> _rebalance(KafkaGroupConsumer owner, KafkaGroupAssignment old,
    String group, String topic, int port, void Function() check) async {
  final other = KafkaGroupSession(
      brokers: [Broker('127.0.0.1', port)],
      groupId: group,
      topics: {topic},
      sessionTimeout: const Duration(seconds: 6),
      heartbeatInterval: const Duration(milliseconds: 500),
      localExpiry: const Duration(seconds: 4));
  // Own start errors immediately, and always close this exact member.
  final joined = other.start().then<Object>((value) => value,
      onError: (Object error, StackTrace _) => error);
  try {
    await _until(() => !owner.isCurrent(old), check);
    final token = await joined.timeout(_deadline);
    expect(token, isA<KafkaGroupAssignment>());
    expect((token as KafkaGroupAssignment).generationId,
        greaterThan(old.generationId));
  } finally {
    expect((await other.close().timeout(_deadline)).cleanupError, isNull);
  }
}

void main() {
  final env = Platform.environment;
  final port = int.tryParse(env['KAFKA_OPERATIONAL_PORT'] ?? '');
  final topic = env['KAFKA_OPERATIONAL_TOPIC'];
  final control = env['KAFKA_OPERATIONAL_CONTROL'];
  final seconds = int.parse(env['KAFKA_GROUP_ENDURANCE_SECONDS'] ?? '30');
  final interval =
      int.parse(env['KAFKA_GROUP_REBALANCE_INTERVAL_SECONDS'] ?? '60');
  if (seconds < 1 || seconds > 1800 || interval < 15 || interval > 600) {
    throw ArgumentError(
        'Endurance 1..1800 seconds; rebalance interval 15..600');
  }

  KafkaClient client() => KafkaClient(
      brokers: [Broker('127.0.0.1', port!)],
      maxReadRetries: 10,
      retryBaseDelay: const Duration(milliseconds: 100),
      maxInFlightRequests: 16);

  KafkaGroupConsumer owner(String group, int fallback) => KafkaGroupConsumer(
      brokers: [Broker('127.0.0.1', port!)],
      groupId: group,
      topics: {topic!},
      commitAfterProcessing: true,
      missingOffsets: {
        topic: {0: fallback}
      },
      maxRejoins: 100,
      processingTimeout: const Duration(seconds: 90),
      sessionTimeout: const Duration(seconds: 6),
      heartbeatInterval: const Duration(milliseconds: 500),
      localExpiry: const Duration(seconds: 4),
      pollInterval: const Duration(milliseconds: 10),
      maxBytes: 64 * 1024,
      maxBufferedRecords: 16,
      maxBufferedPayloadBytes: 64 * 1024);

  group('real Kafka managed consumer endurance', () {
    test(
        'serial commits survive repeated real rebalance with held-record replay',
        () async {
      final producer = client();
      final observer = client();
      final group = membership.uniqueGroup('consumer-endurance');
      final clock = Stopwatch();
      final initialRss = ProcessInfo.currentRss;
      var peakRss = initialRss;
      var active = 0;
      var peakActive = 0;
      var uniqueRecords = 0;
      var replayedRecords = 0;
      var acknowledgedRecords = 0;
      var produceCalls = 0;
      var commitReadbacks = 0;
      var rebalances = 0;
      var batches = 0;
      var nextRebalance = interval;
      var nextProgress = 60;
      var batch = _Batch(List.generate(_batchSize, _marker));
      KafkaGroupConsumer? consumer;
      KafkaGroupConsumerTermination? ended;
      Timer? sampling;
      Object? healthError;
      var lastCommitted = -1;
      var firstOffset = -1;

      void sample() {
        final rss = ProcessInfo.currentRss;
        if (rss > peakRss) peakRss = rss;
        if (rss > _rssLimit) {
          healthError ??= StateError('RSS $rss exceeded fixed $_rssLimit');
        }
        for (final c in [producer, observer]) {
          if (c.pendingRequestCount < 0 ||
              c.pendingRequestCount > 16 ||
              c.admittedRequestCount < 0 ||
              c.admittedRequestCount > 16 ||
              c.queuedRequestBytes < 0 ||
              c.queuedRequestBytes > c.maxPendingRequestBytes ||
              c.bufferedResponseBytes < 0 ||
              c.bufferedResponseBytes > c.maxBufferedResponseBytes ||
              c.connectionCount < 0 ||
              c.connectionCount > c.maxConnections) {
            healthError ??=
                StateError('Observed client counters out of bounds');
          }
        }
      }

      void check() {
        sample();
        if (healthError != null) throw healthError!;
        if (ended != null) {
          fail('Runner ended early: ${ended!.reason}, ${ended!.error}, '
              '${ended!.cleanupErrors}');
        }
      }

      Map<String, Object?> summary() => {
            'requested_seconds': seconds,
            'elapsed_seconds': clock.elapsedMilliseconds / 1000,
            'rebalance_interval_seconds': interval,
            'completed_rebalances': rebalances,
            'generations_started': consumer?.generationsStarted,
            'batches': batches,
            'produce_calls': produceCalls,
            'acknowledged_records': acknowledgedRecords,
            'unique_verified_records': uniqueRecords,
            'expected_held_record_replays': replayedRecords,
            'first_offset': firstOffset,
            'verified_committed_next_offset': lastCommitted,
            'independent_commit_readbacks': commitReadbacks,
            'final_held_record_intentionally_uncommitted': true,
            'active_callbacks': active,
            'peak_active_callbacks': peakActive,
            'expected_records_per_batch': _batchSize,
            'verified_payload_bytes': uniqueRecords * _payloadBytes,
            'initial_rss_bytes': initialRss,
            'sampled_peak_rss_bytes': peakRss,
            'rss_limit_bytes': _rssLimit,
            'health_error': healthError?.toString(),
          };

      Future<void> readback(int expected) async {
        final offsets = await observer.committedOffsets(group, topic!,
            partitions: {0}).timeout(_deadline);
        expect(offsets[0]!.nextOffset, expected);
        expect(expected, greaterThanOrEqualTo(lastCommitted));
        lastCommitted = expected;
        commitReadbacks++;
      }

      try {
        await membership.checkedCoordinator(producer, group, topic!);
        produceCalls++;
        firstOffset =
            await produceDeliveryRecords(producer, topic, batch.records);
        acknowledgedRecords += _batchSize;
        batch.acknowledged.complete(firstOffset);
        consumer = owner(group, firstOffset);
        final running = consumer.run((record) async {
          expect(active, 0, reason: 'No overlapping application callbacks');
          active++;
          if (active > peakActive) peakActive = active;
          final selected = batch;
          try {
            expect(consumer!.isCurrent(record.assignment), isTrue);
            // A fetch can race the Produce response. Own and await the single
            // acknowledgement, never infer it or replay Produce after failure.
            final base = await selected.acknowledged.future;
            if (selected.expectReplay) {
              expect(selected.received, _batchSize);
              _matches(
                  record, topic, base + _batchSize - 1, selected.records.last);
              selected.expectReplay = false;
              replayedRecords++;
              selected.replayed!.complete(record.assignment);
            } else {
              expect(selected.received, lessThan(_batchSize),
                  reason: 'No duplicate, skipped or unexpected callback');
              _matches(record, topic, base + selected.received,
                  selected.records[selected.received]);
              selected.received++;
              uniqueRecords++;
              if (selected.received < _batchSize) return;
              selected.held.complete(record.assignment);
            }
            await selected.release.future;
          } finally {
            active--;
          }
        });
        unawaited(running.then((result) {
          ended = result;
        }));
        clock.start();
        sampling = Timer.periodic(const Duration(milliseconds: 500), (_) {
          sample();
        });
        while (true) {
          await _until(() => batch.held.isCompleted, check);
          final old = await batch.held.future;
          final base = await batch.acknowledged.future;
          final heldOffset = base + _batchSize - 1;
          batches++;
          // Entry to the held callback proves every preceding commit response
          // settled. Readback separately establishes the broker-stored value.
          await readback(heldOffset);
          expect(active, 1);
          if (clock.elapsed >= Duration(seconds: seconds)) break;
          if (clock.elapsed >= Duration(seconds: nextRebalance)) {
            final before = consumer.generationsStarted;
            await _rebalance(consumer, old, group, topic, port!, check);
            expect(consumer.generationsStarted, before);
            expect(active, 1);
            expect(ended, isNull);
            await readback(heldOffset);
            batch.expectReplay = true;
            batch.replayed = Completer<KafkaGroupAssignment>();
            final originalRelease = batch.release;
            // The admitted old callback already owns originalRelease.future.
            // The next callback gets a fresh gate and cannot start until it ends.
            batch.release = Completer<void>();
            originalRelease.complete();
            await _until(() => batch.replayed!.isCompleted, check);
            final replacement = await batch.replayed!.future;
            expect(replacement, isNot(same(old)));
            expect(replacement.generationId, greaterThan(old.generationId));
            expect(consumer.isCurrent(old), isFalse);
            expect(consumer.generationsStarted, before + 1);
            expect(active, 1);
            await readback(heldOffset);
            rebalances++;
            nextRebalance += interval;
          }
          if (clock.elapsed.inSeconds >= nextProgress) {
            print('GROUP_ENDURANCE_PROGRESS ${jsonEncode(summary())}');
            nextProgress += 60;
          }
          batch.release.complete();
          await _until(() => active == 0, check);
          await Future<void>.delayed(const Duration(milliseconds: 100));
          final next = _Batch(List.generate(
              _batchSize, (i) => _marker(acknowledgedRecords + i)));
          batch = next;
          produceCalls++;
          final nextBase =
              await produceDeliveryRecords(producer, topic, next.records);
          expect(nextBase, firstOffset + acknowledgedRecords);
          acknowledgedRecords += _batchSize;
          next.acknowledged.complete(nextBase);
        }
        expect(rebalances, (seconds - 1) ~/ interval);
        expect(replayedRecords, rebalances);
        expect(uniqueRecords, acknowledgedRecords);
        expect(peakActive, 1);
        consumer.stop();
        expect(active, 1);
        expect(ended, isNull,
            reason: 'Stop cannot abandon held application work');
        batch.release.complete();
        final result = await running.timeout(_deadline);
        consumer_checks.expectCleanConsumerStop(
            result, KafkaGroupConsumerStopReason.cancelled, rebalances + 1);
        expect(active, 0);
        expect(consumer.assignment, isNull);
        expect(result.lastCommit!.partitions[0]!.outcome,
            KafkaGroupCommitOutcome.confirmed);
        await readback(firstOffset + acknowledgedRecords - 1);
        sample();
        expect(healthError, isNull);
        print('GROUP_ENDURANCE_RESULT ${jsonEncode(summary())}');
      } finally {
        sampling?.cancel();
        consumer?.stop();
        if (!batch.acknowledged.isCompleted) {
          batch.acknowledged
              .completeError(StateError('Produce did not confirm'));
        }
        if (!batch.release.isCompleted) batch.release.complete();
        await Future.wait<void>([
          if (consumer != null)
            consumer.closed.timeout(_deadline).then<void>((_) {}),
          producer.close().timeout(_deadline),
          observer.close().timeout(_deadline),
        ]);
        await fixture.idle(producer);
        await fixture.idle(observer);
        expect(producer.connectionCount, 0);
        expect(observer.connectionCount, 0);
      }
    }, timeout: Timeout(Duration(seconds: seconds + 180)));

    test(
        'broker crash fails closed and an explicit fresh owner resumes stored offset',
        () async {
      final producer = client();
      final group = membership.uniqueGroup('consumer-crash');
      final release = Completer<void>();
      KafkaGroupConsumer? first;
      KafkaGroupConsumer? replacement;
      var runningBroker = true;
      var calls = 0;
      var closed = false;
      KafkaGroupAssignment? held;
      try {
        await membership.checkedCoordinator(producer, group, topic!);
        final markers = [_marker(1000000), _marker(1000001)];
        final base = await produceDeliveryRecords(producer, topic, markers);
        first = owner(group, base);
        final running = first.run((record) async {
          _matches(record, topic, base + calls, markers[calls]);
          calls++;
          if (calls == 2) {
            held = record.assignment;
            await release.future;
          }
        });
        unawaited(running.then((_) {
          closed = true;
        }));
        await _until(() => held != null, () {});
        expect(
            (await producer.committedOffsets(group, topic, partitions: {0}))[0]!
                .nextOffset,
            base + 1);
        await fixture.control(control!, 'crash');
        runningBroker = false;
        await _until(() => !first!.isCurrent(held!), () {});
        expect(first.generationsStarted, 1);
        expect(closed, isFalse);
        expect(calls, 2);
        release.complete();
        final result = await running.timeout(_deadline);
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(result.error, isNotNull);
        expect(result.generationsStarted, 1);
        expect(result.cleanupErrors, isEmpty);
        await fixture.control(control, 'start');
        runningBroker = true;
        final observer = client();
        try {
          await membership.checkedCoordinator(observer, group, topic);
          expect(
              (await observer
                      .committedOffsets(group, topic, partitions: {0}))[0]!
                  .nextOffset,
              base + 1);
          replacement = owner(group, base);
          var replayed = 0;
          final next = await replacement.run((record) {
            _matches(record, topic, base + 1, markers.last);
            replayed++;
            replacement!.stop();
          }).timeout(_deadline);
          consumer_checks.expectCleanConsumerStop(
              next, KafkaGroupConsumerStopReason.cancelled, 1);
          expect(replayed, 1);
          expect(
              (await observer
                      .committedOffsets(group, topic, partitions: {0}))[0]!
                  .nextOffset,
              base + 1);
          print('GROUP_CRASH_RESULT ${jsonEncode({
                'original_generations': result.generationsStarted,
                'original_callbacks': calls,
                'failure': result.error.toString(),
                'cleanup_errors': result.cleanupErrors.length,
                'manual_replacement_generations': next.generationsStarted,
                'replayed_uncommitted_records': replayed,
                'stored_next_offset': base + 1,
                'automatic_crash_recovery_claimed': false,
              })}');
        } finally {
          await observer.close().timeout(_deadline);
          await fixture.idle(observer);
          expect(observer.connectionCount, 0);
        }
      } finally {
        first?.stop();
        replacement?.stop();
        if (!release.isCompleted) release.complete();
        try {
          await Future.wait<void>([
            if (first != null)
              first.closed.timeout(_deadline).then<void>((_) {}),
            if (replacement != null)
              replacement.closed.timeout(_deadline).then<void>((_) {}),
            producer.close().timeout(_deadline),
          ]);
          await fixture.idle(producer);
          expect(producer.connectionCount, 0);
        } finally {
          if (!runningBroker) await fixture.control(control!, 'start');
        }
      }
    }, timeout: const Timeout(Duration(minutes: 4)));
  },
      skip: port == null || topic == null || control == null
          ? 'Run owned operational fixture with --concurrency=1'
          : false);
}
