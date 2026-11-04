import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

// Import the bounded cleanup assertion, never the fixture's main(). The parent
// runner owns the disposable broker and this test's dedicated single partition.
import 'operational_integration_test.dart' as fixture show idle;

const _deadline = Duration(seconds: 15);
const _rssLimit = 768 * 1024 * 1024;
const _batchSize = 4;
const _payloadSize = 512;
const _restartInterval = Duration(seconds: 60);

Future<void> _until(bool Function() condition,
    {required String reason, required void Function() check}) async {
  final clock = Stopwatch()..start();
  while (true) {
    check();
    if (condition()) return;
    if (clock.elapsed >= _deadline) {
      throw TimeoutException(reason, _deadline);
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

// Only the current subscription is retained. Capture asynchronous listener
// failures for the awaited test instead of throwing outside its control flow.
class _Probe {
  late final StreamSubscription<KafkaTopicRecord> subscription;
  Object? _error;
  StackTrace? _stack;
  Future<void>? _cancellation;
  bool _stopping = false;
  int recordCount = 0;

  _Probe(Stream<KafkaTopicRecord> stream,
      void Function(KafkaTopicRecord) inspect) {
    subscription = stream.listen((record) {
      recordCount++;
      if (_error != null) return;
      try {
        inspect(record);
      } catch (error, stack) {
        _capture(error, stack);
      }
    }, onError: (Object error, StackTrace stack) {
      _capture(error, stack);
    }, onDone: () {
      if (!_stopping) {
        _capture(StateError('Subscription ended before cancellation'),
            StackTrace.current);
      }
    });
    // Produce acknowledgments are checked before any record is admitted to the
    // verifier. The listener pauses again after synchronously checking a batch.
    subscription.pause();
  }

  void _capture(Object error, StackTrace stack) {
    _error ??= error;
    _stack ??= stack;
  }

  void checkHealthy() {
    if (_error != null) Error.throwWithStackTrace(_error!, _stack!);
  }

  Future<void> cancel() {
    _stopping = true;
    return _cancellation ??= subscription.cancel().timeout(_deadline);
  }
}

Future<int> _produceOnce(
    KafkaClient client, String topic, List<KafkaRecord> records) async {
  // Exactly one call per batch. Never replay a Produce after a timeout or an
  // ambiguous outcome; fail and let finally close the shared client instead.
  final response =
      await client.produce(topic, records, acks: -1).timeout(_deadline);
  expect(response, isNotNull);
  expect(response!.responses.single.name, topic);
  final partition = response.responses.single.partitionResponses.single;
  expect(partition.index, 0);
  expect(partition.errorCode, 0);
  expect(partition.baseOffset, greaterThanOrEqualTo(0));
  return partition.baseOffset;
}

KafkaRecord _record(int sequence) => KafkaRecord(
    key: [0, 255, ...utf8.encode('subscription-endurance/$sequence')],
    value: List.generate(_payloadSize, (byte) => (sequence + byte) % 256),
    headers: [
      KafkaRecordHeader('sequence', utf8.encode('$sequence')),
      KafkaRecordHeader('duplicate', [0, 255, sequence % 256]),
      KafkaRecordHeader('duplicate', null),
      KafkaRecordHeader('empty', []),
    ]);

// Logical key/value/header bytes, excluding protocol framing and timestamps.
int _contentBytes(KafkaRecord record) =>
    (record.key?.length ?? 0) +
    (record.value?.length ?? 0) +
    record.headers.fold<int>(
        0,
        (total, header) =>
            total +
            utf8.encode(header.key).length +
            (header.value?.length ?? 0));

void _matches(
    KafkaTopicRecord actual, String topic, int offset, KafkaRecord expected) {
  expect(actual.topic, topic);
  expect(actual.partition, 0);
  expect(actual.offset, offset);
  expect(actual.record.offset, offset);
  expect(actual.record.key, expected.key);
  expect(actual.record.value, expected.value);
  expect(actual.record.timestamp, expected.timestamp);
  expect(actual.record.headers.map((header) => header.key),
      expected.headers.map((header) => header.key));
  expect(actual.record.headers.map((header) => header.value),
      expected.headers.map((header) => header.value));
}

void main() {
  final port =
      int.tryParse(Platform.environment['KAFKA_OPERATIONAL_PORT'] ?? '');
  final topic = Platform.environment['KAFKA_OPERATIONAL_TOPIC'];
  final controlPath = Platform.environment['KAFKA_OPERATIONAL_CONTROL'];
  final seconds = int.parse(
      Platform.environment['KAFKA_SUBSCRIPTION_ENDURANCE_SECONDS'] ?? '30');
  if (seconds < 1 || seconds > 1800) {
    throw ArgumentError(
        'KAFKA_SUBSCRIPTION_ENDURANCE_SECONDS must be in 1..1800');
  }

  test('bounded subscription endurance resumes from verified next offsets',
      () async {
    final client = KafkaClient(
        brokers: [Broker('127.0.0.1', port!)],
        requestTimeout: const Duration(seconds: 2),
        connectionTimeout: const Duration(milliseconds: 500),
        retryBaseDelay: const Duration(milliseconds: 20),
        maxReadRetries: 1,
        maxInFlightRequests: 16);
    final clock = Stopwatch();
    final duration = Duration(seconds: seconds);
    final initialRss = ProcessInfo.currentRss;
    var peakRss = initialRss;
    var peakPending = 0;
    var peakAdmitted = 0;
    var peakQueuedBytes = 0;
    var peakBufferedBytes = 0;
    var peakConnections = 0;
    var peakSubscriptions = 0;
    var maxOutstanding = 0;
    var acknowledged = 0;
    var verified = 0;
    var delivered = 0;
    var acknowledgedBytes = 0;
    var verifiedBytes = 0;
    var batches = 0;
    var produceCalls = 0;
    var restarts = 0;
    var verifiedRestarts = 0;
    var restartNeedsVerification = false;
    var retiredDeliveries = 0;
    int? firstOffset;
    int? batchBase;
    // Application-owned, IN-MEMORY ONLY. Initialization selects the first
    // acknowledged offset; advancement happens only after synchronous checking.
    // This is not a durable checkpoint, a Kafka group commit or exactly-once use.
    int? nextOffset;
    var records = <KafkaRecord>[];
    var batchVerified = 0;
    var nextRestart = _restartInterval;
    var nextProgress = _restartInterval;
    Object? asynchronousFailure;
    StackTrace? asynchronousStack;
    String? healthFailure;
    Timer? sampling;
    _Probe? probe;

    void sample() {
      final rss = ProcessInfo.currentRss;
      if (rss > peakRss) peakRss = rss;
      if (rss > _rssLimit) {
        healthFailure ??= 'RSS $rss exceeded fixed limit $_rssLimit';
      }
      final pending = client.pendingRequestCount;
      final admitted = client.admittedRequestCount;
      final queued = client.queuedRequestBytes;
      final buffered = client.bufferedResponseBytes;
      final connections = client.connectionCount;
      final subscriptions = client.activeSubscriptionCount;
      if (pending > peakPending) peakPending = pending;
      if (admitted > peakAdmitted) peakAdmitted = admitted;
      if (queued > peakQueuedBytes) peakQueuedBytes = queued;
      if (buffered > peakBufferedBytes) peakBufferedBytes = buffered;
      if (connections > peakConnections) peakConnections = connections;
      if (subscriptions > peakSubscriptions) peakSubscriptions = subscriptions;
      if (pending < 0 || admitted < 0 || pending > 16 || admitted > 16) {
        healthFailure ??= 'Request counters exceeded their admission bounds';
      }
      if (queued < 0 ||
          queued > client.maxPendingRequestBytes ||
          buffered < 0 ||
          buffered > client.maxBufferedResponseBytes ||
          connections < 0 ||
          connections > client.maxConnections ||
          subscriptions < 0 ||
          subscriptions > 1) {
        healthFailure ??= 'Byte, connection or subscription bounds exceeded';
      }
      final outstanding = records.length - batchVerified;
      if (outstanding > maxOutstanding) maxOutstanding = outstanding;
      if (outstanding < 0 || outstanding > _batchSize) {
        healthFailure ??= 'More than one four-record batch was outstanding';
      }
    }

    void check() {
      probe?.checkHealthy();
      if (asynchronousFailure != null) {
        Error.throwWithStackTrace(asynchronousFailure!, asynchronousStack!);
      }
      sample();
      expect(healthFailure, isNull, reason: healthFailure);
    }

    Map<String, Object?> summary() => {
          'requested_seconds': seconds,
          'elapsed_seconds': clock.elapsedMilliseconds / 1000,
          'batches': batches,
          'produce_calls': produceCalls,
          'acknowledged_records': acknowledged,
          'verified_records': verified,
          'delivered_records': delivered,
          'acknowledged_content_bytes': acknowledgedBytes,
          'verified_content_bytes': verifiedBytes,
          'verified_payload_bytes': verified * _payloadSize,
          'first_offset': firstOffset,
          'next_offset_checkpoint': nextOffset,
          'checkpoint_is_durable': false,
          'subscription_restarts': restarts,
          'verified_subscription_restarts': verifiedRestarts,
          'expected_subscription_restarts': (seconds - 1) ~/ 60,
          'outstanding_records': records.length - batchVerified,
          'max_outstanding_records': maxOutstanding,
          'initial_rss_bytes': initialRss,
          'current_rss_bytes': ProcessInfo.currentRss,
          'sampled_peak_rss_bytes': peakRss,
          'rss_limit_bytes': _rssLimit,
          'active_subscriptions': client.activeSubscriptionCount,
          'connections': client.connectionCount,
          'pending_requests': client.pendingRequestCount,
          'admitted_requests': client.admittedRequestCount,
          'queued_bytes': client.queuedRequestBytes,
          'buffered_bytes': client.bufferedResponseBytes,
          'peak_subscriptions': peakSubscriptions,
          'peak_connections': peakConnections,
          'peak_pending_requests': peakPending,
          'peak_admitted_requests': peakAdmitted,
          'peak_queued_bytes': peakQueuedBytes,
          'peak_buffered_bytes': peakBufferedBytes,
        };

    _Probe subscribe() => _Probe(
            client.subscribe(topic!,
                startOffsets: {0: nextOffset!},
                pollInterval: const Duration(milliseconds: 10),
                retryDelay: const Duration(milliseconds: 50),
                maxWaitMs: 100,
                maxBytes: 64 * 1024), (event) {
          delivered++;
          expect(batchVerified, lessThan(records.length),
              reason: 'No duplicate or unexpected delivery');
          expect(verified, lessThan(acknowledged),
              reason: 'Verify only acknowledged records');
          expect(event.offset, nextOffset);
          final expected = records[batchVerified];
          _matches(event, topic, batchBase! + batchVerified, expected);
          // No await occurs between complete verification and checkpointing.
          verifiedBytes += _contentBytes(expected);
          verified++;
          batchVerified++;
          nextOffset = event.offset + 1;
          if (restartNeedsVerification) {
            verifiedRestarts++;
            restartNeedsVerification = false;
          }
          if (batchVerified == records.length) probe!.subscription.pause();
        });

    Future<void> settled() async {
      await fixture.idle(client).timeout(_deadline);
      check();
      expect(client.activeSubscriptionCount, 0);
    }

    try {
      check();
      clock.start();
      sampling = Timer.periodic(const Duration(milliseconds: 100), (_) {
        try {
          sample();
          if (clock.elapsed >= nextProgress) {
            print('SUBSCRIPTION_ENDURANCE_PROGRESS ${jsonEncode(summary())}');
            nextProgress += _restartInterval;
          }
        } catch (error, stack) {
          asynchronousFailure ??= error;
          asynchronousStack ??= stack;
        }
      });
      do {
        check();
        expect(verified, acknowledged);
        expect(batchVerified, records.length);
        // Retain only this batch, replacing the previous drained batch. No
        // growing list of records, offsets, subscriptions or resource samples.
        records =
            List.generate(_batchSize, (index) => _record(acknowledged + index));
        batchVerified = 0;
        sample();
        produceCalls++;
        final base = await _produceOnce(client, topic!, records);
        firstOffset ??= base;
        nextOffset ??= base;
        expect(base, firstOffset + acknowledged);
        expect(base, nextOffset,
            reason: 'Every acknowledged batch follows the verified checkpoint');
        batchBase = base;
        acknowledged += records.length;
        acknowledgedBytes += records.fold<int>(
            0, (total, record) => total + _contentBytes(record));
        batches++;
        probe ??= subscribe();
        probe.subscription.resume();
        await _until(() => verified >= acknowledged,
            reason: 'Subscription did not drain the acknowledged batch',
            check: check);
        check();
        expect(verified, acknowledged);
        expect(delivered, acknowledged);
        expect(nextOffset, firstOffset + acknowledged);
        expect(batchVerified, records.length);
        expect(probe.subscription.isPaused, isTrue);
        expect(client.activeSubscriptionCount, 1);

        if (clock.elapsed >= nextRestart && clock.elapsed < duration) {
          // Cancel only at a drained boundary; preserve the shared client and
          // restart from the application's last synchronously verified offset.
          await probe.cancel();
          await settled();
          retiredDeliveries += probe.recordCount;
          expect(retiredDeliveries, delivered);
          final retained = await client
              .fetch(topic,
                  offset: nextOffset! - 1, maxWaitMs: 100, maxBytes: 64 * 1024)
              .timeout(_deadline);
          final fetchedTopic = retained.topics.single;
          final partition = fetchedTopic.partitions.single;
          expect(partition.errorCode, 0);
          _matches(
              KafkaTopicRecord(fetchedTopic.name, partition.partition,
                  partition.records.single),
              topic,
              nextOffset! - 1,
              records.last);
          await settled();
          probe = subscribe();
          restarts++;
          restartNeedsVerification = true;
          nextRestart += _restartInterval;
        }
        // Bound the workload instead of using this correctness test as a
        // throughput benchmark. The next iteration cannot overlap this batch.
        await Future<void>.delayed(const Duration(milliseconds: 10));
      } while (clock.elapsed < duration);

      await probe.cancel();
      await settled();
      expect(retiredDeliveries + probe.recordCount, delivered);
      expect(batches, greaterThan(0));
      expect(produceCalls, batches);
      expect(acknowledged, batches * _batchSize);
      expect(verified, acknowledged);
      expect(delivered, acknowledged);
      expect(verifiedBytes, acknowledgedBytes);
      expect(nextOffset, firstOffset + acknowledged);
      expect(maxOutstanding, _batchSize);
      expect(clock.elapsed, greaterThanOrEqualTo(duration));
      expect(restarts, (seconds - 1) ~/ 60);
      expect(verifiedRestarts, restarts,
          reason: 'Every replacement must verify a record before completion');
      expect(restartNeedsVerification, isFalse);
    } finally {
      sampling?.cancel();
      // Close first even on assertion/timeout failure to interrupt admitted I/O.
      // A paused or already-cancelled listener is still explicitly cancelled.
      try {
        await client.close().timeout(_deadline);
      } finally {
        await probe?.cancel();
      }
      await fixture.idle(client).timeout(_deadline);
      expect(client.activeSubscriptionCount, 0);
      expect(client.connectionCount, 0);
    }
    check();
    clock.stop();
    print('SUBSCRIPTION_ENDURANCE_RESULT ${jsonEncode(summary())}');
  },
      timeout: Timeout(Duration(seconds: seconds + 90)),
      skip: port == null || topic == null || controlPath == null
          ? 'Run tool/run_guarded_soak.py -- dart test '
              'test/topic_subscription_endurance_integration_test.dart '
              '--concurrency=1'
          : false);
}
