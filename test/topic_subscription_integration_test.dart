import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

// Import only the disposable fixture helpers. Its main() is never invoked.
import 'operational_integration_test.dart' as fixture show control, idle;

const _deadline = Duration(seconds: 15);
const _rssLimit = 768 * 1024 * 1024;

Future<void> _until(bool Function() condition,
    {required String reason,
    void Function()? check,
    Duration timeout = _deadline}) async {
  final clock = Stopwatch()..start();
  while (true) {
    check?.call();
    if (condition()) return;
    if (clock.elapsed >= timeout) throw TimeoutException(reason, timeout);
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

// Capture listener assertions and stream errors rather than allowing an async
// callback to fail outside the awaited test. Keep only the first of each error.
class _Probe {
  final done = Completer<void>();
  late final StreamSubscription<KafkaTopicRecord> subscription;
  Object? streamError;
  StackTrace? _streamStack;
  Object? _callbackError;
  StackTrace? _callbackStack;
  int errorCount = 0;
  int recordCount = 0;

  _Probe(Stream<KafkaTopicRecord> stream,
      void Function(KafkaTopicRecord) inspect) {
    subscription = stream.listen((record) {
      recordCount++;
      if (_callbackError != null) return;
      try {
        inspect(record);
      } catch (error, stack) {
        _callbackError = error;
        _callbackStack = stack;
      }
    }, onError: (Object error, StackTrace stack) {
      errorCount++;
      streamError ??= error;
      _streamStack ??= stack;
    }, onDone: () {
      if (!done.isCompleted) done.complete();
    });
  }

  void checkHealthy() {
    if (_callbackError != null) {
      Error.throwWithStackTrace(_callbackError!, _callbackStack!);
    }
    if (streamError != null) {
      Error.throwWithStackTrace(streamError!, _streamStack!);
    }
  }

  Future<void> received(int count) => _until(() => recordCount >= count,
      reason: 'Subscription did not deliver $count records',
      check: checkHealthy);

  Future<void> cancel() => subscription.cancel().timeout(_deadline);
}

Future<void> _cleanup(KafkaClient client, List<_Probe> probes) async {
  // Close first to interrupt any admitted I/O, including on assertion failure.
  // Cancelling paused listeners afterward also releases their queued done event.
  try {
    await client.close().timeout(_deadline);
  } finally {
    await Future.wait(probes.map((probe) => probe.cancel()));
  }
  await fixture.idle(client);
  expect(client.activeSubscriptionCount, 0);
  expect(client.connectionCount, 0);
}

Future<int> _produceOnce(
    KafkaClient client, String topic, List<KafkaRecord> records) async {
  // Never retry a Produce, including after an ambiguous transport failure.
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

KafkaRecord _marker(String value) => KafkaRecord(
    key: utf8.encode('subscription/$value'), value: utf8.encode(value));

KafkaRecord _sustainedRecord(int sequence) => KafkaRecord(
        key: utf8.encode('subscription-soak/$sequence'),
        value: List.generate(512, (byte) => (byte + sequence) % 256),
        headers: [
          KafkaRecordHeader('sequence', utf8.encode('$sequence')),
          KafkaRecordHeader('nullable', null),
        ]);

void main() {
  final port =
      int.tryParse(Platform.environment['KAFKA_OPERATIONAL_PORT'] ?? '');
  final topic = Platform.environment['KAFKA_OPERATIONAL_TOPIC'];
  final controlPath = Platform.environment['KAFKA_OPERATIONAL_CONTROL'];
  final seconds = int.parse(
      Platform.environment['KAFKA_SUBSCRIPTION_SOAK_SECONDS'] ?? '30');
  if (seconds < 1 || seconds > 300) {
    throw ArgumentError('KAFKA_SUBSCRIPTION_SOAK_SECONDS must be in 1..300');
  }

  KafkaClient make() => KafkaClient(
      brokers: [Broker('127.0.0.1', port!)],
      requestTimeout: const Duration(seconds: 2),
      connectionTimeout: const Duration(milliseconds: 500),
      retryBaseDelay: const Duration(milliseconds: 20),
      maxReadRetries: 1,
      maxInFlightRequests: 16);

  Stream<KafkaTopicRecord> stream(KafkaClient client, int offset,
          {int maxWaitMs = 100}) =>
      client.subscribe(topic!,
          startOffsets: {0: offset},
          pollInterval: const Duration(milliseconds: 10),
          retryDelay: const Duration(milliseconds: 50),
          maxWaitMs: maxWaitMs,
          maxBytes: 64 * 1024);

  // This runner owns one dedicated partition and must run serially with other
  // destructive fixture tests. It does not establish multi-partition ordering,
  // consumer-group semantics, processing acknowledgments or exactly-once use.
  group('real Kafka topic subscriptions', () {
    test('inclusive offset preserves bytes, duplicate headers and tombstones',
        () async {
      final client = make();
      final probes = <_Probe>[];
      try {
        final records = [
          _marker('skip-prefix'),
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
          KafkaRecord(key: utf8.encode('deleted'), value: null),
          KafkaRecord(key: null, value: []),
        ];
        final base = await _produceOnce(client, topic!, records);
        var next = 1;
        final probe = _Probe(stream(client, base + 1), (event) {
          expect(next, lessThan(records.length),
              reason: 'No duplicate or unexpected records');
          _matches(event, topic, base + next, records[next]);
          next++;
        });
        probes.add(probe);
        await probe.received(3);
        await probe.cancel();
        probe.checkHealthy();
        expect(probe.recordCount, 3);
        expect(next, 4);
        expect(client.activeSubscriptionCount, 0);
        await fixture.idle(client);
      } finally {
        await _cleanup(client, probes);
      }
    });

    test('pause, resume and cancellation preserve a usable shared client',
        () async {
      final client = make();
      final probes = <_Probe>[];
      try {
        final records = List.generate(4, (index) => _marker('pause/$index'));
        final base = await _produceOnce(client, topic!, records);
        var next = 0;
        late _Probe probe;
        probe = _Probe(stream(client, base), (event) {
          expect(next, lessThan(records.length));
          _matches(event, topic, base + next, records[next]);
          next++;
          if (next == 1) probe.subscription.pause();
        });
        probes.add(probe);
        await probe.received(1);
        await fixture.idle(client);
        expect(probe.subscription.isPaused, isTrue);
        expect(client.activeSubscriptionCount, 1);
        await Future<void>.delayed(const Duration(milliseconds: 200));
        probe.checkHealthy();
        expect(probe.recordCount, 1);
        expect(client.pendingRequestCount, 0);

        final whilePaused = _marker('pause/appended');
        records.add(whilePaused);
        expect(await _produceOnce(client, topic, [whilePaused]), base + 4);
        expect(probe.recordCount, 1);
        probe.subscription.resume();
        await probe.received(5);
        await probe.cancel();
        probe.checkHealthy();
        expect(probe.recordCount, 5);
        expect(client.activeSubscriptionCount, 0);
        await fixture.idle(client);

        final afterCancel = _marker('pause/after-cancel');
        final offset = await _produceOnce(client, topic, [afterCancel]);
        expect(offset, base + 5);
        final fetched =
            await client.fetch(topic, offset: offset).timeout(_deadline);
        expect(fetched.topics.single.partitions.single.records.single.value,
            afterCancel.value);
        expect(probe.recordCount, 5);
      } finally {
        await _cleanup(client, probes);
      }
    });

    test('same stream survives an owned broker crash and persisted restart',
        () async {
      final client = make();
      final probes = <_Probe>[];
      var brokerStopped = false;
      try {
        final records = [_marker('restart/0'), _marker('restart/1')];
        final base = await _produceOnce(client, topic!, records);
        var next = 0;
        final probe = _Probe(stream(client, base, maxWaitMs: 1000), (event) {
          expect(next, lessThan(records.length));
          _matches(event, topic, base + next, records[next]);
          next++;
        });
        probes.add(probe);
        await probe.received(2);
        await _until(
            () =>
                client.pendingRequestCount == 1 &&
                client.queuedRequestBytes == 0,
            reason: 'Subscription did not start its next broker read',
            check: probe.checkHealthy);
        await fixture.control(controlPath!, 'crash');
        brokerStopped = true;
        await Future<void>.delayed(const Duration(milliseconds: 500));
        probe.checkHealthy();
        expect(probe.done.isCompleted, isFalse);
        expect(probe.recordCount, 2);
        expect(client.activeSubscriptionCount, 1);

        await fixture.control(controlPath, 'start');
        brokerStopped = false;
        // This read is retry-safe; never use retries for the following Produce.
        final persisted =
            await client.fetch(topic, offset: base).timeout(_deadline);
        expect(
            persisted.topics.single.partitions.single.records
                .map((record) => record.value),
            records.map((record) => record.value));
        final appended = [_marker('restart/2'), _marker('restart/3')];
        records.addAll(appended);
        expect(await _produceOnce(client, topic, appended), base + 2,
            reason: 'Earlier acknowledged Produce was not replayed');
        await probe.received(4);

        final last = _marker('restart/4');
        records.add(last);
        expect(await _produceOnce(client, topic, [last]), base + 4);
        await probe.received(5);
        await probe.cancel();
        probe.checkHealthy();
        expect(probe.recordCount, 5);
        expect(next, 5,
            reason: 'Every fixture-acknowledged offset arrived once in order');
        await fixture.idle(client);
        expect(client.activeSubscriptionCount, 0);
      } finally {
        try {
          await _cleanup(client, probes);
        } finally {
          // Restore the owned fixture even when assertions fail while it is down.
          if (brokerStopped) await fixture.control(controlPath!, 'start');
        }
      }
    }, timeout: const Timeout(Duration(minutes: 4)));

    test('out-of-range offset reports one terminal error without resetting',
        () async {
      final client = make();
      final probes = <_Probe>[];
      try {
        final marker = _marker('out-of-range/retained');
        final base = await _produceOnce(client, topic!, [marker]);
        final probe = _Probe(stream(client, 9223372036854775807), (_) {});
        probes.add(probe);
        await probe.done.future.timeout(_deadline);
        expect(probe.recordCount, 0);
        expect(probe.errorCount, 1);
        expect(probe.streamError,
            isA<KafkaBrokerException>().having((e) => e.errorCode, 'code', 1));
        await fixture.idle(client);
        expect(client.activeSubscriptionCount, 0);
        await Future<void>.delayed(const Duration(milliseconds: 200));
        expect(probe.errorCount, 1);
        expect(probe.recordCount, 0);
        final retained =
            await client.fetch(topic, offset: base).timeout(_deadline);
        expect(retained.topics.single.partitions.single.records.single.value,
            marker.value);
      } finally {
        await _cleanup(client, probes);
      }
    });

    test('client close stops active and paused streams and clears counters',
        () async {
      final client = make();
      final probes = <_Probe>[];
      try {
        final records = [_marker('close/0'), _marker('close/1')];
        final base = await _produceOnce(client, topic!, records);
        late _Probe paused;
        paused = _Probe(stream(client, base), (_) {
          paused.subscription.pause();
        });
        probes.add(paused);
        await paused.received(1);
        final active =
            _Probe(stream(client, base + 2, maxWaitMs: 1000), (_) {});
        probes.add(active);
        await _until(
            () =>
                client.activeSubscriptionCount == 2 &&
                client.pendingRequestCount == 1 &&
                client.queuedRequestBytes == 0,
            reason: 'Active and paused streams were not both established',
            check: () {
          paused.checkHealthy();
          active.checkHealthy();
        });
        expect(paused.subscription.isPaused, isTrue);
        await client.close().timeout(_deadline);
        await active.done.future.timeout(_deadline);
        await fixture.idle(client);
        expect(client.activeSubscriptionCount, 0);
        expect(client.connectionCount, 0);
        paused.subscription.resume();
        await paused.done.future.timeout(_deadline);
        paused.checkHealthy();
        active.checkHealthy();
        expect(paused.recordCount, 1);
        expect(active.recordCount, 0);
        await client.close().timeout(_deadline);
      } finally {
        await _cleanup(client, probes);
      }
    });

    test(
        'bounded sustained produce and subscription preserve acknowledged data',
        () async {
      final client = make();
      final probes = <_Probe>[];
      final clock = Stopwatch();
      final initialRss = ProcessInfo.currentRss;
      var peakRss = initialRss;
      var produced = 0;
      var consumed = 0;
      var batches = 0;
      String? healthFailure;
      Timer? sampling;
      void sample() {
        final rss = ProcessInfo.currentRss;
        if (rss > peakRss) peakRss = rss;
        if (rss > _rssLimit) {
          healthFailure ??= 'RSS $rss exceeded fixed limit $_rssLimit';
        }
        if (client.pendingRequestCount > 16 ||
            client.admittedRequestCount > 16) {
          healthFailure ??= 'Request counts exceeded admission limit';
        }
      }

      try {
        sample();
        expect(healthFailure, isNull, reason: healthFailure);
        final anchor = await _produceOnce(client, topic!, [_marker('soak')]);
        final first = anchor + 1;
        final probe = _Probe(stream(client, first), (event) {
          final expected = _sustainedRecord(consumed);
          expect(event.topic, topic);
          expect(event.partition, 0);
          expect(event.offset, first + consumed);
          expect(event.record.key, expected.key);
          expect(event.record.value, expected.value);
          expect(event.record.headers.map((header) => header.key),
              expected.headers.map((header) => header.key));
          expect(event.record.headers.map((header) => header.value),
              expected.headers.map((header) => header.value));
          consumed++;
        });
        probes.add(probe);
        sampling = Timer.periodic(const Duration(milliseconds: 100), (_) {
          sample();
        });
        clock.start();
        while (clock.elapsed < Duration(seconds: seconds)) {
          probe.checkHealthy();
          sample();
          expect(healthFailure, isNull, reason: healthFailure);
          // At most one four-record batch is outstanding. Do not accumulate a
          // history of records or offsets, and do not race ambiguous writes.
          final records =
              List.generate(4, (index) => _sustainedRecord(produced + index));
          final base = await _produceOnce(client, topic, records);
          expect(base, first + produced,
              reason: 'Acknowledged appends are contiguous and never replayed');
          produced += records.length;
          batches++;
          await _until(() => consumed >= produced,
              reason: 'Subscription did not drain the acknowledged batch',
              check: () {
            probe.checkHealthy();
            sample();
            expect(healthFailure, isNull, reason: healthFailure);
          });
          expect(consumed, produced);
          // This is a bounded correctness observation, not a throughput test.
          await Future<void>.delayed(const Duration(milliseconds: 10));
        }
        await probe.cancel();
        probe.checkHealthy();
        await fixture.idle(client);
        sample();
        expect(healthFailure, isNull, reason: healthFailure);
        expect(client.activeSubscriptionCount, 0);
        expect(batches, greaterThan(0));
        expect(produced, consumed);
        expect(probe.recordCount, consumed);
        print('SUBSCRIPTION_SOAK ${jsonEncode({
              'seconds': clock.elapsedMilliseconds / 1000,
              'batches': batches,
              'acknowledged_records': produced,
              'consumed_records': consumed,
              'payload_bytes_per_record': 512,
              'initial_rss_bytes': initialRss,
              'sampled_peak_rss_bytes': peakRss,
              'rss_limit_bytes': _rssLimit,
              'active_subscriptions': client.activeSubscriptionCount,
              'pending_requests': client.pendingRequestCount,
              'admitted_requests': client.admittedRequestCount,
              'queued_bytes': client.queuedRequestBytes,
              'buffered_bytes': client.bufferedResponseBytes,
            })}');
      } finally {
        sampling?.cancel();
        await _cleanup(client, probes);
      }
    }, timeout: Timeout(Duration(seconds: seconds + 60)));
  },
      skip: port == null || topic == null || controlPath == null
          ? 'Run tool/run_guarded_soak.py -- dart test '
              'test/topic_subscription_integration_test.dart --concurrency=1'
          : false);
}
