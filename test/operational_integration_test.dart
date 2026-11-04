import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';

// Destructive lifecycle commands go only through this disposable fixture's
// private control directory, never arbitrary broker PIDs or remote URLs.
Future<void> control(String path, String action) async {
  final id = '$pid-${DateTime.now().microsecondsSinceEpoch}';
  final temporary = File('$path/$id.temporary');
  final request = File('$path/$id.request');
  final responseFile = File('$path/$id.response');
  await temporary.writeAsString(jsonEncode({'action': action}), flush: true);
  await temporary.rename(request.path);
  final deadline = DateTime.now().add(const Duration(seconds: 75));
  while (!await responseFile.exists()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('Fixture $action command timed out');
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
  final response =
      jsonDecode(await responseFile.readAsString()) as Map<String, dynamic>;
  await responseFile.delete();
  expect(response['ok'], isTrue, reason: '$action: ${response['error']}');
  expect(response['running'], action == 'start');
}

Future<void> dispatched(KafkaClient client) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (client.queuedRequestBytes != 0 && DateTime.now().isBefore(deadline)) {
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  expect(client.queuedRequestBytes, 0);
  expect(client.pendingRequestCount, 1);
}

Future<void> idle(KafkaClient client) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (
      client.admittedRequestCount != 0 && DateTime.now().isBefore(deadline)) {
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  expect(client.admittedRequestCount, 0);
  expect(client.pendingRequestCount, 0);
  expect(client.queuedRequestBytes, 0);
  expect(client.bufferedResponseBytes, 0);
}

// Merge adjacent acknowledged ranges instead of retaining one entry per record.
// Bookkeeping stays small during long runs and is not a decoded-heap benchmark.
class OffsetCoverage {
  final List<List<int>> ranges = [];
  void add(int start, int count) {
    final end = start + count - 1;
    for (final range in ranges) {
      expect(end < range[0] || start > range[1], isTrue,
          reason: 'Acknowledged offset ranges must not overlap');
    }
    ranges.add([start, end]);
    ranges.sort((a, b) => a[0].compareTo(b[0]));
    for (var index = ranges.length - 1; index > 0; index--) {
      if (ranges[index - 1][1] + 1 == ranges[index][0]) {
        ranges[index - 1][1] = ranges[index][1];
        ranges.removeAt(index);
      }
    }
  }
}

void main() {
  final port =
      int.tryParse(Platform.environment['KAFKA_OPERATIONAL_PORT'] ?? '');
  final topic = Platform.environment['KAFKA_OPERATIONAL_TOPIC'];
  final path = Platform.environment['KAFKA_OPERATIONAL_CONTROL'];
  final seconds = int.parse(Platform.environment['KAFKA_SOAK_SECONDS'] ?? '60');
  if (seconds < 1 || seconds > 86400) {
    throw ArgumentError('KAFKA_SOAK_SECONDS must be in 1..86400');
  }
  final rssLimit =
      int.parse(Platform.environment['KAFKA_SOAK_MAX_RSS_BYTES'] ?? '0');
  if (rssLimit < 0) throw ArgumentError('RSS limit must be nonnegative');
  KafkaClient make({Duration timeout = const Duration(seconds: 5)}) =>
      KafkaClient(
        brokers: [Broker('127.0.0.1', port!)],
        requestTimeout: timeout,
        connectionTimeout: const Duration(milliseconds: 500),
        retryBaseDelay: const Duration(milliseconds: 20),
        maxReadRetries: 1,
        maxInFlightRequests: 32,
      );
  FetchRequest waitingFetch(int offset) => FetchRequest(-1, 60000, 1048576, [
        FetchTopic(topic!, [FetchPartition(0, offset, 1048576)])
      ]);

  group('real Kafka operational recovery', () {
    test('same client survives three broker crashes and persisted-log restarts',
        () async {
      final client = make();
      addTearDown(client.close);
      var offset = (await client.produce(
              topic!,
              [
                KafkaRecord(value: [42])
              ],
              acks: -1))!
          .responses
          .single
          .partitionResponses
          .single
          .baseOffset;
      for (var generation = 0; generation < 3; generation++) {
        final pending = expectLater(client.send(waitingFetch(offset + 1)),
            throwsA(isA<KafkaConnectionException>()));
        await dispatched(client);
        await control(path!, 'crash');
        await pending;
        await idle(client);
        await expectLater(
            client.metadata({topic}),
            throwsA(anyOf(
                isA<SocketException>(), isA<KafkaConnectionException>())));
        await control(path, 'start');
        final recovered = (await client.fetch(topic, offset: offset))
            .topics
            .single
            .partitions
            .single
            .records;
        expect(recovered.single.value, [42]);
        final next = (await client.produce(
                topic,
                [
                  KafkaRecord(value: [42])
                ],
                acks: -1))!
            .responses
            .single
            .partitionResponses
            .single
            .baseOffset;
        expect(next, offset + 1,
            reason: 'No implicit Produce replay on restart');
        offset = next;
        await idle(client);
      }
    }, timeout: const Timeout(Duration(minutes: 5)));

    test('broker long-poll deadline retires socket and next request reconnects',
        () async {
      final client = make(timeout: const Duration(milliseconds: 300));
      addTearDown(client.close);
      final offset = (await client.produce(topic!, [
        KafkaRecord(value: [43])
      ]))!
          .responses
          .single
          .partitionResponses
          .single
          .baseOffset;
      final clock = Stopwatch()..start();
      await expectLater(client.send(waitingFetch(offset + 1)),
          throwsA(isA<TimeoutException>()));
      expect(clock.elapsed, lessThan(const Duration(seconds: 2)));
      await idle(client);
      expect(client.connectionCount, 0);
      expect(
          (await client.fetch(topic, offset: offset))
              .topics
              .single
              .partitions
              .single
              .records
              .single
              .value,
          [43]);
      await idle(client);
    });

    test('cancel broker long-poll then reconnect using same client', () async {
      final client = make();
      addTearDown(client.close);
      final offset = (await client.produce(topic!, [
        KafkaRecord(value: [44])
      ]))!
          .responses
          .single
          .partitionResponses
          .single
          .baseOffset;
      final token = KafkaCancellationToken();
      final pending = expectLater(
          client.send(waitingFetch(offset + 1), cancellationToken: token),
          throwsA(isA<KafkaCancelledException>()));
      await dispatched(client);
      token.cancel();
      await pending;
      await idle(client);
      expect(client.connectionCount, 0);
      expect(
          (await client.fetch(topic, offset: offset))
              .topics
              .single
              .partitions
              .single
              .records
              .single
              .value,
          [44]);
      await idle(client);
    });

    test('repeated client close interrupts long polls and releases counters',
        () async {
      for (var cycle = 0; cycle < 20; cycle++) {
        final client = make();
        try {
          final offset = (await client.produce(topic!, [
            KafkaRecord(value: [45])
          ]))!
              .responses
              .single
              .partitionResponses
              .single
              .baseOffset;
          final pending = expectLater(
              client.send(waitingFetch(offset + 1)), throwsStateError);
          await dispatched(client);
          await client.close();
          await pending;
          await client.close();
          await idle(client);
          expect(client.connectionCount, 0);
        } finally {
          await client.close();
        }
      }
    });

    test(
        'sustained concurrent produce/fetch preserves every acknowledged batch',
        () async {
      final client = make();
      addTearDown(client.close);
      final clock = Stopwatch()..start();
      final initialRss = ProcessInfo.currentRss;
      var peakRss = initialRss;
      var batches = 0;
      var recordCount = 0;
      var failed = false;
      String? healthFailure;
      var lastProgressMs = 0;
      var lastProgressRecords = 0;
      final acknowledgedOffsets = OffsetCoverage();
      void sample({bool report = false}) {
        final rss = ProcessInfo.currentRss;
        if (rss > peakRss) peakRss = rss;
        if (rssLimit > 0 && rss > rssLimit && healthFailure == null) {
          healthFailure = 'RSS $rss exceeded configured limit $rssLimit';
          failed = true; // Stop new batches, then await all in-flight work.
        }
        final elapsedMs = clock.elapsedMilliseconds;
        if (report || elapsedMs - lastProgressMs >= 60000) {
          final intervalMs = elapsedMs - lastProgressMs;
          print('SOAK_PROGRESS ${jsonEncode({
                'seconds': elapsedMs / 1000,
                'records': recordCount,
                'batches': batches,
                'interval_records_per_second': intervalMs == 0
                    ? 0
                    : (recordCount - lastProgressRecords) * 1000 / intervalMs,
                'rss_bytes': rss,
                'sampled_peak_rss_bytes': peakRss,
                'rss_limit_bytes': rssLimit,
                'pending_requests': client.pendingRequestCount,
                'admitted_requests': client.admittedRequestCount,
                'queued_bytes': client.queuedRequestBytes,
                'buffered_bytes': client.bufferedResponseBytes,
                'health_failure': healthFailure,
              })}');
          lastProgressMs = elapsedMs;
          lastProgressRecords = recordCount;
        }
      }

      sample(report: true);
      final timer = Timer.periodic(const Duration(milliseconds: 100), (_) {
        sample();
      });
      try {
        await Future.wait(List.generate(4, (worker) async {
          var sequence = 0;
          try {
            while (!failed && clock.elapsed < Duration(seconds: seconds)) {
              final records = List.generate(
                  8,
                  (index) => KafkaRecord(
                          key: utf8.encode('$worker/$sequence/$index'),
                          value: List.generate(
                              512, (byte) => (byte + sequence + index) % 256),
                          headers: [
                            KafkaRecordHeader('worker', [worker])
                          ]));
              final produced =
                  (await client.produce(topic!, records, acks: -1))!
                      .responses
                      .single
                      .partitionResponses
                      .single
                      .baseOffset;
              final fetched = (await client.fetch(topic, offset: produced))
                  .topics
                  .single
                  .partitions
                  .single
                  .records;
              expect(fetched.length, greaterThanOrEqualTo(records.length));
              for (var i = 0; i < records.length; i++) {
                expect(fetched[i].offset, produced + i);
                expect(fetched[i].key, records[i].key);
                expect(fetched[i].value, records[i].value);
                expect(fetched[i].headers.single.key, 'worker');
                expect(fetched[i].headers.single.value, [worker]);
              }
              acknowledgedOffsets.add(produced, records.length);
              recordCount += records.length;
              batches++;
              sequence++;
              expect(client.pendingRequestCount, lessThanOrEqualTo(32));
              expect(client.admittedRequestCount, lessThanOrEqualTo(32));
              expect(client.connectionCount, 1);
            }
          } catch (_) {
            failed = true; // Stop new batches; await other in-flight work.
            rethrow;
          }
        }));
      } finally {
        timer.cancel();
      }
      await idle(client);
      sample(report: true);
      expect(healthFailure, isNull, reason: healthFailure);
      expect(batches, greaterThanOrEqualTo(4));
      expect(acknowledgedOffsets.ranges, hasLength(1));
      final range = acknowledgedOffsets.ranges.single;
      expect(range[1] - range[0] + 1, recordCount,
          reason: 'All acknowledged offsets must be contiguous and unique');
      print('SOAK ${jsonEncode({
            'seconds': clock.elapsedMilliseconds / 1000,
            'workers': 4,
            'batches': batches,
            'records': recordCount,
            'payload_bytes_per_record': 512,
            'initial_rss_bytes': initialRss,
            'sampled_peak_rss_bytes': peakRss,
            'final_rss_bytes': ProcessInfo.currentRss,
            'pending_requests': client.pendingRequestCount,
            'admitted_requests': client.admittedRequestCount,
            'queued_bytes': client.queuedRequestBytes,
            'buffered_bytes': client.bufferedResponseBytes,
          })}');
    }, timeout: Timeout(Duration(seconds: seconds + 60)));
  },
      skip: port == null || topic == null || path == null
          ? 'Run python3 tool/run_operational_integration.py'
          : false);
}
