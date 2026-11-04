// Standalone VM probe: deliberately no package:test or test-runner isolate.
// KAFKA_MEMORY_MODE=live|idle|codec (default live)
// KAFKA_SOAK_SECONDS=10..1800 (default 10). The RSS guard is not configurable.
// Live mode requires an already-approved disposable operational fixture and its
// KAFKA_OPERATIONAL_PORT/TOPIC. This program never starts/stops a broker.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:kafka_client/src/codec/record_batch.dart'
    show decodeRecordBatches;

const rssLimit = 805306368;
const workers = 4;
const recordsPerBatch = 8;

int? probeDurationSeconds(String? value) {
  final seconds = int.tryParse(value ?? '10');
  return seconds != null && seconds >= 10 && seconds <= 1800 ? seconds : null;
}

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

bool bytesEqual(List<int>? a, List<int>? b) {
  if (a == null || b == null) return a == null && b == null;
  if (a.length != b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) return false;
  }
  return true;
}

class OffsetCoverage {
  final ranges = <List<int>>[];
  void add(int start, int count) {
    final end = start + count - 1;
    for (final range in ranges) {
      check(end < range[0] || start > range[1], 'Overlapping offset ranges');
    }
    ranges.add([start, end]);
    ranges.sort((a, b) => a[0].compareTo(b[0]));
    for (var i = ranges.length - 1; i > 0; i--) {
      if (ranges[i - 1][1] + 1 == ranges[i][0]) {
        ranges[i - 1][1] = ranges[i][1];
        ranges.removeAt(i);
      }
    }
    // Normally <= workers+1 for the fixture's exclusive single-partition topic.
    // A foreign producer or stalled gap must not create unbounded bookkeeping.
    check(ranges.length <= 64, 'Offset coverage exceeded 64 ranges');
  }
}

List<KafkaRecord> makeRecords(int worker, int sequence) => List.generate(
    recordsPerBatch,
    (index) => KafkaRecord(
            key: utf8.encode('$worker/$sequence/$index'),
            value:
                List.generate(512, (byte) => (byte + sequence + index) % 256),
            headers: [
              KafkaRecordHeader('worker', [worker])
            ]));

void validate(List<KafkaRecord> expected, List<KafkaRecord> actual,
    int baseOffset, int worker) {
  check(actual.length >= expected.length, 'Fetch missing records');
  for (var i = 0; i < expected.length; i++) {
    final got = actual[i];
    check(got.offset == baseOffset + i, 'Offset mismatch at $i');
    check(bytesEqual(got.key, expected[i].key), 'Key mismatch at $i');
    check(bytesEqual(got.value, expected[i].value), 'Payload mismatch at $i');
    check(got.headers.length == 1, 'Header count mismatch at $i');
    check(got.headers.single.key == 'worker', 'Header key mismatch at $i');
    check(bytesEqual(got.headers.single.value, [worker]),
        'Header value mismatch at $i');
  }
}

// Serialize the identical Produce v3 request once, then extract its encoded
// record batch for codec validation. This is not a synthetic network response:
// it omits transport, metadata, Fetch framing, and broker-side read amplification.
List<KafkaRecord> codecRoundTrip(String topic, List<KafkaRecord> records) {
  final wire = ProduceRequest(acks: -1, timeoutMs: 5000, topicData: [
    TopicData(
        name: topic, partitions: [PartitionData(index: 0, records: records)])
  ]).toBytes();
  final reader = KafkaBytesReader.fromBytes(wire);
  check(reader.readInt32() == wire.length - 4, 'Frame length mismatch');
  check(reader.readInt16() == 0, 'Produce API mismatch');
  check(reader.readInt16() == 3, 'Produce version mismatch');
  reader.readInt32(); // Correlation ID.
  reader.readString(); // Client ID.
  reader.readString(); // Transaction ID.
  check(reader.readInt16() == -1, 'Acknowledgments mismatch');
  reader.readInt32(); // Broker timeout.
  check(reader.readInt32() == 1, 'Topic count mismatch');
  check(reader.readString() == topic, 'Topic mismatch');
  check(reader.readInt32() == 1, 'Partition count mismatch');
  check(reader.readInt32() == 0, 'Partition mismatch');
  final batch = reader.readRaw(reader.readInt32());
  check(reader.isEOF, 'Trailing Produce bytes');
  return decodeRecordBatches(batch);
}

Future<void> settle(KafkaClient client) async {
  final clock = Stopwatch()..start();
  while (client.admittedRequestCount != 0 && clock.elapsedMilliseconds < 5000) {
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  check(
      client.admittedRequestCount == 0 &&
          client.pendingRequestCount == 0 &&
          client.queuedRequestBytes == 0 &&
          client.bufferedResponseBytes == 0,
      'Client counters did not settle');
}

Future<void> main() async {
  final env = Platform.environment;
  final mode = env['KAFKA_MEMORY_MODE'] ?? 'live';
  final seconds = probeDurationSeconds(env['KAFKA_SOAK_SECONDS']);
  if (!['live', 'idle', 'codec'].contains(mode) || seconds == null) {
    stderr.writeln('Use mode live|idle|codec and KAFKA_SOAK_SECONDS=10..1800');
    exitCode = 64;
    return;
  }
  final port = int.tryParse(env['KAFKA_OPERATIONAL_PORT'] ?? '');
  final topic = env['KAFKA_OPERATIONAL_TOPIC'] ?? 'memory-codec';
  if (mode == 'live' &&
      (port == null ||
          port < 1 ||
          port > 65535 ||
          env['KAFKA_OPERATIONAL_TOPIC'] == null ||
          topic.isEmpty)) {
    stderr.writeln('Live mode requires operational fixture port and topic');
    exitCode = 64;
    return;
  }
  final client = mode == 'live'
      ? KafkaClient(
          brokers: [Broker('127.0.0.1', port!)],
          requestTimeout: const Duration(seconds: 5),
          connectionTimeout: const Duration(milliseconds: 500),
          retryBaseDelay: const Duration(milliseconds: 20),
          maxReadRetries: 1,
          maxInFlightRequests: 32,
          maxConnections: 8,
          maxFrameBytes: 16 * 1024 * 1024,
          maxPendingRequestBytes: 32 * 1024 * 1024,
          maxBufferedResponseBytes: 32 * 1024 * 1024,
        )
      : null;
  final clock = Stopwatch()..start();
  final initialRss = ProcessInfo.currentRss;
  var peakRss = initialRss;
  var batches = 0;
  var recordCount = 0;
  var failed = false;
  String? failure;
  var lastProgressMs = 0;
  var lastProgressRecords = 0;
  final coverage = OffsetCoverage();
  void fail(Object error) {
    failed = true;
    failure ??= error.toString();
  }

  void sample(String phase, {bool report = false}) {
    final rss = ProcessInfo.currentRss;
    if (rss > peakRss) peakRss = rss;
    if (rss > rssLimit) fail('RSS $rss exceeded fixed limit $rssLimit');
    final elapsedMs = clock.elapsedMilliseconds;
    if (report || elapsedMs - lastProgressMs >= 1000) {
      final intervalMs = elapsedMs - lastProgressMs;
      stdout.writeln('MEMORY_PROBE ${jsonEncode({
            'phase': phase,
            'mode': mode,
            'dart_version': Platform.version,
            'seconds': elapsedMs / 1000,
            'requested_seconds': seconds,
            'workers': workers,
            'records_per_batch': recordsPerBatch,
            'payload_bytes_per_record': 512,
            'batches': batches,
            'records': recordCount,
            'initial_rss_bytes': initialRss,
            'rss_bytes': rss,
            'sampled_peak_rss_bytes': peakRss,
            'rss_limit_bytes': rssLimit,
            'sample_interval_ms': 100,
            'interval_records_per_second': intervalMs == 0
                ? 0
                : (recordCount - lastProgressRecords) * 1000 / intervalMs,
            'pending_requests': client?.pendingRequestCount ?? 0,
            'admitted_requests': client?.admittedRequestCount ?? 0,
            'queued_bytes': client?.queuedRequestBytes ?? 0,
            'buffered_bytes': client?.bufferedResponseBytes ?? 0,
            'connections': client?.connectionCount ?? 0,
            'offset_ranges': coverage.ranges.length,
            'failure': failure,
          })}');
      lastProgressMs = elapsedMs;
      lastProgressRecords = recordCount;
    }
  }

  sample('initial', report: true);
  final timer = Timer.periodic(const Duration(milliseconds: 100), (_) {
    sample('progress');
  });
  try {
    if (mode == 'idle') {
      while (!failed && clock.elapsed < Duration(seconds: seconds)) {
        await Future<void>.delayed(const Duration(milliseconds: 100));
      }
    } else {
      await Future.wait(List.generate(workers, (worker) async {
        var sequence = 0;
        try {
          while (!failed && clock.elapsed < Duration(seconds: seconds)) {
            final records = makeRecords(worker, sequence);
            if (client != null) {
              final produced = (await client.produce(topic, records, acks: -1))!
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
              validate(records, fetched, produced, worker);
              coverage.add(produced, records.length);
              check(
                  client.pendingRequestCount <= 32 &&
                      client.admittedRequestCount <= 32,
                  'Request counter overflow');
              check(client.connectionCount == 1, 'Unexpected connection count');
            } else {
              validate(records, codecRoundTrip(topic, records), 0, worker);
            }
            recordCount += records.length;
            batches++;
            sequence++;
            // Codec work is synchronous; yield to timers and the other workers.
            // Live mode keeps the original workload's request-driven scheduling.
            if (client == null) await Future<void>.delayed(Duration.zero);
          }
        } catch (error, stack) {
          fail(error);
          stderr.writeln('$error\n$stack');
        }
      })); // Worker errors are caught: all current batches settle before close.
      check(failed || batches >= workers, 'Too few completed batches');
      if (!failed && client != null) {
        check(coverage.ranges.length == 1, 'Offsets are not contiguous');
        final range = coverage.ranges.single;
        check(range[1] - range[0] + 1 == recordCount,
            'Acknowledged record coverage mismatch');
      }
    }
  } catch (error, stack) {
    fail(error);
    stderr.writeln('$error\n$stack');
  } finally {
    if (client != null) {
      try {
        await settle(client);
      } catch (error) {
        fail(error);
      }
      sample('settled', report: true);
      try {
        await client.close();
        await settle(client);
        check(client.connectionCount == 0, 'Connections remain after close');
      } catch (error) {
        fail(error);
      }
    }
    sample('final', report: true);
    timer.cancel();
    exitCode = failed ? 1 : 0;
  }
}
