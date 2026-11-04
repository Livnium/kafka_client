import 'dart:typed_data';

import 'package:kafka_client/src/api/response/api1_fetch_response.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/record_batch.dart';
import 'package:test/test.dart';

Uint8List _checksum(Uint8List bytes) {
  ByteData.sublistView(bytes)
      .setUint32(17, crc32c(bytes.sublist(21)), Endian.big);
  return bytes;
}

Uint8List _batch({
  int base = 10,
  int lastDelta = 0,
  bool control = false,
  bool empty = false,
}) {
  var bytes = encodeRecordBatch([
    KafkaRecord(value: [1], timestamp: 0)
  ]);
  if (empty) bytes = Uint8List.fromList(bytes.sublist(0, 61));
  final data = ByteData.sublistView(bytes);
  data.setInt64(0, base, Endian.big);
  data.setInt32(8, bytes.length - 12, Endian.big);
  data.setInt16(21, control ? 0x20 : 0, Endian.big);
  data.setInt32(23, lastDelta, Endian.big);
  if (empty) data.setInt32(57, 0, Endian.big);
  return _checksum(bytes);
}

List<int> _fetchBytes(List<int>? bytes,
    {int error = 0, int watermark = 999, int stable = 998}) {
  final wire = KafkaBytesBuilder()
    ..addInt32(7) // Correlation id.
    ..addInt32(0) // Throttle.
    ..addInt32(1)
    ..addString('progress')
    ..addInt32(1)
    ..addInt32(0) // Partition.
    ..addInt16(error)
    ..addInt64(watermark) // High watermark must not become progress.
    ..addInt64(stable)
    ..addInt32(-1) // Aborted transactions.
    ..addBytes(bytes);
  return wire.takeBytes();
}

FetchPartitionResponse _fetch(List<int>? bytes) {
  return FetchResponse.fromBytes(_fetchBytes(bytes))
      .topics
      .single
      .partitions
      .single;
}

void main() {
  test('empty input has no validated batch progress', () {
    final decoded = decodeRecordBatchesWithProgress([]);
    expect(decoded.records, isEmpty);
    expect(decoded.lastBatchOffset, isNull);
    expect(decodeRecordBatches([]), isEmpty);
  });

  test('normal batches preserve records and last inclusive batch offset', () {
    final bytes = encodeRecordBatch([
      KafkaRecord(value: [1], timestamp: 0),
      KafkaRecord(value: [2], timestamp: 0),
    ]);
    ByteData.sublistView(bytes).setInt64(0, 100, Endian.big);
    final decoded = decodeRecordBatchesWithProgress(bytes);
    expect(decoded.records.map((r) => r.offset), [100, 101]);
    expect(decoded.lastBatchOffset, 101);
    expect(decodeRecordBatches(bytes).map((r) => r.offset), [100, 101]);
  });

  test('control-only batches advance progress without exposing records', () {
    final bytes = _batch(base: 20, lastDelta: 4, control: true);
    final decoded = decodeRecordBatchesWithProgress(bytes);
    expect(decoded.records, isEmpty);
    expect(decoded.lastBatchOffset, 24);
    expect(decodeRecordBatches(bytes), isEmpty);
  });

  test('compacted sparse suffix advances beyond the last visible record', () {
    final decoded =
        decodeRecordBatchesWithProgress(_batch(base: 30, lastDelta: 9));
    expect(decoded.records.single.offset, 30);
    expect(decoded.lastBatchOffset, 39);
  });

  test('zero-record compacted batch still reports its last offset', () {
    final decoded = decodeRecordBatchesWithProgress(
        _batch(base: 40, lastDelta: 12, empty: true));
    expect(decoded.records, isEmpty);
    expect(decoded.lastBatchOffset, 52);
  });

  test('last batch wins without changing duplicate batch offset acceptance',
      () {
    final bytes = _batch();
    final decoded = decodeRecordBatchesWithProgress([...bytes, ...bytes]);
    expect(decoded.records.map((r) => r.offset), [10, 10]);
    expect(decoded.lastBatchOffset, 10);
    expect(decodeRecordBatches([...bytes, ...bytes]).length, 2);
    final tail = decodeRecordBatchesWithProgress([
      ...bytes,
      ..._batch(base: 20, lastDelta: 5, empty: true),
    ]);
    expect(tail.records.single.offset, 10);
    expect(tail.lastBatchOffset, 25);
  });

  test('INT64_MAX remains a valid inclusive record and batch offset', () {
    final bytes = _batch(base: 9223372036854775807);
    final decoded = decodeRecordBatchesWithProgress(bytes);
    expect(decoded.records.single.offset, 9223372036854775807);
    expect(decoded.lastBatchOffset, 9223372036854775807);
    expect(decodeRecordBatches(bytes).single.offset, 9223372036854775807);
  });

  test('batch end overflow rejected even without a visible overflowing record',
      () {
    for (final bytes in [
      _batch(base: 9223372036854775807, lastDelta: 1),
      _batch(base: 9223372036854775807, lastDelta: 1, control: true),
      _batch(base: 9223372036854775807, lastDelta: 1, empty: true),
    ]) {
      expect(
          () => decodeRecordBatchesWithProgress(bytes), throwsFormatException);
      expect(() => decodeRecordBatches(bytes), throwsFormatException);
    }
  });

  test('negative broker batch positions are rejected', () {
    for (final empty in [false, true]) {
      expect(
          () => decodeRecordBatchesWithProgress(
              _batch(base: -1, lastDelta: 2, empty: empty)),
          throwsFormatException);
    }
  });

  test('every nonempty truncation fails, including after a valid prefix', () {
    final bytes = _batch();
    for (var end = 1; end < bytes.length; end++) {
      final partial = bytes.sublist(0, end);
      expect(
          () => decodeRecordBatchesWithProgress(partial), throwsFormatException,
          reason: 'truncation $end');
      expect(() => decodeRecordBatchesWithProgress([...bytes, ...partial]),
          throwsFormatException,
          reason: 'truncated second batch $end');
    }
  });

  test('Fetch retains complete prefix and ignores every partial tail boundary',
      () {
    final prefix = _batch(base: 10, lastDelta: 4);
    final tail = _batch(base: 15, lastDelta: 9);
    for (var end = 1; end < tail.length; end++) {
      final partial = tail.sublist(0, end);
      final result = _fetch([...prefix, ...partial]);
      expect(result.records.map((r) => r.offset), [10], reason: 'tail $end');
      expect(result.lastBatchOffset, 14, reason: 'tail $end');
      // The standalone decoder remains strict for the identical bytes.
      expect(() => decodeRecordBatchesWithProgress([...prefix, ...partial]),
          throwsFormatException);
    }
  });

  test('Fetch rejects every nonempty incomplete first batch', () {
    final bytes = _batch();
    for (var end = 1; end < bytes.length; end++) {
      expect(() => _fetch(bytes.sublist(0, end)), throwsFormatException,
          reason: 'first batch $end');
    }
  });

  for (final kind in ['control', 'empty']) {
    test('Fetch $kind prefix advances only through the complete batch', () {
      final prefix = _batch(
          base: 20,
          lastDelta: 4,
          control: kind == 'control',
          empty: kind == 'empty');
      final tail = _batch(base: 25, lastDelta: 9);
      for (var end = 1; end < tail.length; end++) {
        final result = _fetch([...prefix, ...tail.take(end)]);
        expect(result.records, isEmpty);
        expect(result.lastBatchOffset, 24, reason: 'tail $end');
      }
      final complete = _fetch([...prefix, ...tail]);
      expect(complete.records.single.offset, 25);
      expect(complete.lastBatchOffset, 34);
    });
  }

  test('Fetch does not treat malformed complete tails as partial data', () {
    final prefix = _batch();
    final corrupt = _batch(base: 20)..[23] ^= 1;
    final badCount = _batch(base: 20);
    ByteData.sublistView(badCount).setInt32(57, -1, Endian.big);
    final badMagic = _batch(base: 20)..[16] = 3;
    for (final bytes in [corrupt, _checksum(badCount), badMagic]) {
      expect(() => _fetch([...prefix, ...bytes]), throwsFormatException);
    }
    final compressed = _batch(base: 20);
    ByteData.sublistView(compressed).setInt16(21, 1, Endian.big);
    expect(() => _fetch([...prefix, ..._checksum(compressed)]),
        throwsUnsupportedError);
  });

  test('Fetch rejects malformed partial-tail headers when available', () {
    final prefix = _batch();
    final badLength = _batch(base: 20);
    ByteData.sublistView(badLength).setInt32(8, -1, Endian.big);
    expect(() => _fetch([...prefix, ...badLength.take(12)]),
        throwsFormatException);
    final negativeBase = _batch(base: -1);
    expect(() => _fetch([...prefix, ...negativeBase.take(12)]),
        throwsFormatException);
    final badMagic = _batch(base: 20)..[16] = 3;
    expect(
        () => _fetch([...prefix, ...badMagic.take(17)]), throwsFormatException);
  });

  test('partial record-set handling does not tolerate a truncated response',
      () {
    final wire = _fetchBytes([..._batch(), ..._batch(base: 20).take(30)]);
    expect(() => FetchResponse.fromBytes(wire.sublist(0, wire.length - 1)),
        throwsFormatException);
    expect(() => FetchResponse.fromBytes([...wire, 0]), throwsFormatException);
  });

  test('partial tail cannot consume the following partition response', () {
    final wire = KafkaBytesBuilder()
      ..addInt32(7)
      ..addInt32(0)
      ..addInt32(1)
      ..addString('progress')
      ..addInt32(2);
    for (var partition = 0; partition < 2; partition++) {
      wire
        ..addInt32(partition)
        ..addInt16(0)
        ..addInt64(99)
        ..addInt64(-1)
        ..addInt32(-1)
        ..addBytes(partition == 0
            ? [..._batch(base: 10), ..._batch(base: 11).take(30)]
            : _batch(base: 50));
    }
    final partitions =
        FetchResponse.fromBytes(wire.takeBytes()).topics.single.partitions;
    expect(partitions[0].records.single.offset, 10);
    expect(partitions[0].lastBatchOffset, 10);
    expect(partitions[1].records.single.offset, 50);
    expect(partitions[1].lastBatchOffset, 50);
  });

  test('raw Fetch preserves broker errors and unavailable offset sentinels',
      () {
    final response = FetchResponse.fromBytes(
        _fetchBytes(null, error: 9, watermark: -1, stable: -1));
    final partition = response.topics.single.partitions.single;
    expect(partition.errorCode, 9);
    expect(partition.highWatermark, -1);
    expect(partition.lastStableOffset, -1);
    expect(partition.records, isEmpty);
    expect(partition.lastBatchOffset, isNull);
  });

  test('read-uncommitted decoding does not clamp to earlier HWM or LSO', () {
    // Kafka snapshots response positions before its independently bounded log
    // read. Concurrent advancement can make the reported positions lag data.
    final response = FetchResponse.fromBytes(
        _fetchBytes(_batch(base: 100), watermark: 99, stable: -1));
    final partition = response.topics.single.partitions.single;
    expect(partition.records.single.offset, 100);
    expect(partition.lastBatchOffset, 100);
    expect(partition.highWatermark, 99);
    expect(partition.lastStableOffset, -1);
  });

  test('CRC corruption and unsupported compression cannot report progress', () {
    final corrupt = _batch();
    corrupt[23] ^= 1;
    expect(
        () => decodeRecordBatchesWithProgress(corrupt), throwsFormatException);
    expect(() => _fetch(corrupt), throwsFormatException);
    final badMagic = _batch();
    badMagic[16] = 1;
    expect(
        () => decodeRecordBatchesWithProgress(badMagic), throwsFormatException);
    final compressed = _batch();
    ByteData.sublistView(compressed).setInt16(21, 1, Endian.big);
    _checksum(compressed);
    expect(() => decodeRecordBatchesWithProgress(compressed),
        throwsUnsupportedError);
  });

  test('valid CRC does not excuse malformed structure, even for control data',
      () {
    for (final control in [false, true]) {
      final badCount = _batch(control: control);
      ByteData.sublistView(badCount).setInt32(57, -1, Endian.big);
      expect(() => decodeRecordBatchesWithProgress(_checksum(badCount)),
          throwsFormatException);
      final badDelta = _batch(control: control, lastDelta: -1);
      expect(() => decodeRecordBatchesWithProgress(badDelta),
          throwsFormatException);
      final trailing = _batch(control: control);
      ByteData.sublistView(trailing).setInt32(57, 0, Endian.big);
      expect(() => decodeRecordBatchesWithProgress(_checksum(trailing)),
          throwsFormatException);
      final badRecord = _batch(control: control);
      badRecord[62] = 1; // Record attributes must be zero.
      expect(() => decodeRecordBatchesWithProgress(_checksum(badRecord)),
          throwsFormatException);
    }
  });

  test('fetch propagates batch progress, not watermark or visible last offset',
      () {
    for (final bytes in [
      _batch(base: 80, lastDelta: 9),
      _batch(base: 80, lastDelta: 9, control: true),
      _batch(base: 80, lastDelta: 9, empty: true),
    ]) {
      final partition = _fetch(bytes);
      expect(partition.lastBatchOffset, 89);
      expect(partition.highWatermark, 999);
    }
    expect(_fetch([]).lastBatchOffset, isNull);
    expect(_fetch(null).lastBatchOffset, isNull);
  });

  test('fetch constructor remains compatible and JSON shape remains unchanged',
      () {
    final original = FetchPartitionResponse(0, 0, 99, 98, []);
    final withProgress =
        FetchPartitionResponse(0, 0, 99, 98, [], lastBatchOffset: 40);
    expect(original.lastBatchOffset, isNull);
    expect(withProgress.lastBatchOffset, 40);
    expect(withProgress.toJson(), original.toJson());
    expect(withProgress.toJson().keys, [
      'partition',
      'errorCode',
      'highWatermark',
      'lastStableOffset',
      'records'
    ]);
  });
}
