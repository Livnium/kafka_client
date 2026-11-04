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

FetchPartitionResponse _fetch(List<int>? bytes) {
  final wire = KafkaBytesBuilder()
    ..addInt32(7) // Correlation id.
    ..addInt32(0) // Throttle.
    ..addInt32(1)
    ..addString('progress')
    ..addInt32(1)
    ..addInt32(0) // Partition.
    ..addInt16(0) // Error.
    ..addInt64(999) // High watermark must not become progress.
    ..addInt64(998)
    ..addInt32(-1) // Aborted transactions.
    ..addBytes(bytes);
  return FetchResponse.fromBytes(wire.takeBytes())
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
