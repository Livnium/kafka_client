import 'dart:convert';
import 'dart:typed_data';
import 'package:kafka_client/src/codec/record_batch.dart';
import 'package:test/test.dart';

Uint8List recalculate(Uint8List bytes) {
  ByteData.sublistView(bytes)
      .setUint32(17, crc32c(bytes.sublist(21)), Endian.big);
  return bytes;
}

void main() {
  test('Castagnoli standard check vector', () {
    expect(crc32c(ascii.encode('123456789')), 0xe3069283);
    expect(crc32c([]), 0);
  });
  test('magic2 fixed layout and independently expected single record', () {
    final bytes = encodeRecordBatch([
      KafkaRecord(value: [97], timestamp: 1000)
    ]);
    final data = ByteData.sublistView(bytes);
    expect(bytes.length, 69);
    expect(data.getInt32(8), 57);
    expect(bytes[16], 2);
    expect(data.getInt16(21), 0);
    expect(data.getInt64(27), 1000);
    expect(data.getInt64(43), -1);
    expect(data.getInt32(57), 1);
    expect(bytes.sublist(61), [14, 0, 0, 0, 1, 2, 97, 0]);
  });
  test('binary keys null empty headers timestamp deltas and offsets', () {
    final bytes = encodeRecordBatch([
      KafkaRecord(
          key: [0, 255],
          value: null,
          timestamp: 1000,
          headers: [KafkaRecordHeader('π', null), KafkaRecordHeader('π', [])]),
      KafkaRecord(key: null, value: [], timestamp: 999),
      KafkaRecord(key: [], value: [255, 0, 128], timestamp: 1020),
    ]);
    ByteData.sublistView(bytes).setInt64(0, 400, Endian.big);
    final records = decodeRecordBatches(bytes);
    expect(records.map((r) => r.offset), [400, 401, 402]);
    expect(records.map((r) => r.timestamp), [1000, 999, 1020]);
    expect(records[0].key, [0, 255]);
    expect(records[0].value, isNull);
    expect(records[1].key, isNull);
    expect(records[1].value, isEmpty);
    expect(records[2].key, isEmpty);
    expect(records[2].value, [255, 0, 128]);
    expect(records[0].headers.map((h) => h.key), ['π', 'π']);
    expect(records[0].headers[0].value, isNull);
    expect(records[0].headers[1].value, isEmpty);
  });
  test('multiple batches and empty fetch', () {
    final b = encodeRecordBatch([
      KafkaRecord(value: [1])
    ]);
    expect(decodeRecordBatches([...b, ...b]).length, 2);
    expect(decodeRecordBatches([]), isEmpty);
    expect(() => encodeRecordBatch([]), throwsArgumentError);
  });
  test('every truncation is rejected', () {
    final bytes = encodeRecordBatch([
      KafkaRecord(value: [1, 2, 3])
    ]);
    for (var end = 1; end < bytes.length; end++) {
      expect(() => decodeRecordBatches(bytes.sublist(0, end)),
          throwsFormatException,
          reason: 'truncation $end');
    }
  });
  test('corruption and bad magic rejected', () {
    final b = encodeRecordBatch([
      KafkaRecord(value: [1])
    ]);
    b[b.length - 2] ^= 1;
    expect(() => decodeRecordBatches(b), throwsFormatException);
    b[16] = 1;
    expect(() => decodeRecordBatches(b), throwsFormatException);
  });
  test('compression rejected explicitly and control batches filtered', () {
    final b = encodeRecordBatch([
      KafkaRecord(value: [1])
    ]);
    b[22] = 1;
    recalculate(b);
    expect(() => decodeRecordBatches(b), throwsUnsupportedError);
    b[22] = 32;
    recalculate(b);
    expect(decodeRecordBatches(b), isEmpty);
  });
  test('LogAppendTime applies batch timestamp to all records', () {
    final b = encodeRecordBatch(
        [KafkaRecord(timestamp: 10), KafkaRecord(timestamp: 20)]);
    b[22] = 8;
    recalculate(b);
    expect(decodeRecordBatches(b).map((r) => r.timestamp), [20, 20]);
  });
  test('invalid record count and negative length rejected after valid CRC', () {
    final b = encodeRecordBatch([
      KafkaRecord(value: [1])
    ]);
    ByteData.sublistView(b).setInt32(57, 2147483647, Endian.big);
    recalculate(b);
    expect(() => decodeRecordBatches(b), throwsFormatException);
    ByteData.sublistView(b).setInt32(57, 1, Endian.big);
    b[61] = 1;
    recalculate(b);
    expect(() => decodeRecordBatches(b), throwsFormatException);
  });
  test('offset and timestamp int64 overflow rejected instead of wrapping', () {
    final offsets = encodeRecordBatch([KafkaRecord(), KafkaRecord()]);
    ByteData.sublistView(offsets).setInt64(0, 9223372036854775807, Endian.big);
    expect(() => decodeRecordBatches(offsets), throwsFormatException);
    final timestamps = encodeRecordBatch(
        [KafkaRecord(timestamp: 0), KafkaRecord(timestamp: 1)]);
    ByteData.sublistView(timestamps)
        .setInt64(27, 9223372036854775807, Endian.big);
    recalculate(timestamps);
    expect(() => decodeRecordBatches(timestamps), throwsFormatException);
  });
  test('negative producer timestamps and invalid byte values rejected', () {
    expect(() => encodeRecordBatch([KafkaRecord(timestamp: -1)]),
        throwsArgumentError);
    expect(
        () => encodeRecordBatch([
              KafkaRecord(value: [256])
            ]),
        throwsArgumentError);
    expect(
        () => encodeRecordBatch([
              KafkaRecord(key: [-1])
            ]),
        throwsArgumentError);
  });
  test('omitted timestamp defaults to now; explicit epoch remains unchanged',
      () {
    final before = DateTime.now().millisecondsSinceEpoch;
    final record = KafkaRecord(value: [1]);
    final after = DateTime.now().millisecondsSinceEpoch;
    expect(record.timestamp, inInclusiveRange(before, after));
    expect(KafkaRecord(timestamp: 0).timestamp, 0);
    expect(decodeRecordBatches(encodeRecordBatch([record])).single.timestamp,
        record.timestamp);
  });
}
