import 'dart:convert';
import 'dart:typed_data';

import 'byte_builder.dart';
import 'byte_reader.dart' hide KafkaType;

/// A record header. Duplicate keys and null values are preserved.
class KafkaRecordHeader {
  final String key;
  final List<int>? value;
  KafkaRecordHeader(this.key, List<int>? value)
      : value = value == null ? null : List<int>.unmodifiable(value);
}

/// A binary record. Null values are tombstones, distinct from empty values.
class KafkaRecord {
  final List<int>? key;
  final List<int>? value;

  /// Creation time in milliseconds; defaults to the current wall-clock time.
  final int timestamp;
  final int offset;
  final List<KafkaRecordHeader> headers;

  KafkaRecord(
      {List<int>? key,
      List<int>? value,
      int? timestamp,
      this.offset = 0,
      List<KafkaRecordHeader> headers = const []})
      : timestamp = timestamp ?? DateTime.now().millisecondsSinceEpoch,
        key = key == null ? null : List<int>.unmodifiable(key),
        value = value == null ? null : List<int>.unmodifiable(value),
        headers = List<KafkaRecordHeader>.unmodifiable(headers);
}

/// CRC-32C Castagnoli (not the legacy Kafka message CRC-32).
int crc32c(List<int> bytes) {
  var crc = 0xffffffff;
  for (final byte in bytes) {
    crc ^= byte;
    for (var bit = 0; bit < 8; bit++) {
      crc = (crc >>> 1) ^ ((crc & 1) == 1 ? 0x82f63b78 : 0);
    }
  }
  return (crc ^ 0xffffffff) & 0xffffffff;
}

void _varBytes(KafkaBytesBuilder builder, List<int>? bytes) {
  builder.addVarInt(bytes?.length ?? -1);
  if (bytes != null) builder.addRaw(bytes);
}

List<int>? _readVarBytes(KafkaBytesReader reader) {
  final length = reader.readVarInt();
  if (length == -1) return null;
  if (length < -1) {
    throw const FormatException('Invalid nullable record length');
  }
  return reader.readRaw(length);
}

int _checkedAdd(int left, int right) {
  final result = BigInt.from(left) + BigInt.from(right);
  if (result < BigInt.from(-9223372036854775808) ||
      result > BigInt.from(9223372036854775807)) {
    throw const FormatException('Record offset or timestamp overflows int64');
  }
  return result.toInt();
}

/// Encodes an uncompressed, non-idempotent magic-2 producer batch.
/// Record offsets are assigned sequentially; the broker assigns the base offset.
Uint8List encodeRecordBatch(List<KafkaRecord> records) {
  if (records.isEmpty) {
    throw ArgumentError('A producer batch must not be empty');
  }
  final baseTimestamp = records.first.timestamp;
  var maxTimestamp = baseTimestamp;
  final encoded = KafkaBytesBuilder();
  for (var index = 0; index < records.length; index++) {
    final record = records[index];
    if (record.timestamp < 0) {
      throw ArgumentError('Timestamp must be nonnegative');
    }
    if (record.timestamp > maxTimestamp) maxTimestamp = record.timestamp;
    final body = KafkaBytesBuilder()
      ..addInt8(0)
      ..addVarLong(record.timestamp - baseTimestamp)
      ..addVarInt(index);
    _varBytes(body, record.key);
    _varBytes(body, record.value);
    body.addVarInt(record.headers.length);
    for (final header in record.headers) {
      final key = utf8.encode(header.key);
      body
        ..addVarInt(key.length)
        ..addRaw(key);
      _varBytes(body, header.value);
    }
    encoded
      ..addVarInt(body.length)
      ..addRaw(body.takeBytes());
  }
  final payload = KafkaBytesBuilder()
    ..addInt16(0)
    ..addInt32(records.length - 1)
    ..addInt64(baseTimestamp)
    ..addInt64(maxTimestamp)
    ..addInt64(-1)
    ..addInt16(-1)
    ..addInt32(-1)
    ..addInt32(records.length)
    ..addRaw(encoded.takeBytes());
  final bytes = payload.takeBytes();
  final checksum = ByteData(4)..setUint32(0, crc32c(bytes), Endian.big);
  return Uint8List.fromList((KafkaBytesBuilder()
        ..addInt64(0)
        ..addInt32(9 + bytes.length)
        ..addInt32(-1)
        ..addInt8(2)
        ..addRaw(checksum.buffer.asUint8List())
        ..addRaw(bytes))
      .takeBytes());
}

/// Decodes complete magic-2 batches, verifies CRCs, and omits control records.
/// Compression is deliberately unsupported and fails rather than losing data.
List<KafkaRecord> decodeRecordBatches(List<int> bytes) =>
    decodeRecordBatchesWithProgress(bytes).records;

/// Package-internal result including progress through omitted batch records.
class DecodedRecordBatches {
  final List<KafkaRecord> records;

  /// Inclusive final offset of the last fully validated batch, or null if empty.
  /// This is neither the high watermark nor the next offset to request.
  final int? lastBatchOffset;

  DecodedRecordBatches(this.records, this.lastBatchOffset);
}

/// Decodes records and preserves the last fully validated batch's log position.
/// Control and compacted batches can advance this position without visible data.
/// Batch order is preserved, without imposing cross-batch offset ordering.
DecodedRecordBatches decodeRecordBatchesWithProgress(List<int> bytes) =>
    _decodeRecordBatchesWithProgress(bytes, allowIncompleteTail: false);

/// Decodes a Fetch record set, ignoring a partial final batch only after at
/// least one complete batch has passed validation. Kafka's byte-limited Fetch
/// responses may cut the next batch short. A partial first batch still fails;
/// omitted bytes never contribute records or progress. Standalone decoding
/// through [decodeRecordBatchesWithProgress] remains strict.
DecodedRecordBatches decodeFetchRecordBatchesWithProgress(List<int> bytes) =>
    _decodeRecordBatchesWithProgress(bytes, allowIncompleteTail: true);

DecodedRecordBatches _decodeRecordBatchesWithProgress(List<int> bytes,
    {required bool allowIncompleteTail}) {
  final input = KafkaBytesReader.fromBytes(bytes);
  final result = <KafkaRecord>[];
  int? lastBatchOffset;
  while (input.isNotEOF) {
    final canIgnoreTail = allowIncompleteTail && lastBatchOffset != null;
    if (canIgnoreTail && input.remaining < 12) break;
    final baseOffset = input.readInt64();
    if (baseOffset < 0) {
      throw const FormatException('Negative record batch base offset');
    }
    final batchLength = input.readInt32();
    if (batchLength < 49) {
      throw const FormatException('Invalid record batch length');
    }
    if (canIgnoreTail && input.remaining < batchLength) {
      // Validate the magic byte when the partial header contains it, but never
      // interpret a partial payload, its CRC, or its claimed final offset.
      if (input.remaining >= 5 && input.peekNextBytes(5)[4] != 2) {
        throw const FormatException(
            'Only magic-2 record batches are supported');
      }
      break;
    }
    final batchBytes = input.readRaw(batchLength);
    final batch = KafkaBytesReader.fromBytes(batchBytes);
    batch.readInt32(); // Partition leader epoch.
    if (batch.readInt8() != 2) {
      throw const FormatException('Only magic-2 record batches are supported');
    }
    final checksum = batch.readInt32() & 0xffffffff;
    if (crc32c(batchBytes.sublist(9)) != checksum) {
      throw const FormatException('Record batch CRC-32C mismatch');
    }
    final attributes = batch.readInt16();
    if ((attributes & 7) != 0) {
      throw UnsupportedError('Compressed record batches are not supported');
    }
    final lastOffsetDelta = batch.readInt32();
    final baseTimestamp = batch.readInt64();
    final maxTimestamp = batch.readInt64();
    batch.readInt64(); // Producer id.
    batch.readInt16(); // Producer epoch.
    batch.readInt32(); // Base sequence.
    final count = batch.readInt32();
    if (count < 0 || count > batch.remaining || lastOffsetDelta < 0) {
      throw const FormatException('Invalid record count or offset delta');
    }
    var previousOffset = -1;
    for (var index = 0; index < count; index++) {
      final length = batch.readVarInt();
      if (length < 6) throw const FormatException('Invalid record length');
      final record = KafkaBytesReader.fromBytes(batch.readRaw(length));
      if (record.readInt8() != 0) {
        throw const FormatException('Invalid record attributes');
      }
      final timestampDelta = record.readVarLong();
      final offsetDelta = record.readVarInt();
      if (offsetDelta <= previousOffset || offsetDelta > lastOffsetDelta) {
        throw const FormatException('Invalid record offset delta');
      }
      previousOffset = offsetDelta;
      final key = _readVarBytes(record);
      final value = _readVarBytes(record);
      final headerCount = record.readVarInt();
      if (headerCount < 0 || headerCount > record.remaining ~/ 2) {
        throw const FormatException('Invalid record header count');
      }
      final headers = <KafkaRecordHeader>[];
      for (var h = 0; h < headerCount; h++) {
        final keyLength = record.readVarInt();
        if (keyLength < 0) throw const FormatException('Null header key');
        final headerKey = utf8.decode(record.readRaw(keyLength));
        headers.add(KafkaRecordHeader(headerKey, _readVarBytes(record)));
      }
      if (record.isNotEOF) throw const FormatException('Trailing record bytes');
      if ((attributes & 0x20) == 0) {
        result.add(KafkaRecord(
            key: key,
            value: value,
            timestamp: (attributes & 8) == 0
                ? _checkedAdd(baseTimestamp, timestampDelta)
                : maxTimestamp,
            offset: _checkedAdd(baseOffset, offsetDelta),
            headers: headers));
      }
    }
    if (batch.isNotEOF) {
      throw const FormatException('Trailing record batch bytes');
    }
    lastBatchOffset = _checkedAdd(baseOffset, lastOffsetDelta);
  }
  return DecodedRecordBatches(result, lastBatchOffset);
}
