import 'dart:convert';
import 'dart:typed_data';

enum KafkaType { int8, int16, int32, int64, string, bytes, object, boolean }

/// The `KafkaBytesReader` class provides a set of convenience methods for reading
/// Kafka-specific data types from a stream of bytes. It is primarily used for decoding
/// messages received from a Kafka broker, handling the intricacies of Kafka's binary
/// data format.
///
/// The class uses a `Uint8List` to represent the byte stream, providing efficient
/// access and manipulation of the underlying data. It maintains an internal offset
/// to keep track of the current read position within the byte stream, allowing
/// sequential reads of various data types without manual offset management.
class KafkaBytesReader {
  final Uint8List _data;
  int _offset = 0;

  int get remaining => _data.length - _offset;

  int readVarInt() => _readVarNumber(32);
  int readVarLong() => _readVarNumber(64);

  int _readVarNumber(int bits) {
    var encoded = BigInt.zero;
    final maxBytes = (bits + 6) ~/ 7;
    for (var i = 0; i < maxBytes; i++) {
      final byte = readInt8() & 255;
      final payload = byte & 127;
      if (i == maxBytes - 1 && payload >= (1 << (bits - 7 * i))) {
        throw const FormatException('Varint overflow');
      }
      encoded |= BigInt.from(payload) << (i * 7);
      if (byte & 128 == 0) {
        if (i > 0 && payload == 0) {
          throw const FormatException('Non-canonical varint');
        }
        return ((encoded >> 1) ^ -(encoded & BigInt.one)).toInt();
      }
    }
    throw const FormatException('Unterminated varint');
  }

  /// Returns the current position in the byte stream, indicating how many
  /// bytes have been read.
  int get offset => _offset;

  /// Returns the total length of the byte stream.
  int get length => _data.length;

  /// Checks if the end of the byte stream has been reached, useful for
  /// validating that all data has been read.
  bool get isEOF => _data.length == _offset;

  /// The inverse of `isEOF`, indicating if there are more bytes to read.
  bool get isNotEOF => !isEOF;

  /// Initializes a new instance with a list of bytes, typically obtained
  /// from Kafka message payloads.
  KafkaBytesReader.fromBytes(List<int> data) : _data = Uint8List.fromList(data);

  /// Ensures that there are at least [numBytes] bytes left to read in the buffer.
  void _ensureBytesAvailable(int numBytes) {
    if (numBytes < 0 || _offset + numBytes > _data.length) {
      throw FormatException(
          'Not enough data to read $numBytes bytes at offset $_offset');
    }
  }

  /// Reads a boolean value from the byte stream and advances the offset.
  bool readBoolean() {
    _ensureBytesAvailable(1);
    final value = _data.buffer.asByteData().getInt8(_offset);
    _offset += 1;
    if (value != 0 && value != 1) {
      throw FormatException('Invalid boolean: $value');
    }
    return value == 1;
  }

  /// Reads an 8-bit integer from the byte stream and advances the offset.
  int readInt8() {
    _ensureBytesAvailable(1);
    final value = _data.buffer.asByteData().getInt8(_offset);
    _offset += 1;
    return value;
  }

  /// Reads a 16-bit integer from the current position and advances the offset.
  int readInt16() {
    _ensureBytesAvailable(2);
    final value = _data.buffer.asByteData().getInt16(_offset, Endian.big);
    _offset += 2;
    return value;
  }

  /// Reads a 32-bit integer from the current position and advances the offset.
  int readInt32() {
    _ensureBytesAvailable(4);
    final value = _data.buffer.asByteData().getInt32(_offset, Endian.big);
    _offset += 4;
    return value;
  }

  /// Reads a 64-bit integer from the byte stream and advances the offset.
  int readInt64() {
    _ensureBytesAvailable(8);
    final value = _data.buffer.asByteData().getInt64(_offset, Endian.big);
    _offset += 8;
    return value;
  }

  /// Reads a Kafka-encoded string, which is prefixed with its length as a 16-bit
  /// integer, and decodes it using UTF-8. Returns `null` if the length is -1,
  /// following Kafka's convention for null strings. Advances the offset by the
  /// length of the string.
  String? readString() {
    final length = readInt16();
    if (length == -1) {
      return null;
    }
    if (length < -1 || length > 32767) {
      throw FormatException(
          'Invalid string length: $length at offset $_offset');
    }
    _ensureBytesAvailable(length);
    final bytes = _data.sublist(_offset, _offset + length);
    _offset += length;
    return utf8.decode(bytes);
  }

  /// Reads a byte array prefixed with its length as a 32-bit integer. Returns `null`
  /// if the length is -1, following Kafka's convention for null values. Advances the offset
  /// by the length of the array.
  List<int>? readBytes() {
    final length = readInt32();
    if (length == -1) return null;
    if (length < -1 || length > 2147483647) {
      throw FormatException('Invalid bytes length: $length at offset $_offset');
    }
    _ensureBytesAvailable(length);
    final bytes = _data.sublist(_offset, _offset + length);
    _offset += length;
    return bytes;
  }

  /// Reads an array of items, where the array length is a 32-bit integer followed by the
  /// items themselves. Each item is read using a provided reader function. Returns `null`
  /// if the array length is -1, following Kafka's convention for null arrays. Advances the offset
  /// by the total length of the array.
  List<dynamic>? readArray(
    KafkaType itemType, [
    dynamic Function(KafkaBytesReader reader)? objectReadHandler,
  ]) {
    var length = readInt32();
    if (length == -1) {
      return null; // Return null if the array length is -1
    }
    if (length < -1 || length > 2147483647) {
      throw FormatException('Invalid array length: $length at offset $_offset');
    }
    if (length > remaining) {
      throw const FormatException('Array count exceeds remaining bytes');
    }
    var items = <dynamic>[];
    for (var i = 0; i < length; i++) {
      switch (itemType) {
        case KafkaType.int8:
          items.add(readInt8());
          break;
        case KafkaType.int16:
          items.add(readInt16());
          break;
        case KafkaType.int32:
          items.add(readInt32());
          break;
        case KafkaType.int64:
          items.add(readInt64());
          break;
        case KafkaType.string:
          items.add(readString());
          break;
        case KafkaType.bytes:
          items.add(readBytes());
          break;
        case KafkaType.boolean:
          items.add(readBoolean());
          break;
        case KafkaType.object:
          if (objectReadHandler == null) {
            throw StateError('ObjectReadHandler must be provided');
          }
          items.add(objectReadHandler(this));
          break;
      }
    }
    return items;
  }

  /// Reads a raw byte array of a specified length without any length prefix or special
  /// encoding, useful for custom or non-standard data formats. Advances the offset by the
  /// length of the array.
  List<int> readRaw(int length) {
    if (length < 0) {
      throw FormatException('Invalid raw length: $length at offset $_offset');
    }
    _ensureBytesAvailable(length);
    final bytes = _data.sublist(_offset, _offset + length);
    _offset += length;
    return bytes;
  }

  /// Peeks at the next [count] bytes without advancing the offset.
  List<int> peekNextBytes(int count) {
    if (count < 0) {
      throw FormatException('Invalid peek count: $count at offset $_offset');
    }
    _ensureBytesAvailable(count);
    return _data.sublist(_offset, _offset + count);
  }
}
