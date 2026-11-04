import 'dart:convert';
import 'dart:typed_data';

enum KafkaType { int8, int16, int32, int64, string, bytes, object, boolean }

/// Builder for creating binary data conforming to the Kafka protocol.
class KafkaBytesBuilder {
  final BytesBuilder _builder = BytesBuilder();

  /// Gets the current length of the binary data being built.
  int get length => _builder.length;

  /// Creates a new KafkaBytesBuilder with an empty buffer.
  KafkaBytesBuilder();

  /// Creates a new KafkaBytesBuilder and initializes the buffer with a proper request header.
  ///
  /// The request header includes API key, API version, correlation ID, and a string identifier.
  KafkaBytesBuilder.withRequestHeader(
    int apiKey,
    int apiVersion,
    int correlationId, {
    String clientId = "dart_kafka",
  }) {
    addInt16(apiKey);
    addInt16(apiVersion);
    addInt32(correlationId);
    addString(clientId);
  }

  /// Adds an 8-bit integer to the buffer.
  void addInt8(int value) {
    if (value < -128 || value > 127) {
      throw ArgumentError('Value out of range for int8: $value');
    }
    final data = ByteData(1);
    data.setInt8(0, value);
    _add(data);
  }

  /// Adds a 16-bit integer to the buffer.
  void addInt16(int value) {
    if (value < -32768 || value > 32767) {
      throw ArgumentError('Value out of range for int16: $value');
    }
    final data = ByteData(2);
    data.setInt16(
        0, value, Endian.big); // Use Endian.big for network byte order
    _add(data);
  }

  /// Adds a 32-bit integer to the buffer.
  void addInt32(int value) {
    if (value < -2147483648 || value > 2147483647) {
      throw ArgumentError('Value out of range for int32: $value');
    }
    final data = ByteData(4);
    data.setInt32(
        0, value, Endian.big); // Use Endian.big for network byte order
    _add(data);
  }

  /// Adds a 64-bit integer to the buffer.
  void addInt64(int value) {
    const minInt64 = -9223372036854775808;
    const maxInt64 = 9223372036854775807;
    if (value < minInt64 || value > maxInt64) {
      throw ArgumentError('Value out of range for int64: $value');
    }
    final data = ByteData(8);
    data.setInt64(
        0, value, Endian.big); // Use Endian.big for network byte order
    _add(data);
  }

  /// Adds a boolean value to the buffer.
  void addBoolean(bool value) {
    addInt8(value ? 1 : 0); // Encode boolean as 1 for true, 0 for false
  }

  /// Adds a Kafka string to the builder.
  ///
  /// Kafka strings start with a 16-bit length followed by the actual string value.
  /// A null string is represented with a length of -1.
  void addString(String? value) {
    if (value == null) {
      addInt16(-1);
    } else {
      final data = utf8.encode(value);
      if (data.length > 32767) {
        throw ArgumentError('String is too long: ${data.length} bytes');
      }
      addInt16(data.length);
      _builder.add(data);
    }
  }

  /// Adds a Kafka array to the builder.
  ///
  /// Kafka arrays start with a 32-bit size followed by array items encoded according to their [KafkaType].
  /// A null array is represented with a size of -1.
  void addArray(Iterable? items, KafkaType itemType, [Function? serialize]) {
    if (items == null) {
      addInt32(-1); // Null array
      return;
    }
    addInt32(items.length);
    for (final item in items) {
      switch (itemType) {
        case KafkaType.int8:
          if (item is int) {
            addInt8(item);
          } else {
            throw ArgumentError(
                'Expected int for KafkaType.int8, got ${item.runtimeType}');
          }
          break;
        case KafkaType.int16:
          if (item is int) {
            addInt16(item);
          } else {
            throw ArgumentError(
                'Expected int for KafkaType.int16, got ${item.runtimeType}');
          }
          break;
        case KafkaType.int32:
          if (item is int) {
            addInt32(item);
          } else {
            throw ArgumentError(
                'Expected int for KafkaType.int32, got ${item.runtimeType}');
          }
          break;
        case KafkaType.int64:
          if (item is int) {
            addInt64(item);
          } else {
            throw ArgumentError(
                'Expected int for KafkaType.int64, got ${item.runtimeType}');
          }
          break;
        case KafkaType.string:
          if (item is String? || item == null) {
            addString(item as String?);
          } else {
            throw ArgumentError(
                'Expected String for KafkaType.string, got ${item.runtimeType}');
          }
          break;
        case KafkaType.bytes:
          if (item is List<int>? || item == null) {
            addBytes(item as List<int>?);
          } else {
            throw ArgumentError(
                'Expected List<int> for KafkaType.bytes, got ${item.runtimeType}');
          }
          break;
        case KafkaType.boolean:
          if (item is bool) {
            addBoolean(item);
          } else {
            throw ArgumentError(
                'Expected bool for KafkaType.boolean, got ${item.runtimeType}');
          }
          break;
        case KafkaType.object:
          if (serialize != null) {
            // Ensure the serialize function returns a List<int> and serialize the object
            List<int> serializedObject = serialize(item) as List<int>;
            addRaw(serializedObject);
          } else {
            throw ArgumentError(
                'Serialization function must be provided for KafkaType.object');
          }
          break;
      }
    }
  }

  /// Adds a value of Kafka-specific Bytes type to the builder.
  ///
  /// Kafka Bytes type starts with a 32-bit size followed by actual value bytes.
  /// A null value is represented with a size of -1.
  void addBytes(List<int>? value) {
    if (value == null) {
      addInt32(-1);
    } else {
      if (value.length > 2147483647) {
        throw ArgumentError('Byte array is too long: ${value.length} bytes');
      }
      addInt32(value.length);
      addRaw(value);
    }
  }

  /// Adds arbitrary raw data to the buffer.
  void addRaw(List<int> data) {
    if (data.any((byte) => byte < 0 || byte > 255)) {
      throw ArgumentError('Raw bytes must be in the range 0..255');
    }
    _builder.add(Uint8List.fromList(data));
  }

  /// Encodes a signed 32-bit integer using Kafka zigzag varint encoding.
  void addVarInt(int value) {
    if (value < -2147483648 || value > 2147483647) {
      throw ArgumentError.value(value, 'value', 'outside int32');
    }
    addVarLong(value);
  }

  /// Encodes a signed 64-bit integer using Kafka zigzag varlong encoding.
  void addVarLong(int value) {
    final v = BigInt.from(value);
    var encoded = (v << 1) ^ (v >> 63);
    final mask = BigInt.from(127);
    while (encoded > mask) {
      _builder.addByte((encoded & mask).toInt() | 128);
      encoded >>= 7;
    }
    _builder.addByte(encoded.toInt());
  }

  // Internal method to add ByteData to the buffer.
  void _add(ByteData data) {
    _builder.add(Uint8List.sublistView(data.buffer.asUint8List()));
  }

  /// Takes the binary data from the builder and returns it as a list of bytes.
  List<int> takeBytes() => _builder.takeBytes();

  /// Converts the binary data from the builder to a list of bytes.
  List<int> toBytes() => _builder.toBytes();
}
