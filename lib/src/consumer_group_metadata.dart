import 'dart:convert';
import 'dart:typed_data';

/// Immutable classic consumer subscription metadata, including its INT16 version.
///
/// This codec does not implement partition assignment or consumer lifecycle.
/// Only versions 0 through 3 are supported; unknown versions are rejected.
class ConsumerGroupSubscription {
  final int version;
  final List<String> topics;
  final List<int>? userData;
  final Map<String, List<int>> ownedPartitions;
  final int generationId;
  final String? rackId;

  ConsumerGroupSubscription(List<String> topics,
      {this.version = 0,
      List<int>? userData,
      Map<String, List<int>> ownedPartitions = const {},
      this.generationId = -1,
      this.rackId})
      : topics = _topics(topics),
        userData = _bytes(userData),
        ownedPartitions = _partitions(ownedPartitions) {
    _version(version);
    _int32(generationId, 'generationId');
    if (rackId != null) _string(rackId!, 'rackId');
    if (version < 1 && this.ownedPartitions.isNotEmpty ||
        version < 2 && generationId != -1 ||
        version < 3 && rackId != null) {
      throw ArgumentError(
          'Fields are not supported by subscription version $version');
    }
  }

  factory ConsumerGroupSubscription.fromBytes(List<int> bytes) {
    final reader = _Reader(bytes);
    final version = reader.version();
    final topics = <String>[];
    final count = reader.count(3);
    for (var i = 0; i < count; i++) {
      topics.add(reader.string()!);
    }
    final userData = reader.bytes();
    final owned = version >= 1 ? reader.partitions() : <String, List<int>>{};
    final generation = version >= 2 ? reader.int32() : -1;
    final rack = version >= 3 ? reader.string(nullable: true) : null;
    reader.end();
    try {
      return ConsumerGroupSubscription(topics,
          version: version,
          userData: userData,
          ownedPartitions: owned,
          generationId: generation,
          rackId: rack);
    } on ArgumentError catch (error) {
      throw FormatException('Invalid consumer subscription: $error');
    }
  }

  Uint8List toBytes() {
    final writer = _Writer()..int16(version);
    writer.int32(topics.length);
    for (final topic in topics) {
      writer.string(topic);
    }
    writer.bytes(userData);
    if (version >= 1) writer.partitions(ownedPartitions);
    if (version >= 2) writer.int32(generationId);
    if (version >= 3) writer.string(rackId);
    return writer.result();
  }
}

/// Immutable classic consumer assignment metadata, including its INT16 version.
///
/// Versions 0 through 3 have the same assignment body. This class only encodes
/// an assignment supplied by the caller; it does not choose partition owners.
class ConsumerGroupAssignment {
  final int version;
  final Map<String, List<int>> partitions;
  final List<int>? userData;

  ConsumerGroupAssignment(Map<String, List<int>> partitions,
      {this.version = 0, List<int>? userData})
      : partitions = _partitions(partitions),
        userData = _bytes(userData) {
    _version(version);
  }

  factory ConsumerGroupAssignment.fromBytes(List<int> bytes) {
    final reader = _Reader(bytes);
    final version = reader.version();
    final partitions = reader.partitions();
    final userData = reader.bytes();
    reader.end();
    try {
      return ConsumerGroupAssignment(partitions,
          version: version, userData: userData);
    } on ArgumentError catch (error) {
      throw FormatException('Invalid consumer assignment: $error');
    }
  }

  Uint8List toBytes() {
    final writer = _Writer()
      ..int16(version)
      ..partitions(partitions)
      ..bytes(userData);
    return writer.result();
  }
}

void _version(int value) {
  if (value < 0 || value > 3) {
    throw ArgumentError.value(value, 'version', 'Expected version 0 through 3');
  }
}

void _int32(int value, String name) {
  if (value < -2147483648 || value > 2147483647) {
    throw ArgumentError.value(value, name, 'Outside INT32 range');
  }
}

void _string(String value, String name, {bool topic = false}) {
  if (topic && value.isEmpty) throw ArgumentError('$name must not be empty');
  // Dart's UTF-8 encoder replaces unpaired surrogates. Reject them rather than
  // silently changing caller-supplied metadata during encoding.
  final units = value.codeUnits;
  for (var i = 0; i < units.length; i++) {
    final unit = units[i];
    if (unit >= 0xd800 && unit <= 0xdbff) {
      if (++i == units.length || units[i] < 0xdc00 || units[i] > 0xdfff) {
        throw ArgumentError('$name contains an unpaired surrogate');
      }
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw ArgumentError('$name contains an unpaired surrogate');
    }
  }
  if (utf8.encode(value).length > 32767) {
    throw ArgumentError('$name exceeds the INT16 UTF-8 length limit');
  }
}

List<String> _topics(List<String> values) {
  final copy = List<String>.unmodifiable(values);
  final seen = <String>{};
  for (final value in copy) {
    _string(value, 'topic', topic: true);
    if (!seen.add(value)) throw ArgumentError('Duplicate topic: $value');
  }
  _int32(copy.length, 'topics length');
  return copy;
}

List<int>? _bytes(List<int>? values) {
  if (values == null) return null;
  final copy = List<int>.unmodifiable(values);
  _int32(copy.length, 'userData length');
  if (copy.any((value) => value < 0 || value > 255)) {
    throw ArgumentError('User data must contain bytes in 0 through 255');
  }
  return copy;
}

Map<String, List<int>> _partitions(Map<String, List<int>> values) {
  final copy = <String, List<int>>{};
  _int32(values.length, 'topics length');
  for (final entry in values.entries) {
    _string(entry.key, 'topic', topic: true);
    final ids = List<int>.unmodifiable(entry.value);
    _int32(ids.length, 'partitions length');
    final seen = <int>{};
    for (final id in ids) {
      if (id < 0 || id > 2147483647 || !seen.add(id)) {
        throw ArgumentError(
            'Invalid or duplicate partition $id for ${entry.key}');
      }
    }
    copy[entry.key] = ids;
  }
  return Map<String, List<int>>.unmodifiable(copy);
}

class _Reader {
  final Uint8List data;
  int position = 0;

  _Reader(List<int> bytes) : data = _input(bytes);

  static Uint8List _input(List<int> bytes) {
    if (bytes.any((byte) => byte < 0 || byte > 255)) {
      throw const FormatException('Input must contain bytes in 0 through 255');
    }
    return Uint8List.fromList(bytes);
  }

  void need(int length) {
    if (length < 0 || length > data.length - position) {
      throw const FormatException('Truncated consumer metadata');
    }
  }

  int int16() {
    need(2);
    final value = ByteData.sublistView(data).getInt16(position);
    position += 2;
    return value;
  }

  int int32() {
    need(4);
    final value = ByteData.sublistView(data).getInt32(position);
    position += 4;
    return value;
  }

  int version() {
    final value = int16();
    if (value < 0 || value > 3) {
      throw FormatException('Unsupported consumer metadata version $value');
    }
    return value;
  }

  int count(int minimumElementBytes) {
    final value = int32();
    if (value < 0 || value > (data.length - position) ~/ minimumElementBytes) {
      throw const FormatException('Invalid or impossible array count');
    }
    return value;
  }

  String? string({bool nullable = false}) {
    final length = int16();
    if (length == -1 && nullable) return null;
    need(length);
    final value = utf8.decode(data.sublist(position, position + length));
    position += length;
    return value;
  }

  List<int>? bytes() {
    final length = int32();
    if (length == -1) return null;
    need(length);
    final value = data.sublist(position, position + length);
    position += length;
    return value;
  }

  Map<String, List<int>> partitions() {
    final result = <String, List<int>>{};
    final topics = count(7);
    for (var i = 0; i < topics; i++) {
      final topic = string()!;
      if (result.containsKey(topic)) {
        throw FormatException('Duplicate topic: $topic');
      }
      final ids = <int>[];
      final length = count(4);
      for (var j = 0; j < length; j++) {
        ids.add(int32());
      }
      result[topic] = ids;
    }
    return result;
  }

  void end() {
    if (position != data.length) {
      throw const FormatException('Trailing consumer metadata bytes');
    }
  }
}

class _Writer {
  final BytesBuilder builder = BytesBuilder(copy: false);

  void int16(int value) {
    final bytes = ByteData(2)..setInt16(0, value);
    builder.add(bytes.buffer.asUint8List());
  }

  void int32(int value) {
    final bytes = ByteData(4)..setInt32(0, value);
    builder.add(bytes.buffer.asUint8List());
  }

  void string(String? value) {
    if (value == null) {
      int16(-1);
    } else {
      final bytes = utf8.encode(value);
      int16(bytes.length);
      builder.add(bytes);
    }
  }

  void bytes(List<int>? value) {
    int32(value?.length ?? -1);
    if (value != null) builder.add(value);
  }

  void partitions(Map<String, List<int>> values) {
    int32(values.length);
    for (final entry in values.entries) {
      string(entry.key);
      int32(entry.value.length);
      for (final id in entry.value) {
        int32(id);
      }
    }
  }

  Uint8List result() => builder.takeBytes();
}
