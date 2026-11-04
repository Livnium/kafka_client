import 'dart:convert';

import 'api/abstract/kafka_request.dart';
import 'api/abstract/kafka_response.dart';
import 'codec/byte_builder.dart';
import 'codec/byte_reader.dart' hide KafkaType;

// Schemas verified against Apache Kafka 4.0.2 message JSON:
// FindCoordinator v2, OffsetFetch v5/v7, and OffsetCommit v6.
void _validateString(String value, String name, {bool allowEmpty = false}) {
  final encoded = utf8.encode(value);
  if ((!allowEmpty && value.isEmpty) ||
      encoded.length > 32767 ||
      utf8.decode(encoded) != value) {
    throw ArgumentError('Invalid $name');
  }
}

void _validatePartition(int index) {
  if (index < 0 || index > 2147483647) {
    throw ArgumentError('Partition index must be in 0..2147483647');
  }
}

KafkaBytesReader _reader(List<int> bytes) {
  if (bytes.any((byte) => byte < 0 || byte > 255)) {
    throw const FormatException('Invalid response bytes');
  }
  return KafkaBytesReader.fromBytes(bytes);
}

String _requiredString(KafkaBytesReader reader) {
  final value = reader.readString();
  if (value == null) throw const FormatException('Unexpected null string');
  return value;
}

int _count(KafkaBytesReader reader, int minimumSize) {
  final count = reader.readInt32();
  if (count < 0 || count > reader.remaining ~/ minimumSize) {
    throw const FormatException('Invalid response array length');
  }
  return count;
}

int _partition(KafkaBytesReader reader) {
  final index = reader.readInt32();
  if (index < 0) throw const FormatException('Negative partition index');
  return index;
}

void _end(KafkaBytesReader reader) {
  if (!reader.isEOF) throw const FormatException('Trailing response bytes');
}

// Flexible OffsetFetch uses unsigned varints, not record-batch zigzag varints.
int _unsignedVarInt(KafkaBytesReader r) {
  var value = 0;
  for (var i = 0; i < 5; i++) {
    final byte = r.readInt8() & 255;
    final payload = byte & 127;
    if (i == 4 && payload > 15) {
      throw const FormatException('Unsigned varint overflow');
    }
    value |= payload << (7 * i);
    if (byte & 128 == 0) {
      if (i > 0 && payload == 0) {
        throw const FormatException('Non-canonical unsigned varint');
      }
      return value;
    }
  }
  throw const FormatException('Unterminated unsigned varint');
}

void _addUnsignedVarInt(KafkaBytesBuilder b, int value) {
  if (value < 0 || value > 0xffffffff) {
    throw ArgumentError('Unsigned varint out of range');
  }
  while (value > 127) {
    b.addRaw([(value & 127) | 128]);
    value >>= 7;
  }
  b.addInt8(value);
}

void _addCompactString(KafkaBytesBuilder b, String value) {
  final bytes = utf8.encode(value);
  _addUnsignedVarInt(b, bytes.length + 1);
  b.addRaw(bytes);
}

String? _compactString(KafkaBytesReader r, {bool nullable = false}) {
  final size = _unsignedVarInt(r);
  if (size == 0) {
    if (nullable) return null;
    throw const FormatException('Unexpected null compact string');
  }
  return utf8.decode(r.readRaw(size - 1));
}

int _compactCount(KafkaBytesReader r, int minimumSize) {
  final count = _unsignedVarInt(r) - 1;
  if (count < 0 || count > r.remaining ~/ minimumSize) {
    throw const FormatException('Invalid compact array length');
  }
  return count;
}

void _tags(KafkaBytesReader r) {
  final count = _unsignedVarInt(r);
  if (count > r.remaining ~/ 2) {
    throw const FormatException('Invalid tagged field count');
  }
  var previous = -1;
  for (var i = 0; i < count; i++) {
    final tag = _unsignedVarInt(r);
    if (tag <= previous) {
      throw const FormatException('Tagged fields must be strictly increasing');
    }
    previous = tag;
    r.readRaw(_unsignedVarInt(r));
  }
}

T _matching<T extends KafkaResponse>(T response, int correlationId) {
  if (response.correlationId != correlationId) {
    throw const FormatException('Mismatched response correlation ID');
  }
  return response;
}

/// Discovers the coordinator of a consumer group without joining it.
class FindCoordinatorRequest extends KafkaRequest {
  final int apiKey = 10;
  final int apiVersion = 2;
  final String groupId;

  FindCoordinatorRequest(this.groupId) {
    _validateString(groupId, 'groupId');
  }

  @override
  List<int> toBytes() => KafkaRequest.frame(
      (KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
            ..addString(groupId)
            ..addInt8(0))
          .takeBytes());

  @override
  FindCoordinatorResponse parseResponse(List<int> bytes) =>
      _matching(FindCoordinatorResponse.fromBytes(bytes), correlationId);
}

/// The non-flexible FindCoordinator v2 response.
class FindCoordinatorResponse extends KafkaResponse {
  final int throttleTimeMs;
  final int errorCode;
  final String? errorMessage;
  final int nodeId;
  final String host;
  final int port;

  FindCoordinatorResponse(super.correlationId, this.throttleTimeMs,
      this.errorCode, this.errorMessage, this.nodeId, this.host, this.port);

  factory FindCoordinatorResponse.fromBytes(List<int> bytes) {
    final r = _reader(bytes);
    final result = FindCoordinatorResponse(
        r.readInt32(),
        r.readInt32(),
        r.readInt16(),
        r.readString(),
        r.readInt32(),
        _requiredString(r),
        r.readInt32());
    _end(r);
    return result;
  }

  @override
  Map<String, dynamic> toJson() => {
        'correlationId': correlationId,
        'throttleTimeMs': throttleTimeMs,
        'errorCode': errorCode,
        'errorMessage': errorMessage,
        'nodeId': nodeId,
        'host': host,
        'port': port,
      };
}

/// Reads committed next offsets for explicit partitions of one topic.
class GroupOffsetFetchRequest extends KafkaRequest {
  final int apiKey = 9;
  final int apiVersion;
  final bool requireStable;
  final String groupId;
  final String topic;
  final Set<int> partitions;

  GroupOffsetFetchRequest(this.groupId, this.topic, Set<int> partitions,
      {this.requireStable = false})
      : apiVersion = requireStable ? 7 : 5,
        partitions = Set.unmodifiable(partitions) {
    _validateString(groupId, 'groupId');
    _validateString(topic, 'topic');
    if (this.partitions.isEmpty) {
      throw ArgumentError('Partitions must not be empty');
    }
    for (final index in this.partitions) {
      _validatePartition(index);
    }
  }

  @override
  List<int> toBytes() {
    final b =
        KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId);
    if (apiVersion == 7) {
      // Header v2 retains a normal nullable client ID string, then tags.
      b.addInt8(0);
      _addCompactString(b, groupId);
      _addUnsignedVarInt(b, 2);
      _addCompactString(b, topic);
      _addUnsignedVarInt(b, partitions.length + 1);
      for (final index in partitions) {
        b.addInt32(index);
      }
      b
        ..addInt8(0) // Topic tags.
        ..addBoolean(requireStable)
        ..addInt8(0); // Request tags.
    } else {
      b
        ..addString(groupId)
        ..addInt32(1)
        ..addString(topic)
        ..addInt32(partitions.length);
      for (final index in partitions) {
        b.addInt32(index);
      }
    }
    return KafkaRequest.frame(b.takeBytes());
  }

  @override
  GroupOffsetFetchResponse parseResponse(List<int> bytes) => _matching(
      GroupOffsetFetchResponse.fromBytes(bytes, apiVersion: apiVersion),
      correlationId);
}

/// OffsetFetch v5 or flexible v7 response, including its group-level error.
class GroupOffsetFetchResponse extends KafkaResponse {
  final int throttleTimeMs;
  final List<GroupOffsetFetchTopicResponse> topics;
  final int errorCode;

  GroupOffsetFetchResponse(super.correlationId, this.throttleTimeMs,
      List<GroupOffsetFetchTopicResponse> topics, this.errorCode)
      : topics = List.unmodifiable(topics);

  factory GroupOffsetFetchResponse.fromBytes(List<int> bytes,
      {int apiVersion = 5}) {
    if (apiVersion != 5 && apiVersion != 7) {
      throw ArgumentError('Only OffsetFetch v5 and v7 are supported');
    }
    final flexible = apiVersion == 7;
    final r = _reader(bytes);
    final id = r.readInt32();
    if (flexible) _tags(r); // Response header v1.
    final throttle = r.readInt32();
    final count = flexible ? _compactCount(r, 3) : _count(r, 6);
    final topics = <GroupOffsetFetchTopicResponse>[];
    for (var i = 0; i < count; i++) {
      final name = flexible ? _compactString(r)! : _requiredString(r);
      final partitionCount = flexible ? _compactCount(r, 20) : _count(r, 20);
      final partitions = <GroupOffsetFetchPartitionResponse>[];
      for (var j = 0; j < partitionCount; j++) {
        final index = _partition(r);
        final offset = r.readInt64();
        final epoch = r.readInt32();
        if (offset < -1 || epoch < -1) {
          throw const FormatException(
              'Invalid committed offset or leader epoch');
        }
        final metadata =
            flexible ? _compactString(r, nullable: true) : r.readString();
        partitions.add(GroupOffsetFetchPartitionResponse(
            index, offset, epoch, metadata, r.readInt16()));
        if (flexible) _tags(r);
      }
      if (flexible) _tags(r);
      topics.add(GroupOffsetFetchTopicResponse(name, partitions));
    }
    final error = r.readInt16();
    if (flexible) _tags(r);
    _end(r);
    return GroupOffsetFetchResponse(id, throttle, topics, error);
  }

  @override
  Map<String, dynamic> toJson() => {
        'correlationId': correlationId,
        'throttleTimeMs': throttleTimeMs,
        'topics': topics.map((topic) => topic.toJson()).toList(),
        'errorCode': errorCode,
      };
}

class GroupOffsetFetchTopicResponse {
  final String name;
  final List<GroupOffsetFetchPartitionResponse> partitions;

  GroupOffsetFetchTopicResponse(
      this.name, List<GroupOffsetFetchPartitionResponse> partitions)
      : partitions = List.unmodifiable(partitions);

  Map<String, dynamic> toJson() => {
        'name': name,
        'partitions':
            partitions.map((partition) => partition.toJson()).toList(),
      };
}

class GroupOffsetFetchPartitionResponse {
  final int partitionIndex;

  /// Next offset to read, or -1 when no committed offset exists.
  final int committedOffset;
  final int committedLeaderEpoch;
  final String? metadata;
  final int errorCode;

  GroupOffsetFetchPartitionResponse(this.partitionIndex, this.committedOffset,
      this.committedLeaderEpoch, this.metadata, this.errorCode);

  Map<String, dynamic> toJson() => {
        'partitionIndex': partitionIndex,
        'committedOffset': committedOffset,
        'committedLeaderEpoch': committedLeaderEpoch,
        'metadata': metadata,
        'errorCode': errorCode,
      };
}

/// Manually commits next offsets without group membership or automatic commits.
class GroupOffsetCommitRequest extends KafkaRequest {
  final int apiKey = 8;
  final int apiVersion = 6;
  final String groupId;
  final String topic;
  final Map<int, int> nextOffsets;
  final String? metadata;

  /// -1 for a manual commit; otherwise a caller-supplied classic generation.
  final int generationId;

  /// Empty for a manual commit; not a locally managed membership lease.
  final String memberId;

  GroupOffsetCommitRequest(
      String groupId, String topic, Map<int, int> nextOffsets,
      {String? metadata})
      : this._(groupId, topic, nextOffsets, -1, '', metadata: metadata);

  GroupOffsetCommitRequest._(this.groupId, this.topic,
      Map<int, int> nextOffsets, this.generationId, this.memberId,
      {this.metadata})
      : nextOffsets = Map.unmodifiable(nextOffsets) {
    _validateString(groupId, 'groupId');
    _validateString(topic, 'topic');
    if (metadata != null) {
      _validateString(metadata!, 'metadata', allowEmpty: true);
    }
    if (this.nextOffsets.isEmpty) {
      throw ArgumentError('Offsets must not be empty');
    }
    for (final entry in this.nextOffsets.entries) {
      _validatePartition(entry.key);
      if (entry.value < 0 || entry.value > 9223372036854775807) {
        throw ArgumentError('Next offset must be in 0..9223372036854775807');
      }
    }
  }

  @override
  List<int> toBytes() {
    final b =
        KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
          ..addString(groupId)
          ..addInt32(generationId)
          ..addString(memberId)
          ..addInt32(1)
          ..addString(topic)
          ..addInt32(nextOffsets.length);
    for (final entry in nextOffsets.entries) {
      b
        ..addInt32(entry.key)
        ..addInt64(entry.value)
        ..addInt32(-1)
        ..addString(metadata);
    }
    return KafkaRequest.frame(b.takeBytes());
  }

  @override
  GroupOffsetCommitResponse parseResponse(List<int> bytes) =>
      _matching(GroupOffsetCommitResponse.fromBytes(bytes), correlationId);
}

/// An explicit OffsetCommit v6 carrying a classic member and generation.
///
/// This advanced request does not join, heartbeat, assign partitions, track a
/// live lease, retry, or fall back to a manual commit. The caller must discover
/// the coordinator, check its supported API versions, and inspect every returned
/// partition error. Responses must match the requested topic and every partition
/// exactly; missing, duplicate or unexpected entries are malformed outcomes.
/// A valid generation/member pair does not prove ownership of
/// the partitions being committed, enforce monotonic progress, or make
/// application effects atomic. Commit only completed application processing.
///
/// A lost response can leave an unknown outcome. Closing a socket cannot undo a
/// commit or prove that the member left. Static membership is not supported by
/// this protocol version. Existing [GroupOffsetCommitRequest] and high-level
/// manual commit behavior remain generation -1 with an empty member ID.
class MemberOffsetCommitRequest extends GroupOffsetCommitRequest {
  MemberOffsetCommitRequest(
      String groupId, String topic, Map<int, int> nextOffsets,
      {required int generationId, required String memberId, String? metadata})
      : super._(groupId, topic, nextOffsets, generationId, memberId,
            metadata: metadata) {
    if (generationId < 0 || generationId > 2147483647) {
      throw ArgumentError('Generation ID must be in 0..2147483647');
    }
    _validateString(memberId, 'memberId');
  }

  @override
  GroupOffsetCommitResponse parseResponse(List<int> bytes) {
    final response = super.parseResponse(bytes);
    if (response.topics.length != 1 ||
        response.topics.single.name != topic ||
        response.topics.single.partitions.length != nextOffsets.length) {
      throw const FormatException(
          'Member OffsetCommit response does not match request');
    }
    final seen = <int>{};
    for (final partition in response.topics.single.partitions) {
      if (!nextOffsets.containsKey(partition.partitionIndex) ||
          !seen.add(partition.partitionIndex)) {
        throw const FormatException(
            'Unexpected or duplicate member commit partition');
      }
    }
    return response;
  }
}

/// The non-flexible OffsetCommit v6 response, with per-partition errors.
class GroupOffsetCommitResponse extends KafkaResponse {
  final int throttleTimeMs;
  final List<GroupOffsetCommitTopicResponse> topics;

  GroupOffsetCommitResponse(super.correlationId, this.throttleTimeMs,
      List<GroupOffsetCommitTopicResponse> topics)
      : topics = List.unmodifiable(topics);

  factory GroupOffsetCommitResponse.fromBytes(List<int> bytes) {
    final r = _reader(bytes);
    final id = r.readInt32();
    final throttle = r.readInt32();
    final count = _count(r, 6);
    final topics = <GroupOffsetCommitTopicResponse>[];
    for (var i = 0; i < count; i++) {
      final name = _requiredString(r);
      final partitionCount = _count(r, 6);
      final partitions = <GroupOffsetCommitPartitionResponse>[];
      for (var j = 0; j < partitionCount; j++) {
        partitions.add(
            GroupOffsetCommitPartitionResponse(_partition(r), r.readInt16()));
      }
      topics.add(GroupOffsetCommitTopicResponse(name, partitions));
    }
    _end(r);
    return GroupOffsetCommitResponse(id, throttle, topics);
  }

  @override
  Map<String, dynamic> toJson() => {
        'correlationId': correlationId,
        'throttleTimeMs': throttleTimeMs,
        'topics': topics.map((topic) => topic.toJson()).toList(),
      };
}

class GroupOffsetCommitTopicResponse {
  final String name;
  final List<GroupOffsetCommitPartitionResponse> partitions;

  GroupOffsetCommitTopicResponse(
      this.name, List<GroupOffsetCommitPartitionResponse> partitions)
      : partitions = List.unmodifiable(partitions);

  Map<String, dynamic> toJson() => {
        'name': name,
        'partitions':
            partitions.map((partition) => partition.toJson()).toList(),
      };
}

class GroupOffsetCommitPartitionResponse {
  final int partitionIndex;
  final int errorCode;

  GroupOffsetCommitPartitionResponse(this.partitionIndex, this.errorCode);

  Map<String, dynamic> toJson() => {
        'partitionIndex': partitionIndex,
        'errorCode': errorCode,
      };
}
