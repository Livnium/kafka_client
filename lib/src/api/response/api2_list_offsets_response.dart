import '../../codec/byte_reader.dart';
import '../abstract/kafka_response.dart';

/// Immutable ListOffsets v2 results, including raw per-partition Kafka errors.
///
/// The codec retains the -1 unavailable offset/timestamp sentinel. A high-level
/// earliest/latest lookup rejects a successful unavailable offset. Timestamps
/// for earliest/latest normally remain -1. No leader epochs are present in v2.
class ListOffsetsResponse extends KafkaResponse {
  final int _responseCorrelationId;
  final int throttleTimeMs;
  final List<ListOffsetsTopicResponse> topics;

  ListOffsetsResponse(super.correlationId, this.throttleTimeMs,
      List<ListOffsetsTopicResponse> topics)
      : _responseCorrelationId = correlationId,
        topics = List.unmodifiable(topics);

  @override
  int get correlationId => _responseCorrelationId;

  @override
  set correlationId(int value) =>
      throw UnsupportedError('ListOffsets responses are immutable');

  /// Parses correlation ID and v2 body, excluding the network size prefix.
  factory ListOffsetsResponse.fromBytes(List<int> bytes) {
    if (bytes.any((byte) => byte < 0 || byte > 255)) {
      throw const FormatException('Invalid ListOffsets response bytes');
    }
    final r = KafkaBytesReader.fromBytes(bytes);
    final correlation = r.readInt32(), throttle = r.readInt32();
    if (throttle < 0) {
      throw const FormatException('Invalid ListOffsets throttle duration');
    }
    final topics = <ListOffsetsTopicResponse>[];
    final topicCount = _count(r, 6);
    for (var i = 0; i < topicCount; i++) {
      final name = r.readString();
      if (name == null || name.isEmpty) {
        throw const FormatException('Invalid ListOffsets topic');
      }
      final partitions = <ListOffsetsPartitionResponse>[];
      final partitionCount = _count(r, 22);
      for (var j = 0; j < partitionCount; j++) {
        final partition = r.readInt32(), errorCode = r.readInt16();
        final timestamp = r.readInt64(), offset = r.readInt64();
        if (partition < 0 || timestamp < -1 || offset < -1) {
          throw const FormatException('Invalid ListOffsets partition result');
        }
        partitions.add(ListOffsetsPartitionResponse(
            partition, errorCode, timestamp, offset));
      }
      topics.add(ListOffsetsTopicResponse(name, partitions));
    }
    if (!r.isEOF) {
      throw const FormatException('Trailing ListOffsets response bytes');
    }
    return ListOffsetsResponse(correlation, throttle, topics);
  }

  static int _count(KafkaBytesReader r, int minimumSize) {
    final count = r.readInt32();
    if (count < 0 || count > r.remaining ~/ minimumSize) {
      throw const FormatException('Invalid ListOffsets array count');
    }
    return count;
  }

  @override
  Map<String, dynamic> toJson() => {
        'correlationId': correlationId,
        'throttleTimeMs': throttleTimeMs,
        'topics': topics.map((topic) => topic.toJson()).toList(),
      };
}

/// Results for one topic; the partition list is copied and immutable.
class ListOffsetsTopicResponse {
  final String name;
  final List<ListOffsetsPartitionResponse> partitions;

  ListOffsetsTopicResponse(
      this.name, List<ListOffsetsPartitionResponse> partitions)
      : partitions = List.unmodifiable(partitions);

  Map<String, dynamic> toJson() => {
        'name': name,
        'partitions':
            partitions.map((partition) => partition.toJson()).toList(),
      };
}

/// Broker result for a single partition, without interpreting [errorCode].
class ListOffsetsPartitionResponse {
  final int partition;
  final int errorCode;

  /// Associated epoch-millisecond timestamp, or -1 when unavailable.
  final int timestamp;

  /// Earliest retained offset or exclusive latest boundary; -1 is unavailable.
  final int offset;

  const ListOffsetsPartitionResponse(
      this.partition, this.errorCode, this.timestamp, this.offset);

  Map<String, dynamic> toJson() => {
        'partition': partition,
        'errorCode': errorCode,
        'timestamp': timestamp,
        'offset': offset,
      };
}
