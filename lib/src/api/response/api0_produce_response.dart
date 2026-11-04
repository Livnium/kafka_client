import '../../codec/byte_reader.dart';
import '../abstract/kafka_response.dart';

/// Decoded experimental Produce v3 response for acknowledged writes.
///
/// An `acks=0` write has no response. Raw responses retain Kafka error codes;
/// receiving a response does not itself imply that every partition succeeded.
class ProduceResponse extends KafkaResponse {
  /// Per-topic and per-partition append results.
  final List<TopicResponse> responses;

  /// Broker-reported quota throttle duration in milliseconds.
  final int throttleTimeMs;

  /// Creates a response associated with the original request correlation ID.
  ProduceResponse(super.correlationId, this.responses,
      [this.throttleTimeMs = 0]);

  /// Parses v3 response [data], including correlation ID but not size prefix.
  ///
  /// Rejects truncated, malformed or trailing data; does not throw merely
  /// because a partition contains a nonzero Kafka error code.
  factory ProduceResponse.fromBytes(List<int> data) {
    var reader = KafkaBytesReader.fromBytes(data);
    final correlationId = reader.readInt32();

    // Parse the topic responses
    final responseCount = reader.readInt32();
    if (responseCount < 0 || responseCount > reader.length - reader.offset) {
      throw FormatException("Invalid responseCount");
    }
    final responses = <TopicResponse>[];
    for (var i = 0; i < responseCount; i++) {
      final topicName = reader.readString();
      if (topicName == null) {
        throw Exception('Topic name cannot be null');
      }
      final partitionCount = reader.readInt32();
      if (partitionCount < 0 ||
          partitionCount > reader.length - reader.offset) {
        throw FormatException("Invalid partitionCount");
      }
      final partitionResponses = <PartitionResponse>[];
      for (var j = 0; j < partitionCount; j++) {
        final partitionIndex = reader.readInt32();
        final errorCode = reader.readInt16();
        final baseOffset = reader.readInt64();
        final logAppendTimeMs = reader.readInt64();
        partitionResponses.add(
          PartitionResponse(
            index: partitionIndex,
            errorCode: errorCode,
            baseOffset: baseOffset,
            logAppendTimeMs: logAppendTimeMs,
          ),
        );
      }
      responses.add(
        TopicResponse(
          name: topicName,
          partitionResponses: partitionResponses,
        ),
      );
    }

    final throttle = reader.readInt32();
    if (!reader.isEOF) throw FormatException("Trailing produce bytes");
    return ProduceResponse(correlationId, responses, throttle);
  }

  @override
  String toString() {
    return 'ProduceResponse(correlationId: $correlationId, responses: $responses)';
  }

  /// Returns a JSON-compatible summary of the append result.
  @override
  Map<String, dynamic> toJson() {
    return {
      'correlationId': correlationId,
      'responses': responses.map((response) => response.toJson()).toList(),
    };
  }
}

/// Represents the response for a specific topic in a produce request.
class TopicResponse {
  /// Topic name returned by the broker.
  final String name;

  /// Append results for the requested partitions.
  final List<PartitionResponse> partitionResponses;

  /// Creates a topic result from decoded partition results.
  TopicResponse({
    required this.name,
    required this.partitionResponses,
  });

  @override
  String toString() {
    return 'TopicResponse(name: $name, partitionResponses: $partitionResponses)';
  }

  /// Returns a JSON-compatible summary of the append result.
  Map<String, dynamic> toJson() {
    return {
      'name': name,
      'partitionResponses':
          partitionResponses.map((partition) => partition.toJson()).toList(),
    };
  }
}

/// Represents the response for a specific partition in a produce request.
class PartitionResponse {
  /// Zero-based partition index.
  final int index;

  /// Kafka protocol error code; `0` means success.
  final int errorCode;

  /// Broker-assigned offset of the first appended record on success.
  ///
  /// Treat this value as unavailable when [errorCode] is nonzero.
  final int baseOffset;

  /// Broker append timestamp in epoch milliseconds, or `-1` for CreateTime.
  final int logAppendTimeMs;

  /// Creates a partition append result without interpreting [errorCode].
  PartitionResponse({
    required this.index,
    required this.errorCode,
    required this.baseOffset,
    this.logAppendTimeMs = -1,
  });

  @override
  String toString() {
    return 'PartitionResponse(index: $index, errorCode: $errorCode, baseOffset: $baseOffset)';
  }

  /// Returns a JSON-compatible summary of the append result.
  Map<String, dynamic> toJson() {
    return {
      'index': index,
      'errorCode': errorCode,
      'baseOffset': baseOffset,
    };
  }
}
