import '../../codec/byte_builder.dart';
import '../abstract/kafka_request.dart';
import '../response/api1_fetch_response.dart';

/// Experimental Fetch v4 request for explicitly selected partitions.
///
/// Only read-uncommitted consumption of uncompressed magic-2 batches is
/// supported. This request does not join a consumer group or commit offsets.
/// Low-level callers must discover partition leaders and API compatibility.
class FetchRequest extends KafkaRequest {
  /// Kafka Fetch API key.
  final int apiKey = 1;

  /// Implemented wire version; this request always uses Fetch v4.
  final int apiVersion = 4;

  /// Consumer identity sentinel; only `-1` is accepted.
  final int replicaId;

  /// Maximum broker wait in milliseconds for [minBytes] to accumulate.
  final int maxWaitMs;

  /// Minimum desired response bytes before the broker returns early.
  final int minBytes;

  /// Requested response byte limit, not a strict memory bound.
  ///
  /// Kafka may return an oversized first batch to permit consumer progress.
  final int maxBytes;

  /// Topics and manually assigned partitions to fetch.
  final List<FetchTopic> topics;

  /// Creates a request and validates consumer identity and byte/wait limits.
  FetchRequest(this.replicaId, this.maxWaitMs, this.minBytes, this.topics,
      {this.maxBytes = 1048576}) {
    if (replicaId != -1 || maxWaitMs < 0 || minBytes < 0 || maxBytes <= 0) {
      throw ArgumentError('Invalid consumer fetch limits or replicaId');
    }
  }

  /// Encodes the request including its four-byte network size prefix.
  @override
  List<int> toBytes() {
    final b =
        KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
          ..addInt32(replicaId)
          ..addInt32(maxWaitMs)
          ..addInt32(minBytes)
          ..addInt32(maxBytes)
          ..addInt8(0)
          ..addArray(topics, KafkaType.object, (FetchTopic t) => t.toBytes());
    return KafkaRequest.frame(b.takeBytes());
  }

  /// Decodes a v4 response including correlation ID but excluding size prefix.
  @override
  FetchResponse parseResponse(List<int> bytes) =>
      FetchResponse.fromBytes(bytes);
}

/// A topic and the explicit partitions requested from it.
class FetchTopic {
  /// Kafka topic name, encoded as UTF-8 on the wire.
  final String name;

  /// Per-partition starting offsets and requested byte limits.
  final List<FetchPartition> partitions;

  /// Creates a topic selection without automatic partition assignment.
  FetchTopic(this.name, this.partitions);

  /// Encodes this structure without a request header or network size prefix.
  List<int> toBytes() => (KafkaBytesBuilder()
        ..addString(name)
        ..addArray(
            partitions, KafkaType.object, (FetchPartition p) => p.toBytes()))
      .takeBytes();
}

/// Fetch position and byte limit for one manually selected partition.
class FetchPartition {
  /// Zero-based partition index.
  final int partition;

  /// Absolute log offset at which to begin reading, inclusive.
  ///
  /// A low-level response may include earlier records from the same batch.
  final int fetchOffset;

  /// Requested per-partition byte limit; an oversized first batch may exceed it.
  final int partitionMaxBytes;

  /// Creates a selection; rejects negative indices/offsets or nonpositive limits.
  FetchPartition(this.partition, this.fetchOffset, this.partitionMaxBytes) {
    if (partition < 0 || fetchOffset < 0 || partitionMaxBytes <= 0) {
      throw ArgumentError('Invalid partition, offset, or fetch byte limit');
    }
  }

  /// Encodes this structure without a request header or network size prefix.
  List<int> toBytes() => (KafkaBytesBuilder()
        ..addInt32(partition)
        ..addInt64(fetchOffset)
        ..addInt32(partitionMaxBytes))
      .takeBytes();
}
