import 'dart:convert';
import '../../codec/byte_builder.dart';
import '../../codec/record_batch.dart';
import '../abstract/kafka_request.dart';
import '../response/api0_produce_response.dart';

/// Records grouped by topic in an experimental Produce v3 request.
class TopicData {
  /// Kafka topic name, encoded as UTF-8 on the wire.
  final String name;

  /// Explicit destination partitions and their record batches.
  final List<PartitionData> partitions;

  /// Creates topic data without selecting or balancing partitions.
  TopicData({required this.name, required this.partitions});
}

/// One uncompressed record batch for an explicitly selected partition.
class PartitionData {
  /// Zero-based destination partition index.
  final int index;

  /// Ordered records; the broker assigns their absolute log offsets.
  final List<KafkaRecord> records;

  /// Groups [records] for partition [index].
  PartitionData({required this.index, required this.records});
}

/// Experimental Produce v3 request with uncompressed magic-2 record batches.
///
/// Transactions, idempotence, compression and automatic retries are unsupported.
/// Low-level callers must route this request to the partition leader and verify
/// broker API compatibility before sending it.
class ProduceRequest extends KafkaRequest {
  /// Kafka Produce API key.
  final int apiKey = 0;

  /// Implemented wire version; this request always uses Produce v3.
  final int apiVersion = 3;

  /// Required acknowledgments: `0` none, `1` leader, `-1` in-sync replicas.
  ///
  /// With `0`, local socket flush is not proof of broker receipt or durability.
  final int acks;

  /// Broker acknowledgment timeout in milliseconds, not a client deadline.
  final int timeoutMs;

  /// Topics and explicit partitions containing the records to append.
  final List<TopicData> topicData;

  /// Creates a request; rejects invalid [acks] or negative [timeoutMs].
  ProduceRequest(
      {required this.acks, required this.timeoutMs, required this.topicData}) {
    if (![-1, 0, 1].contains(acks) || timeoutMs < 0) {
      throw ArgumentError(
          'acks must be -1, 0 or 1; timeoutMs must be nonnegative');
    }
  }

  /// Whether the broker is expected to return a response for this request.
  @override
  bool get expectsResponse => acks != 0;

  /// Encodes the request including its four-byte network size prefix.
  @override
  List<int> toBytes() {
    final b =
        KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
          ..addString(null)
          ..addInt16(acks)
          ..addInt32(timeoutMs)
          ..addInt32(topicData.length);
    for (final topic in topicData) {
      b
        ..addString(topic.name)
        ..addInt32(topic.partitions.length);
      for (final partition in topic.partitions) {
        b
          ..addInt32(partition.index)
          ..addBytes(encodeRecordBatch(partition.records));
      }
    }
    return KafkaRequest.frame(b.takeBytes());
  }

  /// Decodes a v3 response including correlation ID but excluding size prefix.
  @override
  ProduceResponse parseResponse(List<int> bytes) =>
      ProduceResponse.fromBytes(bytes);
}

/// Backward-compatible record name; payloads are bytes, not implicit JSON.
class Record extends KafkaRecord {
  /// Creates a record with byte payloads and an optional epoch-ms timestamp.
  Record({super.key, required super.value, super.timestamp, super.headers});

  /// UTF-8 encodes [value]; no JSON serialization is performed.
  factory Record.fromString(String value) => Record(value: utf8.encode(value));
}
