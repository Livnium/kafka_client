import '../../codec/byte_reader.dart';
import '../../codec/record_batch.dart';
import '../abstract/kafka_response.dart';

/// Decoded experimental Fetch v4 response with read-uncommitted records.
///
/// Only uncompressed magic-2 batches are supported. Control records are omitted;
/// this type does not provide read-committed transaction filtering or commits.
class FetchResponse extends KafkaResponse {
  /// Broker-reported quota throttle duration in milliseconds.
  final int throttleTimeMs;

  /// Topic results containing records and per-partition errors.
  /// Quota-throttled replies can omit every topic; inspect [throttleTimeMs].
  final List<FetchTopicResponse> topics;

  /// Creates a response with the original request correlation ID.
  FetchResponse(super.correlationId, this.throttleTimeMs, this.topics);

  /// Parses v4 bytes including correlation ID but excluding network size prefix.
  ///
  /// Validates complete batch CRCs and structure. A partial final batch after a
  /// complete validated prefix is ignored, without advancing past that prefix.
  /// A nonempty incomplete first batch still fails. Unsupported compression in
  /// a complete batch fails explicitly. Kafka partition error codes remain
  /// available in the returned response.
  factory FetchResponse.fromBytes(List<int> bytes) {
    final r = KafkaBytesReader.fromBytes(bytes);
    final correlation = r.readInt32(), throttle = r.readInt32();
    final topics = <FetchTopicResponse>[];
    final n = _count(r);
    for (var i = 0; i < n; i++) {
      final name = r.readString();
      if (name == null) throw FormatException('Null topic');
      final partitions = <FetchPartitionResponse>[];
      final count = _count(r);
      for (var j = 0; j < count; j++) {
        final index = r.readInt32(), error = r.readInt16();
        final watermark = r.readInt64(), stable = r.readInt64();
        final aborted = r.readInt32();
        if (aborted < -1 || aborted > (r.length - r.offset) ~/ 16) {
          throw FormatException('Invalid aborted transaction count');
        }
        for (var k = 0; k < aborted; k++) {
          r.readInt64();
          r.readInt64();
        }
        final records = r.readBytes();
        final decoded =
            decodeFetchRecordBatchesWithProgress(records ?? const []);
        partitions.add(FetchPartitionResponse(
            index, error, watermark, stable, decoded.records,
            lastBatchOffset: decoded.lastBatchOffset));
      }
      topics.add(FetchTopicResponse(name, partitions));
    }
    if (!r.isEOF) throw FormatException('Trailing fetch bytes');
    return FetchResponse(correlation, throttle, topics);
  }
  static int _count(KafkaBytesReader r) {
    final n = r.readInt32();
    if (n < 0 || n > r.length - r.offset) {
      throw FormatException('Invalid array count');
    }
    return n;
  }

  /// Returns a JSON-compatible summary; byte keys/values remain integer lists.
  ///
  /// This diagnostic summary does not include record headers.
  @override
  Map<String, dynamic> toJson() => {
        'correlationId': correlationId,
        'throttleTimeMs': throttleTimeMs,
        'topics': topics.map((t) => t.toJson()).toList()
      };
}

/// Fetch results for one topic.
class FetchTopicResponse {
  /// Topic name returned by the broker.
  final String name;

  /// Per-partition records, watermarks and errors.
  final List<FetchPartitionResponse> partitions;

  /// Creates a topic result from decoded partitions.
  FetchTopicResponse(this.name, this.partitions);

  /// Returns a JSON-compatible summary; byte keys/values remain integer lists.
  ///
  /// This diagnostic summary does not include record headers.
  Map<String, dynamic> toJson() =>
      {'name': name, 'partitions': partitions.map((p) => p.toJson()).toList()};
}

/// Records and log positions returned for a single partition.
class FetchPartitionResponse {
  /// Zero-based partition index.
  final int partition;

  /// Kafka protocol error code; `0` means success.
  final int errorCode;

  /// Offset immediately after the last record replicated to the in-sync replicas.
  ///
  /// This is an exclusive log position, not a count of returned records.
  final int highWatermark;

  /// First offset whose transaction status may be undecided, or `-1` if unavailable.
  ///
  /// This boundary does not make these read-uncommitted records transaction-safe.
  final int lastStableOffset;

  /// Decoded records with absolute offsets and epoch-millisecond timestamps.
  ///
  /// A raw fetch can include earlier records from the batch containing the
  /// requested offset. The high-level client filters those earlier records.
  final List<KafkaRecord> records;

  /// Inclusive final offset of the last fully validated returned record batch.
  ///
  /// Includes control-only and fully compacted batches. Null means no batch was
  /// returned (or no progress metadata was supplied by a manual constructor).
  /// This is not the high watermark and does not include an increment.
  final int? lastBatchOffset;

  /// Creates a partition result without interpreting its Kafka error code.
  FetchPartitionResponse(this.partition, this.errorCode, this.highWatermark,
      this.lastStableOffset, this.records,
      {this.lastBatchOffset});

  /// Returns a JSON-compatible summary; byte keys/values remain integer lists.
  ///
  /// This diagnostic summary does not include record headers.
  Map<String, dynamic> toJson() => {
        'partition': partition,
        'errorCode': errorCode,
        'highWatermark': highWatermark,
        'lastStableOffset': lastStableOffset,
        'records': records
            .map((r) => {
                  'offset': r.offset,
                  'timestamp': r.timestamp,
                  'key': r.key,
                  'value': r.value
                })
            .toList()
      };
}
