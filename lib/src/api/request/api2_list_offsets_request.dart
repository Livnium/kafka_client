import 'dart:convert';

import '../../codec/byte_builder.dart';
import '../../list_offsets.dart';
import '../abstract/kafka_request.dart';
import '../response/api2_list_offsets_response.dart';

/// ListOffsets v2 for one explicitly selected partition and earliest/latest.
///
/// Uses consumer replica ID -1 and read-uncommitted isolation. Low-level callers
/// must discover the leader and check API compatibility themselves. This request
/// does not join a group, read commits, change a position or reset an offset.
class ListOffsetsRequest extends KafkaRequest {
  final int apiKey = 2;
  final int apiVersion = 2;
  final String topic;
  final int partition;
  final KafkaOffsetPosition position;

  /// Validates the topic's nonempty UTF-8 STRING and nonnegative INT32 partition.
  ListOffsetsRequest(this.topic, {this.partition = 0, required this.position}) {
    final encoded = utf8.encode(topic);
    if (topic.isEmpty ||
        encoded.length > 32767 ||
        utf8.decode(encoded) != topic ||
        partition < 0 ||
        partition > 2147483647) {
      throw ArgumentError('Invalid ListOffsets topic or partition');
    }
  }

  @override
  List<int> toBytes() {
    final b =
        KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
          ..addInt32(-1)
          ..addInt8(0)
          ..addInt32(1)
          ..addString(topic)
          ..addInt32(1)
          ..addInt32(partition)
          ..addInt64(position == KafkaOffsetPosition.earliest ? -2 : -1);
    return KafkaRequest.frame(b.takeBytes());
  }

  @override
  ListOffsetsResponse parseResponse(List<int> bytes) {
    final response = ListOffsetsResponse.fromBytes(bytes);
    if (response.correlationId != correlationId) {
      throw const FormatException('Mismatched ListOffsets correlation ID');
    }
    return response;
  }
}
