import '../../codec/byte_builder.dart';
import '../abstract/kafka_request.dart';
import '../response/api3_metadata_response.dart';

/// Kafka MetadataRequest.
class MetadataRequest extends KafkaRequest {
  /// API key of [MetadataRequest]
  final int apiKey = 3;

  /// API version of [MetadataRequest]
  final int apiVersion = 0;

  /// List of topic names to fetch metadata for. If set to null or empty
  /// this request will fetch metadata for all topics.
  final Set<String>? topicNames;

  /// Creates a new instance of Kafka MetadataRequest.
  ///
  /// If [topicNames] is omitted or empty, then metadata for all existing topics
  /// will be returned.
  MetadataRequest([this.topicNames]) : super();

  @override
  List<int> toBytes() {
    // Create a KafkaBytesBuilder with the request header.
    var builder =
        KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId);

    // Ensure topicNames is a Set.
    Set<String> topicNameSet =
        topicNames is Set<String> ? topicNames! : <String>{};

    // Add the topic names to the builder.
    builder.addArray(topicNameSet, KafkaType.string);

    // Take the bytes from the builder.
    var body = builder.takeBytes();

    // Add the body to the builder.
    builder.addBytes(body);

    // Return the bytes from the builder.
    return builder.takeBytes();
  }

  @override
  MetadataResponse parseResponse(List<int> bytes) {
    return MetadataResponse.fromBytes(bytes);
  }
}
