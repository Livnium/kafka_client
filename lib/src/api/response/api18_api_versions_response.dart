import '../../codec/byte_reader.dart';
import '../abstract/kafka_response.dart';

class ApiVersionsResponse extends KafkaResponse {
  final int errorCode;
  final List<ApiVersion> apiVersions;

  ApiVersionsResponse(this.errorCode, this.apiVersions, super.correlationId);

  /// Creates an ApiVersionsResponse from binary data.
  factory ApiVersionsResponse.fromBytes(List<int> data) {
    var reader = KafkaBytesReader.fromBytes(data);

    var correlationId = reader.readInt32();
    var errorCode = reader.readInt16();

    var apiKeysCount = reader.readInt32();
    if (apiKeysCount < 0 || apiKeysCount > reader.length - reader.offset) {
      throw FormatException("Invalid apiKeysCount");
    }
    List<ApiVersion> apiVersions = List.generate(apiKeysCount, (_) {
      var apiKey = reader.readInt16();
      var minVersion = reader.readInt16();
      var maxVersion = reader.readInt16();
      return ApiVersion(apiKey, minVersion, maxVersion);
    });

    if (!reader.isEOF) throw FormatException("Trailing ApiVersions bytes");
    return ApiVersionsResponse(errorCode, apiVersions, correlationId);
  }

  @override
  Map<String, dynamic> toJson() {
    return {
      'correlationId': correlationId,
      'errorCode': errorCode,
      'apiVersions':
          apiVersions.map((apiVersion) => apiVersion.toJson()).toList(),
    };
  }
}

class ApiVersion {
  final int apiKey;
  final int minVersion;
  final int maxVersion;

  ApiVersion(this.apiKey, this.minVersion, this.maxVersion);

  Map<String, dynamic> toJson() {
    return {
      'apiKey': apiKey,
      'minVersion': minVersion,
      'maxVersion': maxVersion,
    };
  }

  @override
  String toString() {
    return 'ApiVersion{apiKey: $apiKey, minVersion: $minVersion, maxVersion: $maxVersion}';
  }
}
