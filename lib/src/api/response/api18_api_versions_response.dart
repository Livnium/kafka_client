import '../../codec/byte_reader.dart';
import '../abstract/kafka_response.dart';

class ApiVersionsResponse extends KafkaResponse {
  final int errorCode;
  final List<ApiVersion> apiVersions;

  ApiVersionsResponse(this.errorCode, this.apiVersions, super.correlationId);

  /// Validates a successful capability table before it authorizes requests.
  ///
  /// Keys must be unique and all keys/version bounds must be nonnegative
  /// signed INT16 values, with minimum <= maximum. Unknown future keys and
  /// versions are allowed. Raw decoding retains the original diagnostics;
  /// callers must handle a nonzero [errorCode] before using the table.
  void validateSupportedVersions() {
    final keys = <int>{};
    for (final api in apiVersions) {
      if (api.apiKey < 0 ||
          api.apiKey > 32767 ||
          api.minVersion < 0 ||
          api.minVersion > 32767 ||
          api.maxVersion < api.minVersion ||
          api.maxVersion > 32767 ||
          !keys.add(api.apiKey)) {
        throw const FormatException('Invalid or duplicate API version range');
      }
    }
  }

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
