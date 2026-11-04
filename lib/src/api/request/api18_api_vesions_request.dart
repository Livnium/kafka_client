import 'dart:typed_data';

import '../../codec/byte_builder.dart';
import '../abstract/kafka_request.dart';
import '../response/api18_api_versions_response.dart';

class ApiVersionsRequest extends KafkaRequest {
  final int apiKey = 18; // API key for ApiVersionsRequest
  final int apiVersion = 0; // Version 0 of the ApiVersions API

  ApiVersionsRequest() : super();

  @override
  List<int> toBytes() {
    var builder =
        KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId);
    final messageBytes = builder.takeBytes();
    final sizeBytes = ByteData(4);
    sizeBytes.setInt32(0, messageBytes.length, Endian.big);
    final sizePrefix = Uint8List.sublistView(sizeBytes.buffer.asUint8List());
    return Uint8List.fromList(sizePrefix + messageBytes);
  }

  @override
  ApiVersionsResponse parseResponse(List<int> bytes) {
    return ApiVersionsResponse.fromBytes(bytes);
  }
}
