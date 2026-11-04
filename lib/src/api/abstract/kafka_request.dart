import '../../codec/byte_builder.dart';

abstract class KafkaRequest {
  static int _nextId = 0;
  final int correlationId = _nextId++ & 0x7fffffff;
  bool get expectsResponse => true;
  List<int> toBytes();
  dynamic parseResponse(List<int> bytes);

  /// Kafka's outer INT32 excludes the four bytes of the length itself.
  static List<int> frame(List<int> body) =>
      (KafkaBytesBuilder()..addBytes(body)).takeBytes();
}
