import '../../codec/byte_reader.dart';

abstract class KafkaResponse {
  int correlationId; // The correlation ID to match the response with its request

  KafkaResponse(this.correlationId);

  // A static method to read the common header of a Kafka response (size and correlationId)
  static KafkaResponseHeader readHeader(KafkaBytesReader reader) {
    int correlationId = reader.readInt32(); // Read the correlation ID
    return KafkaResponseHeader(correlationId);
  }

  Map<String, dynamic> toJson();
}

// A simple class to hold the common header information of Kafka responses
class KafkaResponseHeader {
  final int correlationId;

  KafkaResponseHeader(this.correlationId);
}
