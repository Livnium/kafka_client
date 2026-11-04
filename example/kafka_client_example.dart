import 'dart:convert';
import 'dart:io';
import 'package:kafka_client/kafka_client.dart';

/// Create `dart-example` on your local plaintext broker before running.
Future<void> main() async {
  final host = Platform.environment['KAFKA_HOST'] ?? '127.0.0.1';
  final port = int.parse(Platform.environment['KAFKA_PORT'] ?? '9092');
  final client = KafkaClient(brokers: [Broker(host, port)]);
  try {
    final response = await client.produce('dart-example', [
      KafkaRecord(
          value: utf8.encode('Hello from Dart'),
          timestamp: DateTime.now().millisecondsSinceEpoch),
    ]);
    final offset =
        response!.responses.single.partitionResponses.single.baseOffset;
    final fetched = await client.fetch('dart-example', offset: offset);
    if (fetched.topics.isEmpty) {
      print('Fetch throttled for ${fetched.throttleTimeMs} ms; '
          'retry the same offset after that delay');
      return;
    }
    for (final record in fetched.topics.single.partitions.single.records) {
      print(
          '${record.offset}: ${record.value == null ? "<null>" : utf8.decode(record.value!)}');
    }
  } finally {
    await client.close();
  }
}
