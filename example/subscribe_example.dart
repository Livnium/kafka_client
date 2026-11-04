import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_client.dart';

/// Appends one record to a pre-created topic on a trusted local plaintext broker.
/// For TLS/SASL configuration, see kafka_client_secure_example.dart.
Future<void> main() async {
  final host = Platform.environment['KAFKA_HOST'] ?? '127.0.0.1';
  final port = int.parse(Platform.environment['KAFKA_PORT'] ?? '9092');
  final topic = Platform.environment['KAFKA_TOPIC'] ?? 'dart-subscribe-example';
  final partition = int.parse(Platform.environment['KAFKA_PARTITION'] ?? '0');
  final marker = 'Dart subscription ${DateTime.now().microsecondsSinceEpoch}';
  final client = KafkaClient(brokers: [Broker(host, port)]);

  try {
    // Send once. An uncertain acknowledgement must not trigger blind replay.
    final produced = await client.produce(
      topic,
      [KafkaRecord(value: utf8.encode(marker))],
      partition: partition,
      acks: 1,
    );
    final startOffset =
        produced!.responses.single.partitionResponses.single.baseOffset;
    if (startOffset < 0) {
      throw StateError('Produce did not return a usable acknowledged offset');
    }

    // Inclusive and explicit. Literal zero would not mean "earliest retained".
    final records = client.subscribe(
      topic,
      startOffsets: {partition: startOffset},
    );
    var received = false;
    await for (final event in records.timeout(const Duration(seconds: 30))) {
      if (event.topic != topic ||
          event.partition != partition ||
          event.offset != startOffset ||
          event.record.value == null ||
          utf8.decode(event.record.value!) != marker) {
        throw StateError('Subscription returned an unexpected record');
      }

      // Awaiting processing in an await-for body applies stream backpressure.
      await _process(event);
      final nextOffsets = {event.partition: event.offset + 1};
      print('Next offsets (in memory only): $nextOffsets');
      // In an application, durably save this position AFTER successful work.
      // Delivery itself is neither processing acknowledgement nor a commit.
      received = true;
      break; // Awaits stream cancellation; it does not close the shared client.
    }
    if (!received) throw StateError('Subscription ended before the record');
  } finally {
    // This example owns the client. Cancellation/timeout may first wait for an
    // admitted Fetch to settle under its existing per-phase deadlines.
    await client.close();
  }
}

Future<void> _process(KafkaTopicRecord event) async {
  stdout.writeln('${event.topic}[${event.partition}]@${event.offset}: '
      '${utf8.decode(event.record.value!)}');
  await stdout.flush();
}
