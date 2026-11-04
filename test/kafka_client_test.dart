import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

void main() {
  test('configuration rejects invalid endpoints list and limits', () {
    expect(() => KafkaClient(brokers: []), throwsArgumentError);
    expect(
        () => KafkaClient(
            brokers: [Broker('localhost', 9092)],
            requestTimeout: Duration.zero),
        throwsArgumentError);
    expect(
        () =>
            KafkaClient(brokers: [Broker('localhost', 9092)], maxFrameBytes: 3),
        throwsArgumentError);
  });
}
