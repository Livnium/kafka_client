import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';

void main() {
  final brokers = [Broker('127.0.0.1', 1)];

  KafkaGroupConsumer make({
    KafkaTlsConfig? tls,
    bool fallback = false,
    KafkaTlsFetchRecovery? policy,
  }) =>
      KafkaGroupConsumer(
          brokers: brokers,
          groupId: 'g',
          topics: {'t'},
          commitAfterProcessing: false,
          tls: tls,
          allowTlsBootstrapFallback: fallback,
          tlsFetchRecovery: policy);

  Future<void> expectIdleAndStop(KafkaGroupConsumer consumer) async {
    expect(consumer.state, KafkaGroupConsumerState.idle);
    expect(consumer.generationsStarted, 0);
    expect(consumer.assignment, isNull);
    consumer.stop();
    final result = await consumer.closed;
    expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
    expect(result.generationsStarted, 0);
    expect(result.error, isNull);
    expect(result.cleanupErrors, isEmpty);
    expect(consumer.state, KafkaGroupConsumerState.closed);
  }

  test('default and explicit null policy preserve idle plaintext construction',
      () async {
    final defaultConsumer = KafkaGroupConsumer(
        brokers: brokers,
        groupId: 'g',
        topics: {'t'},
        commitAfterProcessing: false);
    await expectIdleAndStop(defaultConsumer);
    await expectIdleAndStop(make(policy: null));
  });

  test('rejects incomplete Fetch recovery opt-in at construction', () {
    final policy = KafkaTlsFetchRecovery();
    for (final tls in <KafkaTlsConfig?>[null, KafkaTlsConfig()]) {
      for (final fallback in [false, true]) {
        if (tls != null && fallback) continue;
        expect(() => make(tls: tls, fallback: fallback, policy: policy),
            throwsArgumentError);
      }
    }
  });

  test('valid policy combinations remain idle without parsing certificates',
      () async {
    // Constructor input-shape validation succeeds; certificate-container parsing
    // belongs to the first connection, not consumer/session construction.
    final tls =
        KafkaTlsConfig(withTrustedRoots: false, trustedCertificates: [1, 2, 3]);
    await expectIdleAndStop(make(tls: tls));
    await expectIdleAndStop(make(tls: tls, fallback: true));
    await expectIdleAndStop(make(
        tls: tls,
        fallback: true,
        policy: KafkaTlsFetchRecovery(maxAttempts: 1)));
  });

  test('policy does not bypass eager native client resource validation', () {
    expect(
        () => KafkaGroupConsumer(
            brokers: brokers,
            groupId: 'g',
            topics: {'t'},
            commitAfterProcessing: false,
            tls: KafkaTlsConfig(),
            allowTlsBootstrapFallback: true,
            tlsFetchRecovery: KafkaTlsFetchRecovery(),
            maxConnections: 0),
        throwsArgumentError);
  });
}
