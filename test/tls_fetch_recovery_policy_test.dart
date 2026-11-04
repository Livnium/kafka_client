import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

// Constructor-only coverage runs in the ordinary suite, without TLS fixtures,
// network peers, credentials, OpenSSL, or environment opt-in.
void main() {
  test('TLS Fetch recovery policy has bounded documented defaults', () {
    final policy = KafkaTlsFetchRecovery();
    expect(policy.maxAttempts, 10);
    expect(policy.window, const Duration(seconds: 5));
    expect(policy.retryDelay, const Duration(milliseconds: 150));
  });

  test('TLS Fetch recovery accepts exact positive lower and upper bounds', () {
    final lower = KafkaTlsFetchRecovery(
        maxAttempts: 1,
        window: const Duration(microseconds: 1),
        retryDelay: Duration.zero);
    final upper = KafkaTlsFetchRecovery(
        maxAttempts: 20,
        window: const Duration(minutes: 1),
        retryDelay: const Duration(seconds: 1));
    expect(lower.maxAttempts, 1);
    expect(upper.maxAttempts, 20);
  });

  for (final attempts in [0, 21]) {
    test('TLS Fetch recovery rejects total attempt count $attempts', () {
      expect(() => KafkaTlsFetchRecovery(maxAttempts: attempts),
          throwsArgumentError);
    });
  }
  for (final window in [
    Duration.zero,
    const Duration(microseconds: -1),
    const Duration(minutes: 1, microseconds: 1)
  ]) {
    test('TLS Fetch recovery rejects window ${window.inMicroseconds}', () {
      expect(() => KafkaTlsFetchRecovery(window: window), throwsArgumentError);
    });
  }
  for (final delay in [
    const Duration(microseconds: -1),
    const Duration(seconds: 1, microseconds: 1)
  ]) {
    test('TLS Fetch recovery rejects delay ${delay.inMicroseconds}', () {
      expect(
          () => KafkaTlsFetchRecovery(retryDelay: delay), throwsArgumentError);
    });
  }
  for (final fallback in [false, true]) {
    test('TLS Fetch recovery requires TLS with fallback=$fallback', () {
      expect(
          () => KafkaClient(
              brokers: [Broker('localhost', 1)],
              allowTlsBootstrapFallback: fallback,
              tlsFetchRecovery: KafkaTlsFetchRecovery()),
          throwsArgumentError);
    });
  }
  test('TLS Fetch recovery requires enabled bootstrap fallback', () {
    expect(
        () => KafkaClient(
            brokers: [Broker('localhost', 1)],
            tls: KafkaTlsConfig(),
            tlsFetchRecovery: KafkaTlsFetchRecovery()),
        throwsArgumentError);
  });
  test('TLS Fetch recovery valid configuration remains lazy', () async {
    final policy = KafkaTlsFetchRecovery();
    final client = KafkaClient(
        brokers: [Broker('localhost', 1)],
        tls: KafkaTlsConfig(),
        allowTlsBootstrapFallback: true,
        tlsFetchRecovery: policy);
    expect(client.tlsFetchRecovery, same(policy));
    expect(client.connectionCount, 0);
    expect(client.admittedRequestCount, 0);
    await client.close();
  });
  test('null TLS Fetch recovery preserves plaintext default construction',
      () async {
    final client = KafkaClient(brokers: [Broker('localhost', 1)]);
    expect(client.tlsFetchRecovery, isNull);
    expect(client.connectionCount, 0);
    await client.close();
  });
}
