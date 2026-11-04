import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';

void main() {
  final brokers = [Broker('127.0.0.1', 1)];
  KafkaGroupSession make({
    String group = 'group',
    Set<String> topics = const {'topic'},
    Duration session = const Duration(seconds: 10),
    Duration rebalance = const Duration(seconds: 20),
    Duration heartbeat = const Duration(seconds: 2),
    Duration expiry = const Duration(seconds: 8),
    Duration startup = const Duration(seconds: 60),
    Duration leave = const Duration(seconds: 3),
    Duration request = const Duration(seconds: 35),
  }) =>
      KafkaGroupSession(
          brokers: brokers,
          groupId: group,
          topics: topics,
          sessionTimeout: session,
          rebalanceTimeout: rebalance,
          heartbeatInterval: heartbeat,
          localExpiry: expiry,
          startupTimeout: startup,
          leaveTimeout: leave,
          requestTimeout: request);

  test('public entrypoint is lazy and copies topics deeply enough', () async {
    final topics = {'one', 'two'};
    final session = make(topics: topics);
    topics.clear();
    expect(session.groupId, 'group');
    expect(session.topics, {'one', 'two'});
    expect(() => session.topics.add('other'), throwsUnsupportedError);
    expect(session.state, KafkaGroupSessionState.idle);
    expect(session.assignment, isNull);
    final stopped = await session.close();
    expect(stopped.reason, KafkaGroupStopReason.closed);
    expect(stopped.leaveOutcome, KafkaGroupLeaveOutcome.notAttempted);
    expect(stopped.cleanupError, isNull);
    expect(session.state, KafkaGroupSessionState.closed);
    expect((await session.revoked).reason, KafkaGroupStopReason.closed);
    expect(await session.close(), same(stopped));
    await expectLater(session.start(), throwsStateError);
  });

  test('rejects invalid identity and subscription without connecting', () {
    for (final group in ['', '\ud800', 'x' * 32768]) {
      expect(() => make(group: group), throwsArgumentError);
    }
    for (final topics in <Set<String>>[
      {},
      {''},
      {'\ud800'},
      {'x' * 32768}
    ]) {
      expect(() => make(topics: topics), throwsArgumentError);
    }
  });

  test('rejects unsafe or non-wire-exact timing budgets', () {
    final cases = <void Function()>[
      () => make(session: Duration.zero),
      () => make(session: const Duration(microseconds: 6000001)),
      () => make(session: const Duration(milliseconds: 2147483648)),
      () => make(rebalance: Duration.zero),
      () => make(rebalance: const Duration(microseconds: 20000001)),
      () => make(rebalance: const Duration(milliseconds: 2147483648)),
      () => make(heartbeat: Duration.zero),
      () => make(heartbeat: const Duration(seconds: 8)),
      () => make(expiry: Duration.zero),
      () => make(expiry: const Duration(seconds: 10)),
      () => make(expiry: const Duration(seconds: 11)),
      () => make(startup: Duration.zero),
      () => make(leave: Duration.zero),
      () => make(request: const Duration(seconds: 20)),
      () => make(request: Duration.zero),
    ];
    for (final create in cases) {
      expect(create, throwsArgumentError);
    }
  });

  test('retains strict TLS and client resource preflight', () {
    expect(
        () => KafkaGroupSession(
            brokers: brokers,
            groupId: 'g',
            topics: {'t'},
            sasl: KafkaSaslPlain(username: 'u', password: 'p')),
        throwsArgumentError);
    expect(
        () => KafkaGroupSession(
            brokers: brokers,
            groupId: 'g',
            topics: {'t'},
            allowTlsBootstrapFallback: true),
        throwsArgumentError);
    expect(() => KafkaGroupSession(brokers: [], groupId: 'g', topics: {'t'}),
        throwsArgumentError);
    expect(
        () => KafkaGroupSession(
            brokers: brokers, groupId: 'g', topics: {'t'}, maxConnections: 0),
        throwsArgumentError);
    expect(
        () => KafkaGroupSession(
            brokers: brokers, groupId: 'g', topics: {'t'}, maxReadRetries: 11),
        throwsArgumentError);
  });

  test('Fetch recovery requires both TLS and explicit bootstrap fallback', () {
    final policy = KafkaTlsFetchRecovery();
    for (final tls in <KafkaTlsConfig?>[null, KafkaTlsConfig()]) {
      for (final fallback in [false, true]) {
        if (tls != null && fallback) continue;
        expect(
            () => KafkaGroupSession(
                brokers: brokers,
                groupId: 'g',
                topics: {'t'},
                tls: tls,
                allowTlsBootstrapFallback: fallback,
                tlsFetchRecovery: policy),
            throwsArgumentError);
      }
    }
  });

  test('TLS Fetch policy is opt-in and session construction stays lazy',
      () async {
    // Container parsing remains lazy, just like the ordinary client. These
    // nonempty bytes pass input-shape checks but are not a certificate.
    final tls =
        KafkaTlsConfig(withTrustedRoots: false, trustedCertificates: [1, 2, 3]);
    for (final policy in <KafkaTlsFetchRecovery?>[
      null,
      KafkaTlsFetchRecovery(maxAttempts: 1)
    ]) {
      final session = KafkaGroupSession(
          brokers: brokers,
          groupId: 'g',
          topics: {'t'},
          tls: tls,
          allowTlsBootstrapFallback: true,
          tlsFetchRecovery: policy);
      expect(session.state, KafkaGroupSessionState.idle);
      expect(session.assignment, isNull);
      final result = await session.close();
      expect(result.leaveOutcome, KafkaGroupLeaveOutcome.notAttempted);
      expect(result.cleanupError, isNull);
    }
  });
}
