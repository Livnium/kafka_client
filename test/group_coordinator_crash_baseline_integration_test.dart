// Owned Kafka coordinator crash characterization. A red recovery assertion is
// deliberate: diagnostic receipts are printed before asserting automatic rejoin.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';
import 'combined_secure_fault_integration_test.dart' as fault;
import 'group_membership_integration_test.dart' as membership;

Future<void> waitFor(bool Function() predicate, String label) async {
  final clock = Stopwatch()..start();
  while (!predicate()) {
    if (clock.elapsed > const Duration(seconds: 20)) {
      throw TimeoutException(label);
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
}

void main() {
  final env = Platform.environment;
  final mode = env['KAFKA_COMBINED_MODE'];
  test('$mode: automatic coordinator crash recovery baseline', () async {
    final topic = env['KAFKA_COMBINED_TOPIC']!;
    final directory = env['KAFKA_COMBINED_CONTROL']!;
    final ports =
        env['KAFKA_COMBINED_PORTS']!.split(',').map(int.parse).toList();
    final brokers = ports.map((p) => Broker('localhost', p)).toList();
    final tls = KafkaTlsConfig(
        trustedCertificates: File(env['KAFKA_COMBINED_CA']!).readAsBytesSync(),
        withTrustedRoots: false);
    final sasl = mode == 'sasl'
        ? KafkaSaslPlain(
            username: env['KAFKA_COMBINED_USERNAME']!,
            password: env['KAFKA_COMBINED_PASSWORD']!)
        : null;
    KafkaClient client({List<Broker>? endpoints}) => KafkaClient(
        brokers: endpoints ?? brokers,
        tls: tls,
        sasl: sasl,
        allowTlsBootstrapFallback: true,
        maxConnections: 2,
        maxReadRetries: 10,
        retryBaseDelay: const Duration(milliseconds: 100),
        connectionTimeout: const Duration(milliseconds: 700));
    final reader = client();
    addTearDown(reader.close);
    final receipts = <Map<String, Object?>>[];
    for (final commit in [false, true]) {
      await fault.readUntil(
          reader,
          () => reader.metadata({topic}),
          (m) => fault.metadataReady(
              m, (ps) => ps.every((p) => p.isrNodes.toSet().length == 3)),
          'full ISR');
      final marker = '$mode-$commit-$pid';
      final offsets = <int, int>{};
      for (var p = 0; p < 3; p++) {
        final produced = await reader.produce(
            topic, [KafkaRecord(value: utf8.encode('$marker-$p'))],
            partition: p, acks: -1);
        offsets[p] =
            produced!.responses.single.partitionResponses.single.baseOffset;
      }
      final group = membership.uniqueGroup('coordinator-crash-$commit');
      final coordinator =
          await membership.checkedCoordinator(reader, group, topic);
      final victim = ports.indexOf(coordinator.port) + 1;
      expect(victim, inInclusiveRange(1, 3));
      final consumer = KafkaGroupConsumer(
          brokers: brokers,
          groupId: group,
          topics: {topic},
          commitAfterProcessing: commit,
          missingOffsets: {topic: offsets},
          tls: tls,
          sasl: sasl,
          allowTlsBootstrapFallback: true,
          tlsFetchRecovery: KafkaTlsFetchRecovery(
              maxAttempts: 20,
              window: const Duration(seconds: 15),
              retryDelay: const Duration(milliseconds: 500)),
          maxRejoins: 3,
          maxConnections: 2,
          connectionTimeout: const Duration(milliseconds: 700),
          heartbeatInterval: const Duration(milliseconds: 300),
          localExpiry: const Duration(seconds: 4),
          sessionTimeout: const Duration(seconds: 6),
          leaveTimeout: const Duration(milliseconds: 500));
      addTearDown(() async {
        consumer.stop();
        await consumer.closed;
      });
      final seen = <String>[];
      KafkaGroupAssignment? initial;
      KafkaGroupConsumerTermination? termination;
      final running = consumer.run((record) {
        initial ??= record.assignment;
        expect(consumer.isCurrent(record.assignment), isTrue);
        seen.add('${record.partition}:${record.offset}');
      }).then((value) {
        termination = value;
        return value;
      });
      await waitFor(() => seen.length == 3, 'initial processing');
      if (commit) {
        await fault.readUntil(
            reader,
            () => reader.committedOffsets(group, topic, partitions: {0, 1, 2}),
            (stored) => offsets.entries
                .every((e) => stored[e.key]?.nextOffset == e.value + 1),
            'confirmed pre-fault positions');
      }
      await Future<void>.delayed(const Duration(milliseconds: 650));
      expect(consumer.isCurrent(initial!), isTrue);
      final before =
          await reader.committedOffsets(group, topic, partitions: {0, 1, 2});
      final clock = Stopwatch()..start();
      final crash = await fault.control(directory, 'crash:$victim');
      expect(crash['nodes']['$victim']['running'], isFalse);
      await waitFor(() => !consumer.isCurrent(initial!),
          'revocation after coordinator crash');
      final revokedMs = clock.elapsedMilliseconds;
      // A separate read-only probe verifies Kafka migrated coordination. No
      // membership mutation or commit is retried by this observation code.
      final probe = client(
          endpoints: brokers.where((b) => b.port != coordinator.port).toList());
      addTearDown(probe.close);
      final migrated = await fault.readUntil(
          probe,
          () => probe.findGroupCoordinator(group),
          (b) => b.port != coordinator.port,
          'coordinator migration');
      final migratedMs = clock.elapsedMilliseconds;
      final stored =
          await probe.committedOffsets(group, topic, partitions: {0, 1, 2});
      await Future<void>.delayed(const Duration(seconds: 2));
      final closedAutomatically = termination != null;
      if (!closedAutomatically) consumer.stop();
      final result = await running.timeout(const Duration(seconds: 15));
      final receipt = <String, Object?>{
        'mode': mode,
        'commit_after_processing': commit,
        'migration_probe_uses_surviving_bootstraps': true,
        'bootstrap_node_order': [1, 2, 3],
        'victim_is_first_bootstrap': victim == 1,
        'session_error_type': result.lastSession?.error.runtimeType.toString(),
        'session_error_operation':
            result.lastSession?.error is KafkaBrokerException
                ? (result.lastSession!.error as KafkaBrokerException).operation
                : null,
        'session_error_code': result.lastSession?.error is KafkaBrokerException
            ? (result.lastSession!.error as KafkaBrokerException).errorCode
            : null,
        'victim': victim,
        'new_coordinator': ports.indexOf(migrated.port) + 1,
        'initial_generation': initial!.generationId,
        'generations_started': result.generationsStarted,
        'callbacks': seen,
        'revoked_ms': revokedMs,
        'migration_ms': migratedMs,
        'closed_automatically': closedAutomatically,
        'consumer_reason': result.reason.name,
        'session_reason': result.lastSession?.reason.name,
        'consumer_error': result.error.toString(),
        'session_error': result.lastSession?.error.toString(),
        'processing_reason': result.lastProcessing?.reason.name,
        'commit_outcomes': result.lastCommit?.partitions
            .map((p, v) => MapEntry('$p', v.outcome.name)),
        'stored_before': before.map((p, v) => MapEntry('$p', v?.nextOffset)),
        'stored_after': stored.map((p, v) => MapEntry('$p', v?.nextOffset)),
        'cleanup_errors': result.cleanupErrors.map((e) => '$e').toList(),
        'stale_assignment_current': consumer.isCurrent(initial!),
      };
      receipts.add(receipt);
      print('COORDINATOR_CRASH_BASELINE ${jsonEncode(receipt)}');
      expect(stored.map((p, v) => MapEntry(p, v?.nextOffset)),
          before.map((p, v) => MapEntry(p, v?.nextOffset)));
      expect(result.cleanupErrors, isEmpty);
      await probe.close();
      final restart = await fault.control(directory, 'start:$victim');
      expect(restart['nodes']['$victim']['running'], isTrue);
    }
    await reader.close();
    // Deliberately red on the sealed baseline. This tests the missing
    // automatic recovery capability, not merely Kafka's coordinator election.
    expect(receipts.every((r) => (r['generations_started'] as int) > 1), isTrue,
        reason:
            'Managed consumer must automatically replace crashed coordinator generation');
  },
      skip: mode == null ? 'Owned secure fixture required' : false,
      timeout: const Timeout(Duration(seconds: 220)));
}
