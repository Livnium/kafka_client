// Owned real Kafka evidence only: this does not assert a stale-leader TLS
// rejection occurred, nor claim transparent coordinator failover.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';

import 'combined_secure_fault_integration_test.dart' as fault;
import 'group_delivery_fixture.dart';
import 'group_membership_integration_test.dart' as membership;

Future<void> until(bool Function() ready) async {
  final clock = Stopwatch()..start();
  while (!ready()) {
    if (clock.elapsed > const Duration(seconds: 15)) {
      throw TimeoutException('Managed secure fault condition not reached');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

void main() {
  final env = Platform.environment;
  final mode = env['KAFKA_COMBINED_MODE'];
  test('$mode: managed opt-in delivery after leader crash and expiry fencing',
      () async {
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
    final policy = KafkaTlsFetchRecovery(
        maxAttempts: 20,
        window: const Duration(seconds: 10),
        retryDelay: const Duration(milliseconds: 500));
    final reader = KafkaClient(
        brokers: brokers,
        tls: tls,
        sasl: sasl,
        allowTlsBootstrapFallback: true,
        maxConnections: 2,
        maxReadRetries: 10,
        retryBaseDelay: const Duration(milliseconds: 100),
        connectionTimeout: const Duration(milliseconds: 700));
    addTearDown(reader.close);
    await fault.control(directory, 'pass');
    final initial = await fault.readUntil(
        reader,
        () => reader.metadata({topic}),
        (m) => fault.metadataReady(
            m, (ps) => ps.every((p) => p.isrNodes.toSet().length == 3)),
        'initial ISR');
    final victim = initial.topics.single.partitions
        .singleWhere((p) => p.partitionIndex == 0)
        .leaderId;
    final markers = <int, String>{};
    for (var p = 0; p < 3; p++) {
      markers[p] = '$mode-managed-$pid-$p';
      // Exactly one all-ISR Produce per marker. No ambiguous write replay.
      final produced = await reader.produce(
          topic, [KafkaRecord(value: utf8.encode(markers[p]!))],
          partition: p, acks: -1);
      expect(
          produced!.responses.single.partitionResponses.single.baseOffset, 0);
    }
    String? group;
    Broker? coordinator;
    for (var i = 0; i < 20; i++) {
      final candidate = membership.uniqueGroup('managed-crash-$i');
      final found =
          await membership.checkedCoordinator(reader, candidate, topic);
      if (found.port != ports[victim - 1]) {
        group = candidate;
        coordinator = found;
        break;
      }
    }
    expect(group, isNotNull, reason: 'Require a non-victim coordinator');
    final session = KafkaGroupSession(
        brokers: brokers,
        groupId: group!,
        topics: {topic},
        tls: tls,
        sasl: sasl,
        allowTlsBootstrapFallback: true,
        tlsFetchRecovery: policy,
        maxConnections: 2,
        connectionTimeout: const Duration(milliseconds: 700),
        heartbeatInterval: const Duration(milliseconds: 500),
        localExpiry: const Duration(seconds: 8));
    addTearDown(session.close);
    final token = await session.start();
    expect(token.partitions[topic], [0, 1, 2]);
    final crashed = await fault.control(directory, 'crash:$victim');
    expect(crashed['nodes']['$victim']['running'], isFalse);
    expect(session.isCurrent(token), isTrue);
    // No metadata election wait and no outer Fetch retry. The data client is
    // created after the crash: this proves managed fault usability, not that
    // stale leader metadata or a specific TLS failure was actually observed.
    final delivery = session.subscribe(
        assignment: token,
        startOffsets: {
          topic: {0: 0, 1: 0, 2: 0}
        },
        pollInterval: const Duration(milliseconds: 20));
    final seen = <int>{};
    final probe = GroupDeliveryProbe(delivery, (record) {
      expect(record.assignment, same(token));
      expect(session.isCurrent(token), isTrue);
      expect(record.offset, 0);
      expect(utf8.decode(record.record.value!), markers[record.partition]);
      expect(seen.add(record.partition), isTrue,
          reason: 'No duplicate records');
    });
    await probe.received(3);
    expectCleanDeliveryStop(
        await delivery.cancel(), KafkaGroupDeliveryStopReason.cancelled);
    await probe.done.future;
    probe.checkHealthy();
    expect(probe.records, hasLength(3));
    expect((await session.closed).cleanupError, isNull);
    final elected = await fault.readUntil(
        reader,
        () => reader.metadata({topic}),
        (m) => fault.metadataReady(
            m, (ps) => ps.every((p) => p.leaderId != victim)),
        'elected survivors');
    final restarted = await fault.control(directory, 'start:$victim');
    expect(restarted['nodes']['$victim']['running'], isTrue);
    expect(restarted['nodes']['$victim']['generation'],
        greaterThan(crashed['nodes']['$victim']['generation'] as int));
    await fault.readUntil(
        reader,
        () => reader.metadata({topic}),
        (m) => fault.metadataReady(
            m, (ps) => ps.every((p) => p.isrNodes.toSet().length == 3)),
        'restored ISR');
    // Independent coordinator discovery is verified only after restoration;
    // it is not covered by the managed data client's Fetch recovery policy.
    expect(
        (await reader.committedOffsets(group, topic, partitions: {0, 1, 2}))
            .values
            .every((value) => value == null),
        isTrue);
    print('MANAGED_SECURE_LEADER_RESULT ${jsonEncode({
          'mode': mode,
          'victim': victim,
          'coordinator_node': ports.indexOf(coordinator!.port) + 1,
          'generation': token.generationId,
          'verified_records': probe.records.length,
          'same_assignment': true,
          'commits': 0,
          'restarted': true,
          'restored_isr': true,
          'stale_leader_tls_observed': false,
          'elected_leaders':
              elected.topics.single.partitions.map((p) => p.leaderId).toList(),
        })}');

    final consumerGroup = membership.uniqueGroup('managed-expiry');
    await membership.checkedCoordinator(reader, consumerGroup, topic);
    final consumer = KafkaGroupConsumer(
        brokers: brokers,
        groupId: consumerGroup,
        topics: {topic},
        commitAfterProcessing: true,
        missingOffsets: {
          topic: {0: 0, 1: 0, 2: 0}
        },
        tls: tls,
        sasl: sasl,
        allowTlsBootstrapFallback: true,
        tlsFetchRecovery: policy,
        maxRejoins: 3,
        maxConnections: 2,
        connectionTimeout: const Duration(milliseconds: 700),
        heartbeatInterval: const Duration(milliseconds: 300),
        localExpiry: const Duration(seconds: 2),
        sessionTimeout: const Duration(seconds: 6),
        leaveTimeout: const Duration(milliseconds: 500));
    final release = Completer<void>();
    addTearDown(() async {
      if (!release.isCompleted) release.complete();
      await fault.control(directory, 'pass');
      consumer.stop();
      await consumer.closed;
    });
    KafkaGroupAssignment? held;
    var callbacks = 0;
    final running = consumer.run((record) async {
      callbacks++;
      held = record.assignment;
      expect(consumer.isCurrent(record.assignment), isTrue);
      expect(record.offset, 0);
      expect(utf8.decode(record.record.value!), markers[record.partition]);
      await release.future;
    });
    await until(() => callbacks == 1);
    final before = await fault.control(directory, 'blackhole');
    await until(() => !consumer.isCurrent(held!));
    final during = await fault.control(directory, 'status');
    final dropped = fault
            .proxies(during)
            .fold<int>(0, (n, p) => n + (p['dropped_upstream'] as int)) -
        fault
            .proxies(before)
            .fold<int>(0, (n, p) => n + (p['dropped_upstream'] as int));
    expect(dropped, greaterThan(0),
        reason: 'Actually observed blocked traffic');
    expect(
        fault.proxies(during).every((p) => p['mode'] == 'blackhole'), isTrue);
    // Restore connectivity BEFORE releasing old work. It still cannot commit,
    // resume buffered callbacks or authorize an automatic replacement session.
    await fault.control(directory, 'pass');
    release.complete();
    final result = await running.timeout(const Duration(seconds: 15));
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(result.generationsStarted, 1);
    expect(result.lastSession!.reason, KafkaGroupStopReason.localExpiry);
    expect(callbacks, 1);
    expect(result.lastCommit, isNull);
    expect(result.cleanupErrors, isEmpty);
    expect(consumer.isCurrent(held!), isFalse);
    expect(consumer.assignment, isNull);
    expect(
        (await reader
                .committedOffsets(consumerGroup, topic, partitions: {0, 1, 2}))
            .values
            .every((value) => value == null),
        isTrue);
    await reader.close();
    await fault.settled(reader);
    expect(reader.connectionCount, 0);
    await until(() => consumer.state == KafkaGroupConsumerState.closed);
    var closed = await fault.control(directory, 'status');
    final cleanup = Stopwatch()..start();
    while (fault
            .proxies(closed)
            .any((p) => p['active'] != 0 || p['workers'] != 0) &&
        cleanup.elapsed < const Duration(seconds: 3)) {
      await Future<void>.delayed(const Duration(milliseconds: 20));
      closed = await fault.control(directory, 'status');
    }
    for (final proxy in fault.proxies(closed)) {
      expect(proxy['active'], 0);
      expect(proxy['workers'], 0);
      expect(proxy['rejected'], 0);
    }
    print('MANAGED_SECURE_EXPIRY_RESULT ${jsonEncode({
          'mode': mode,
          'dropped_upstream': dropped,
          'stop_reason': result.lastSession!.reason.name,
          'generations_started': result.generationsStarted,
          'callbacks': callbacks,
          'commits': 0,
          'old_assignment_current': false,
          'cleanup_errors': result.cleanupErrors.length,
          'proxy_active':
              fault.proxies(closed).map((p) => p['active']).toList(),
          'proxy_workers':
              fault.proxies(closed).map((p) => p['workers']).toList(),
        })}');
  },
      skip: mode == null ? 'Run owned combined secure fault runner' : false,
      timeout: const Timeout(Duration(minutes: 3)));
}
