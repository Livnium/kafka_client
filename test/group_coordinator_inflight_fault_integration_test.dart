// Public managed consumer with the coordinator victim DOWN through replacement
// and verified commits. Restart occurs only after the result and survivor checks.
// Original failure/restoration evidence is retained separately; no TLS bypass.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';
import 'combined_secure_fault_integration_test.dart' as fault;
import 'group_membership_integration_test.dart' as membership;
import 'group_coordinator_crash_baseline_integration_test.dart' show waitFor;

void main() {
  final env = Platform.environment;
  final mode = env['KAFKA_COMBINED_MODE'];
  test('$mode: survivor-only in-flight coordinator crash recovery', () async {
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
    for (final optIn in [false, true]) {
      final reader = client();
      addTearDown(reader.close);
      await fault.readUntil(
          reader,
          () => reader.metadata({topic}),
          (m) => fault.metadataReady(
              m, (ps) => ps.every((p) => p.isrNodes.toSet().length == 3)),
          'restored ISR');
      final offsets = <int, int>{};
      final expected = <String, String>{};
      for (var p = 0; p < 3; p++) {
        final values =
            List.generate(p == 0 ? 2 : 1, (i) => '$mode-$optIn-$p-$i');
        final produced = await reader.produce(topic,
            values.map((v) => KafkaRecord(value: utf8.encode(v))).toList(),
            partition: p, acks: -1);
        offsets[p] =
            produced!.responses.single.partitionResponses.single.baseOffset;
        for (var i = 0; i < values.length; i++) {
          expected['$p:${offsets[p]! + i}'] = values[i];
        }
      }
      late String group;
      Broker? coordinator;
      for (var i = 0; i < 30; i++) {
        group = membership.uniqueGroup('inflight-$optIn-$i');
        final found = await membership.checkedCoordinator(reader, group, topic);
        if (found.port != ports.first) {
          coordinator = found;
          break;
        }
      }
      expect(coordinator, isNotNull,
          reason: 'Explicit non-first-bootstrap coordinator scenario');
      final victim = ports.indexOf(coordinator!.port) + 1;
      final consumer = KafkaGroupConsumer(
          brokers: brokers,
          groupId: group,
          topics: {topic},
          tls: tls,
          sasl: sasl,
          allowTlsBootstrapFallback: true,
          tlsFetchRecovery: KafkaTlsFetchRecovery(
              maxAttempts: 20,
              window: const Duration(seconds: 10),
              retryDelay: const Duration(milliseconds: 500)),
          maxConnections: 2,
          connectionTimeout: const Duration(milliseconds: 700),
          heartbeatInterval: const Duration(milliseconds: 300),
          localExpiry: const Duration(seconds: 8),
          sessionTimeout: const Duration(seconds: 10),
          leaveTimeout: const Duration(milliseconds: 500),
          processingTimeout: const Duration(seconds: 45),
          pollInterval: const Duration(milliseconds: 20),
          commitAfterProcessing: true,
          missingOffsets: {topic: offsets},
          maxRejoins: 3,
          rejoinOnCoordinatorLoss: optIn,
          rejoinDelay: const Duration(milliseconds: 250));
      final release = Completer<void>();
      addTearDown(() async {
        if (!release.isCompleted) release.complete();
        await fault.control(directory, 'pass');
        consumer.stop();
        await consumer.closed;
      });
      final callbacks = <Map<String, Object?>>[];
      KafkaGroupAssignment? oldToken;
      var held = false;
      final running = consumer.run((record) async {
        final key = '${record.partition}:${record.offset}';
        expect(utf8.decode(record.record.value!), expected[key]);
        expect(consumer.isCurrent(record.assignment), isTrue);
        oldToken ??= record.assignment;
        callbacks.add({
          'key': key,
          'old': identical(record.assignment, oldToken),
          'generation': record.assignment.generationId
        });
        if (identical(record.assignment, oldToken) &&
            record.partition == 0 &&
            record.offset == offsets[0]! + 1) {
          held = true;
          await release.future;
        }
      });
      await waitFor(() => held, 'held second record');
      final committed =
          await reader.committedOffsets(group, topic, partitions: {0, 1, 2});
      expect(committed[0]?.nextOffset, offsets[0]! + 1);
      expect(committed[1], isNull);
      expect(committed[2], isNull);
      final offsetsMetadata = await reader.metadata({'__consumer_offsets'});
      var groupHash = 0;
      for (final unit in group.codeUnits) {
        groupHash = (31 * groupHash + unit) & 0xffffffff;
      }
      final signedHash =
          groupHash >= 0x80000000 ? groupHash - 0x100000000 : groupHash;
      final positiveHash = signedHash == -2147483648 ? 0 : signedHash.abs();
      final groupPartition =
          positiveHash % offsetsMetadata.topics.single.partitions.length;
      final offsetShard = offsetsMetadata.topics.single.partitions
          .singleWhere((p) => p.partitionIndex == groupPartition);
      final offsetShardReceipt = {
        'partition': groupPartition,
        'leader': offsetShard.leaderId,
        'code': offsetShard.errorCode,
        'replicas': offsetShard.replicaNodes,
        'isr': offsetShard.isrNodes
      };
      expect(offsetShard.leaderId, victim,
          reason: 'Actual group shard matches discovered coordinator');
      print('COORDINATOR_PREFAULT_POSITIONS ${jsonEncode({
            'mode': mode,
            'opt_in': optIn,
            'group': group,
            'topic': topic,
            'victim': victim,
            'offsets_topic_shard': offsetShardReceipt,
            'confirmed': committed.map((p, v) => MapEntry('$p', v?.nextOffset))
          })}');
      await reader.close();
      await fault.settled(reader);
      await Future<void>.delayed(const Duration(milliseconds: 350));
      final blackhole = await fault.control(directory, 'blackhole');
      final before = fault.proxy(blackhole, victim)['dropped_upstream'] as int;
      var dropped = 0;
      final blocked = Stopwatch()..start();
      while (dropped == 0 && blocked.elapsed < const Duration(seconds: 2)) {
        await Future<void>.delayed(const Duration(milliseconds: 20));
        final snapshot = await fault.control(directory, 'status');
        dropped =
            (fault.proxy(snapshot, victim)['dropped_upstream'] as int) - before;
      }
      expect(dropped, greaterThan(0),
          reason:
              'Opaque upstream ciphertext observed after all observers closed and Fetch paused');
      final crash = await fault.control(directory, 'crash:$victim');
      expect(crash['nodes']['$victim']['running'], isFalse);
      await fault.control(directory, 'pass');
      final blockedMs = blocked.elapsedMilliseconds;
      expect(blockedMs, lessThan(8000));
      await waitFor(
          () => !consumer.isCurrent(oldToken!), 'old assignment revoked');
      final preliminary = {
        'mode': mode,
        'opt_in': optIn,
        'group': group,
        'topic': topic,
        'victim': victim,
        'bootstrap_node_order': [1, 2, 3],
        'condition':
            'non-first-bootstrap coordinator, callback-held Fetch pause, observer clients closed',
        'observed_opaque_dropped_upstream_bytes': dropped,
        'blackhole_through_restore_ms': blockedMs,
      };
      print('COORDINATOR_INFLIGHT_FAULT ${jsonEncode(preliminary)}');
      final probe = client(
          endpoints:
              brokers.where((b) => b.port != coordinator!.port).toList());
      addTearDown(probe.close);
      final migrated = await fault.readUntil(
          probe,
          () => probe.findGroupCoordinator(group),
          (b) => b.port != coordinator!.port,
          'survivor coordinator migration');
      final beforeRelease =
          await probe.committedOffsets(group, topic, partitions: {0, 1, 2});
      print('COORDINATOR_FIRST_MIGRATED_OFFSETS ${jsonEncode({
            'mode': mode,
            'opt_in': optIn,
            'group': group,
            'topic': topic,
            'new_coordinator': ports.indexOf(migrated.port) + 1,
            'expected': committed.map((p, v) => MapEntry('$p', v?.nextOffset)),
            'observed':
                beforeRelease.map((p, v) => MapEntry('$p', v?.nextOffset)),
            'outer_offset_retries': 0
          })}');
      expect(beforeRelease.map((p, v) => MapEntry(p, v?.nextOffset)),
          committed.map((p, v) => MapEntry(p, v?.nextOffset)),
          reason:
              'First successful migrated read must preserve prior confirmed position; never wait to mask null');
      Future<Map<String, Object?>> survivorMetadata() async {
        final m = await probe.metadata({topic});
        expect(m.topics.single.errorCode, 0);
        var warningCount = 0;
        for (final p in m.topics.single.partitions) {
          expect([0, 9], contains(p.errorCode));
          final leaders =
              m.brokers.where((b) => b.nodeId == p.leaderId).toList();
          expect(p.leaderId, greaterThanOrEqualTo(0));
          expect(p.leaderId, isNot(victim));
          expect(leaders, hasLength(1));
          expect(leaders.single.host, isNotEmpty);
          expect(leaders.single.port, inInclusiveRange(1, 65535));
          if (p.errorCode == 9) warningCount++;
        }
        expect(warningCount, greaterThan(0),
            reason: 'Actual qualified Metadata-v0 warning observed');
        return {
          'qualified_warning_count': warningCount,
          'brokers': m.brokers
              .map((b) => {'node': b.nodeId, 'host': b.host, 'port': b.port})
              .toList(),
          'partitions': m.topics.single.partitions
              .map((p) => {
                    'partition': p.partitionIndex,
                    'code': p.errorCode,
                    'leader': p.leaderId,
                    'replicas': p.replicaNodes,
                    'isr': p.isrNodes
                  })
              .toList()
        };
      }

      final metadataBeforeRelease = await survivorMetadata();
      final downBeforeRelease = await fault.control(directory, 'status');
      expect(downBeforeRelease['nodes']['$victim']['running'], isFalse);
      expect(downBeforeRelease['nodes']['$victim']['generation'],
          crash['nodes']['$victim']['generation']);
      expect(callbacks, hasLength(2));
      expect(consumer.generationsStarted, 1);
      expect(consumer.isCurrent(oldToken!), isFalse);
      release.complete();
      if (optIn) {
        await waitFor(
            () =>
                callbacks.length == 5 ||
                consumer.state == KafkaGroupConsumerState.closed,
            'replacement processing');
        if (callbacks.length == 5) {
          await fault.readUntil(
              probe,
              () => probe.committedOffsets(group, topic, partitions: {0, 1, 2}),
              (values) =>
                  values[0]?.nextOffset == offsets[0]! + 2 &&
                  values[1]?.nextOffset == offsets[1]! + 1 &&
                  values[2]?.nextOffset == offsets[2]! + 1,
              'replacement commits');
          consumer.stop();
        }
      }
      final result = await running.timeout(const Duration(seconds: 20));
      final stored =
          await probe.committedOffsets(group, topic, partitions: {0, 1, 2});
      final metadataAfterResult = await survivorMetadata();
      final downAfterResult = await fault.control(directory, 'status');
      expect(downAfterResult['nodes']['$victim']['running'], isFalse);
      expect(downAfterResult['nodes']['$victim']['generation'],
          crash['nodes']['$victim']['generation']);
      final receipt = {
        ...preliminary,
        'survivor_only_recovery_scenario': true,
        'broker_restored_before_result': false,
        'victim_control_before_release': downBeforeRelease['nodes']['$victim'],
        'victim_control_after_result': downAfterResult['nodes']['$victim'],
        'metadata_before_release': metadataBeforeRelease,
        'metadata_after_result': metadataAfterResult,
        'prefault_offset_shard': offsetShardReceipt,
        'new_coordinator': ports.indexOf(migrated.port) + 1,
        'migration_before_release': true,
        'generations_started': result.generationsStarted,
        'consumer_reason': result.reason.name,
        'consumer_error': result.error.toString(),
        'callbacks': callbacks,
        'old_cause_observation': optIn
            ? 'Not publicly exposed after rejoin; inferred from isolated fault and eligibility checks'
            : 'Direct final session evidence',
        'last_session_reason': result.lastSession?.reason.name,
        'last_session_error_type':
            result.lastSession?.error.runtimeType.toString(),
        'stable_heartbeat_marker':
            result.lastSession?.stableHeartbeatFailure != null,
        'stored_before_release':
            beforeRelease.map((p, v) => MapEntry('$p', v?.nextOffset)),
        'no_new_generation_before_release': true,
        'old_token_revoked_before_release': true,
        'stored_after': stored.map((p, v) => MapEntry('$p', v?.nextOffset)),
        'cleanup_errors': result.cleanupErrors.map((e) => '$e').toList(),
      };
      print('COORDINATOR_INFLIGHT_RESULT ${jsonEncode(receipt)}');
      expect(result.cleanupErrors, isEmpty);
      if (optIn) {
        expect(result.generationsStarted, 2);
        expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
        expect(callbacks.map((c) => c['key']).toList(), [
          '0:${offsets[0]}',
          '0:${offsets[0]! + 1}',
          '0:${offsets[0]! + 1}',
          '1:${offsets[1]}',
          '2:${offsets[2]}'
        ]);
        expect(callbacks.skip(2).every((c) => c['old'] == false), isTrue);
        expect(stored[0]?.nextOffset, offsets[0]! + 2);
        expect(stored[1]?.nextOffset, offsets[1]! + 1);
        expect(stored[2]?.nextOffset, offsets[2]! + 1);
      } else {
        expect(result.generationsStarted, 1);
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(callbacks, hasLength(2));
        final original = result.lastSession!;
        expect(original.reason, KafkaGroupStopReason.failure);
        expect(original.error, isA<KafkaConnectionException>());
        final marker = original.stableHeartbeatFailure;
        expect(marker, isNotNull);
        expect(identical(marker!.error, original.error), isTrue);
        expect(identical(marker.stackTrace, original.stackTrace), isTrue);
      }
      await probe.close();
      await fault.settled(probe);
      // No broker restart is permitted until all result assertions above.
      final restarted = await fault.control(directory, 'start:$victim');
      expect(restarted['nodes']['$victim']['running'], isTrue);
    }
  },
      skip: mode == null ? 'Owned secure fixture required' : false,
      timeout: const Timeout(Duration(seconds: 220)));
}
