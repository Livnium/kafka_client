// Low-level classic protocol interoperability only. This is NOT a high-level
// ownership, revocation, fencing, or automatic membership lifecycle guarantee.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';
import 'operational_integration_test.dart' as fixture;

String uniqueGroup(String label) =>
    'classic-$label-$pid-${DateTime.now().microsecondsSinceEpoch}';

Future<Broker> checkedCoordinator(
    KafkaClient client, String group, String topic) async {
  // Test precondition only: a cold owned broker returned NOT_COORDINATOR (16)
  // on the first JoinGroup before __consumer_offsets was ready. Establish
  // readiness using the existing bounded, read-only OffsetFetch retry path,
  // then rediscover the coordinator. Never replay a failed membership mutation.
  await client.committedOffsets(group, topic, partitions: {0});
  final broker = await client.findGroupCoordinator(group);
  final versions = await client.send(ApiVersionsRequest(), broker: broker)
      as ApiVersionsResponse;
  expect(versions.errorCode, 0);
  for (final required in {11: 4, 14: 2, 12: 2, 13: 2, 8: 6}.entries) {
    expect(
        versions.apiVersions.any((v) =>
            v.apiKey == required.key &&
            v.minVersion <= required.value &&
            v.maxVersion >= required.value),
        isTrue,
        reason: 'Required API ${required.key} v${required.value}');
  }
  return broker;
}

Future<JoinGroupResponse> joinMember(
    KafkaClient c, Broker b, String group, String topic,
    {String member = '', int session = 10000}) async {
  Future<JoinGroupResponse> send(String id) async => await c.send(
      JoinGroupRequest(group,
          sessionTimeoutMs: session,
          rebalanceTimeoutMs: 20000,
          memberId: id,
          protocols: [
            GroupJoinProtocol(
                'range', ConsumerGroupSubscription([topic]).toBytes())
          ]),
      broker: b) as JoinGroupResponse;
  var joined = await send(member);
  if (member.isEmpty) {
    expect(joined.errorCode, 79);
    expect(joined.memberId, isNotEmpty);
    joined = await send(joined.memberId);
  }
  expect(joined.errorCode, 0);
  expect(joined.protocolName, 'range');
  return joined;
}

Future<SyncGroupResponse> syncMember(KafkaClient c, Broker b, String group,
    String topic, JoinGroupResponse j) async {
  final assignments = <GroupAssignment>[];
  if (j.leaderId == j.memberId) {
    final members = j.members.toList()
      ..sort((a, b) => a.memberId.compareTo(b.memberId));
    for (var i = 0; i < members.length; i++) {
      final sub = ConsumerGroupSubscription.fromBytes(members[i].metadata);
      expect(sub.topics, contains(topic));
      assignments.add(GroupAssignment(
          members[i].memberId,
          ConsumerGroupAssignment(i == 0
                  ? {
                      topic: [0]
                    }
                  : {})
              .toBytes()));
    }
  }
  final s = await c.send(
      SyncGroupRequest(group,
          generationId: j.generationId,
          memberId: j.memberId,
          assignments: assignments),
      broker: b) as SyncGroupResponse;
  expect(s.errorCode, 0);
  ConsumerGroupAssignment.fromBytes(s.assignment);
  return s;
}

Future<int> memberCommit(KafkaClient c, Broker b, String group, String topic,
    JoinGroupResponse j, int offset,
    {int? generation, String? member}) async {
  final result = await c.send(
      MemberOffsetCommitRequest(group, topic, {0: offset},
          generationId: generation ?? j.generationId,
          memberId: member ?? j.memberId,
          metadata: 'member-π'),
      broker: b) as GroupOffsetCommitResponse;
  return result.topics.single.partitions.single.errorCode;
}

Future<int> heartbeat(
        KafkaClient c, Broker b, String group, JoinGroupResponse j) async =>
    (await c.send(
            HeartbeatRequest(group,
                generationId: j.generationId, memberId: j.memberId),
            broker: b) as HeartbeatResponse)
        .errorCode;
Future<void> leave(
    KafkaClient c, Broker b, String group, JoinGroupResponse j) async {
  expect(
      (await c.send(LeaveGroupRequest(group, memberId: j.memberId), broker: b)
              as LeaveGroupResponse)
          .errorCode,
      0);
}

void main() {
  final env = Platform.environment;
  final port = int.tryParse(env['KAFKA_OPERATIONAL_PORT'] ?? '');
  final topic = env['KAFKA_OPERATIONAL_TOPIC'];
  final classpath = env['KAFKA_GROUP_MEMBERSHIP_ORACLE_CLASSPATH'];
  KafkaClient make() {
    final c = KafkaClient(
        brokers: [Broker('127.0.0.1', port!)],
        requestTimeout: const Duration(seconds: 35),
        connectionTimeout: const Duration(seconds: 3),
        maxReadRetries: 10,
        retryBaseDelay: const Duration(milliseconds: 100));
    addTearDown(() async {
      await c.close();
      await fixture.idle(c);
      expect(c.activeSubscriptionCount, 0);
      expect(c.connectionCount, 0);
    });
    return c;
  }

  Future<int?> read(KafkaClient c, String group) async =>
      (await c.committedOffsets(group, topic!, partitions: {0}))[0]?.nextOffset;
  group('real Kafka low-level classic membership', () {
    test(
        'single member, valid commit, stale generation and unknown member preserve offset',
        () async {
      final c = make(), reader = make();
      final g = uniqueGroup('single');
      final b = await checkedCoordinator(c, g, topic!);
      final j = await joinMember(c, b, g, topic);
      expect(j.leaderId, j.memberId);
      final s = await syncMember(c, b, g, topic, j);
      expect(ConsumerGroupAssignment.fromBytes(s.assignment).partitions, {
        topic: [0]
      });
      expect(await heartbeat(c, b, g, j), 0);
      expect(await memberCommit(c, b, g, topic, j, 123), 0);
      expect(await read(reader, g), 123);
      expect(
          await memberCommit(c, b, g, topic, j, 999,
              generation: j.generationId + 1),
          22);
      expect(await read(reader, g), 123);
      expect(
          await memberCommit(c, b, g, topic, j, 999, member: 'unknown-member'),
          25);
      expect(await read(reader, g), 123);
      await leave(c, b, g, j);
      expect(await heartbeat(c, b, g, j), 25);
      expect(await read(reader, g), 123);
    });
    test(
        'two members eager rebalance advances generation and rejects old generation',
        () async {
      final a = make(), c = make(), reader = make();
      final g = uniqueGroup('two');
      final b = await checkedCoordinator(a, g, topic!);
      final old = await joinMember(a, b, g, topic);
      await syncMember(a, b, g, topic, old);
      expect(await memberCommit(a, b, g, topic, old, 20), 0);
      final second = joinMember(c, b, g, topic);
      final deadline = DateTime.now().add(const Duration(seconds: 10));
      while (await heartbeat(a, b, g, old) == 0) {
        if (DateTime.now().isAfter(deadline)) fail('Rebalance not observed');
        await Future<void>.delayed(const Duration(milliseconds: 100));
      }
      final first = joinMember(a, b, g, topic, member: old.memberId);
      final joined = await Future.wait([first, second]);
      expect(joined[0].generationId, greaterThan(old.generationId));
      expect(joined[1].generationId, joined[0].generationId);
      final synced = await Future.wait([
        syncMember(a, b, g, topic, joined[0]),
        syncMember(c, b, g, topic, joined[1])
      ]);
      expect(
          synced
              .expand((s) =>
                  ConsumerGroupAssignment.fromBytes(s.assignment)
                      .partitions[topic] ??
                  <int>[])
              .toList(),
          [0]);
      expect(await memberCommit(a, b, g, topic, old, 999), 22);
      expect(await read(reader, g), 20);
      expect(await memberCommit(a, b, g, topic, joined[0], 21), 0);
      expect(await read(reader, g), 21);
      await leave(c, b, g, joined[1]);
      await leave(a, b, g, joined[0]);
    });
    test('expired member cannot heartbeat or overwrite committed offset',
        () async {
      final c = make(), reader = make();
      final g = uniqueGroup('expiry');
      final b = await checkedCoordinator(c, g, topic!);
      final j = await joinMember(c, b, g, topic, session: 6000);
      await syncMember(c, b, g, topic, j);
      expect(await memberCommit(c, b, g, topic, j, 30), 0);
      await Future<void>.delayed(const Duration(seconds: 9));
      expect(await heartbeat(c, b, g, j), 25);
      expect(await memberCommit(c, b, g, topic, j, 999), 25);
      expect(await read(reader, g), 30);
    });
    for (final javaLeader in [false, true]) {
      test(
          'official Java RangeAssignor coexistence, ${javaLeader ? 'Java' : 'Dart'} leader',
          () async {
        final c = make();
        final g = uniqueGroup(javaLeader ? 'java-leader' : 'dart-leader');
        final b = await checkedCoordinator(c, g, topic!);
        JoinGroupResponse? old;
        if (!javaLeader) {
          old = await joinMember(c, b, g, topic);
          await syncMember(c, b, g, topic, old);
        }
        final p = await Process.start(env['JAVA'] ?? 'java', [
          '-cp',
          classpath!,
          'GroupMembershipOracle',
          'peer',
          '127.0.0.1:$port',
          g,
          topic
        ]);
        final output = <String>[];
        final errors = StringBuffer();
        final stdoutSub = p.stdout
            .transform(utf8.decoder)
            .transform(const LineSplitter())
            .listen(output.add);
        final stderrSub = p.stderr.transform(utf8.decoder).listen(errors.write);
        Future<void> waitAssigned(int count) async {
          final deadline = DateTime.now().add(const Duration(seconds: 30));
          while (output.where((s) => s.startsWith('ORACLE_ASSIGNED')).length <
              count) {
            if (DateTime.now().isAfter(deadline)) {
              fail('Java assignment timed out: $output\n$errors');
            }
            await Future<void>.delayed(const Duration(milliseconds: 100));
          }
        }

        try {
          if (javaLeader) await waitAssigned(1);
          if (old != null) {
            final deadline = DateTime.now().add(const Duration(seconds: 15));
            while (await heartbeat(c, b, g, old) == 0) {
              if (DateTime.now().isAfter(deadline)) {
                fail('Java did not trigger rebalance');
              }
              await Future<void>.delayed(const Duration(milliseconds: 100));
            }
          }
          final j =
              await joinMember(c, b, g, topic, member: old?.memberId ?? '');
          expect(j.leaderId == j.memberId, !javaLeader);
          if (!javaLeader) {
            final java = j.members.singleWhere((m) => m.memberId != j.memberId);
            expect(
                ConsumerGroupSubscription.fromBytes(java.metadata).version, 3);
          }
          await syncMember(c, b, g, topic, j);
          await waitAssigned(javaLeader ? 2 : 1);
          expect(await heartbeat(c, b, g, j), 0);
          expect(await memberCommit(c, b, g, topic, j, 44), 0);
          expect(await read(make(), g), 44);
          await leave(c, b, g, j);
        } finally {
          p.kill();
          try {
            await p.exitCode.timeout(const Duration(seconds: 10));
          } on TimeoutException {
            p.kill(ProcessSignal.sigkill);
            await p.exitCode;
          }
          await stdoutSub.cancel();
          await stderrSub.cancel();
        }
      },
          skip: classpath == null
              ? 'Set KAFKA_GROUP_MEMBERSHIP_ORACLE_CLASSPATH to compiled official Java oracle'
              : false);
    }
  },
      skip: port == null || topic == null
          ? 'Run owned tool/run_operational_integration.py fixture'
          : false,
      timeout: const Timeout(Duration(minutes: 2)));
}
