// Opt-in, disposable-broker tests for single-generation managed local assignment validity.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/kafka_protocol.dart'
    show HeartbeatRequest, HeartbeatResponse;
import 'package:test/test.dart';

import 'group_membership_integration_test.dart' as membership;
import 'operational_integration_test.dart' as fixture;

void main() {
  final env = Platform.environment;
  final port = int.tryParse(env['KAFKA_OPERATIONAL_PORT'] ?? '');
  final topic = env['KAFKA_OPERATIONAL_TOPIC'];
  final control = env['KAFKA_OPERATIONAL_CONTROL'];
  final classpath = env['KAFKA_GROUP_MEMBERSHIP_ORACLE_CLASSPATH'];
  KafkaClient reader() {
    final c = KafkaClient(
        brokers: [Broker('127.0.0.1', port!)],
        maxReadRetries: 10,
        retryBaseDelay: const Duration(milliseconds: 100));
    addTearDown(c.close);
    return c;
  }

  KafkaGroupSession session(String group) {
    final s = KafkaGroupSession(
        brokers: [Broker('127.0.0.1', port!)],
        groupId: group,
        topics: {topic!},
        sessionTimeout: const Duration(seconds: 6),
        heartbeatInterval: const Duration(seconds: 1),
        localExpiry: const Duration(seconds: 4));
    addTearDown(s.close);
    return s;
  }

  Future<void> warm(KafkaClient c, String group) async {
    await membership.checkedCoordinator(c, group, topic!);
  }

  Future<int?> offset(KafkaClient c, String group) async =>
      (await c.committedOffsets(group, topic!, partitions: {0}))[0]?.nextOffset;

  group('real Kafka managed single-generation membership', () {
    test(
        'stable beyond two broker windows; close revokes immediately and leaves without committing',
        () async {
      final c = reader(), group = membership.uniqueGroup('managed-stable');
      await warm(c, group);
      final before = await offset(c, group);
      expect(before, isNull);
      final s = session(group);
      final token = await s.start();
      expect(token.partitions, {
        topic!: [0]
      });
      expect(token.generationId, greaterThanOrEqualTo(0));
      expect(token.memberId, isNotEmpty);
      expect(s.assignment, same(token));
      // Closing an unrelated application client must not own control transport.
      final unrelated = reader();
      await unrelated.close();
      for (var i = 0; i < 13; i++) {
        await Future<void>.delayed(const Duration(seconds: 1));
        expect(s.isCurrent(token), isTrue);
        s.assertCurrent(token);
        expect(s.assignment, same(token));
      }
      expect(await offset(c, group), before);
      final coordinator = await c.findGroupCoordinator(group);
      Future<int> brokerHeartbeat() async => (await c.send(
              HeartbeatRequest(group,
                  generationId: token.generationId, memberId: token.memberId),
              broker: coordinator) as HeartbeatResponse)
          .errorCode;
      expect(await brokerHeartbeat(), 0);
      // Let the just-admitted heartbeat settle before testing eligible Leave.
      await Future<void>.delayed(const Duration(milliseconds: 250));
      final ending = s.close();
      expect(s.isCurrent(token), isFalse);
      expect(s.assignment, isNull);
      expect(() => s.assertCurrent(token), throwsStateError);
      expect((await s.revoked).reason, KafkaGroupStopReason.closed);
      final end = await ending;
      expect(end.leaveOutcome, KafkaGroupLeaveOutcome.confirmed);
      expect(await brokerHeartbeat(), 25);
      expect(s.state, KafkaGroupSessionState.closed);
      expect(await s.closed, same(end));
      expect(await s.close(), same(end));
      await expectLater(s.start(), throwsStateError);
      expect(await offset(c, group), before);
    });

    test('second session causes eager error 27; first never rejoins', () async {
      final c = reader(), group = membership.uniqueGroup('managed-rebalance');
      await warm(c, group);
      final first = session(group), second = session(group);
      final old = await first.start();
      final joining = second.start();
      final revoked = await first.revoked.timeout(const Duration(seconds: 10));
      expect(revoked.reason, KafkaGroupStopReason.brokerError);
      expect(
          revoked.error,
          isA<KafkaBrokerException>()
              .having((e) => e.errorCode, 'rebalance error', 27));
      expect(first.isCurrent(old), isFalse);
      expect(first.assignment, isNull);
      expect(() => first.assertCurrent(old), throwsStateError);
      final current = await joining;
      expect(current.generationId, greaterThan(old.generationId));
      expect(second.isCurrent(old), isFalse);
      expect(first.isCurrent(current), isFalse);
      expect(current.partitions, {
        topic!: [0]
      });
      await first.closed;
      await Future<void>.delayed(const Duration(seconds: 7));
      expect(first.state, KafkaGroupSessionState.closed);
      expect(first.assignment, isNull);
      expect(second.isCurrent(current), isTrue);
      await expectLater(first.start(), throwsStateError);
    });

    test('owned broker crash revokes permanently even after broker restart',
        () async {
      final c = reader(), group = membership.uniqueGroup('managed-crash');
      await warm(c, group);
      final s = session(group);
      final current = await s.start();
      var stopped = false;
      try {
        await fixture.control(control!, 'crash');
        stopped = true;
        final revoked = await s.revoked.timeout(const Duration(seconds: 8));
        expect(
            revoked.reason,
            anyOf(KafkaGroupStopReason.failure,
                KafkaGroupStopReason.localExpiry));
        expect(s.isCurrent(current), isFalse);
        expect(s.assignment, isNull);
        await s.closed;
        await fixture.control(control, 'start');
        stopped = false;
        await warm(c, group);
        await Future<void>.delayed(const Duration(seconds: 7));
        expect(s.state, KafkaGroupSessionState.closed);
        expect(s.isCurrent(current), isFalse);
        await expectLater(s.start(), throwsStateError);
      } finally {
        if (stopped) await fixture.control(control!, 'start');
      }
    },
        skip: control == null
            ? 'Requires owned fixture control directory'
            : false);

    test(
        'official Java leader assigns managed follower and permits empty member',
        () async {
      final c = reader(), group = membership.uniqueGroup('managed-java');
      await warm(c, group);
      final process = await Process.start(env['JAVA'] ?? 'java', [
        '-cp',
        classpath!,
        'GroupMembershipOracle',
        'peer',
        '127.0.0.1:$port',
        group,
        topic!
      ]);
      final output = <String>[];
      final errors = StringBuffer();
      final out = process.stdout
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen(output.add);
      final err = process.stderr.transform(utf8.decoder).listen(errors.write);
      Future<void> assigned(int count) async {
        final deadline = DateTime.now().add(const Duration(seconds: 30));
        while (
            output.where((line) => line.startsWith('ORACLE_ASSIGNED')).length <
                count) {
          if (DateTime.now().isAfter(deadline)) {
            fail('Java assignment timeout: $output\n$errors');
          }
          await Future<void>.delayed(const Duration(milliseconds: 100));
        }
      }

      try {
        await assigned(1);
        final s = session(group);
        final current = await s.start();
        await assigned(2);
        final javaCount = int.parse(output
            .lastWhere((line) => line.startsWith('ORACLE_ASSIGNED'))
            .split(' ')
            .last);
        final nativeCount =
            current.partitions.values.fold<int>(0, (a, b) => a + b.length);
        expect(javaCount + nativeCount, 1);
        // The official consumer client ID sorts before dart_kafka. With one
        // partition, Java owns it and the managed follower is valid but empty.
        expect(javaCount, 1);
        expect(nativeCount, 0);
        expect(current.partitions.values.expand((ids) => ids), isEmpty);
        await Future<void>.delayed(const Duration(seconds: 7));
        expect(s.isCurrent(current), isTrue);
        await s.close();
      } finally {
        process.kill();
        try {
          await process.exitCode.timeout(const Duration(seconds: 10));
        } on TimeoutException {
          process.kill(ProcessSignal.sigkill);
          await process.exitCode;
        }
        await out.cancel();
        await err.cancel();
      }
    },
        skip: classpath == null
            ? 'Requires compiled official Java oracle'
            : false);
  },
      skip: port == null || topic == null
          ? 'Run owned operational fixture'
          : false,
      timeout: const Timeout(Duration(minutes: 2)));
}
