import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/src/group_consumer.dart';
import 'package:kafka_client/src/group_membership_protocol.dart';
import 'package:kafka_client/src/group_offsets_protocol.dart';
import 'package:test/test.dart';

import 'classic_group_session_test.dart' as session_fixture;
import 'group_consumer_controller_test.dart' as fixture;
import 'group_processing_controller_test.dart' as processing_fixture;

// Fail through the real scheduled stable heartbeat, never by constructing
// termination provenance or labelling an application failure "Heartbeat".
void loseCoordinator(fixture.ConsumerCycle cycle, Object error) {
  cycle.driver.onHeartbeat = () async {
    if (error is KafkaBrokerException) {
      return HeartbeatResponse(1, 0, error.errorCode);
    }
    throw error;
  };
  cycle.clock.advance(session_fixture.tick);
}

class SubstitutedTerminationCycle extends fixture.ConsumerCycle {
  final bool replaceError;
  SubstitutedTerminationCycle({required this.replaceError});
  @override
  Future<KafkaGroupTermination> close() async {
    final original = await super.close();
    return KafkaGroupTermination(
        reason: original.reason,
        error: replaceError ? KafkaConnectionException() : original.error,
        stackTrace: replaceError ? original.stackTrace : StackTrace.current,
        leaveOutcome: original.leaveOutcome,
        cleanupError: original.cleanupError,
        stableHeartbeatFailure: original.stableHeartbeatFailure);
  }
}

// The native runner commits each serial record immediately. Seed a second
// acknowledged partition through the real ledger's internal admission seam to
// exercise its more general mixed-partition evidence at the runner boundary.
class MixedCommitCycle extends fixture.ConsumerCycle {
  MixedCommitCycle()
      : super(partitions: const {
          'a': [0, 1]
        });
  @override
  KafkaGroupProcessing processing(
      KafkaGroupAssignment token, Map<String, Map<int, int>> positions) {
    final processing = super.processing(token, positions);
    final record =
        KafkaGroupRecord(token, 'a', 1, KafkaRecord(offset: 10, value: [10]));
    progress!.admit(record);
    progress!.acknowledge(record);
    return processing;
  }
}

void main() {
  final controllers = <GroupConsumerController>[];
  final releases = <void Function()>[];
  GroupConsumerController make(List<fixture.ConsumerCycle> cycles,
      {bool enabled = true,
      bool commit = false,
      int limit = 1,
      Map<String, Map<int, int>> fallback = const {},
      Duration delay = const Duration(milliseconds: 1)}) {
    var index = 1;
    final controller = GroupConsumerController(
        initial: cycles.first,
        createCycle: () => cycles[index++],
        commitAfterProcessing: commit,
        missingOffsets: fallback,
        maxRejoins: limit,
        rejoinDelay: delay,
        rejoinOnCoordinatorLoss: enabled);
    controllers.add(controller);
    return controller;
  }

  Completer<void> gate() {
    final result = Completer<void>();
    releases.add(() {
      if (!result.isCompleted) result.complete();
    });
    return result;
  }

  tearDown(() async {
    for (final c in controllers) {
      c.stop();
    }
    for (final release in releases) {
      release();
    }
    for (final c in controllers) {
      await c.closed.timeout(const Duration(seconds: 3));
    }
    controllers.clear();
    releases.clear();
  });

  for (final replaceError in [false, true]) {
    test(
        'copied heartbeat evidence cannot authorize substituted ${replaceError ? 'error' : 'stack'}',
        () async {
      final a = SubstitutedTerminationCycle(replaceError: replaceError)
        ..emit = false;
      final b = fixture.ConsumerCycle();
      final c = make([a, b]);
      final done = c.run((_) {});
      await fixture.until(() => c.state == KafkaGroupConsumerState.processing);
      loseCoordinator(a, KafkaConnectionException());
      final result = await done;
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.lastSession!.stableHeartbeatFailure, isNotNull);
      expect(b.starts, 0);
    });
  }

  final allowed = <String, Object Function()>{
    'broker15': () => KafkaBrokerException(15, 'Heartbeat'),
    'broker16': () => KafkaBrokerException(16, 'Heartbeat'),
    'connection': KafkaConnectionException.new,
    'socket': () => const SocketException('closed'),
    'timeout': () => TimeoutException('heartbeat lost'),
  };
  for (final entry in allowed.entries) {
    for (final enabled in [false, true]) {
      test('${entry.key} stable heartbeat replacement enabled=$enabled',
          () async {
        final a = fixture.ConsumerCycle()..emit = false;
        final b = fixture.ConsumerCycle()..emit = false;
        b.read = (_, __) async =>
            {0: const KafkaCommittedOffset(nextOffset: 90, leaderEpoch: -1)};
        final c = make([a, b], enabled: enabled);
        final done = c.run((_) {});
        await fixture
            .until(() => c.state == KafkaGroupConsumerState.processing);
        final old = c.assignment!;
        final error = entry.value();
        loseCoordinator(a, error);
        final revoked = await a.session.revoked;
        final closed = await a.session.closed;
        expect(revoked.stableHeartbeatFailure, isNotNull);
        expect(closed.stableHeartbeatFailure,
            same(revoked.stableHeartbeatFailure));
        expect(closed.error, same(revoked.error));
        expect(closed.stackTrace, same(revoked.stackTrace));
        if (error is! KafkaBrokerException) expect(closed.error, same(error));
        expect(c.isCurrent(old), isFalse);
        if (enabled) {
          await fixture.until(() => b.loaded != null);
          expect(b.loaded!['a']![0], 90);
          expect(b.reads, ['a']);
          c.stop();
        }
        final result = await done;
        expect(b.starts, enabled ? 1 : 0);
        expect(
            result.reason,
            enabled
                ? KafkaGroupConsumerStopReason.cancelled
                : KafkaGroupConsumerStopReason.failure);
      });
    }

    for (final origin in [
      'prepare',
      'join',
      'sync',
      'initialHeartbeat',
      'lookup',
      'owned',
      'callback',
      'fetch'
    ]) {
      test('${entry.key} $origin remains terminal with opt-in', () async {
        final a = fixture.ConsumerCycle(), b = fixture.ConsumerCycle();
        final error = entry.value();
        if (origin == 'prepare') {
          a.driver.onPrepare = () async {
            throw error;
          };
        }
        if (origin == 'join') {
          a.driver.onJoin = (_) async {
            throw error;
          };
        }
        if (origin == 'sync') {
          a.driver.onSync = () async {
            throw error;
          };
        }
        if (origin == 'initialHeartbeat') {
          a.driver.onHeartbeat = () async {
            throw error;
          };
        }
        if (origin == 'lookup') {
          a.read = (_, __) async {
            throw error;
          };
        }
        if (origin == 'fetch') a.fetchError = error;
        final c = make([a, b]);
        final done = c.run((_) {
          if (origin == 'callback') throw error;
        });
        if (origin == 'owned') {
          await fixture
              .until(() => c.state == KafkaGroupConsumerState.processing);
          a.session.failOwnedProcessing(error);
        }
        final result = await done;
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(b.starts, 0);
        expect(result.lastSession!.stableHeartbeatFailure, isNull);
      });
    }
  }

  final forbidden = <String, Object>{
    'handshake': const HandshakeException('certificate rejected'),
    'tls': const TlsException('TLS failure'),
    'auth': const KafkaAuthenticationException(errorCode: 58),
    'protocol': const FormatException('bad heartbeat'),
    'resource': KafkaResourceLimitException('response'),
    'cancelled': KafkaCancelledException(),
    'generic': StateError('closed'),
    for (final code in [14, 22, 25, 29, 30, 31])
      'broker$code': KafkaBrokerException(code, 'Heartbeat'),
  };
  for (final entry in forbidden.entries) {
    test('${entry.key} stable heartbeat remains terminal with provenance',
        () async {
      final a = fixture.ConsumerCycle()..emit = false;
      final b = fixture.ConsumerCycle();
      final c = make([a, b]);
      final done = c.run((_) {});
      await fixture.until(() => c.state == KafkaGroupConsumerState.processing);
      loseCoordinator(a, entry.value);
      final result = await done;
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.lastSession!.stableHeartbeatFailure, isNotNull);
      expect(b.starts, 0);
    });
  }

  for (final code in [15, 16]) {
    test('heartbeat $code waits for callback data and control drains',
        () async {
      final a = fixture.ConsumerCycle(), b = fixture.ConsumerCycle();
      final app = gate(), data = gate(), control = gate();
      a.closeData = () => data.future;
      a.driver.onClose = () => control.future;
      final c = make([a, b], commit: true);
      var calls = 0;
      c.run((_) {
        calls++;
        return app.future;
      });
      await fixture.until(() => calls == 1);
      loseCoordinator(a, KafkaBrokerException(code, 'Heartbeat'));
      await fixture
          .until(() => a.dataCloses == 1 && a.driver.calls.contains('close'));
      expect(b.starts, 0);
      app.complete();
      await fixture.flush();
      expect(b.starts, 0);
      data.complete();
      await fixture.flush();
      expect(b.starts, 0);
      control.complete();
      await fixture.until(() => b.starts == 1);
      expect(a.commits, isEmpty);
    });
  }

  test('late callback failure wins over eligible transport failure', () async {
    final a = fixture.ConsumerCycle(), b = fixture.ConsumerCycle();
    final app = gate();
    final c = make([a, b]);
    var called = false;
    final done = c.run((_) {
      called = true;
      return app.future;
    });
    await fixture.until(() => called);
    loseCoordinator(a, KafkaConnectionException());
    await a.session.revoked;
    final error = StateError('late application failure');
    app.completeError(error);
    final result = await done;
    expect(result.error, same(error));
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(b.starts, 0);
  });

  for (final outcome in ['unknown', 'rejected', 'confirmed']) {
    test('coordinator loss racing $outcome commit retains evidence and drains',
        () async {
      final a = fixture.ConsumerCycle(), b = fixture.ConsumerCycle();
      final pending = Completer<GroupOffsetCommitResponse>();
      releases.add(() {
        if (!pending.isCompleted) {
          pending.complete(processing_fixture.committed('a', {0: 11}));
        }
      });
      a.commit = (_, __) => pending.future;
      final c = make([a, b], commit: true);
      final done = c.run((_) {});
      await fixture.until(() => a.commits.length == 1);
      loseCoordinator(a, KafkaConnectionException());
      await a.session.revoked;
      await fixture.flush();
      expect(b.starts, 0);
      if (outcome == 'unknown') {
        pending.completeError(KafkaConnectionException());
      } else {
        pending.complete(processing_fixture.committed('a', {0: 11},
            codes: {0: outcome == 'rejected' ? 16 : 0}));
      }
      if (outcome == 'confirmed') {
        b.read = (_, __) async =>
            {0: const KafkaCommittedOffset(nextOffset: 11, leaderEpoch: -1)};
        await fixture.until(() => b.starts == 1);
        c.stop();
        await done;
      } else {
        final result = await done;
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(
            result.lastCommit!.partitions[0]!.outcome,
            outcome == 'unknown'
                ? KafkaGroupCommitOutcome.unknown
                : KafkaGroupCommitOutcome.rejected);
        expect(b.starts, 0);
      }
    });
  }

  test(
      'deadline check wins over late typed heartbeat error without timer firing',
      () async {
    final a = fixture.ConsumerCycle()..emit = false;
    final b = fixture.ConsumerCycle();
    final pending = Completer<HeartbeatResponse>();
    final c = make([a, b]);
    final done = c.run((_) {});
    await fixture.until(() => c.state == KafkaGroupConsumerState.processing);
    a.driver.onHeartbeat = () => pending.future;
    a.clock.advance(session_fixture.tick);
    a.clock.advance(session_fixture.lease, fire: false);
    pending.completeError(TimeoutException('late wire timeout'));
    final result = await done;
    expect(result.lastSession!.reason, KafkaGroupStopReason.localExpiry);
    expect(result.lastSession!.stableHeartbeatFailure, isNull);
    expect(b.starts, 0);
  });

  test('stable heartbeat failure during position lookup cannot rejoin',
      () async {
    final a = fixture.ConsumerCycle(), b = fixture.ConsumerCycle();
    final pending = Completer<Map<int, KafkaCommittedOffset?>>();
    a.read = (_, __) => pending.future;
    final c = make([a, b]);
    final done = c.run((_) {});
    await fixture.until(() => a.reads.isNotEmpty);
    loseCoordinator(a, KafkaConnectionException());
    await a.session.revoked;
    pending.complete(
        {0: const KafkaCommittedOffset(nextOffset: 10, leaderEpoch: -1)});
    final result = await done;
    expect(result.lastSession!.stableHeartbeatFailure, isNotNull);
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(a.fetches, isEmpty);
    expect(b.starts, 0);
  });

  for (final mode in ['dataError', 'controlError', 'controlTimeout']) {
    test('$mode vetoes coordinator replacement', () async {
      final a = fixture.ConsumerCycle()..emit = false;
      final b = fixture.ConsumerCycle();
      final error = StateError('cleanup failed');
      if (mode == 'dataError') {
        a.closeData = () async {
          throw error;
        };
      }
      if (mode == 'controlError') {
        a.driver.onClose = () async {
          throw error;
        };
      }
      if (mode == 'controlTimeout') {
        final pending = gate();
        a.driver.onClose = () => pending.future;
      }
      final c = make([a, b]);
      final done = c.run((_) {});
      await fixture.until(() => c.state == KafkaGroupConsumerState.processing);
      loseCoordinator(a, KafkaConnectionException());
      if (mode == 'controlTimeout') {
        await fixture.until(() => a.driver.calls.contains('close'));
        a.clock.advance(session_fixture.tick);
      }
      final result = await done;
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.cleanupErrors, isNotEmpty);
      expect(b.starts, 0);
    });
  }

  test('coordinator loss and 27 share one replacement budget', () async {
    final a = fixture.ConsumerCycle()..emit = false;
    final b = fixture.ConsumerCycle()..emit = false;
    final c = make([a, b]);
    final done = c.run((_) {});
    await fixture.until(() => a.loaded != null);
    loseCoordinator(a, KafkaConnectionException());
    await fixture.until(() => b.loaded != null);
    loseCoordinator(b, KafkaBrokerException(27, 'Heartbeat'));
    final result = await done;
    expect(result.reason, KafkaGroupConsumerStopReason.rejoinLimit);
    expect(result.generationsStarted, 2);
  });

  for (final phase in ['drain', 'backoff']) {
    test('stop during $phase blocks coordinator replacement', () async {
      final a = fixture.ConsumerCycle(), b = fixture.ConsumerCycle();
      final app = gate();
      final c = make([a, b], delay: const Duration(minutes: 1));
      var called = false;
      final done = c.run((_) {
        called = true;
        return app.future;
      });
      await fixture.until(() => called);
      loseCoordinator(a, KafkaConnectionException());
      await a.session.revoked;
      if (phase == 'backoff') {
        app.complete();
        await fixture.until(() => c.state == KafkaGroupConsumerState.backoff);
      }
      c.stop();
      if (!app.isCompleted) app.complete();
      expect((await done).reason, KafkaGroupConsumerStopReason.cancelled);
      expect(b.starts, 0);
      expect(a.commits, isEmpty);
    });
  }
  final malformedCommits = <String, GroupOffsetCommitResponse>{
    'missing topic': GroupOffsetCommitResponse(0, 0, []),
    'wrong topic': processing_fixture.committed('b', {0: 11}),
    'missing partition': processing_fixture.committed('a', {}),
    'extra partition': processing_fixture.committed('a', {0: 11, 1: 11}),
    'wrong partition': processing_fixture.committed('a', {1: 11}),
    'duplicate partition': GroupOffsetCommitResponse(0, 0, [
      GroupOffsetCommitTopicResponse('a', [
        GroupOffsetCommitPartitionResponse(0, 0),
        GroupOffsetCommitPartitionResponse(0, 0)
      ])
    ]),
    'negative throttle':
        processing_fixture.committed('a', {0: 11}, throttle: -1),
  };
  for (final entry in malformedCommits.entries) {
    test(
        '${entry.key} commit response after heartbeat loss remains unknown and terminal',
        () async {
      final a = fixture.ConsumerCycle(), b = fixture.ConsumerCycle();
      final pending = Completer<GroupOffsetCommitResponse>();
      releases.add(() {
        if (!pending.isCompleted) pending.complete(entry.value);
      });
      a.commit = (_, __) => pending.future;
      final c = make([a, b], commit: true);
      final done = c.run((_) {});
      await fixture.until(() => a.commits.length == 1);
      loseCoordinator(a, KafkaConnectionException());
      await a.session.revoked;
      expect(b.starts, 0);
      pending.complete(entry.value);
      final result = await done;
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.lastCommit!.error, isA<FormatException>());
      expect(result.lastCommit!.partitions[0]!.outcome,
          KafkaGroupCommitOutcome.unknown);
      expect(result.lastSession!.stableHeartbeatFailure, isNotNull);
      expect(b.starts, 0);
    });
  }

  test(
      'mixed confirmed and rejected commit after heartbeat loss preserves both and cannot rejoin',
      () async {
    final a = MixedCommitCycle(), b = fixture.ConsumerCycle();
    final pending = Completer<GroupOffsetCommitResponse>();
    final response =
        processing_fixture.committed('a', {0: 11, 1: 11}, codes: {1: 16});
    releases.add(() {
      if (!pending.isCompleted) pending.complete(response);
    });
    a.commit = (_, __) => pending.future;
    final c = make([a, b], commit: true);
    final done = c.run((_) {});
    await fixture.until(() => a.commits.length == 1);
    expect(a.commits.single, {0: 11, 1: 11});
    loseCoordinator(a, KafkaConnectionException());
    await a.session.revoked;
    await fixture.flush();
    expect(b.starts, 0);
    pending.complete(response);
    final result = await done;
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(result.lastCommit!.partitions[0]!.outcome,
        KafkaGroupCommitOutcome.confirmed);
    expect(result.lastCommit!.partitions[1]!.outcome,
        KafkaGroupCommitOutcome.rejected);
    expect(result.lastCommit!.partitions[1]!.errorCode, 16);
    expect(result.lastSession!.stableHeartbeatFailure, isNotNull);
    expect(b.starts, 0);
  });
  for (final enabled in [false, true]) {
    for (final next in <int?>[null, 9, 10, 11]) {
      test('fresh position $next compared with broker floor10 enabled=$enabled',
          () async {
        final a = fixture.ConsumerCycle()..emit = false;
        final b = fixture.ConsumerCycle()..emit = false;
        b.read = (_, __) async => {
              0: next == null
                  ? null
                  : KafkaCommittedOffset(nextOffset: next, leaderEpoch: -1)
            };
        final c = make([a, b],
            enabled: enabled,
            fallback: const {
              'a': {0: 100}
            });
        final done = c.run((_) {});
        await fixture.until(() => a.loaded != null);
        loseCoordinator(a, KafkaBrokerException(27, 'Heartbeat'));
        final rejects = enabled && (next == null || next < 10);
        if (!rejects) {
          await fixture.until(() => b.loaded != null);
          expect(b.loaded!['a']![0], next ?? 100);
          c.stop();
        }
        final result = await done;
        expect(
            result.reason,
            rejects
                ? KafkaGroupConsumerStopReason.failure
                : KafkaGroupConsumerStopReason.cancelled);
        expect(b.reads, ['a']);
        expect(result.generationsStarted, 2);
        if (rejects) {
          expect(
              result.error.toString(),
              contains(next == null
                  ? 'Missing fresh broker position'
                  : 'Lower fresh broker position'));
          expect(b.loaded, isNull);
          expect(b.fetches, isEmpty);
        }
      });
    }
  }

  test('fallback and uncommitted acknowledged progress establish no floor',
      () async {
    final a = fixture.ConsumerCycle(),
        b = fixture.ConsumerCycle()..emit = false;
    a.read = (_, __) async => {0: null};
    b.read = (_, __) async => {0: null};
    final c = make([
      a,
      b
    ], fallback: const {
      'a': {0: 20}
    });
    var calls = 0;
    c.run((_) {
      calls++;
    });
    await fixture.until(
        () => calls == 2 && a.progress!.acknowledgedNextOffsets['a']?[0] == 22);
    loseCoordinator(a, KafkaConnectionException());
    await fixture.until(() => b.loaded != null);
    expect(b.loaded!['a']![0], 20);
    expect(a.commits, isEmpty);
    expect(b.reads, ['a']);
  });

  for (final next in <int?>[null, 11, 12, 13]) {
    test('confirmed commits raise fallback-only start floor to12, fresh=$next',
        () async {
      final a = fixture.ConsumerCycle(),
          b = fixture.ConsumerCycle()..emit = false;
      a.read = (_, __) async => {0: null};
      b.read = (_, __) async => {
            0: next == null
                ? null
                : KafkaCommittedOffset(nextOffset: next, leaderEpoch: -1)
          };
      final c = make([a, b],
          commit: true,
          fallback: const {
            'a': {0: 10}
          });
      final done = c.run((_) {});
      await fixture
          .until(() => a.progress?.confirmedNextOffsets['a']?[0] == 12);
      loseCoordinator(a, KafkaConnectionException());
      final rejects = next == null || next < 12;
      if (!rejects) {
        await fixture.until(() => b.loaded != null);
        c.stop();
      }
      final result = await done;
      expect(
          result.reason,
          rejects
              ? KafkaGroupConsumerStopReason.failure
              : KafkaGroupConsumerStopReason.cancelled);
      expect(b.reads, ['a']);
      if (rejects) {
        expect(b.loaded, isNull);
        expect(b.fetches, isEmpty);
      }
    });
  }

  for (final next in <int?>[null, 10, 11]) {
    test(
        'late valid confirmation after revoke establishes floor11, fresh=$next',
        () async {
      final a = fixture.ConsumerCycle(),
          b = fixture.ConsumerCycle()..emit = false;
      final pending = Completer<GroupOffsetCommitResponse>();
      releases.add(() {
        if (!pending.isCompleted) {
          pending.complete(processing_fixture.committed('a', {0: 11}));
        }
      });
      a.commit = (_, __) => pending.future;
      b.read = (_, __) async => {
            0: next == null
                ? null
                : KafkaCommittedOffset(nextOffset: next, leaderEpoch: -1)
          };
      final c = make([a, b],
          commit: true,
          fallback: const {
            'a': {0: 100}
          });
      final done = c.run((_) {});
      await fixture.until(() => a.commits.length == 1);
      loseCoordinator(a, KafkaConnectionException());
      await a.session.revoked;
      expect(b.starts, 0);
      pending.complete(processing_fixture.committed('a', {0: 11}));
      if (next == 11) {
        await fixture.until(() => b.loaded != null);
        c.stop();
      }
      final result = await done;
      expect(
          result.reason,
          next == 11
              ? KafkaGroupConsumerStopReason.cancelled
              : KafkaGroupConsumerStopReason.failure);
      expect(a.progress!.confirmedNextOffsets['a']![0], 11);
      expect(b.reads, ['a']);
      if (next != 11) expect(b.fetches, isEmpty);
    });
  }

  for (final lowerTopic in ['a', 'b', 'none']) {
    test(
        'successive multi-topic confirmations retain all floors: lower $lowerTopic',
        () async {
      final a = fixture.ConsumerCycle(partitions: const {
        'a': [0],
        'b': [0]
      })
        ..emitOncePerTopic = {'a', 'b'};
      final b = fixture.ConsumerCycle(partitions: const {
        'a': [0],
        'b': [0]
      })
        ..emit = false;
      b.read = (topic, __) async => {
            0: KafkaCommittedOffset(
                nextOffset: topic == lowerTopic ? 11 : 12, leaderEpoch: -1)
          };
      final c = make([a, b], commit: true);
      final done = c.run((_) {});
      await fixture.until(() =>
          a.progress?.confirmedNextOffsets['a']?[0] == 12 &&
          a.progress?.confirmedNextOffsets['b']?[0] == 12);
      expect(a.commits, hasLength(4));
      loseCoordinator(a, KafkaConnectionException());
      if (lowerTopic == 'none') {
        await fixture.until(() => b.loaded != null);
        c.stop();
      }
      final result = await done;
      expect(
          result.reason,
          lowerTopic == 'none'
              ? KafkaGroupConsumerStopReason.cancelled
              : KafkaGroupConsumerStopReason.failure);
      expect(b.reads, lowerTopic == 'a' ? ['a'] : ['a', 'b']);
      if (lowerTopic != 'none') {
        expect(b.loaded, isNull);
        expect(b.fetches, isEmpty);
      }
    });
  }

  test('departed partitions are forgotten and new partitions keep fallback',
      () async {
    final a = fixture.ConsumerCycle()..emit = false;
    a.read = (_, __) async =>
        {0: const KafkaCommittedOffset(nextOffset: 90, leaderEpoch: -1)};
    final b = fixture.ConsumerCycle(partitions: const {
      'a': [1]
    })
      ..emit = false;
    b.read = (_, __) async => {1: null};
    final d = fixture.ConsumerCycle()..emit = false;
    d.read = (_, __) async => {0: null};
    final c = make([a, b, d],
        limit: 2,
        fallback: const {
          'a': {0: 5, 1: 6}
        });
    c.run((_) {});
    await fixture.until(() => a.loaded != null);
    loseCoordinator(a, KafkaConnectionException());
    await fixture.until(() => b.loaded != null);
    expect(b.loaded!['a'], {1: 6});
    loseCoordinator(b, KafkaConnectionException());
    await fixture.until(() => d.loaded != null);
    expect(d.loaded!['a'], {0: 5});
    expect(d.reads, ['a']);
    expect(c.generationsStarted, 3);
  });
  for (final lowerPartition in [0, 1]) {
    test(
        'successive commits retain confirmed floor for partition $lowerPartition',
        () async {
      final a = MixedCommitCycle();
      final b = fixture.ConsumerCycle(partitions: const {
        'a': [0, 1]
      })
        ..emit = false;
      b.read = (_, __) async => {
            0: KafkaCommittedOffset(
                nextOffset: lowerPartition == 0 ? 11 : 12, leaderEpoch: -1),
            1: KafkaCommittedOffset(
                nextOffset: lowerPartition == 1 ? 10 : 11, leaderEpoch: -1)
          };
      final c = make([a, b], commit: true);
      final done = c.run((_) {});
      await fixture
          .until(() => a.progress?.confirmedNextOffsets['a']?[0] == 12);
      expect(a.progress!.confirmedNextOffsets['a'], {0: 12, 1: 11});
      expect(a.commits.last, {0: 12});
      loseCoordinator(a, KafkaConnectionException());
      final result = await done;
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.error.toString(),
          contains('Lower fresh broker position for a:$lowerPartition'));
      expect(b.reads, ['a']);
      expect(b.loaded, isNull);
      expect(b.fetches, isEmpty);
    });
  }
}
