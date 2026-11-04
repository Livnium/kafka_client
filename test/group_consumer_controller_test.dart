import 'dart:async';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/src/consumer_group_metadata.dart';
import 'package:kafka_client/src/group_consumer.dart';
import 'package:kafka_client/src/group_delivery.dart';
import 'package:kafka_client/src/group_offsets_protocol.dart';
import 'package:kafka_client/src/group_membership_protocol.dart';
import 'package:kafka_client/src/group_processing.dart';
import 'package:test/test.dart';

import 'classic_group_session_test.dart' as session_fixture;
import 'group_delivery_controller_test.dart' as delivery_fixture;
import 'group_processing_controller_test.dart' as processing_fixture;

/// Uses real session identity, admission receipts, processing ledger and cleanup.
/// Only broker I/O and its controllable completion gates are replaced.
class ConsumerCycle implements GroupConsumerCycle {
  final clock = session_fixture.Clock();
  final driver = session_fixture.Driver();
  late final session = session_fixture.controller(driver, clock);
  final Map<String, List<int>> partitions;
  final reads = <String>[];
  final fetches = <int>[];
  final commits = <Map<int, int>>[];
  int starts = 0, closes = 0, positionCloses = 0, dataCloses = 0;
  Map<String, Map<int, int>>? loaded;
  GroupProcessingController? progress;
  Future<Map<int, KafkaCommittedOffset?>> Function(String, Set<int>)? read;
  Future<void> Function()? closePosition;
  Future<void> Function()? closeData;
  Future<GroupOffsetCommitResponse> Function(String, Map<int, int>)? commit;
  Duration processingTimeout = const Duration(seconds: 30);
  bool emit = true;
  Set<String>? emitOncePerTopic;
  Object? closeError;
  Object? fetchError;
  Future<void>? _positionClose;

  ConsumerCycle(
      {this.partitions = const {
        'a': [0]
      }}) {
    driver.onSync = () async => ConsumerGroupAssignment(partitions);
  }
  @override
  Future<KafkaGroupAssignment> start() {
    starts++;
    return session.start();
  }

  @override
  bool isCurrent(KafkaGroupAssignment token) => session.isCurrent(token);
  @override
  Future<KafkaGroupTermination> get revoked => session.revoked;
  @override
  Future<KafkaGroupTermination> close() {
    closes++;
    final closed = session.close();
    if (closeError != null) return Future.error(closeError!);
    return closed;
  }

  @override
  Future<Map<int, KafkaCommittedOffset?>> readPositions(String t, Set<int> ps) {
    reads.add(t);
    return read?.call(t, ps) ??
        Future.value({
          for (final p in ps)
            p: const KafkaCommittedOffset(nextOffset: 10, leaderEpoch: -1)
        });
  }

  @override
  Future<void> closePositions() => _positionClose ??= Future.sync(() {
        positionCloses++;
        return closePosition?.call();
      });
  @override
  KafkaGroupProcessing processing(
      KafkaGroupAssignment token, Map<String, Map<int, int>> positions) {
    loaded = positions;
    final p = progress = GroupProcessingController(
        session: session,
        assignment: token,
        maxPendingRecords: 1,
        processingTimeout: processingTimeout,
        commit: (t, offsets, cancellation) {
          commits.add(Map.of(offsets));
          return commit?.call(t, offsets) ??
              Future.value(processing_fixture.committed(t, offsets));
        });
    final delivery = GroupDeliveryController(
        session: session,
        assignment: token,
        startOffsets: positions,
        fetch: (t, part, offset) async {
          fetches.add(offset);
          if (fetchError != null) throw fetchError!;
          final records = (emitOncePerTopic?.remove(t) ?? emit)
              ? [offset, offset + 1]
              : <int>[];
          emit = false;
          return delivery_fixture.reply(t, part, records);
        },
        closeData: () async {
          dataCloses++;
          await closeData?.call();
        },
        pollInterval: const Duration(milliseconds: 1),
        maxBufferedRecords: 10,
        maxBufferedPayloadBytes: 1000,
        waitForProcessingCapacity: p.waitForCapacity,
        admitProcessing: p.admit,
        stopProcessing: p.stop);
    p.bind(delivery);
    return p.processing;
  }

  void reject({int code = 27, String operation = 'Heartbeat'}) =>
      session.failOwnedProcessing(KafkaBrokerException(code, operation));
}

Future<void> until(bool Function() condition) =>
    delivery_fixture.until(condition);
Future<void> flush() => delivery_fixture.tick();

void main() {
  final controllers = <GroupConsumerController>[];
  final releases = <void Function()>[];
  GroupConsumerController make(List<ConsumerCycle> cycles,
      {bool commit = false,
      int limit = 1,
      Duration delay = const Duration(milliseconds: 1),
      Map<String, Map<int, int>> fallback = const {},
      GroupConsumerCycle Function()? factory}) {
    var index = 1;
    final c = GroupConsumerController(
        initial: cycles.first,
        createCycle: factory ?? () => cycles[index++],
        commitAfterProcessing: commit,
        missingOffsets: fallback,
        maxRejoins: limit,
        rejoinDelay: delay);
    controllers.add(c);
    return c;
  }

  Completer<void> gate() {
    final c = Completer<void>();
    releases.add(() {
      if (!c.isCompleted) c.complete();
    });
    return c;
  }

  setUp(() {
    controllers.clear();
    releases.clear();
  });
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
  });

  test(
      'revocation drains callback and independent data and session cleanup before replacement',
      () async {
    final a = ConsumerCycle(), b = ConsumerCycle();
    final app = gate(), data = gate(), control = gate();
    a.closeData = () => data.future;
    a.driver.onClose = () => control.future;
    final c = make([a, b]);
    var callbacks = 0;
    c.run((_) {
      callbacks++;
      return app.future;
    });
    await until(() => callbacks == 1);
    final old = c.assignment!;
    a.reject();
    await until(() => a.dataCloses == 1 && a.driver.calls.contains('close'));
    expect(c.isCurrent(old), isFalse);
    expect(c.assignment, isNull);
    expect(b.starts, 0);
    app.complete();
    await flush();
    expect(b.starts, 0);
    data.complete();
    await flush();
    expect(b.starts, 0);
    control.complete();
    await until(() => b.starts == 1);
    expect(a.commits, isEmpty);
    c.stop();
    await c.closed;
  });

  test('callback failure after revocation remains terminal', () async {
    final a = ConsumerCycle(), b = ConsumerCycle();
    final app = gate();
    final c = make([a, b]);
    var called = false;
    final done = c.run((_) {
      called = true;
      return app.future;
    });
    await until(() => called);
    a.reject();
    final error = StateError('application failed late');
    app.completeError(error);
    final result = await done;
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(result.error, same(error));
    expect(b.starts, 0);
  });

  for (final outcome in ['unknown', 'rejected', 'confirmed']) {
    test('heartbeat 27 racing $outcome commit preserves commit evidence',
        () async {
      final a = ConsumerCycle(), b = ConsumerCycle();
      final pending = Completer<GroupOffsetCommitResponse>();
      releases.add(() {
        if (!pending.isCompleted) {
          pending.complete(processing_fixture.committed('a', {0: 11}));
        }
      });
      a.commit = (_, __) => pending.future;
      final c = make([a, b], commit: true);
      final done = c.run((_) {});
      await until(() => a.commits.length == 1);
      a.reject();
      await flush();
      expect(b.starts, 0);
      if (outcome == 'unknown') {
        pending.completeError(StateError('lost response'));
      } else {
        pending.complete(processing_fixture.committed('a', {0: 11},
            codes: {0: outcome == 'rejected' ? 27 : 0}));
      }
      if (outcome == 'confirmed') {
        await until(() => b.starts == 1);
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

  for (final op in [
    'Heartbeat',
    'heartbeat',
    'HEARTBEAT',
    'OffsetCommit',
    'JoinGroup'
  ]) {
    test('rejoin eligibility requires exact heartbeat operation $op', () async {
      final a = ConsumerCycle(), b = ConsumerCycle();
      a.emit = false;
      final eligible = op == 'Heartbeat' || op == 'heartbeat';
      final c = make([a, b]);
      final done = c.run((_) {});
      await until(() => a.loaded != null);
      a.reject(operation: op);
      if (eligible) {
        await until(() => b.starts == 1);
        c.stop();
      }
      final result = await done;
      expect(b.starts, eligible ? 1 : 0);
      if (!eligible) {
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
      }
    });
  }
  for (final code in [22, 25, 16, 0]) {
    test('heartbeat code $code is terminal', () async {
      final a = ConsumerCycle(), b = ConsumerCycle();
      a.emit = false;
      final c = make([a, b]);
      final done = c.run((_) {});
      await until(() => a.loaded != null);
      a.reject(code: code);
      expect((await done).reason, KafkaGroupConsumerStopReason.failure);
      expect(b.starts, 0);
    });
  }

  test('fresh lookup every generation overrides fallback and is immutable',
      () async {
    final a = ConsumerCycle(), b = ConsumerCycle();
    a.emit = false;
    b.emit = false;
    b.read = (_, __) async =>
        {0: const KafkaCommittedOffset(nextOffset: 50, leaderEpoch: -1)};
    final input = {
      'a': {0: 99}
    };
    final frozen = GroupConsumerController.freezeFallbacks(input, {'a'});
    input['a']![0] = 123;
    expect(frozen['a']![0], 99);
    expect(() => frozen['a']![0] = 1, throwsUnsupportedError);
    final c = make([a, b], fallback: frozen);
    c.run((_) {});
    await until(() => a.loaded != null);
    expect(a.loaded!['a']![0], 10);
    a.reject();
    await until(() => b.loaded != null);
    expect(b.loaded!['a']![0], 50);
    expect(a.reads, ['a']);
    expect(b.reads, ['a']);
    expect(() => b.loaded!['a']![0] = 2, throwsUnsupportedError);
  });

  for (final kind in ['null', 'missing', 'extra', 'negative', 'error']) {
    test('position $kind fails closed, except explicit null fallback',
        () async {
      final a = ConsumerCycle();
      a.read = (_, __) async {
        if (kind == 'error') throw StateError('lookup failed');
        if (kind == 'missing') return {};
        if (kind == 'extra') return {0: null, 1: null};
        if (kind == 'negative') {
          return {
            0: const KafkaCommittedOffset(nextOffset: -1, leaderEpoch: -1)
          };
        }
        return {0: null};
      };
      final c = make([
        a
      ], fallback: const {
        'a': {0: 8}
      });
      final done = c.run((_) {
        c.stop();
      });
      final result = await done;
      expect(
          result.reason,
          kind == 'null'
              ? KafkaGroupConsumerStopReason.cancelled
              : KafkaGroupConsumerStopReason.failure);
      if (kind == 'null') {
        expect(a.fetches.first, 8);
      } else {
        expect(a.fetches, isEmpty);
      }
    });
  }
  test('missing offset without explicit fallback fails before Fetch', () async {
    final a = ConsumerCycle()..read = ((_, __) async => {0: null});
    final c = make([a]);
    final result = await c.run((_) {});
    expect(result.error, isA<StateError>());
    expect(a.fetches, isEmpty);
  });
  test('empty assignment remains stable without lookup Fetch or callbacks',
      () async {
    final a = ConsumerCycle(partitions: {});
    final c = make([a]);
    var calls = 0;
    c.run((_) {
      calls++;
    });
    await until(() => a.loaded != null);
    expect(a.reads, isEmpty);
    expect(a.fetches, isEmpty);
    expect(calls, 0);
  });

  for (final stop in [false, true]) {
    test(
        '${stop ? 'stop' : 'revoke'} during pending position lookup never Fetches or rejoins',
        () async {
      final a = ConsumerCycle(), b = ConsumerCycle();
      final pending = Completer<Map<int, KafkaCommittedOffset?>>();
      releases.add(() {
        if (!pending.isCompleted) pending.complete({0: null});
      });
      a.read = (_, __) => pending.future;
      final c = make([a, b]);
      final done = c.run((_) {});
      await until(() => a.reads.isNotEmpty);
      if (stop) {
        c.stop();
      } else {
        a.reject();
      }
      pending.complete(
          {0: const KafkaCommittedOffset(nextOffset: 10, leaderEpoch: -1)});
      final result = await done;
      expect(
          result.reason,
          stop
              ? KafkaGroupConsumerStopReason.cancelled
              : KafkaGroupConsumerStopReason.failure);
      expect(a.fetches, isEmpty);
      expect(b.starts, 0);
    });
  }
  test('stop in callback is synchronous and does not commit', () async {
    final a = ConsumerCycle();
    final c = make([a], commit: true);
    var calls = 0;
    final result = await c.run((record) {
      calls++;
      c.stop();
      expect(c.isCurrent(record.assignment), isFalse);
    });
    expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
    expect(calls, 1);
    expect(a.commits, isEmpty);
  });
  test('commit false never sends commits even after successful callbacks',
      () async {
    final a = ConsumerCycle();
    final c = make([a]);
    var calls = 0;
    c.run((_) {
      calls++;
    });
    await until(() => calls == 2);
    expect(a.commits, isEmpty);
    c.stop();
    await c.closed;
  });
  test('stop drains pending callback and forbids post-stop commit', () async {
    final a = ConsumerCycle();
    final app = gate();
    final c = make([a], commit: true);
    var called = false, ended = false;
    final done = c.run((_) {
      called = true;
      return app.future;
    });
    done.then((_) {
      ended = true;
    });
    await until(() => called);
    c.stop();
    await flush();
    expect(ended, isFalse);
    app.complete();
    expect((await done).reason, KafkaGroupConsumerStopReason.cancelled);
    expect(a.commits, isEmpty);
  });
  test('stop during backoff cancels delay without next Join', () async {
    final a = ConsumerCycle(), b = ConsumerCycle();
    a.emit = false;
    final c = make([a, b], delay: const Duration(minutes: 1));
    final done = c.run((_) {});
    await until(() => a.loaded != null);
    a.reject();
    await until(() => c.state == KafkaGroupConsumerState.backoff);
    c.stop();
    expect((await done).reason, KafkaGroupConsumerStopReason.cancelled);
    expect(b.starts, 0);
  });
  test('finite rejoin budget counts replacements only', () async {
    final a = ConsumerCycle(), b = ConsumerCycle();
    a.emit = false;
    b.emit = false;
    final c = make([a, b]);
    final done = c.run((_) {});
    await until(() => a.loaded != null);
    a.reject();
    await until(() => b.loaded != null);
    b.reject();
    final result = await done;
    expect(result.reason, KafkaGroupConsumerStopReason.rejoinLimit);
    expect(result.generationsStarted, 2);
    expect(a.starts, 1);
    expect(b.starts, 1);
  });
  test(
      'stopped before run closes initial cycle without joining and rejects run',
      () async {
    final a = ConsumerCycle();
    final c = make([a]);
    c.stop();
    c.stop();
    final result = await c.closed;
    expect(result.generationsStarted, 0);
    expect(a.starts, 0);
    expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
    await expectLater(c.run((_) {}), throwsStateError);
  });
  test('run is single use while active and after completion', () async {
    final a = ConsumerCycle();
    final c = make([a]);
    final done = c.run((_) {});
    await expectLater(c.run((_) {}), throwsStateError);
    c.stop();
    await done;
    await expectLater(c.run((_) {}), throwsStateError);
  });
  test('stop during native-shaped Join cancels cleanly and never fetches',
      () async {
    final a = ConsumerCycle(), b = ConsumerCycle();
    final pending = Completer<JoinGroupResponse>();
    a.driver.onJoin = (_) => pending.future;
    releases.add(() {
      if (!pending.isCompleted) pending.complete(session_fixture.joined());
    });
    final c = make([a, b]);
    final done = c.run((_) {});
    await until(() => a.driver.calls.contains('join:'));
    c.stop();
    pending.completeError(KafkaCancelledException());
    final result = await done;
    expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
    expect(result.error, isNull);
    expect(result.interruptedOperationError, isA<StateError>());
    expect(a.fetches, isEmpty);
    expect(b.starts, 0);
    expect(a.closes, 1);
  });
  test(
      'stop during native-shaped OffsetFetch cancellation preserves interruption',
      () async {
    final a = ConsumerCycle();
    final pending = Completer<Map<int, KafkaCommittedOffset?>>();
    releases.add(() {
      if (!pending.isCompleted) pending.complete({0: null});
    });
    a.read = (_, __) => pending.future;
    final c = make([a]);
    final done = c.run((_) {});
    await until(() => a.reads.isNotEmpty);
    c.stop();
    final interrupted = KafkaCancelledException();
    pending.completeError(interrupted);
    final result = await done;
    expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
    expect(result.error, isNull);
    expect(result.interruptedOperationError, same(interrupted));
    expect(a.fetches, isEmpty);
    expect(a.closes, 1);
  });
  test('initial heartbeat 27 cannot enter replacement processing cycle',
      () async {
    final a = ConsumerCycle(), b = ConsumerCycle();
    a.driver.onHeartbeat = () async => HeartbeatResponse(1, 0, 27);
    final c = make([a, b]);
    final result = await c.run((_) {});
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(a.fetches, isEmpty);
    expect(b.starts, 0);
  });
  test('position close failure is terminal before processing admission',
      () async {
    final a = ConsumerCycle();
    final error = StateError('position close failed');
    a.closePosition = () async {
      throw error;
    };
    final c = make([a]);
    final result = await c.run((_) {});
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(result.error, same(error));
    expect(a.fetches, isEmpty);
    expect(result.cleanupErrors, contains(same(error)));
  });
  test('data cleanup failure after heartbeat 27 blocks replacement', () async {
    final a = ConsumerCycle(), b = ConsumerCycle();
    a.emit = false;
    final error = StateError('data close failed');
    a.closeData = () async {
      throw error;
    };
    final c = make([a, b]);
    final done = c.run((_) {});
    await until(() => a.loaded != null);
    a.reject();
    final result = await done;
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(result.cleanupErrors, contains(same(error)));
    expect(b.starts, 0);
  });
  test(
      'early session close rejection is captured while data cleanup is pending',
      () async {
    final uncaught = <Object>[];
    final finished = Completer<void>();
    runZonedGuarded(() async {
      try {
        final a = ConsumerCycle(), b = ConsumerCycle();
        a.emit = false;
        final data = gate();
        final error = StateError('session close failed');
        a.closeData = () => data.future;
        a.closeError = error;
        final c = make([a, b]);
        final done = c.run((_) {});
        await until(() => a.loaded != null);
        a.reject();
        await until(() => a.dataCloses == 1 && a.closes == 1);
        await flush();
        await flush();
        expect(uncaught, isEmpty);
        expect(b.starts, 0);
        data.complete();
        final result = await done;
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(result.cleanupErrors, contains(same(error)));
        expect(a.closes, 1);
        expect(b.starts, 0);
        finished.complete();
      } catch (error, stack) {
        finished.completeError(error, stack);
      }
    }, (error, stack) {
      uncaught.add(error);
    });
    await finished.future.timeout(const Duration(seconds: 3));
    expect(uncaught, isEmpty);
  });
  test('idle stop closes position resource as well as membership', () async {
    final a = ConsumerCycle();
    final c = make([a]);
    c.stop();
    await c.closed;
    expect(a.positionCloses, 1);
    expect(a.closes, 1);
  });
  test(
      'settled cleanup cannot replace a generation while callback Future is pending',
      () async {
    final a = ConsumerCycle(), b = ConsumerCycle();
    final app = gate();
    final c = make([a, b]);
    var calls = 0;
    c.run((_) {
      calls++;
      return app.future;
    });
    await until(() => calls == 1);
    a.reject();
    await a.session.closed;
    await a.progress!.stopped;
    await flush();
    await flush();
    expect(b.starts, 0);
    expect(calls, 1);
    app.complete();
    await until(() => b.starts == 1);
  });
  test('serial callback awaits explicit commit before admitting next record',
      () async {
    final a = ConsumerCycle();
    final pending = Completer<GroupOffsetCommitResponse>();
    releases.add(() {
      if (!pending.isCompleted) {
        pending.complete(processing_fixture.committed('a', {0: 11}));
      }
    });
    a.commit = (t, offsets) => a.commits.length == 1
        ? pending.future
        : Future.value(processing_fixture.committed(t, offsets));
    final c = make([a], commit: true);
    var calls = 0;
    c.run((_) {
      calls++;
    });
    await until(() => a.commits.length == 1);
    await flush();
    expect(calls, 1);
    expect(a.commits.single, {0: 11});
    pending.complete(processing_fixture.committed('a', {0: 11}));
    await until(() => a.commits.length == 2);
    expect(calls, 2);
    expect(a.commits.last, {0: 12});
  });
  test('public constructor rejects invalid configuration before any I/O',
      () async {
    final brokers = [Broker('127.0.0.1', 1)];
    final invalid = <void Function()>[
      () => KafkaGroupConsumer(
          brokers: brokers,
          groupId: 'g',
          topics: {'a'},
          commitAfterProcessing: false,
          maxRejoins: -1),
      () => KafkaGroupConsumer(
          brokers: brokers,
          groupId: 'g',
          topics: {'a'},
          commitAfterProcessing: false,
          processingTimeout: Duration.zero),
      () => KafkaGroupConsumer(
          brokers: brokers,
          groupId: 'g',
          topics: {'a'},
          commitAfterProcessing: false,
          pollInterval: Duration.zero),
      () => KafkaGroupConsumer(
          brokers: brokers,
          groupId: 'g',
          topics: {'a'},
          commitAfterProcessing: false,
          maxWaitMs: -1),
      () => KafkaGroupConsumer(
          brokers: brokers,
          groupId: 'g',
          topics: {'a'},
          commitAfterProcessing: false,
          maxBytes: 0),
      () => KafkaGroupConsumer(
          brokers: brokers,
          groupId: 'g',
          topics: {'a'},
          commitAfterProcessing: false,
          maxBufferedRecords: 0),
      () => KafkaGroupConsumer(
          brokers: brokers,
          groupId: 'g',
          topics: {'a'},
          commitAfterProcessing: false,
          maxBufferedPayloadBytes: 0),
      () => KafkaGroupConsumer(
          brokers: brokers,
          groupId: 'g',
          topics: {'a'},
          commitAfterProcessing: false,
          maxConnections: 0),
      () => KafkaGroupConsumer(
          brokers: brokers,
          groupId: '',
          topics: {'a'},
          commitAfterProcessing: false),
    ];
    for (final construct in invalid) {
      expect(construct, throwsArgumentError);
    }
    final c = KafkaGroupConsumer(
        brokers: brokers,
        groupId: 'g',
        topics: {'a'},
        commitAfterProcessing: false);
    expect(c.state, KafkaGroupConsumerState.idle);
    expect(c.generationsStarted, 0);
    c.stop();
    expect((await c.closed).reason, KafkaGroupConsumerStopReason.cancelled);
  });
  test(
      'position client close barrier prevents processing and Fetch until settled',
      () async {
    final a = ConsumerCycle();
    final position = gate();
    a.closePosition = () => position.future;
    final c = make([a]);
    var calls = 0;
    c.run((_) {
      calls++;
    });
    await until(() => a.positionCloses == 1);
    expect(c.state, KafkaGroupConsumerState.loadingPositions);
    expect(a.loaded, isNull);
    expect(a.fetches, isEmpty);
    expect(calls, 0);
    position.complete();
    await until(() => calls > 0);
    expect(a.loaded!['a']![0], 10);
    expect(a.positionCloses, 1);
  });
  test(
      'multi-topic exact assignments mix stored and missing offsets and snapshot mutable response',
      () async {
    final a = ConsumerCycle(partitions: {
      'b': [3],
      'a': [0, 2],
      'empty': []
    });
    a.emit = false;
    final storedA = <int, KafkaCommittedOffset?>{
      0: const KafkaCommittedOffset(nextOffset: 20, leaderEpoch: -1),
      2: null
    };
    final storedB = <int, KafkaCommittedOffset?>{
      3: const KafkaCommittedOffset(nextOffset: 30, leaderEpoch: -1)
    };
    final requested = <String, Set<int>>{};
    final position = gate();
    a.read = (topic, ids) async {
      requested[topic] = Set.of(ids);
      expect(() => ids.add(99), throwsUnsupportedError);
      return topic == 'a' ? storedA : storedB;
    };
    a.closePosition = () => position.future;
    final c = make([
      a
    ], fallback: {
      'a': {0: 100, 2: 22},
      'b': {3: 300}
    });
    c.run((_) {});
    await until(() => a.positionCloses == 1);
    storedA.clear();
    storedB[3] = const KafkaCommittedOffset(nextOffset: 999, leaderEpoch: -1);
    position.complete();
    await until(() => a.loaded != null);
    expect(a.reads, ['a', 'b']);
    expect(requested, {
      'a': {0, 2},
      'b': {3}
    });
    expect(a.loaded, {
      'a': {0: 20, 2: 22},
      'b': {3: 30}
    });
    expect(() => a.loaded!['other'] = {}, throwsUnsupportedError);
  });
  for (final stall in [false, true]) {
    test(
        '${stall ? 'processing stall' : 'local expiry'} is terminal but drains callback Future',
        () async {
      final a = ConsumerCycle(), b = ConsumerCycle();
      final app = gate();
      if (stall) a.processingTimeout = const Duration(milliseconds: 5);
      final c = make([a, b]);
      var called = false, ended = false;
      final done = c.run((_) {
        called = true;
        return app.future;
      });
      done.then((_) {
        ended = true;
      });
      await until(() => called);
      a.clock.advance(
          stall ? const Duration(milliseconds: 5) : session_fixture.lease);
      await a.session.revoked;
      await a.progress!.stopped;
      await flush();
      expect(ended, isFalse);
      expect(b.starts, 0);
      expect(c.assignment, isNull);
      app.complete();
      final result = await done;
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(
          result.lastSession!.reason,
          stall
              ? KafkaGroupStopReason.failure
              : KafkaGroupStopReason.localExpiry);
      if (stall) expect(result.error, isA<TimeoutException>());
      expect(b.starts, 0);
    });
  }
  test('replacement factory failure never revives old assignment', () async {
    final a = ConsumerCycle();
    a.emit = false;
    final error = StateError('factory failed');
    var created = 0;
    final c = make([a], factory: () {
      created++;
      throw error;
    });
    final done = c.run((_) {});
    await until(() => a.loaded != null);
    final old = c.assignment!;
    a.reject();
    final result = await done;
    expect(created, 1);
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(result.error, same(error));
    expect(result.generationsStarted, 1);
    expect(c.isCurrent(old), isFalse);
    expect(c.assignment, isNull);
    expect(a.closes, 1);
  });
  for (final position in [false, true]) {
    test('idle ${position ? 'position' : 'session'} cleanup error is failure',
        () async {
      final a = ConsumerCycle();
      final error = StateError('idle cleanup failed');
      if (position) {
        a.closePosition = () async {
          throw error;
        };
      } else {
        a.closeError = error;
      }
      final c = make([a]);
      c.stop();
      final result = await c.closed;
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.cleanupErrors, contains(same(error)));
      expect(a.closes, 1);
      expect(a.positionCloses, 1);
      expect(a.starts, 0);
    });
  }
  test('callback failure racing explicit stop remains failure', () async {
    final a = ConsumerCycle();
    final app = gate();
    final error = StateError('callback failed');
    final c = make([a], commit: true);
    var called = false;
    final done = c.run((_) {
      called = true;
      return app.future;
    });
    await until(() => called);
    c.stop();
    app.completeError(error);
    final result = await done;
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(result.error, same(error));
    expect(a.commits, isEmpty);
  });
  for (final rejected in [false, true]) {
    test(
        '${rejected ? 'rejected' : 'unknown'} commit racing explicit stop remains failure',
        () async {
      final a = ConsumerCycle();
      final pending = Completer<GroupOffsetCommitResponse>();
      releases.add(() {
        if (!pending.isCompleted) {
          pending.complete(processing_fixture.committed('a', {0: 11}));
        }
      });
      a.commit = (_, __) => pending.future;
      final c = make([a], commit: true);
      final done = c.run((_) {});
      await until(() => a.commits.isNotEmpty);
      c.stop();
      if (rejected) {
        pending.complete(
            processing_fixture.committed('a', {0: 11}, codes: {0: 27}));
      } else {
        pending.completeError(StateError('response lost'));
      }
      final result = await done;
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(
          result.lastCommit!.partitions[0]!.outcome,
          rejected
              ? KafkaGroupCommitOutcome.rejected
              : KafkaGroupCommitOutcome.unknown);
    });
  }
  test('invalid budgets delay and fallbacks fail synchronously', () {
    for (final n in [-1, 101]) {
      expect(
          () => GroupConsumerController.validate(
              n, const Duration(milliseconds: 1)),
          throwsArgumentError);
    }
    for (final d in [Duration.zero, const Duration(minutes: 2)]) {
      expect(() => GroupConsumerController.validate(1, d), throwsArgumentError);
    }
    for (final m in [
      {
        'b': {0: 0}
      },
      {
        'a': {-1: 0}
      },
      {
        'a': {0: -1}
      },
      {
        'a': {2147483648: 0}
      }
    ]) {
      expect(() => GroupConsumerController.freezeFallbacks(m, {'a'}),
          throwsArgumentError);
    }
  });
}
