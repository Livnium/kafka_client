import 'dart:async';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/src/consumer_group_metadata.dart';
import 'package:kafka_client/src/group_consumer.dart'
    show GroupConsumerController;
import 'package:kafka_client/src/group_delivery.dart'
    show GroupDeliveryController;
import 'package:kafka_client/src/group_membership_protocol.dart';
import 'package:kafka_client/src/group_processing.dart'
    show GroupProcessingController;
import 'package:test/test.dart';

import 'classic_group_session_test.dart' as session_fixture;
import 'group_consumer_controller_test.dart' show ConsumerCycle, flush, until;
import 'group_delivery_controller_test.dart' as delivery_fixture;
import 'group_processing_controller_test.dart' as processing_fixture;

/// The application deadline has its own clock so tests can distinguish it from
/// membership expiry and the existing per-record processing deadline.
class _DrainClock extends session_fixture.Clock {
  Duration _now = Duration.zero;
  int? readsUntilDeadline;
  Duration? deadline;

  @override
  Duration get now {
    if (readsUntilDeadline != null) {
      readsUntilDeadline = readsUntilDeadline! - 1;
      if (readsUntilDeadline == 0) {
        _now = deadline!;
        readsUntilDeadline = null;
      }
    }
    return _now;
  }

  @override
  set now(Duration value) => _now = value;
}

/// Uses the same real membership, processing ledger and cleanup as ConsumerCycle,
/// replacing only Fetch with a controllable result that may arrive after stop.
class _PendingFetchCycle extends ConsumerCycle {
  final Completer<FetchResponse> fetched;
  _PendingFetchCycle(this.fetched);

  @override
  KafkaGroupProcessing processing(
      KafkaGroupAssignment token, Map<String, Map<int, int>> positions) {
    loaded = positions;
    final p = progress = GroupProcessingController(
        session: session,
        assignment: token,
        maxPendingRecords: 1,
        processingTimeout: processingTimeout,
        commit: (topic, offsets, cancellation) {
          commits.add(Map.of(offsets));
          return Future.value(processing_fixture.committed(topic, offsets));
        });
    final delivery = GroupDeliveryController(
        session: session,
        assignment: token,
        startOffsets: positions,
        fetch: (topic, partition, offset) {
          fetches.add(offset);
          return fetched.future;
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
}

void main() {
  final controllers = <GroupConsumerController>[];
  final releases = <void Function()>[];
  late _DrainClock clock;
  const timeout = Duration(milliseconds: 20);

  GroupConsumerController make(List<ConsumerCycle> cycles,
      {bool commit = false,
      bool rejoinOnCoordinatorLoss = false,
      Duration delay = const Duration(milliseconds: 1),
      KafkaOffsetPosition? missingPosition}) {
    var index = 1;
    final c = GroupConsumerController(
        initial: cycles.first,
        createCycle: () => cycles[index++],
        commitAfterProcessing: commit,
        missingOffsets: const {},
        missingOffsetPosition: missingPosition,
        maxRejoins: cycles.length - 1,
        rejoinOnCoordinatorLoss: rejoinOnCoordinatorLoss,
        rejoinDelay: delay,
        drainScheduler: clock);
    controllers.add(c);
    return c;
  }

  Completer<void> gate() {
    final pending = Completer<void>();
    releases.add(() {
      if (!pending.isCompleted) pending.complete();
    });
    return pending;
  }

  Completer<T> resultGate<T>(T cleanupResult) {
    final pending = Completer<T>();
    releases.add(() {
      if (!pending.isCompleted) pending.complete(cleanupResult);
    });
    return pending;
  }

  setUp(() {
    controllers.clear();
    releases.clear();
    clock = _DrainClock();
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

  for (final commit in [false, true]) {
    test('reentrant grace owns callback and commits only when enabled: $commit',
        () async {
      final a = ConsumerCycle();
      final app = gate();
      final c = make([a], commit: commit);
      Future<KafkaGroupConsumerDrainResult>? grace;
      var calls = 0, reported = false;
      c.run((record) {
        calls++;
        grace = c.drainAndStop(timeout: timeout);
        grace!.then((_) => reported = true);
        expect(c.state, KafkaGroupConsumerState.quiescing);
        expect(c.assignment, same(record.assignment));
        expect(c.isCurrent(record.assignment), isTrue);
        return app.future;
      });
      await until(() => calls == 1);
      await flush();
      expect(reported, isFalse);
      expect(a.commits, isEmpty);
      expect(
          a.driver.calls.where((call) => call.startsWith('leave:')), isEmpty);
      expect(a.progress!.delivery.bufferedRecordCount, 1);
      app.complete();
      final drained = await grace!;
      expect(drained.outcome, KafkaGroupConsumerDrainOutcome.drained);
      expect(drained.callbackPending, isFalse);
      expect(drained.commitPending, isFalse);
      expect(drained.error, isNull);
      expect(a.progress!.acknowledgedNextOffsets, {
        'a': {0: 11}
      });
      expect(
          a.commits,
          commit
              ? [
                  {0: 11}
                ]
              : isEmpty);
      if (commit) {
        expect(drained.lastCommit!.partitions[0]!.outcome,
            KafkaGroupCommitOutcome.confirmed);
      } else {
        expect(drained.lastCommit, isNull);
      }
      final closed = await c.closed;
      expect(closed.reason, KafkaGroupConsumerStopReason.cancelled);
      expect(calls, 1);
      expect(a.fetches, [10]);
      expect(a.progress!.delivery.bufferedRecordCount, 0);
    });
  }

  test('quiescing retains independent heartbeats while callback is pending',
      () async {
    final a = ConsumerCycle();
    final app = gate();
    final c = make([a], commit: true);
    var calls = 0;
    c.run((_) {
      calls++;
      return app.future;
    });
    await until(() => calls == 1);
    final token = c.assignment!;
    final before = a.driver.calls.where((call) => call == 'heartbeat').length;
    final grace = c.drainAndStop(timeout: const Duration(seconds: 1));
    for (var i = 0; i < 6; i++) {
      a.clock.advance(session_fixture.tick);
      await flush();
      expect(c.isCurrent(token), isTrue);
    }
    expect(
        a.driver.calls.where((call) => call == 'heartbeat').length, before + 6);
    expect(c.state, KafkaGroupConsumerState.quiescing);
    expect(calls, 1);
    expect(a.commits, isEmpty);
    expect(a.dataCloses, 0);
    app.complete();
    expect((await grace).outcome, KafkaGroupConsumerDrainOutcome.drained);
    await c.closed;
  });

  test('quiescing retains heartbeats and authority during admitted commit',
      () async {
    final a = ConsumerCycle();
    final pending = resultGate(processing_fixture.committed('a', {0: 11}));
    a.commit = (_, __) => pending.future;
    final c = make([a], commit: true);
    var calls = 0;
    c.run((_) => calls++);
    await until(() => a.commits.length == 1);
    final token = c.assignment!;
    final before = a.driver.calls.where((call) => call == 'heartbeat').length;
    final grace = c.drainAndStop(timeout: timeout);
    var reported = false;
    grace.then((_) => reported = true);
    a.clock.advance(session_fixture.tick);
    await flush();
    expect(reported, isFalse);
    expect(c.isCurrent(token), isTrue);
    expect(
        a.driver.calls.where((call) => call == 'heartbeat').length, before + 1);
    expect(a.driver.calls.where((call) => call.startsWith('leave:')), isEmpty);
    pending.complete(processing_fixture.committed('a', {0: 11}));
    final result = await grace;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
    expect(result.lastCommit!.locallyCurrent, isTrue);
    expect(result.commitPending, isFalse);
    await c.closed;
    expect(calls, 1);
    expect(a.commits, [
      {0: 11}
    ]);
  });

  test('repeat returns identical Future without extending the first deadline',
      () async {
    final a = ConsumerCycle();
    final app = gate();
    final c = make([a], commit: true);
    var called = false;
    c.run((_) {
      called = true;
      return app.future;
    });
    await until(() => called);
    final grace = c.drainAndStop(timeout: timeout);
    clock.advance(const Duration(milliseconds: 19));
    expect(c.drainAndStop(timeout: const Duration(hours: 1)), same(grace));
    clock.advance(const Duration(milliseconds: 1));
    final result = await grace;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.deadlineExpired);
    expect(result.callbackPending, isTrue);
    expect(result.commitPending, isFalse);
    expect(c.drainAndStop(timeout: Duration.zero), same(grace));
    app.complete();
    await c.closed;
    expect(c.drainAndStop(timeout: timeout), same(grace));
    expect(a.commits, isEmpty);
    expect(result.callbackPending, isTrue, reason: 'Completion is a snapshot');
  });

  test('early deadline timer rearms, and consumed callbacks stay harmless',
      () async {
    final a = ConsumerCycle();
    final app = gate();
    final c = make([a]);
    var called = false, reported = false;
    c.run((_) {
      called = true;
      return app.future;
    });
    await until(() => called);
    final grace = c.drainAndStop(timeout: timeout);
    grace.then((_) => reported = true);
    final original = clock.timers.singleWhere((timer) => !timer.cancelled);
    clock.now = timeout - const Duration(microseconds: 1);
    original.cancelled = true;
    original.callback();
    await flush();
    expect(reported, isFalse);
    expect(c.state, KafkaGroupConsumerState.quiescing);
    final rearmed = clock.timers.singleWhere((timer) => !timer.cancelled);
    expect(rearmed.at, greaterThanOrEqualTo(timeout));
    final count = clock.timers.length;
    original.callback();
    expect(clock.timers.length, count);
    expect(rearmed.cancelled, isFalse);
    clock.now = rearmed.at;
    clock.fireDue();
    expect(
        (await grace).outcome, KafkaGroupConsumerDrainOutcome.deadlineExpired);
    app.complete();
    await c.closed;
    final afterClose = clock.timers.length;
    original.callback();
    rearmed.callback();
    expect(clock.timers.length, afterClose);
    expect(a.closes, 1);
  });

  for (final elapsed in [
    timeout - const Duration(microseconds: 1),
    timeout,
    timeout + const Duration(microseconds: 1)
  ]) {
    test('callback completion checks monotonic deadline at $elapsed', () async {
      final a = ConsumerCycle();
      final app = gate();
      final c = make([a], commit: true);
      var called = false;
      c.run((_) {
        called = true;
        return app.future;
      });
      await until(() => called);
      final grace = c.drainAndStop(timeout: timeout);
      clock.advance(elapsed, fire: false);
      app.complete();
      final result = await grace;
      final inTime = elapsed < timeout;
      expect(
          result.outcome,
          inTime
              ? KafkaGroupConsumerDrainOutcome.drained
              : KafkaGroupConsumerDrainOutcome.deadlineExpired);
      expect(
          a.commits,
          inTime
              ? [
                  {0: 11}
                ]
              : isEmpty);
      expect(
          a.progress!.acknowledgedNextOffsets,
          inTime
              ? {
                  'a': {0: 11}
                }
              : isEmpty);
      await c.closed;
    });
  }

  for (final elapsed in [
    timeout - const Duration(microseconds: 1),
    timeout,
    timeout + const Duration(microseconds: 1)
  ]) {
    test('commit completion checks monotonic deadline at $elapsed', () async {
      final a = ConsumerCycle();
      final pending = resultGate(processing_fixture.committed('a', {0: 11}));
      a.commit = (_, __) => pending.future;
      final c = make([a], commit: true);
      c.run((_) {});
      await until(() => a.commits.length == 1);
      final grace = c.drainAndStop(timeout: timeout);
      clock.advance(elapsed, fire: false);
      pending.complete(processing_fixture.committed('a', {0: 11}));
      final result = await grace;
      expect(
          result.outcome,
          elapsed < timeout
              ? KafkaGroupConsumerDrainOutcome.drained
              : KafkaGroupConsumerDrainOutcome.deadlineExpired);
      expect(result.lastCommit!.partitions[0]!.outcome,
          KafkaGroupCommitOutcome.confirmed);
      expect(result.callbackPending, isFalse);
      expect(result.commitPending, isFalse);
      await c.closed;
      expect(a.commits, [
        {0: 11}
      ]);
    });
  }

  test('callback acknowledgement cannot admit commit after deadline advances',
      () async {
    final a = ConsumerCycle();
    final app = gate();
    final c = make([a], commit: true);
    var called = false;
    c.run((_) {
      called = true;
      return app.future;
    });
    await until(() => called);
    final grace = c.drainAndStop(timeout: timeout);
    clock.deadline = timeout;
    clock.readsUntilDeadline = 2;
    app.complete();
    final result = await grace;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.deadlineExpired);
    expect(a.commits, isEmpty);
    await c.closed;
  });

  test('zero timeout expires pending callback synchronously without a commit',
      () async {
    final a = ConsumerCycle();
    final app = gate();
    final c = make([a], commit: true);
    Future<KafkaGroupConsumerDrainResult>? grace;
    c.run((record) {
      grace = c.drainAndStop(timeout: Duration.zero);
      expect(c.isCurrent(record.assignment), isFalse);
      return app.future;
    });
    await until(() => grace != null);
    final result = await grace!;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.deadlineExpired);
    expect(result.callbackPending, isTrue);
    expect(result.commitPending, isFalse);
    app.complete();
    await c.closed;
    expect(a.commits, isEmpty);
  });

  for (final response in ['confirmed', 'rejected', 'unknown']) {
    test('expired grace preserves late $response commit without retry or Leave',
        () async {
      final a = ConsumerCycle(), b = ConsumerCycle();
      final pending = resultGate(processing_fixture.committed('a', {0: 11}));
      a.commit = (_, __) => pending.future;
      final c = make([a, b], commit: true);
      var closed = false, calls = 0;
      c.run((_) => calls++).then((_) => closed = true);
      await until(() => a.commits.length == 1);
      final token = c.assignment!;
      final grace = c.drainAndStop(timeout: timeout);
      clock.advance(timeout);
      final result = await grace;
      expect(result.outcome, KafkaGroupConsumerDrainOutcome.deadlineExpired);
      expect(result.callbackPending, isFalse);
      expect(result.commitPending, isTrue);
      expect(result.lastCommit, isNull);
      expect(c.isCurrent(token), isFalse);
      await flush();
      expect(closed, isFalse);
      expect(
          a.driver.calls.where((call) => call.startsWith('leave:')), isEmpty);
      final lost = StateError('commit response lost');
      if (response == 'unknown') {
        pending.completeError(lost);
      } else {
        pending.complete(processing_fixture.committed('a', {0: 11},
            codes: {0: response == 'rejected' ? 27 : 0}));
      }
      final terminal = await c.closed;
      final expected = response == 'confirmed'
          ? KafkaGroupCommitOutcome.confirmed
          : response == 'rejected'
              ? KafkaGroupCommitOutcome.rejected
              : KafkaGroupCommitOutcome.unknown;
      expect(terminal.lastCommit!.partitions[0]!.outcome, expected);
      expect(terminal.lastCommit!.locallyCurrent, isFalse);
      expect(
          terminal.reason,
          response == 'confirmed'
              ? KafkaGroupConsumerStopReason.cancelled
              : KafkaGroupConsumerStopReason.failure);
      if (response == 'unknown') expect(terminal.error, same(lost));
      expect(result.lastCommit, isNull,
          reason: 'Grace result remains a snapshot');
      expect(result.commitPending, isTrue);
      expect(a.commits, [
        {0: 11}
      ]);
      expect(
          a.driver.calls.where((call) => call.startsWith('leave:')), isEmpty);
      expect(calls, 1);
      expect(b.starts, 0);
    });
  }

  test('pending commit report distinguishes the previously settled commit',
      () async {
    final a = ConsumerCycle();
    final pending = resultGate(processing_fixture.committed('a', {0: 12}));
    a.commit = (topic, offsets) => offsets[0] == 11
        ? Future.value(processing_fixture.committed(topic, offsets))
        : pending.future;
    final c = make([a], commit: true);
    var callbacks = 0;
    c.run((_) => callbacks++);
    await until(() => a.commits.length == 2);
    final grace = c.drainAndStop(timeout: timeout);
    clock.advance(timeout);
    final result = await grace;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.deadlineExpired);
    expect(result.callbackPending, isFalse);
    expect(result.commitPending, isTrue);
    expect(result.lastCommit!.partitions[0]!.nextOffset, 11);
    expect(result.lastCommit!.partitions[0]!.outcome,
        KafkaGroupCommitOutcome.confirmed);
    pending.complete(processing_fixture.committed('a', {0: 12}));
    final terminal = await c.closed;
    expect(terminal.lastCommit!.partitions[0]!.nextOffset, 12);
    expect(result.lastCommit!.partitions[0]!.nextOffset, 11);
    expect(callbacks, 2);
    expect(a.commits, [
      {0: 11},
      {0: 12}
    ]);
    expect(a.driver.calls.where((call) => call.startsWith('leave:')), isEmpty);
  });

  for (final response in ['rejected', 'unknown']) {
    test('in-budget $response commit fails grace and never rejoins', () async {
      final a = ConsumerCycle(), b = ConsumerCycle();
      final pending = resultGate(processing_fixture.committed('a', {0: 11}));
      a.commit = (_, __) => pending.future;
      final c = make([a, b], commit: true);
      c.run((_) {});
      await until(() => a.commits.length == 1);
      final grace = c.drainAndStop(timeout: timeout);
      if (response == 'unknown') {
        pending.completeError(StateError('unknown commit'));
      } else {
        pending.complete(
            processing_fixture.committed('a', {0: 11}, codes: {0: 27}));
      }
      final result = await grace;
      expect(
          result.outcome,
          anyOf(KafkaGroupConsumerDrainOutcome.failed,
              KafkaGroupConsumerDrainOutcome.revoked));
      expect(result.error, isNotNull);
      final terminal = await c.closed;
      expect(terminal.reason, KafkaGroupConsumerStopReason.failure);
      expect(
          terminal.lastCommit!.partitions[0]!.outcome,
          response == 'unknown'
              ? KafkaGroupCommitOutcome.unknown
              : KafkaGroupCommitOutcome.rejected);
      expect(a.commits, [
        {0: 11}
      ]);
      expect(b.starts, 0);
    });
  }

  for (final synchronous in [false, true]) {
    test('callback ${synchronous ? 'throw' : 'Future error'} fails grace',
        () async {
      final a = ConsumerCycle(), b = ConsumerCycle();
      final app = gate();
      final error = StateError('application failed');
      final c = make([a, b], commit: true);
      Future<KafkaGroupConsumerDrainResult>? grace;
      c.run((_) {
        grace = c.drainAndStop(timeout: timeout);
        if (synchronous) throw error;
        return app.future;
      });
      await until(() => grace != null);
      if (!synchronous) app.completeError(error);
      final result = await grace!;
      expect(result.outcome, KafkaGroupConsumerDrainOutcome.failed);
      expect(result.error, same(error));
      expect(result.callbackPending, isFalse);
      final terminal = await c.closed;
      expect(terminal.reason, KafkaGroupConsumerStopReason.failure);
      expect(terminal.error, same(error));
      expect(a.commits, isEmpty);
      expect(b.starts, 0);
    });
  }

  test('late callback failure cannot rewrite an expired drain result',
      () async {
    final a = ConsumerCycle();
    final app = gate();
    final c = make([a], commit: true);
    var called = false;
    c.run((_) {
      called = true;
      return app.future;
    });
    await until(() => called);
    final grace = c.drainAndStop(timeout: timeout);
    clock.advance(timeout);
    final result = await grace;
    final error = StateError('late callback failure');
    app.completeError(error);
    final terminal = await c.closed;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.deadlineExpired);
    expect(result.callbackPending, isTrue);
    expect(terminal.reason, KafkaGroupConsumerStopReason.failure);
    expect(terminal.error, same(error));
    expect(a.commits, isEmpty);
  });

  for (final cause in [
    'heartbeat rebalance',
    'heartbeat transport',
    'local expiry',
    'processing timeout'
  ]) {
    test('$cause during grace fails closed without replacing generation',
        () async {
      final a = ConsumerCycle(), b = ConsumerCycle();
      if (cause == 'processing timeout') {
        a.processingTimeout = const Duration(milliseconds: 15);
      }
      final app = gate();
      final c = make([a, b], commit: true, rejoinOnCoordinatorLoss: true);
      var called = false, closed = false;
      c.run((_) {
        called = true;
        return app.future;
      }).then((_) => closed = true);
      await until(() => called);
      final token = c.assignment!;
      final grace = c.drainAndStop(timeout: const Duration(seconds: 1));
      if (cause == 'heartbeat rebalance') {
        a.reject();
      } else if (cause == 'heartbeat transport') {
        a.driver.onHeartbeat =
            () => Future.error(TimeoutException('heartbeat'));
        a.clock.advance(session_fixture.tick);
      } else if (cause == 'local expiry') {
        a.clock.advance(session_fixture.lease, fire: false);
        expect(c.isCurrent(token), isFalse);
      } else {
        a.clock.advance(session_fixture.tick);
        await flush();
        a.clock.advance(const Duration(milliseconds: 5));
      }
      final result = await grace;
      expect(
          result.outcome,
          anyOf(KafkaGroupConsumerDrainOutcome.revoked,
              KafkaGroupConsumerDrainOutcome.failed));
      expect(result.callbackPending, isTrue);
      expect(result.commitPending, isFalse);
      expect(c.isCurrent(token), isFalse);
      await flush();
      expect(closed, isFalse);
      app.complete();
      await c.closed;
      expect(a.commits, isEmpty);
      expect(b.starts, 0);
    });
  }

  test('grace requested immediately after revocation does not grant authority',
      () async {
    final a = ConsumerCycle(), b = ConsumerCycle();
    final app = gate();
    final c = make([a, b], commit: true);
    var called = false;
    c.run((_) {
      called = true;
      return app.future;
    });
    await until(() => called);
    a.reject();
    final result = await c.drainAndStop(timeout: timeout);
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.revoked);
    expect(result.callbackPending, isTrue);
    app.complete();
    await c.closed;
    expect(a.commits, isEmpty);
    expect(b.starts, 0);
  });

  test('explicit stop supersedes grace and forbids subsequent commit',
      () async {
    final a = ConsumerCycle();
    final app = gate();
    final c = make([a], commit: true);
    var called = false, closed = false;
    c.run((_) {
      called = true;
      return app.future;
    }).then((_) => closed = true);
    await until(() => called);
    final token = c.assignment!;
    final grace = c.drainAndStop(timeout: timeout);
    c.stop();
    expect(c.isCurrent(token), isFalse);
    final result = await grace;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.abortedByStop);
    expect(result.callbackPending, isTrue);
    expect(result.commitPending, isFalse);
    c.stop();
    expect(c.drainAndStop(timeout: const Duration(hours: 1)), same(grace));
    await flush();
    expect(closed, isFalse);
    clock.advance(timeout);
    app.complete();
    await c.closed;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.abortedByStop);
    expect(a.commits, isEmpty);
  });

  test('explicit stop during commit preserves pending and late confirmation',
      () async {
    final a = ConsumerCycle();
    final pending = resultGate(processing_fixture.committed('a', {0: 11}));
    a.commit = (_, __) => pending.future;
    final c = make([a], commit: true);
    c.run((_) {});
    await until(() => a.commits.length == 1);
    final grace = c.drainAndStop(timeout: timeout);
    c.stop();
    final result = await grace;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.abortedByStop);
    expect(result.callbackPending, isFalse);
    expect(result.commitPending, isTrue);
    expect(a.driver.calls.where((call) => call.startsWith('leave:')), isEmpty);
    pending.complete(processing_fixture.committed('a', {0: 11}));
    final terminal = await c.closed;
    expect(terminal.lastCommit!.partitions[0]!.outcome,
        KafkaGroupCommitOutcome.confirmed);
    expect(result.commitPending, isTrue);
    expect(a.commits, [
      {0: 11}
    ]);
  });

  test('idle grace reports no owned work without joining, even at zero timeout',
      () async {
    final a = ConsumerCycle();
    final data = gate();
    a.closePosition = () => data.future;
    final c = make([a]);
    var closed = false;
    c.closed.then((_) => closed = true);
    final grace = c.drainAndStop(timeout: Duration.zero);
    final result = await grace;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
    expect(result.callbackPending, isFalse);
    expect(result.commitPending, isFalse);
    expect(a.starts, 0);
    expect(a.driver.calls.where((call) => call.startsWith('join:')), isEmpty);
    await expectLater(c.run((_) {}), throwsStateError);
    await flush();
    expect(closed, isFalse);
    data.complete();
    await c.closed;
  });

  for (final phase in ['prepare', 'join', 'sync', 'initial heartbeat']) {
    test('grace during $phase stops without waiting for a new assignment',
        () async {
      final a = ConsumerCycle();
      final prepare = gate();
      final join = resultGate(session_fixture.joined());
      final sync = resultGate(ConsumerGroupAssignment({
        'a': [0]
      }));
      final heartbeat = resultGate(HeartbeatResponse(1, 0, 0));
      if (phase == 'prepare') a.driver.onPrepare = () => prepare.future;
      if (phase == 'join') a.driver.onJoin = (_) => join.future;
      if (phase == 'sync') a.driver.onSync = () => sync.future;
      if (phase == 'initial heartbeat') {
        a.driver.onHeartbeat = () => heartbeat.future;
      }
      final target = phase == 'initial heartbeat'
          ? 'heartbeat'
          : phase == 'join'
              ? 'join:'
              : phase;
      final c = make([a]);
      var callbacks = 0;
      c.run((_) => callbacks++);
      await until(() => a.driver.calls.contains(target));
      final result = await c.drainAndStop(timeout: timeout);
      expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
      expect(result.callbackPending, isFalse);
      expect(result.commitPending, isFalse);
      expect(c.assignment, isNull);
      prepare.complete();
      join.complete(session_fixture.joined());
      sync.complete(ConsumerGroupAssignment({
        'a': [0]
      }));
      heartbeat.complete(HeartbeatResponse(1, 0, 0));
      await c.closed;
      await flush();
      expect(a.reads, isEmpty);
      expect(a.fetches, isEmpty);
      expect(callbacks, 0);
      expect(a.starts, 1);
    });
  }

  for (final phase in [
    'committed lookup',
    'boundary lookup',
    'position cleanup'
  ]) {
    test('grace during $phase cannot start processing from late positions',
        () async {
      final a = ConsumerCycle();
      final read = resultGate<Map<int, KafkaCommittedOffset?>>(
          {0: const KafkaCommittedOffset(nextOffset: 10, leaderEpoch: -1)});
      final boundary = resultGate(10);
      final cleanup = gate();
      if (phase == 'committed lookup') a.read = (_, __) => read.future;
      if (phase == 'boundary lookup') {
        a.read = (_, __) async => {0: null};
        a.resolveBoundary = (_, __, ___) => boundary.future;
      }
      if (phase == 'position cleanup') a.closePosition = () => cleanup.future;
      final c = make([a], missingPosition: KafkaOffsetPosition.earliest);
      var callbacks = 0, closed = false;
      c.run((_) => callbacks++).then((_) => closed = true);
      await until(() => phase == 'committed lookup'
          ? a.reads.isNotEmpty
          : phase == 'boundary lookup'
              ? a.boundaryRequests.isNotEmpty
              : a.positionCloses == 1);
      final result = await c.drainAndStop(timeout: timeout);
      expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
      expect(result.callbackPending, isFalse);
      expect(result.commitPending, isFalse);
      await flush();
      expect(closed, isFalse);
      read.complete(
          {0: const KafkaCommittedOffset(nextOffset: 10, leaderEpoch: -1)});
      boundary.complete(10);
      cleanup.complete();
      await c.closed;
      expect(a.loaded, isNull);
      expect(a.fetches, isEmpty);
      expect(callbacks, 0);
    });
  }

  test('grace during rejoin backoff cancels delay and never creates next cycle',
      () async {
    final a = ConsumerCycle()..emit = false;
    final b = ConsumerCycle();
    final c = make([a, b], delay: const Duration(minutes: 1));
    c.run((_) {});
    await until(() => a.loaded != null);
    a.reject();
    await until(() => c.state == KafkaGroupConsumerState.backoff);
    final result = await c.drainAndStop(timeout: timeout);
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
    expect(result.callbackPending, isFalse);
    expect(result.commitPending, isFalse);
    await c.closed;
    expect(b.starts, 0);
    expect(c.generationsStarted, 1);
  });

  test('processing with no application work stops without spending grace',
      () async {
    final a = ConsumerCycle()..emit = false;
    final c = make([a]);
    c.run((_) => fail('No record expected'));
    await until(() => a.fetches.isNotEmpty);
    final result = await c.drainAndStop(timeout: timeout);
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
    expect(result.callbackPending, isFalse);
    expect(result.commitPending, isFalse);
    await c.closed;
    expect(clock.now, Duration.zero);
    expect(a.commits, isEmpty);
  });

  test('late Fetch is discarded and closed still waits actual Fetch settlement',
      () async {
    final pending = resultGate(delivery_fixture.reply('a', 0, [10, 11]));
    final a = _PendingFetchCycle(pending);
    final c = make([a], commit: true);
    var callbacks = 0, closed = false;
    c.run((_) => callbacks++).then((_) => closed = true);
    await until(() => a.fetches.isNotEmpty);
    final result = await c.drainAndStop(timeout: timeout);
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
    expect(result.callbackPending, isFalse);
    expect(result.commitPending, isFalse);
    await flush();
    expect(closed, isFalse);
    expect(a.dataCloses, 1);
    pending.complete(delivery_fixture.reply('a', 0, [10, 11]));
    await c.closed;
    expect(callbacks, 0);
    expect(a.commits, isEmpty);
    expect(a.fetches, [10]);
  });

  test('drained reports work completion before separate cleanup barriers',
      () async {
    final a = ConsumerCycle();
    final app = gate(), data = gate(), control = gate();
    a.closeData = () => data.future;
    a.driver.onClose = () => control.future;
    final c = make([a], commit: true);
    var called = false, closed = false;
    c.run((_) {
      called = true;
      return app.future;
    }).then((_) => closed = true);
    await until(() => called);
    final grace = c.drainAndStop(timeout: timeout);
    app.complete();
    final result = await grace;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
    expect(result.lastCommit!.partitions[0]!.outcome,
        KafkaGroupCommitOutcome.confirmed);
    await until(() => a.dataCloses == 1 && a.driver.calls.contains('close'));
    expect(closed, isFalse);
    clock.advance(const Duration(hours: 1));
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
    data.complete();
    await flush();
    expect(closed, isFalse);
    control.complete();
    expect((await c.closed).reason, KafkaGroupConsumerStopReason.cancelled);
  });

  test('cleanup failure is terminal evidence without rewriting drained work',
      () async {
    final a = ConsumerCycle();
    final cleanup = StateError('data close failed');
    a.closeData = () => Future.error(cleanup);
    final c = make([a]);
    Future<KafkaGroupConsumerDrainResult>? grace;
    c.run((_) {
      grace = c.drainAndStop(timeout: timeout);
    });
    await until(() => grace != null);
    final result = await grace!;
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
    final terminal = await c.closed;
    expect(terminal.reason, KafkaGroupConsumerStopReason.failure);
    expect(terminal.cleanupErrors, contains(cleanup));
    expect(result.outcome, KafkaGroupConsumerDrainOutcome.drained);
  });

  for (final duration in [
    const Duration(microseconds: -1),
    const Duration(hours: 1, microseconds: 1)
  ]) {
    test('invalid grace $duration throws synchronously without changing state',
        () async {
      final a = ConsumerCycle();
      final c = make([a]);
      expect(() => c.drainAndStop(timeout: duration), throwsArgumentError);
      expect(c.state, KafkaGroupConsumerState.idle);
      expect(a.closes, 0);
      final valid = c.drainAndStop(timeout: const Duration(hours: 1));
      expect((await valid).outcome, KafkaGroupConsumerDrainOutcome.drained);
      await c.closed;
    });
  }

  for (final waitForClosed in [false, true]) {
    test(
        'request after explicit stop${waitForClosed ? ' and closed' : ''} is aborted',
        () async {
      final a = ConsumerCycle();
      final c = make([a]);
      c.stop();
      if (waitForClosed) await c.closed;
      final grace = c.drainAndStop(timeout: timeout);
      final result = await grace;
      expect(result.outcome, KafkaGroupConsumerDrainOutcome.abortedByStop);
      expect(result.callbackPending, isFalse);
      expect(result.commitPending, isFalse);
      expect(c.drainAndStop(timeout: timeout), same(grace));
      await c.closed;
      expect(a.starts, 0);
      expect(a.closes, 1);
    });
  }
}
