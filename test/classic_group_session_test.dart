import 'dart:async';

import 'package:kafka_client/src/classic_group_driver.dart';
import 'package:kafka_client/src/classic_group_session.dart';
import 'package:kafka_client/src/consumer_group_metadata.dart';
import 'package:kafka_client/src/group_membership_protocol.dart';
import 'package:kafka_client/src/reliability.dart';
import 'package:test/test.dart';

const tick = Duration(milliseconds: 10);
const lease = Duration(milliseconds: 50);
Future<void> flush() => Future<void>.delayed(Duration.zero);

class FakeTimer implements ClassicGroupTimer {
  final Duration at;
  final void Function() callback;
  bool cancelled = false;
  FakeTimer(this.at, this.callback);
  @override
  void cancel() => cancelled = true;
}

class Clock implements ClassicGroupScheduler {
  @override
  Duration now = Duration.zero;
  final timers = <FakeTimer>[];
  @override
  ClassicGroupTimer schedule(Duration delay, void Function() callback) {
    final timer = FakeTimer(now + delay, callback);
    timers.add(timer);
    return timer;
  }

  void advance(Duration duration, {bool fire = true}) {
    now += duration;
    if (fire) fireDue();
  }

  void fireDue() {
    while (true) {
      final due = timers.where((t) => !t.cancelled && t.at <= now).toList()
        ..sort((a, b) => a.at.compareTo(b.at));
      if (due.isEmpty) return;
      due.first.cancelled = true;
      due.first.callback();
    }
  }
}

JoinGroupResponse joined({int code = 0, String member = 'm'}) =>
    JoinGroupResponse(1, 0, code, 7, 'range', 'leader', member, []);

class Driver extends ClassicGroupDriver {
  final calls = <String>[];
  Future<void> Function()? onPrepare;
  Future<JoinGroupResponse> Function(String)? onJoin;
  Future<ConsumerGroupAssignment> Function()? onSync;
  Future<HeartbeatResponse> Function()? onHeartbeat;
  Future<LeaveGroupResponse> Function()? onLeave;
  Future<void> Function()? onClose;
  @override
  Future<void> prepare() async {
    calls.add('prepare');
    await onPrepare?.call();
  }

  @override
  Future<JoinGroupResponse> join(String memberId) async {
    calls.add('join:$memberId');
    return onJoin == null ? joined() : await onJoin!(memberId);
  }

  @override
  Future<ConsumerGroupAssignment> sync(JoinGroupResponse joined) async {
    calls.add('sync');
    return onSync == null
        ? ConsumerGroupAssignment({
            'a': [0, 2]
          })
        : await onSync!();
  }

  @override
  Future<HeartbeatResponse> heartbeat(JoinGroupResponse joined) async {
    calls.add('heartbeat');
    return onHeartbeat == null
        ? HeartbeatResponse(1, 0, 0)
        : await onHeartbeat!();
  }

  @override
  Future<LeaveGroupResponse> leave(String memberId) async {
    calls.add('leave:$memberId');
    return onLeave == null ? LeaveGroupResponse(1, 0, 0) : await onLeave!();
  }

  @override
  Future<void> close() async {
    calls.add('close');
    await onClose?.call();
  }
}

ClassicGroupSessionController controller(Driver driver, Clock clock) =>
    ClassicGroupSessionController(
        driver: driver,
        scheduler: clock,
        heartbeatInterval: tick,
        localExpiry: lease,
        startupTimeout: const Duration(milliseconds: 100),
        leaveTimeout: tick);

void main() {
  late Clock clock;
  late Driver driver;
  late ClassicGroupSessionController session;
  setUp(() {
    clock = Clock();
    driver = Driver();
    session = controller(driver, clock);
  });

  for (final startup in [true, false]) {
    test(
        'early ${startup ? 'startup' : 'expiry'} watchdog rearms without authority reads',
        () async {
      final pendingJoin = Completer<JoinGroupResponse>();
      final pendingHeartbeat = Completer<HeartbeatResponse>();
      addTearDown(() async {
        if (startup && !pendingJoin.isCompleted) {
          pendingJoin.completeError(TimeoutException('Test cleanup'));
        }
        if (!pendingHeartbeat.isCompleted) {
          pendingHeartbeat.complete(HeartbeatResponse(1, 0, 0));
        }
        await session.close();
      });
      Future<void>? failedStart;
      if (startup) {
        driver.onJoin = (_) => pendingJoin.future;
        failedStart =
            expectLater(session.start(), throwsA(isA<TimeoutException>()));
        await flush();
      } else {
        await session.start();
        driver.onHeartbeat = () => pendingHeartbeat.future;
        clock.advance(tick);
        await flush();
      }
      KafkaGroupTermination? revoked;
      session.revoked.then((value) {
        revoked = value;
      });
      final target = startup ? const Duration(milliseconds: 100) : lease;
      final watchdog = clock.timers
          .singleWhere((timer) => !timer.cancelled && timer.at == target);
      clock.now = target - const Duration(microseconds: 1);
      watchdog.cancelled = true;
      watchdog.callback();
      await flush();
      expect(revoked, isNull);
      expect(driver.calls.where((call) => call == 'close'), isEmpty);
      final rearmed = clock.timers.singleWhere((timer) => !timer.cancelled);
      // The absolute target is unchanged; only the watchdog wake is rounded up.
      expect(rearmed.at, target + const Duration(microseconds: 999));
      final countAfterRearm = clock.timers.length;
      watchdog.callback(); // Stale callback with the same absolute target.
      expect(clock.timers.length, countAfterRearm);
      expect(rearmed.cancelled, isFalse);
      clock.advance(const Duration(milliseconds: 1));
      await flush();
      expect(
          revoked?.reason,
          startup
              ? KafkaGroupStopReason.startupTimeout
              : KafkaGroupStopReason.localExpiry);
      expect(driver.calls.where((call) => call == 'close'), hasLength(1));
      expect((await session.closed).reason, revoked!.reason);
      if (startup) {
        pendingJoin.complete(joined());
        await failedStart;
      } else {
        pendingHeartbeat.complete(HeartbeatResponse(1, 0, 0));
      }
      await flush();
      // Consumed callbacks cannot resurrect a watchdog after closure.
      final timersAfterClose = clock.timers.length;
      watchdog.callback();
      rearmed.callback();
      expect(clock.timers.length, timersAfterClose);
      expect(driver.calls.where((call) => call == 'close'), hasLength(1));
    });
  }

  test('stale expiry watchdog cannot replace a renewed deadline', () async {
    await session.start();
    final old = clock.timers
        .singleWhere((timer) => !timer.cancelled && timer.at == lease);
    clock.advance(tick);
    await flush(); // Successful heartbeat renews send-based target to 60ms.
    final renewed = clock.timers
        .singleWhere((timer) => !timer.cancelled && timer.at == lease + tick);
    driver.onHeartbeat = () => Completer<HeartbeatResponse>().future;
    clock.advance(tick);
    await flush();
    KafkaGroupTermination? revoked;
    session.revoked.then((value) {
      revoked = value;
    });
    clock.now = lease;
    final count = clock.timers.length;
    old.callback();
    expect(clock.timers.length, count);
    expect(renewed.cancelled, isFalse);
    await flush();
    expect(revoked, isNull);
    clock.advance(tick);
    await flush();
    expect(revoked?.reason, KafkaGroupStopReason.localExpiry);
    expect(driver.calls.where((call) => call == 'close'), hasLength(1));
    await session.closed;
  });

  test('stale startup watchdog cannot rearm after startup completes', () async {
    final pendingJoin = Completer<JoinGroupResponse>();
    driver.onJoin = (_) => pendingJoin.future;
    final start = session.start();
    final old = clock.timers.singleWhere((timer) => !timer.cancelled);
    pendingJoin.complete(joined());
    await start;
    final count = clock.timers.length;
    old.callback();
    expect(clock.timers.length, count);
    expect(driver.calls.where((call) => call == 'close'), isEmpty);
    await session.close();
  });

  test('grants only after Join Sync and initial heartbeat; immutable snapshot',
      () async {
    final pending = Completer<HeartbeatResponse>();
    driver.onHeartbeat = () => pending.future;
    final start = session.start();
    await flush();
    expect(session.currentAssignment, isNull);
    expect(session.state, KafkaGroupSessionState.confirming);
    expect(driver.calls, ['prepare', 'join:', 'sync', 'heartbeat']);
    pending.complete(HeartbeatResponse(1, 0, 0));
    final token = await start;
    expect(token.generationId, 7);
    expect(token.memberId, 'm');
    expect(token.epoch, greaterThan(0));
    expect(session.isCurrent(token), isTrue);
    expect(() => token.partitions['a']!.add(9), throwsUnsupportedError);
    expect(() => token.partitions['b'] = [], throwsUnsupportedError);
    final close = session.close();
    expect(session.isCurrent(token), isFalse);
    expect(() => session.assertCurrent(token), throwsStateError);
    expect((await close).leaveOutcome, KafkaGroupLeaveOutcome.confirmed);
  });

  test('empty assignment is a valid empty snapshot', () async {
    driver.onSync = () async => ConsumerGroupAssignment({});
    expect((await session.start()).partitions, isEmpty);
    await session.close();
  });

  test('tokens are exact controller identities, even identical generations',
      () async {
    final other = controller(Driver(), clock);
    final a = await session.start();
    final b = await other.start();
    expect(session.isCurrent(b), isFalse);
    expect(other.isCurrent(a), isFalse);
    await session.close();
    await other.close();
  });

  test('start one-shot and close idempotent including before start', () async {
    final token = await session.start();
    await expectLater(session.start(), throwsStateError);
    final first = session.close();
    expect(identical(first, session.close()), isTrue);
    await first;
    expect(session.isCurrent(token), isFalse);
    expect(driver.calls.where((c) => c == 'close').length, 1);
    await expectLater(session.start(), throwsStateError);
    final untouched = controller(Driver(), clock);
    await untouched.close();
    await expectLater(untouched.start(), throwsStateError);
  });

  test('Join79 continues once with supplied nonempty member ID', () async {
    driver.onJoin = (id) async =>
        id.isEmpty ? joined(code: 79, member: 'assigned') : joined(member: id);
    expect((await session.start()).memberId, 'assigned');
    expect(driver.calls.where((c) => c.startsWith('join:')),
        ['join:', 'join:assigned']);
    await session.close();
  });

  for (final code in [79, 22, 25, 27, 16, 42]) {
    test('continuation error $code terminates without another Join', () async {
      driver.onJoin = (id) async => joined(code: id.isEmpty ? 79 : code);
      await expectLater(session.start(), throwsA(isA<KafkaBrokerException>()));
      expect((await session.closed).reason, KafkaGroupStopReason.brokerError);
      expect(driver.calls.where((c) => c.startsWith('join:')).length, 2);
      expect(driver.calls.any((c) => c.startsWith('leave:')), isFalse);
    });
  }

  test('empty Join79 ID fails rather than replaying', () async {
    driver.onJoin = (_) async => joined(code: 79, member: '');
    await expectLater(session.start(), throwsFormatException);
    await session.closed;
    expect(driver.calls.where((c) => c.startsWith('join:')).length, 1);
  });

  for (final stage in ['prepare', 'join', 'sync', 'initial heartbeat']) {
    test('close during $stage prevents all late publication', () async {
      final gate = Completer<void>();
      switch (stage) {
        case 'prepare':
          driver.onPrepare = () => gate.future;
        case 'join':
          driver.onJoin = (_) async {
            await gate.future;
            return joined();
          };
        case 'sync':
          driver.onSync = () async {
            await gate.future;
            return ConsumerGroupAssignment({});
          };
        case 'initial heartbeat':
          driver.onHeartbeat = () async {
            await gate.future;
            return HeartbeatResponse(1, 0, 0);
          };
      }
      final start = session.start();
      final failed = expectLater(start, throwsStateError);
      await flush();
      final result = await session.close();
      expect(result.leaveOutcome, KafkaGroupLeaveOutcome.notAttempted);
      gate.complete();
      await flush();
      await failed;
      expect(session.currentAssignment, isNull);
      expect(session.state, KafkaGroupSessionState.closed);
      expect(driver.calls.where((c) => c == 'close').length, 1);
    });
  }

  test('startup watchdog closes transport without awaiting stalled Join',
      () async {
    final pending = Completer<JoinGroupResponse>();
    driver.onJoin = (_) => pending.future;
    final failed =
        expectLater(session.start(), throwsA(isA<TimeoutException>()));
    await flush();
    clock.advance(const Duration(milliseconds: 100));
    expect((await session.closed).reason, KafkaGroupStopReason.startupTimeout);
    expect(driver.calls, ['prepare', 'join:', 'close']);
    pending.complete(joined());
    await flush();
    await failed;
    expect(session.currentAssignment, isNull);
  });

  test('await continuation checks startup deadline even if timer stalled',
      () async {
    final pending = Completer<JoinGroupResponse>();
    driver.onJoin = (_) => pending.future;
    final failed =
        expectLater(session.start(), throwsA(isA<TimeoutException>()));
    await flush();
    clock.advance(const Duration(milliseconds: 100), fire: false);
    pending.complete(joined());
    await failed;
    expect((await session.closed).reason, KafkaGroupStopReason.startupTimeout);
    expect(driver.calls, ['prepare', 'join:', 'close']);
  });

  test('initial heartbeat expires from send start before response', () async {
    final pending = Completer<HeartbeatResponse>();
    driver.onHeartbeat = () => pending.future;
    final failed =
        expectLater(session.start(), throwsA(isA<TimeoutException>()));
    await flush();
    clock.advance(lease, fire: false);
    pending.complete(HeartbeatResponse(1, 0, 0));
    await failed;
    expect((await session.closed).reason, KafkaGroupStopReason.localExpiry);
    expect(session.currentAssignment, isNull);
  });

  test('direct guard rejects elapsed deadline without timer execution',
      () async {
    final token = await session.start();
    clock.advance(lease, fire: false);
    expect(session.isCurrent(token), isFalse);
    expect((await session.revoked).reason, KafkaGroupStopReason.localExpiry);
    expect((await session.closed).leaveOutcome,
        KafkaGroupLeaveOutcome.notAttempted);
  });

  test(
      'heartbeat scheduling independent of listeners and no overlapping request',
      () async {
    final token = await session.start();
    final pending = Completer<HeartbeatResponse>();
    driver.onHeartbeat = () => pending.future;
    clock.advance(tick);
    await flush();
    clock.advance(tick);
    await flush();
    clock.advance(tick);
    await flush();
    expect(driver.calls.where((c) => c == 'heartbeat').length, 2);
    expect(session.isCurrent(token), isTrue);
    clock.advance(const Duration(milliseconds: 20));
    expect(session.isCurrent(token), isFalse);
    expect((await session.closed).reason, KafkaGroupStopReason.localExpiry);
    pending.complete(HeartbeatResponse(1, 0, 0));
    await flush();
    expect(session.isCurrent(token), isFalse);
    expect(driver.calls.where((c) => c == 'heartbeat').length, 2);
  });

  test('successful heartbeat lease counted from send, not receive', () async {
    final token = await session.start();
    final pending = Completer<HeartbeatResponse>();
    driver.onHeartbeat = () => pending.future;
    clock.advance(tick);
    await flush();
    clock.advance(const Duration(milliseconds: 20), fire: false);
    pending.complete(HeartbeatResponse(1, 0, 0));
    await flush();
    clock.advance(const Duration(milliseconds: 30), fire: false);
    expect(session.isCurrent(token), isFalse); // send 10 + lease 50
    expect((await session.closed).reason, KafkaGroupStopReason.localExpiry);
  });

  for (final code in [22, 25, 27, 16, 79]) {
    test('heartbeat error $code synchronously revokes, no rejoin or Leave',
        () async {
      final token = await session.start();
      driver.onHeartbeat = () async => HeartbeatResponse(1, 0, code);
      clock.advance(tick);
      await flush();
      expect(session.isCurrent(token), isFalse);
      final stopped = await session.closed;
      expect(stopped.reason, KafkaGroupStopReason.brokerError);
      expect(stopped.leaveOutcome, KafkaGroupLeaveOutcome.notAttempted);
      expect(driver.calls.where((c) => c.startsWith('join:')).length, 1);
    });
  }

  for (final error in [
    TimeoutException('admission timeout'),
    StateError('disconnect'),
    const FormatException('malformed')
  ]) {
    test('uncertain heartbeat $error terminates and keeps original error',
        () async {
      final token = await session.start();
      driver.onHeartbeat = () => Future.error(error);
      driver.onClose = () => Future.error(StateError('cleanup failed'));
      clock.advance(tick);
      await flush();
      final result = await session.closed;
      expect(session.isCurrent(token), isFalse);
      expect(identical(result.error, error), isTrue);
      expect(result.cleanupError, isStateError);
      expect(result.leaveOutcome, KafkaGroupLeaveOutcome.notAttempted);
    });
  }

  test('close with heartbeat in flight skips Leave and ignores late success',
      () async {
    final token = await session.start();
    final pending = Completer<HeartbeatResponse>();
    driver.onHeartbeat = () => pending.future;
    clock.advance(tick);
    await flush();
    final done = session.close();
    expect(session.isCurrent(token), isFalse);
    expect((await done).leaveOutcome, KafkaGroupLeaveOutcome.notAttempted);
    pending.complete(HeartbeatResponse(1, 0, 0));
    await flush();
    expect(session.currentAssignment, isNull);
  });

  test('rejected Leave and failing close are distinct cleanup results',
      () async {
    await session.start();
    driver.onLeave = () async => LeaveGroupResponse(1, 0, 25);
    driver.onClose = () => Future.error(StateError('close failed'));
    final result = await session.close();
    expect(result.reason, KafkaGroupStopReason.closed);
    expect(result.error, isNull);
    expect(result.leaveOutcome, KafkaGroupLeaveOutcome.rejected);
    expect(result.leaveError, isA<KafkaBrokerException>());
    expect(result.cleanupError, isStateError);
  });

  test(
      'bounded Leave timeout closes transport; late success cannot claim departure',
      () async {
    final token = await session.start();
    final leave = Completer<LeaveGroupResponse>();
    driver.onLeave = () => leave.future;
    final done = session.close();
    expect(session.isCurrent(token), isFalse);
    expect((await session.revoked).reason, KafkaGroupStopReason.closed);
    clock.advance(tick);
    await flush();
    final result = await done;
    expect(result.leaveOutcome, KafkaGroupLeaveOutcome.unknown);
    expect(driver.calls.last, 'close');
    leave.complete(LeaveGroupResponse(1, 0, 0));
    await flush();
    expect((await session.closed).leaveOutcome, KafkaGroupLeaveOutcome.unknown);
  });

  test('transport close itself bounded, late error consumed', () async {
    final pending = Completer<void>();
    driver.onClose = () => pending.future;
    final done = session.close();
    clock.advance(tick);
    await flush();
    expect((await done).cleanupError, isA<TimeoutException>());
    pending.completeError(StateError('late close error'));
    await flush();
  });

  for (final stage in ['prepare', 'join', 'sync', 'initial heartbeat']) {
    test('failure during $stage preserves original cause and never grants',
        () async {
      final error = StateError('unknown $stage outcome');
      switch (stage) {
        case 'prepare':
          driver.onPrepare = () => Future.error(error);
        case 'join':
          driver.onJoin = (_) => Future.error(error);
        case 'sync':
          driver.onSync = () => Future.error(error);
        case 'initial heartbeat':
          driver.onHeartbeat = () => Future.error(error);
      }
      await expectLater(session.start(), throwsA(same(error)));
      final result = await session.closed;
      expect(result.error, same(error));
      expect(result.leaveOutcome, KafkaGroupLeaveOutcome.notAttempted);
      expect(session.assignment, isNull);
      expect(driver.calls.where((c) => c == 'close').length, 1);
    });
  }

  test('native Sync broker error remains original broker error', () async {
    final error = KafkaBrokerException(27, 'SyncGroup');
    driver.onSync = () => Future.error(error);
    await expectLater(session.start(), throwsA(same(error)));
    final stopped = await session.closed;
    expect(stopped.reason, KafkaGroupStopReason.brokerError);
    expect(stopped.error, same(error));
  });

  test('native Leave broker error is rejected, not unknown', () async {
    await session.start();
    final error = KafkaBrokerException(25, 'LeaveGroup');
    driver.onLeave = () => Future.error(error);
    final stopped = await session.close();
    expect(stopped.leaveOutcome, KafkaGroupLeaveOutcome.rejected);
    expect(stopped.leaveError, same(error));
  });

  test('initial heartbeat broker rejection cannot grant ownership', () async {
    driver.onHeartbeat = () async => HeartbeatResponse(1, 0, 27);
    await expectLater(session.start(), throwsA(isA<KafkaBrokerException>()));
    expect((await session.closed).reason, KafkaGroupStopReason.brokerError);
    expect(session.assignment, isNull);
  });

  test('successful heartbeat renews timer to send-based deadline', () async {
    final token = await session.start();
    clock.advance(tick);
    await flush();
    clock.advance(const Duration(milliseconds: 40), fire: false);
    expect(session.isCurrent(token), isTrue); // now 50, renewed expiry 60
    clock.advance(tick, fire: false);
    expect(session.isCurrent(token), isFalse);
    await session.closed;
  });

  test('late failing protocol response after explicit close is consumed',
      () async {
    final pending = Completer<JoinGroupResponse>();
    driver.onJoin = (_) => pending.future;
    final failed = expectLater(session.start(), throwsStateError);
    await flush();
    final stopped = await session.close();
    pending.completeError(StateError('late disconnect'));
    await flush();
    await failed;
    expect(stopped.reason, KafkaGroupStopReason.closed);
    expect(stopped.error, isNull);
  });

  test('Leave response after deadline remains unknown when timer was delayed',
      () async {
    await session.start();
    final pending = Completer<LeaveGroupResponse>();
    driver.onLeave = () => pending.future;
    final done = session.close();
    clock.advance(tick, fire: false);
    pending.complete(LeaveGroupResponse(1, 0, 0));
    expect((await done).leaveOutcome, KafkaGroupLeaveOutcome.unknown);
  });

  test('invalid timing configuration rejected', () {
    expect(
        () => ClassicGroupSessionController(
            driver: driver,
            heartbeatInterval: lease,
            localExpiry: lease,
            startupTimeout: tick,
            leaveTimeout: tick),
        throwsArgumentError);
    expect(
        () => ClassicGroupSessionController(
            driver: driver,
            heartbeatInterval: tick,
            localExpiry: lease,
            startupTimeout: Duration.zero,
            leaveTimeout: tick),
        throwsArgumentError);
  });
}
