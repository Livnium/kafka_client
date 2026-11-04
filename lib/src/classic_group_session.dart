import 'dart:async';

import 'classic_group_driver.dart';
import 'group_membership_protocol.dart';
import 'reliability.dart' show KafkaBrokerException;

/// Monotonic scheduling boundary. Tests can advance time without running timers.
abstract class ClassicGroupScheduler {
  Duration get now;
  ClassicGroupTimer schedule(Duration delay, void Function() callback);
}

abstract class ClassicGroupTimer {
  void cancel();
}

class _SystemScheduler implements ClassicGroupScheduler {
  final Stopwatch _clock = Stopwatch()..start();
  @override
  Duration get now => _clock.elapsed;
  @override
  ClassicGroupTimer schedule(Duration delay, void Function() callback) =>
      _SystemTimer(Timer(delay, callback));
}

class _SystemTimer implements ClassicGroupTimer {
  final Timer timer;
  _SystemTimer(this.timer);
  @override
  void cancel() => timer.cancel();
}

/// Local controller state. Closed means a bounded cleanup attempt finished,
/// not proof that transport closure or broker departure was confirmed.
enum KafkaGroupSessionState {
  idle,
  preparing,
  joining,
  syncing,
  confirming,
  stable,
  revoked,
  leaving,
  closed
}

enum KafkaGroupStopReason {
  closed,
  startupTimeout,
  localExpiry,
  brokerError,
  failure
}

/// Broker departure evidence. Even [notAttempted] can leave a broker member
/// alive until broker expiry, including an unidentified member after a lost Join.
enum KafkaGroupLeaveOutcome { notAttempted, confirmed, rejected, unknown }

/// An immutable, controller-issued identity token, not a broker fencing lease.
/// An empty snapshot owns no partitions. Checking this token establishes local
/// validity only at that instant, never the safety of later asynchronous effects.
final class KafkaGroupAssignment {
  final int epoch;
  final int generationId;
  final String memberId;
  final Map<String, List<int>> partitions;
  KafkaGroupAssignment._(this.epoch, this.generationId, this.memberId,
      Map<String, List<int>> partitions)
      : partitions = Map.unmodifiable(partitions
            .map((topic, ids) => MapEntry(topic, List<int>.unmodifiable(ids))));
}

/// Controller-issued evidence that an established session's scheduled heartbeat
/// failed. This is never issued for the initial heartbeat, local expiry, or
/// application/processing failures. It does not itself classify retry safety.
final class KafkaGroupStableHeartbeatFailure {
  /// Exact error and stack observed at this heartbeat boundary. Identity binds
  /// this evidence to its original cause, not a caller-substituted failure.
  final Object error;
  final StackTrace stackTrace;
  KafkaGroupStableHeartbeatFailure._(this.error, this.stackTrace);
}

/// Revocation resolves with the original cause before cleanup. The closed future on the
/// controller resolves with a new snapshot including best-effort departure.
/// Transport closure alone never proves broker-side membership was removed.
final class KafkaGroupTermination {
  final KafkaGroupStopReason reason;
  final Object? error;
  final StackTrace? stackTrace;
  final KafkaGroupLeaveOutcome leaveOutcome;
  final Object? leaveError;
  final Object? cleanupError;

  /// Present only for a failure observed by the stable heartbeat controller.
  /// Preserved in [ClassicGroupSessionController.closed] after cleanup.
  final KafkaGroupStableHeartbeatFailure? stableHeartbeatFailure;
  const KafkaGroupTermination(
      {required this.reason,
      this.error,
      this.stackTrace,
      this.leaveOutcome = KafkaGroupLeaveOutcome.notAttempted,
      this.leaveError,
      this.cleanupError,
      this.stableHeartbeatFailure});
}

/// One generation only: Join, Sync, initial Heartbeat, local token, termination.
/// No replay, rejoin or application-work draining. Owned delivery attaches a
/// synchronous revocation hook; explicit processing commits use a guarded gate.
/// The driver is exclusively owned; closure is attempted on every terminal path.
class ClassicGroupSessionController {
  final ClassicGroupDriver driver;
  final Duration heartbeatInterval;
  final Duration localExpiry;
  final Duration startupTimeout;
  final Duration leaveTimeout;
  final ClassicGroupScheduler _scheduler;
  KafkaGroupSessionState _state = KafkaGroupSessionState.idle;
  KafkaGroupAssignment? _assignment;
  JoinGroupResponse? _joined;
  int _epoch = 0;
  bool _started = false;
  bool _terminal = false;
  bool _heartbeatPending = false;
  bool _commitPending = false;
  void Function()? _revokeDelivery;
  Duration? _startupDeadline;
  Duration? _expiryDeadline;
  ClassicGroupTimer? _startupTimer;
  ClassicGroupTimer? _expiryTimer;
  ClassicGroupTimer? _heartbeatTimer;
  int _startupWatchdogVersion = 0;
  int _expiryWatchdogVersion = 0;
  Completer<KafkaGroupAssignment>? _start;
  final _revoked = Completer<KafkaGroupTermination>();
  final _closed = Completer<KafkaGroupTermination>();

  ClassicGroupSessionController(
      {required this.driver,
      required this.heartbeatInterval,
      required this.localExpiry,
      required this.startupTimeout,
      required this.leaveTimeout,
      ClassicGroupScheduler? scheduler})
      : _scheduler = scheduler ?? _SystemScheduler() {
    for (final duration in [
      heartbeatInterval,
      localExpiry,
      startupTimeout,
      leaveTimeout
    ]) {
      if (duration <= Duration.zero) {
        throw ArgumentError('Durations must be positive');
      }
    }
    if (heartbeatInterval >= localExpiry) {
      throw ArgumentError('Heartbeat interval must be less than local expiry');
    }
  }

  KafkaGroupSessionState get state {
    _checkDeadlines();
    return _state;
  }

  KafkaGroupAssignment? get currentAssignment {
    _checkDeadlines();
    return _assignment;
  }

  KafkaGroupAssignment? get assignment => currentAssignment;

  /// Completes on first local invalidation, before waiting for cleanup. There
  /// are no callbacks to block revocation, heartbeat scheduling, or transport
  /// closure. Its departure fields are provisional; await [closed] for cleanup.
  Future<KafkaGroupTermination> get revoked => _revoked.future;

  /// Cleanup result, preserving the original stop reason and error. A stable
  /// explicit close may spend up to one [leaveTimeout] waiting for Leave and
  /// another waiting for transport closure. A transport cleanup error or timeout
  /// does not establish either broker departure or successful transport closure.
  Future<KafkaGroupTermination> get closed => _closed.future;

  /// Starts exactly once. A second call fails; it does not attach to, restart,
  /// or retry the first attempt. This future fails promptly on startup revocation
  /// even when an outstanding protocol request has not settled.
  Future<KafkaGroupAssignment> start() {
    if (_started || _terminal) {
      return Future.error(StateError('Session can start only once'));
    }
    _started = true;
    _start = Completer<KafkaGroupAssignment>();
    _startupDeadline = _scheduler.now + startupTimeout;
    _armDeadline(_startupDeadline!, startup: true);
    _state = KafkaGroupSessionState.preparing;
    unawaited(_run(++_epoch));
    return _start!.future;
  }

  /// Checks exact token identity and monotonic expiry synchronously, including
  /// when event-loop delay has prevented the watchdog timer from firing. This
  /// cannot undo application effects or grant permission for a later await.
  bool isCurrent(KafkaGroupAssignment token) {
    _checkDeadlines();
    return !_terminal &&
        _state == KafkaGroupSessionState.stable &&
        identical(token, _assignment) &&
        token.epoch == _epoch;
  }

  void assertCurrent(KafkaGroupAssignment token) {
    if (!isCurrent(token)) {
      throw StateError('Assignment is no longer locally current');
    }
  }

  /// Invalidates synchronously and returns the same cleanup future every time.
  /// Sends Leave only from stable membership with no heartbeat/commit in flight; an
  /// outstanding mutation is never raced by an attempted graceful departure.
  Future<KafkaGroupTermination> close() {
    _checkDeadlines();
    if (!_terminal) {
      _stop(KafkaGroupStopReason.closed,
          allowLeave: _state == KafkaGroupSessionState.stable &&
              !_heartbeatPending &&
              !_commitPending);
    }
    return _closed.future;
  }

  /// An owned processing/commit failure revokes without another mutation.
  void failOwnedProcessing(Object error, [StackTrace? stack]) {
    if (!_terminal) _fail(error, stack ?? StackTrace.current);
  }

  /// Internal monotonic clock shared with processing deadlines.
  ClassicGroupScheduler get scheduler => _scheduler;

  /// Internal single commit admission. No await between exact-token validation
  /// and operation invocation. Revocation cancels the request through delivery.
  /// An admitted commit suppresses graceful Leave until it actually settles.
  Future<T> runCommit<T>(
      KafkaGroupAssignment token, Future<T> Function() operation) async {
    assertCurrent(token);
    if (_commitPending) throw StateError('A group commit is already pending');
    _commitPending = true;
    try {
      return await operation();
    } finally {
      _commitPending = false;
    }
  }

  /// Internal one-time hook. It must only stop owned delivery work, without
  /// emitting stream events or invoking application callbacks. The package's
  /// delivery controller installs this only after validating its live token.
  void attachDeliveryRevocation(void Function() invalidate) {
    if (_terminal || _revokeDelivery != null) {
      throw StateError('Cannot attach another delivery to this session');
    }
    _revokeDelivery = invalidate;
  }

  bool _live(int epoch) {
    _checkDeadlines();
    return !_terminal && epoch == _epoch;
  }

  void _checkDeadlines() {
    if (_terminal) return;
    final now = _scheduler.now;
    if (_startupDeadline != null && now >= _startupDeadline!) {
      _stop(KafkaGroupStopReason.startupTimeout,
          error: TimeoutException('Group startup deadline expired'));
    } else if (_expiryDeadline != null && now >= _expiryDeadline!) {
      _stop(KafkaGroupStopReason.localExpiry,
          error: TimeoutException('Local membership deadline expired'));
    }
  }

  Future<void> _run(int epoch) async {
    try {
      await driver.prepare();
      if (!_live(epoch)) return;
      _state = KafkaGroupSessionState.joining;
      var joined = await driver.join('');
      if (!_live(epoch)) return;
      if (joined.errorCode == 79) {
        if (joined.memberId.isEmpty) {
          throw const FormatException('Join79 lacks member ID');
        }
        joined = await driver.join(joined.memberId);
        if (!_live(epoch)) return;
      }
      _checkCode('JoinGroup', joined.errorCode);
      if (joined.generationId < 0 ||
          joined.memberId.isEmpty ||
          joined.leaderId.isEmpty ||
          joined.protocolName.isEmpty) {
        throw const FormatException('Invalid successful JoinGroup identity');
      }
      _joined = joined;
      _state = KafkaGroupSessionState.syncing;
      final assignment = await driver.sync(joined);
      if (!_live(epoch)) return;
      _state = KafkaGroupSessionState.confirming;
      final sent = _scheduler.now;
      _setExpiry(sent + localExpiry);
      _heartbeatPending = true;
      final reply = await driver.heartbeat(joined);
      if (!_live(epoch)) return;
      _heartbeatPending = false;
      _checkCode('Heartbeat', reply.errorCode);
      _setExpiry(sent + localExpiry);
      _startupTimer?.cancel();
      _startupDeadline = null;
      _assignment = KafkaGroupAssignment._(
          epoch, joined.generationId, joined.memberId, assignment.partitions);
      _state = KafkaGroupSessionState.stable;
      _start!.complete(_assignment!);
      _scheduleHeartbeat(epoch, sent);
    } catch (error, stack) {
      if (_live(epoch)) _fail(error, stack);
    }
  }

  void _checkCode(String operation, int code) {
    if (code != 0) throw KafkaBrokerException(code, operation);
  }

  void _fail(Object error, StackTrace stack,
          {KafkaGroupStableHeartbeatFailure? stableHeartbeatFailure}) =>
      _stop(
          error is KafkaBrokerException
              ? KafkaGroupStopReason.brokerError
              : KafkaGroupStopReason.failure,
          error: error,
          stack: stack,
          stableHeartbeatFailure: stableHeartbeatFailure);

  // Timers may wake before a monotonic target (for example when a fractional
  // millisecond delay is truncated). Re-arm against the same absolute deadline
  // rather than losing the watchdog or granting a new lease from wake time.
  void _armDeadline(Duration deadline, {required bool startup}) {
    final version =
        startup ? ++_startupWatchdogVersion : ++_expiryWatchdogVersion;
    final remaining = deadline - _scheduler.now;
    // Round positive waits up so repeated early wakes cannot spin on Timer(0).
    final delay = remaining <= Duration.zero
        ? Duration.zero
        : Duration(milliseconds: (remaining.inMicroseconds + 999) ~/ 1000);
    final timer = _scheduler.schedule(delay, () {
      if (_terminal ||
          (startup ? _startupDeadline : _expiryDeadline) != deadline ||
          (startup ? _startupWatchdogVersion : _expiryWatchdogVersion) !=
              version) {
        return;
      }
      _checkDeadlines();
      if (!_terminal) _armDeadline(deadline, startup: startup);
    });
    if (startup) {
      _startupTimer?.cancel();
      _startupTimer = timer;
    } else {
      _expiryTimer?.cancel();
      _expiryTimer = timer;
    }
  }

  void _setExpiry(Duration deadline) {
    _expiryDeadline = deadline;
    _armDeadline(deadline, startup: false);
  }

  void _scheduleHeartbeat(int epoch, Duration lastSend) {
    final delay = lastSend + heartbeatInterval - _scheduler.now;
    _heartbeatTimer =
        _scheduler.schedule(delay < Duration.zero ? Duration.zero : delay, () {
      if (_live(epoch)) unawaited(_heartbeat(epoch));
    });
  }

  Future<void> _heartbeat(int epoch) async {
    if (!_live(epoch) || _heartbeatPending) return;
    _heartbeatPending = true;
    final sent = _scheduler.now;
    try {
      final response = await driver.heartbeat(_joined!);
      if (!_live(epoch)) return;
      _heartbeatPending = false;
      _checkCode('Heartbeat', response.errorCode);
      // A delayed response must not buy a fresh full lease from reply time.
      if (_scheduler.now >= sent + localExpiry) {
        _stop(KafkaGroupStopReason.localExpiry,
            error: TimeoutException('Heartbeat response arrived too late'));
        return;
      }
      _setExpiry(sent + localExpiry);
      _scheduleHeartbeat(epoch, sent);
    } catch (error, stack) {
      // Deadline invalidation wins over even a retryable transport/broker error.
      if (_live(epoch)) {
        _fail(error, stack,
            stableHeartbeatFailure:
                KafkaGroupStableHeartbeatFailure._(error, stack));
      }
    }
  }

  void _stop(KafkaGroupStopReason reason,
      {Object? error,
      StackTrace? stack,
      bool allowLeave = false,
      KafkaGroupStableHeartbeatFailure? stableHeartbeatFailure}) {
    if (_terminal) return;
    _terminal = true;
    ++_epoch;
    _assignment = null;
    _state = KafkaGroupSessionState.revoked;
    final revokeDelivery = _revokeDelivery;
    _revokeDelivery = null;
    revokeDelivery?.call();
    _startupTimer?.cancel();
    _expiryTimer?.cancel();
    _heartbeatTimer?.cancel();
    final termination = KafkaGroupTermination(
        reason: reason,
        error: error,
        stackTrace: stack,
        stableHeartbeatFailure: stableHeartbeatFailure);
    if (_start != null && !_start!.isCompleted) {
      _start!.completeError(
          error ?? StateError('Session closed before assignment'), stack);
    }
    _revoked.complete(termination);
    unawaited(_cleanup(termination, allowLeave));
  }

  Future<T> _bounded<T>(Future<T> Function() operation) {
    final result = Completer<T>();
    final deadline = _scheduler.now + leaveTimeout;
    final timer = _scheduler.schedule(leaveTimeout, () {
      if (!result.isCompleted) {
        result.completeError(TimeoutException('Cleanup deadline expired'));
      }
    });
    Future<T>.sync(operation).then((value) {
      if (!result.isCompleted) {
        if (_scheduler.now >= deadline) {
          result.completeError(TimeoutException('Cleanup deadline expired'));
        } else {
          result.complete(value);
        }
      }
    }, onError: (Object error, StackTrace stack) {
      if (!result.isCompleted) result.completeError(error, stack);
    }).whenComplete(timer.cancel);
    return result.future;
  }

  Future<void> _cleanup(KafkaGroupTermination cause, bool allowLeave) async {
    var outcome = KafkaGroupLeaveOutcome.notAttempted;
    Object? leaveError;
    Object? cleanupError;
    if (allowLeave && _joined != null) {
      _state = KafkaGroupSessionState.leaving;
      try {
        final reply = await _bounded(() => driver.leave(_joined!.memberId));
        outcome = reply.errorCode == 0
            ? KafkaGroupLeaveOutcome.confirmed
            : KafkaGroupLeaveOutcome.rejected;
        if (reply.errorCode != 0) {
          leaveError = KafkaBrokerException(reply.errorCode, 'LeaveGroup');
        }
      } catch (error) {
        outcome = error is KafkaBrokerException
            ? KafkaGroupLeaveOutcome.rejected
            : KafkaGroupLeaveOutcome.unknown;
        leaveError = error;
      }
    }
    try {
      await _bounded(driver.close);
    } catch (error) {
      cleanupError = error;
    }
    _state = KafkaGroupSessionState.closed;
    _closed.complete(KafkaGroupTermination(
        reason: cause.reason,
        error: cause.error,
        stackTrace: cause.stackTrace,
        stableHeartbeatFailure: cause.stableHeartbeatFailure,
        leaveOutcome: outcome,
        leaveError: leaveError,
        cleanupError: cleanupError));
  }
}
