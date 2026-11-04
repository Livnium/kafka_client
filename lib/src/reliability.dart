/// Opt-in retry of unstable (error 88) committed offsets, requiring stable v7.
///
/// Replaces ordinary read retries with one total round budget and an absolute
/// monotonic deadline, including discovery, connection, authentication and reads.
/// Connections are operation-owned but consume the owning client's aggregate
/// budgets. Concurrent ordinary traffic may therefore require an extra connection.
/// Exhausted rounds preserve the broker error; elapsed timeout throws TimeoutException.
/// DNS lookup before the runtime exposes a cancellable connection task may settle
/// later; late tasks/sockets are fenced and closed, with no further wire phases.
final class KafkaStableOffsetRetry {
  final int maxAttempts;
  final Duration timeout;
  final Duration retryDelay;

  KafkaStableOffsetRetry(
      {this.maxAttempts = 10,
      this.timeout = const Duration(seconds: 5),
      this.retryDelay = const Duration(milliseconds: 150)}) {
    if (maxAttempts < 1 ||
        maxAttempts > 20 ||
        timeout <= Duration.zero ||
        timeout > const Duration(minutes: 1) ||
        retryDelay < Duration.zero ||
        retryDelay > const Duration(seconds: 1)) {
      throw ArgumentError('Invalid stable offset retry limits');
    }
  }
}

/// Opt-in, bounded recovery from TLS-rejected or stale Fetch leaders.
///
/// The owning client must have TLS and TLS bootstrap fallback enabled. Every
/// peer still undergoes strict certificate and hostname validation. A peer
/// rejected during one Fetch is excluded only for that operation, by literal
/// host:port; it is never retried as a leader or bootstrap by that Fetch.
/// Produce and low-level sends never gain recovery or automatic replay.
///
/// These limits replace the client's ordinary read-retry settings for enabled
/// high-level Fetch calls. The window bounds admission of further phases and
/// metadata polling, not the duration of an already admitted phase. Existing
/// connection and request deadlines still bound in-flight work; no work is
/// orphaned with Future.timeout to simulate an overall cancellation deadline.
final class KafkaTlsFetchRecovery {
  /// Total metadata/Fetch rounds, including the first, from 1 through 20.
  final int maxAttempts;

  /// Monotonic admission/polling window, positive and at most one minute.
  final Duration window;

  /// Close-aware delay between rounds, from zero through one second.
  final Duration retryDelay;

  KafkaTlsFetchRecovery(
      {this.maxAttempts = 10,
      this.window = const Duration(seconds: 5),
      this.retryDelay = const Duration(milliseconds: 150)}) {
    if (maxAttempts < 1 ||
        maxAttempts > 20 ||
        window <= Duration.zero ||
        window > const Duration(minutes: 1) ||
        retryDelay < Duration.zero ||
        retryDelay > const Duration(seconds: 1)) {
      throw ArgumentError('Invalid TLS Fetch recovery limits');
    }
  }
}

/// A configured wire-resource budget was exceeded.
///
/// Response-side failures can occur after transmission. This exception alone
/// does not establish whether Produce records were stored and cannot authorize
/// replay. Admission failures can occur before sending, but not all budget
/// failures are admission failures.
class KafkaResourceLimitException implements Exception {
  /// The budget name, never request data or credentials.
  final String resource;
  KafkaResourceLimitException(this.resource);
  @override
  String toString() => 'KafkaResourceLimitException: $resource limit reached';
}

/// A connection ended before a request could complete. Produce delivery may be
/// ambiguous; this exception never authorizes automatic produce replay.
class KafkaConnectionException implements Exception {
  @override
  String toString() => 'KafkaConnectionException: broker connection closed';
}

/// A broker reported a nonzero Kafka error code.
class KafkaBrokerException implements Exception {
  final int errorCode;
  final String operation;
  KafkaBrokerException(this.errorCode, this.operation);
  @override
  String toString() => 'KafkaBrokerException: $operation error $errorCode';
}

/// A canceled local wait. Cancellation cannot undo bytes already sent to Kafka.
class KafkaCancelledException implements Exception {
  @override
  String toString() => 'KafkaCancelledException: request canceled';
}

/// Cooperatively cancel a low-level send. Cancellation never logs a reason or
/// credentials, and does not promise that an already sent Produce was undone.
class KafkaCancellationToken {
  bool _cancelled = false;
  final Set<void Function()> _listeners = {};
  bool get isCancelled => _cancelled;
  void cancel() {
    if (_cancelled) return;
    _cancelled = true;
    Object? firstError;
    StackTrace? firstStack;
    final callbacks = List<void Function()>.of(_listeners);
    _listeners.clear();
    for (final callback in callbacks) {
      try {
        callback();
      } catch (e, st) {
        firstError ??= e;
        firstStack ??= st;
      }
    }
    if (firstError != null) Error.throwWithStackTrace(firstError, firstStack!);
  }

  /// Registers a local listener; returns a function that unregisters it.
  void Function() onCancel(void Function() callback) {
    if (_cancelled) {
      callback();
      return () {};
    }
    _listeners.add(callback);
    return () => _listeners.remove(callback);
  }
}
