import 'dart:async';
import 'dart:collection';

import 'classic_group_session.dart';
import 'group_delivery.dart';
import 'group_offsets_protocol.dart';
import 'reliability.dart';

/// Evidence for one explicitly attempted partition commit. Unknown is not
/// rejection or rollback, and never authorizes automatic replay.
enum KafkaGroupCommitOutcome { notAttempted, confirmed, rejected, unknown }

final class KafkaGroupPartitionCommit {
  final int nextOffset;
  final KafkaGroupCommitOutcome outcome;
  final int? errorCode;
  const KafkaGroupPartitionCommit(this.nextOffset, this.outcome,
      {this.errorCode});
}

/// Immutable evidence for one topic's explicit commit snapshot. Empty partitions
/// means no request was necessary. A confirmed response can arrive after local
/// revocation; that evidence never restores authority or fences outside effects.
final class KafkaGroupCommitResult {
  final KafkaGroupAssignment assignment;
  final String topic;
  final Map<int, KafkaGroupPartitionCommit> partitions;
  final bool locallyCurrent;
  final int? throttleTimeMs;
  final Object? error;
  final StackTrace? stackTrace;
  KafkaGroupCommitResult(
      {required this.assignment,
      required this.topic,
      required Map<int, KafkaGroupPartitionCommit> partitions,
      required this.locallyCurrent,
      this.throttleTimeMs,
      this.error,
      this.stackTrace})
      : partitions = Map.unmodifiable(partitions);
}

/// Both cleanup paths were still attempted. A cancellation-hook failure does
/// not establish cancellation or transport shutdown, even if the request settles.
final class KafkaGroupProcessingCleanupException implements Exception {
  final Object commitCancellationError;
  final Object? dataCleanupError;
  const KafkaGroupProcessingCleanupException(
      this.commitCancellationError, this.dataCleanupError);
  @override
  String toString() => 'KafkaGroupProcessingCleanupException: '
      'commit cancellation failed; inspect cleanup evidence';
}

/// Opt-in, bounded processing for one session generation. Only acknowledgement
/// of actually delivered objects advances processing candidates. Commits remain
/// explicit, one topic and one immutable snapshot at a time; none are replayed.
///
/// An acknowledgement asserts successful application processing. It is not a
/// durable offset, a transaction, a broker lease, or an exactly-once guarantee.
/// Applications must handle duplicate effects and any work still running after
/// revocation. Neither this handle nor cancellation drains application Futures.
class KafkaGroupProcessing {
  final GroupProcessingController _controller;
  KafkaGroupProcessing._(this._controller);

  /// Same original synchronous admission boundary as KafkaGroupDelivery.records.
  /// Async listener callbacks are not awaited. The bounded ledger stops further
  /// admission at capacity while independent heartbeats continue.
  Stream<KafkaGroupRecord> get records => _controller.delivery.stream;

  /// Acknowledge this handle's exact delivered object after processing succeeds.
  /// Returns false for a duplicate acknowledgement while locally current.
  /// Unknown/fabricated objects and all post-revocation acknowledgements throw.
  bool acknowledge(KafkaGroupRecord record) => _controller.acknowledge(record);

  /// Candidate next offsets from acknowledged per-partition prefixes only.
  /// Numeric gaps are permitted; batch ends and watermarks never advance this.
  Map<String, Map<int, int>> get acknowledgedNextOffsets =>
      _controller.acknowledgedNextOffsets;

  /// Offsets confirmed by this handle's successful broker responses only, not
  /// proof of present stored state when another client can overwrite the group.
  Map<String, Map<int, int>> get confirmedNextOffsets =>
      _controller.confirmedNextOffsets;

  /// Unretired ledger entries, including out-of-order acknowledgements blocked
  /// behind an unfinished record. This does not count user-owned work or memory.
  int get pendingRecordCount => _controller.pendingRecordCount;

  /// Commits this assigned topic's advanced candidates, including the exact
  /// current generation/member. No arbitrary offsets or metadata are accepted.
  /// Other topics are separate operations, never an atomic multi-topic commit.
  ///
  /// Overlap and local misuse throw before dispatch. An empty snapshot sends
  /// nothing. After admission, malformed/lost/error responses become explicit
  /// per-partition evidence and end this session; no retry or manual fallback.
  /// Successful partitions in a mixed response remain confirmed. A late valid
  /// response can confirm offsets without restoring local authority. Applying
  /// Future.timeout does not cancel this operation or release its overlap gate.
  Future<KafkaGroupCommitResult> commitAcknowledged(String topic) =>
      _controller.commitAcknowledged(topic);

  /// No further original record callbacks; owned Fetch/data cleanup and the
  /// actually admitted commit have settled. Inspect error/cleanupError. This is
  /// independent of paused stream done and does not drain application work or
  /// replace the session.closed membership/control cleanup result.
  Future<KafkaGroupDeliveryTermination> get stopped => _controller.stopped;

  /// Stops local admission and acknowledgement synchronously, cancels pending
  /// commit transport work, and awaits stopped. Cannot retract accepted writes.
  Future<KafkaGroupDeliveryTermination> cancel() => _controller.cancel();
}

class _PartitionProgress {
  final Queue<_Work> pending = Queue<_Work>();
  int? candidate;
  int? confirmed;
}

class _Work {
  final _PartitionProgress partition;
  final int offset;
  final Duration admitted;
  bool acknowledged = false;
  _Work(this.partition, this.offset, this.admitted);
}

/// Internal integration seam. The ledger keeps no record/key/value payloads;
/// weak identity receipts cannot be reconstructed from public record fields.
class GroupProcessingController {
  final ClassicGroupSessionController session;
  final KafkaGroupAssignment assignment;
  final int maxPendingRecords;
  final Duration processingTimeout;
  final Future<GroupOffsetCommitResponse> Function(
      String, Map<int, int>, KafkaCancellationToken) commit;
  final Map<String, Map<int, _PartitionProgress>> _partitions;
  Expando<_Work> _receipts = Expando<_Work>();
  late final GroupDeliveryController delivery;
  late final Future<KafkaGroupDeliveryTermination> stopped;
  ClassicGroupTimer? _timer;
  Completer<void>? _capacity;
  Completer<void>? _commitSettled;
  KafkaCancellationToken? _commitCancellation;
  Object? _cancellationError;
  bool _active = true;
  int _pending = 0;

  GroupProcessingController({
    required this.session,
    required this.assignment,
    required this.maxPendingRecords,
    required this.processingTimeout,
    required this.commit,
  }) : _partitions = {
          for (final topic in assignment.partitions.entries)
            topic.key: {for (final id in topic.value) id: _PartitionProgress()}
        } {
    validate(maxPendingRecords, processingTimeout);
    session.assertCurrent(assignment);
  }

  static void validate(int maxPendingRecords, Duration timeout) {
    if (maxPendingRecords < 1 ||
        maxPendingRecords > 0x7fffffff ||
        timeout <= Duration.zero ||
        timeout > const Duration(hours: 1)) {
      throw ArgumentError('Invalid group processing bounds');
    }
  }

  void bind(GroupDeliveryController controller) {
    delivery = controller;
    stopped = delivery.stopped.then((termination) async {
      await _commitSettled?.future;
      if (_cancellationError == null) return termination;
      return KafkaGroupDeliveryTermination(
          reason: termination.reason,
          error: termination.error,
          stackTrace: termination.stackTrace,
          cleanupError: KafkaGroupProcessingCleanupException(
              _cancellationError!, termination.cleanupError));
    });
  }

  KafkaGroupProcessing get processing => KafkaGroupProcessing._(this);
  int get pendingRecordCount => _pending;

  Map<String, Map<int, int>> _snapshot(
          int? Function(_PartitionProgress) read) =>
      Map.unmodifiable({
        for (final topic in _partitions.entries)
          if (topic.value.values.any((p) => read(p) != null))
            topic.key: Map<int, int>.unmodifiable({
              for (final p in topic.value.entries)
                if (read(p.value) != null) p.key: read(p.value)!
            })
      });
  Map<String, Map<int, int>> get acknowledgedNextOffsets =>
      _snapshot((p) => p.candidate);
  Map<String, Map<int, int>> get confirmedNextOffsets =>
      _snapshot((p) => p.confirmed);

  void _assertActive() {
    session.assertCurrent(assignment);
    _checkStall();
    if (!_active) throw StateError('Group processing is stopped');
  }

  void admit(KafkaGroupRecord record) {
    _assertActive();
    if (_pending >= maxPendingRecords) {
      throw StateError('Processing capacity was not reserved');
    }
    if (!identical(record.assignment, assignment) ||
        _receipts[record] != null ||
        record.offset < 0 ||
        record.offset == 9223372036854775807) {
      throw StateError('Invalid processing admission');
    }
    final partition = _partitions[record.topic]?[record.partition];
    if (partition == null) throw StateError('Unassigned processing partition');
    final prior = partition.pending.isEmpty
        ? partition.candidate
        : partition.pending.last.offset + 1;
    if (prior != null && record.offset < prior) {
      throw StateError('Non-increasing processing admission');
    }
    final work = _Work(partition, record.offset, session.scheduler.now);
    _receipts[record] = work;
    partition.pending.add(work);
    _pending++;
    _armStall();
  }

  Future<void> waitForCapacity() async {
    while (_active) {
      _assertActive();
      if (_pending < maxPendingRecords) return;
      await (_capacity ??= Completer<void>()).future;
    }
  }

  bool acknowledge(KafkaGroupRecord record) {
    _assertActive();
    final work = _receipts[record];
    if (work == null || !identical(record.assignment, assignment)) {
      throw ArgumentError('Record was not delivered by this processing handle');
    }
    if (work.acknowledged) return false;
    work.acknowledged = true;
    final partition = work.partition;
    while (
        partition.pending.isNotEmpty && partition.pending.first.acknowledged) {
      final retired = partition.pending.removeFirst();
      partition.candidate = retired.offset + 1;
      _pending--;
    }
    _wakeCapacity();
    _armStall();
    return true;
  }

  Duration? get _deadline {
    Duration? oldest;
    for (final topic in _partitions.values) {
      for (final partition in topic.values) {
        if (partition.pending.isEmpty) continue;
        final at = partition.pending.first.admitted + processingTimeout;
        if (oldest == null || at < oldest) oldest = at;
      }
    }
    return oldest;
  }

  void _checkStall() {
    final deadline = _deadline;
    if (_active && deadline != null && session.scheduler.now >= deadline) {
      delivery.failProcessing(
          TimeoutException('Group application processing deadline expired'));
    }
  }

  void _armStall() {
    _timer?.cancel();
    _timer = null;
    if (!_active) return;
    _checkStall();
    if (!_active) return;
    final deadline = _deadline;
    if (deadline == null) return;
    final remaining = deadline - session.scheduler.now;
    // Early timer wakeups never grant completion or lose the watchdog.
    final delay =
        Duration(milliseconds: (remaining.inMicroseconds + 999) ~/ 1000);
    _timer = session.scheduler.schedule(delay, _armStall);
  }

  void _wakeCapacity() {
    final pending = _capacity;
    _capacity = null;
    if (pending != null && !pending.isCompleted) pending.complete();
  }

  /// Runs from synchronous delivery revocation before session.revoked completes.
  void stop() {
    if (!_active) return;
    _active = false;
    _timer?.cancel();
    _timer = null;
    _receipts = Expando<_Work>();
    for (final topic in _partitions.values) {
      for (final partition in topic.values) {
        partition.pending.clear();
      }
    }
    _pending = 0;
    _wakeCapacity();
    try {
      _commitCancellation?.cancel();
    } catch (error) {
      // Internal hooks cannot interrupt revocation, data close or membership
      // cleanup. Preserve the failure on the independent stopped barrier.
      _cancellationError = error;
    }
  }

  Future<KafkaGroupDeliveryTermination> cancel() {
    delivery.cancel();
    return stopped;
  }

  Future<KafkaGroupCommitResult> commitAcknowledged(String topic) async {
    _assertActive();
    if (_commitSettled != null) {
      throw StateError('A group commit is already pending');
    }
    final partitions = _partitions[topic];
    if (partitions == null) throw ArgumentError('Topic is not assigned');
    final offsets = Map<int, int>.unmodifiable({
      for (final p in partitions.entries)
        if (p.value.candidate != null && p.value.candidate != p.value.confirmed)
          p.key: p.value.candidate!
    });
    if (offsets.isEmpty) {
      return KafkaGroupCommitResult(
          assignment: assignment,
          topic: topic,
          partitions: {},
          locallyCurrent: true);
    }
    final settled = _commitSettled = Completer<void>();
    final cancellation = _commitCancellation = KafkaCancellationToken();
    final evidence = <int, KafkaGroupPartitionCommit>{};
    Object? failure;
    StackTrace? failureStack;
    int? throttle;
    var handedToDriver = false;
    try {
      final response = await session.runCommit(assignment, () {
        handedToDriver = true;
        return commit(topic, offsets, cancellation);
      });
      // Revalidate even test/alternative internal drivers. Never partially apply
      // a malformed response. Positive throttle preserves response evidence but
      // ends this bounded session because quota continuation is unsupported.
      if (response.throttleTimeMs < 0 ||
          response.topics.length != 1 ||
          response.topics.single.name != topic ||
          response.topics.single.partitions.length != offsets.length) {
        throw const FormatException('Invalid managed commit response');
      }
      final codes = <int, int>{};
      for (final p in response.topics.single.partitions) {
        if (!offsets.containsKey(p.partitionIndex) ||
            codes.containsKey(p.partitionIndex)) {
          throw const FormatException('Invalid managed commit partition');
        }
        codes[p.partitionIndex] = p.errorCode;
      }
      throttle = response.throttleTimeMs;
      for (final p in offsets.entries) {
        final code = codes[p.key]!;
        final outcome = code == 0
            ? KafkaGroupCommitOutcome.confirmed
            : KafkaGroupCommitOutcome.rejected;
        evidence[p.key] =
            KafkaGroupPartitionCommit(p.value, outcome, errorCode: code);
        if (code == 0) partitions[p.key]!.confirmed = p.value;
        if (code != 0) {
          failure ??= KafkaBrokerException(code, 'managed OffsetCommit');
        }
      }
      if (throttle > 0) {
        failure ??= UnsupportedError('Group quota throttling is not supported');
      }
    } catch (error, stack) {
      failure = error;
      failureStack = stack;
      evidence.clear();
      for (final p in offsets.entries) {
        evidence[p.key] = KafkaGroupPartitionCommit(
            p.value,
            handedToDriver
                ? KafkaGroupCommitOutcome.unknown
                : KafkaGroupCommitOutcome.notAttempted);
      }
    } finally {
      _commitCancellation = null;
      if (failure != null && _active) {
        delivery.failProcessing(failure, failureStack);
      }
      _commitSettled = null;
      settled.complete();
    }
    _checkStall();
    final current = _active && session.isCurrent(assignment);
    return KafkaGroupCommitResult(
        assignment: assignment,
        topic: topic,
        partitions: evidence,
        locallyCurrent: current,
        throttleTimeMs: throttle,
        error: failure,
        stackTrace: failureStack);
  }
}
