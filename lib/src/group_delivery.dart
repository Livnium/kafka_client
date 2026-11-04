import 'dart:async';
import 'dart:collection';
import 'dart:convert';

import 'api/response/api1_fetch_response.dart';
import 'classic_group_session.dart';
import 'close_wait_group.dart';
import 'codec/record_batch.dart';
import 'reliability.dart';

/// One read-uncommitted record admitted under this exact local assignment.
///
/// The token is evidence of local admission, not a broker ownership lease or
/// permission to finish later asynchronous effects. Delivery is not processing
/// acknowledgement, an offset commit, or an exactly-once guarantee.
final class KafkaGroupRecord {
  final KafkaGroupAssignment assignment;
  final String topic;
  final int partition;
  final KafkaRecord record;
  const KafkaGroupRecord(
      this.assignment, this.topic, this.partition, this.record);
  int get offset => record.offset;
}

enum KafkaGroupDeliveryStopReason { cancelled, revoked, failure }

/// Result of stopping record admission and settling owned data-client work.
/// The membership cleanup result remains available separately on the session.
final class KafkaGroupDeliveryTermination {
  final KafkaGroupDeliveryStopReason reason;
  final Object? error;
  final StackTrace? stackTrace;

  /// Non-null means owned data-client shutdown is not confirmed.
  final Object? cleanupError;
  const KafkaGroupDeliveryTermination(
      {required this.reason, this.error, this.stackTrace, this.cleanupError});
}

/// A single-listener, single-generation delivery handle.
///
/// The original stream invokes each data callback synchronously after checking
/// the exact assignment token and local monotonic expiry, with no intervening
/// await. Paused records stay in an epoch-tagged internal buffer, not the stream
/// event queue. Local revocation prevents subsequent original data callbacks.
/// User-added stream transformations and already-started asynchronous callbacks
/// can outlive revocation; neither those effects nor broker ownership are fenced.
///
/// Pausing does not pause the independent membership heartbeat. Cancellation,
/// failure, and revocation close the exclusively owned data client. Cancellation
/// and failure also close membership. No automatic rejoin or commit occurs.
class KafkaGroupDelivery {
  final GroupDeliveryController _controller;
  KafkaGroupDelivery._(this._controller);

  /// Network polling starts on the single listen. An async listen callback is
  /// not awaited; use explicit pause/resume for processing backpressure.
  Stream<KafkaGroupRecord> get records => _controller.stream;

  /// Explicit local revocation barrier: no further original data callbacks,
  /// the polling pump has settled, and owned data-client closure has settled.
  /// Inspect cleanupError before treating transport shutdown as confirmed.
  /// This does not wait for listener callbacks, stream done delivery to a paused
  /// listener, or membership cleanup. It has no independent hard deadline.
  Future<KafkaGroupDeliveryTermination> get stopped => _controller.stopped;

  /// Stops admission synchronously, then awaits [stopped]. Idempotent.
  Future<KafkaGroupDeliveryTermination> cancel() => _controller.cancel();
}

class _Cursor {
  final String topic;
  final int partition;
  int offset;
  _Cursor(this.topic, this.partition, this.offset);
}

class _Batch {
  final KafkaGroupAssignment assignment;
  final _Cursor cursor;
  final Queue<KafkaRecord> records;
  final int? end;
  final int throttleMs;
  final bool quotaOnly;
  _Batch(this.assignment, this.cursor, this.records, this.end, this.throttleMs,
      {this.quotaOnly = false});
}

/// Internal seam for deterministic scheduling and adversarial response tests.
/// No user callback is called while registering a listener or handling resume.
class GroupDeliveryController {
  final ClassicGroupSessionController session;
  final KafkaGroupAssignment assignment;
  final Future<FetchResponse> Function(String, int, int) fetch;
  final Future<void> Function() closeData;
  final Duration pollInterval;
  final int maxBufferedRecords;
  final int maxBufferedPayloadBytes;
  final List<_Cursor> _cursors;
  final Future<void> Function()? waitForProcessingCapacity;
  final void Function(KafkaGroupRecord)? admitProcessing;
  final void Function()? stopProcessing;
  final _waits = CloseWaitGroup();
  final _finished = Completer<KafkaGroupDeliveryTermination>();
  late final StreamController<KafkaGroupRecord> _stream;
  Completer<void>? _resumed;
  _Batch? _buffer;
  KafkaGroupDeliveryTermination? _cause;
  Future<Object?>? _dataClosed;
  bool _started = false;
  bool _finishing = false;
  bool _errorReported = false;

  GroupDeliveryController({
    required this.session,
    required this.assignment,
    required Map<String, Map<int, int>> startOffsets,
    required this.fetch,
    required this.closeData,
    required this.pollInterval,
    required this.maxBufferedRecords,
    required this.maxBufferedPayloadBytes,
    this.waitForProcessingCapacity,
    this.admitProcessing,
    this.stopProcessing,
  }) : _cursors = [
          for (final topic in startOffsets.entries)
            for (final partition in topic.value.entries)
              _Cursor(topic.key, partition.key, partition.value)
        ] {
    validate(assignment, startOffsets, pollInterval, maxBufferedRecords,
        maxBufferedPayloadBytes);
    session.assertCurrent(assignment);
    _stream = StreamController<KafkaGroupRecord>(
      sync: true,
      onListen: () {
        if (_cause != null) return;
        _started = true;
        // Synchronous controllers must never emit from onListen/onResume.
        unawaited(Future<void>(_run));
      },
      onResume: _resume,
      onCancel: () async {
        await cancel();
      },
    );
    // Internal synchronous invalidation clears buffers before revoked resolves.
    session.attachDeliveryRevocation(
        () => _stop(KafkaGroupDeliveryStopReason.revoked));
  }

  static void validate(
      KafkaGroupAssignment assignment,
      Map<String, Map<int, int>> offsets,
      Duration poll,
      int maxRecords,
      int maxPayloadBytes) {
    if (poll < const Duration(milliseconds: 1) ||
        poll > const Duration(minutes: 1) ||
        maxRecords < 1 ||
        maxRecords > 0x7fffffff ||
        maxPayloadBytes < 1 ||
        maxPayloadBytes > 0x7fffffff) {
      throw ArgumentError('Invalid group delivery bounds');
    }
    final expected =
        assignment.partitions.entries.where((e) => e.value.isNotEmpty).toList();
    if (offsets.length != expected.length) {
      throw ArgumentError('Supply offsets for exactly the assigned partitions');
    }
    for (final entry in expected) {
      final positions = offsets[entry.key];
      if (positions == null ||
          positions.length != entry.value.length ||
          !positions.keys.toSet().containsAll(entry.value) ||
          positions.values.any((offset) => offset < 0)) {
        throw ArgumentError(
            'Supply offsets for exactly the assigned partitions');
      }
    }
  }

  KafkaGroupDelivery get delivery => KafkaGroupDelivery._(this);
  Stream<KafkaGroupRecord> get stream => _stream.stream;
  Future<KafkaGroupDeliveryTermination> get stopped => _finished.future;
  int get bufferedRecordCount => _buffer?.records.length ?? 0;

  bool get _active {
    if (_cause != null) return false;
    if (!session.isCurrent(assignment)) {
      _stop(KafkaGroupDeliveryStopReason.revoked);
      return false;
    }
    return true;
  }

  void _resume() {
    final resumed = _resumed;
    _resumed = null;
    if (resumed != null && !resumed.isCompleted) resumed.complete();
  }

  Future<void> _ready() async {
    while (_active) {
      if (_stream.isPaused) {
        await (_resumed ??= Completer<void>()).future;
        continue;
      }
      if (waitForProcessingCapacity != null) {
        await waitForProcessingCapacity!();
        if (_active && _stream.isPaused) continue;
      }
      return;
    }
  }

  /// Internal processing failure, synchronously ending local admission.
  void failProcessing(Object error, [StackTrace? stack]) {
    _stop(KafkaGroupDeliveryStopReason.failure,
        error: error, stack: stack, allowLeave: false);
  }

  Future<KafkaGroupDeliveryTermination> cancel() {
    _stop(KafkaGroupDeliveryStopReason.cancelled);
    return stopped;
  }

  void _stop(KafkaGroupDeliveryStopReason reason,
      {Object? error, StackTrace? stack, bool allowLeave = true}) {
    if (_cause != null) return;
    _cause = KafkaGroupDeliveryTermination(
        reason: reason, error: error, stackTrace: stack);
    stopProcessing?.call();
    _buffer?.records.clear();
    _buffer = null;
    _waits.close();
    _resume();
    _dataClosed = Future<void>.sync(closeData).then<Object?>((_) => null,
        onError: (Object error, StackTrace stack) => error);
    if (reason != KafkaGroupDeliveryStopReason.revoked) {
      if (allowLeave) {
        unawaited(session.close());
      } else {
        session.failOwnedProcessing(
            error ?? StateError('Processing stopped'), stack);
      }
    }
    if (!_started) unawaited(_finish());
  }

  Future<_Batch> _load(_Cursor cursor) async {
    final token = assignment;
    final response = await fetch(cursor.topic, cursor.partition, cursor.offset);
    // A response admitted in a prior local epoch is discarded before validation.
    if (!_active || !identical(token, assignment)) {
      return _Batch(token, cursor, Queue<KafkaRecord>(), null, 0);
    }
    if (response.topics.isEmpty && response.throttleTimeMs > 0) {
      return _Batch(
          token, cursor, Queue<KafkaRecord>(), null, response.throttleTimeMs,
          quotaOnly: true);
    }
    if (response.throttleTimeMs < 0 ||
        response.topics.length != 1 ||
        response.topics.single.name != cursor.topic ||
        response.topics.single.partitions.length != 1 ||
        response.topics.single.partitions.single.partition !=
            cursor.partition) {
      throw const FormatException('Invalid group delivery Fetch response');
    }
    final result = response.topics.single.partitions.single;
    if (result.errorCode != 0) {
      throw KafkaBrokerException(result.errorCode, 'group delivery Fetch');
    }
    if (result.records.length > maxBufferedRecords) {
      throw KafkaResourceLimitException('Group delivery record count exceeded');
    }
    int? previous;
    var payloadBytes = 0;
    for (final record in result.records) {
      if (record.offset < cursor.offset ||
          (previous != null && record.offset <= previous)) {
        throw const FormatException('Non-increasing group delivery offsets');
      }
      previous = record.offset;
      payloadBytes += (record.key?.length ?? 0) + (record.value?.length ?? 0);
      for (final header in record.headers) {
        payloadBytes +=
            utf8.encode(header.key).length + (header.value?.length ?? 0);
      }
      if (payloadBytes > maxBufferedPayloadBytes) {
        throw KafkaResourceLimitException(
            'Group delivery payload bytes exceeded');
      }
    }
    final end = result.lastBatchOffset ?? previous;
    if (end != null &&
        (end < cursor.offset ||
            (previous != null && end < previous) ||
            end == 9223372036854775807)) {
      throw const FormatException('Invalid group delivery batch progress');
    }
    return _Batch(token, cursor, Queue<KafkaRecord>.of(result.records), end,
        response.throttleTimeMs);
  }

  Future<void> _run() async {
    try {
      var index = 0;
      while (_active) {
        do {
          await _ready();
        } while (_active && _stream.isPaused);
        if (!_active) break;
        if (_cursors.isEmpty) {
          // Empty assignments retain membership but issue no Fetch requests.
          await _waits.wait(pollInterval);
          continue;
        }
        final batch = await _load(_cursors[index]);
        if (!_active) break;
        _buffer = batch;
        while (batch.records.isNotEmpty) {
          do {
            await _ready();
          } while (_active && _stream.isPaused);
          if (!_active || !identical(batch.assignment, assignment)) break;
          assert(!_stream.isPaused,
              'No group record may be admitted while paused');
          final record = batch.records.removeFirst();
          // No await between authority/pause check and synchronous data callback.
          final event = KafkaGroupRecord(batch.assignment, batch.cursor.topic,
              batch.cursor.partition, record);
          admitProcessing?.call(event);
          if (!_active) break;
          _stream.add(event);
          await _waits.wait(Duration.zero);
        }
        _buffer = null;
        if (!_active) break;
        if (batch.end != null) batch.cursor.offset = batch.end! + 1;
        if (!batch.quotaOnly) index = (index + 1) % _cursors.length;
        final throttle = Duration(milliseconds: batch.throttleMs);
        final delay = batch.end == null && pollInterval > throttle
            ? pollInterval
            : throttle;
        if (delay > Duration.zero) await _waits.wait(delay);
      }
    } catch (error, stack) {
      if (_active) {
        _stop(KafkaGroupDeliveryStopReason.failure, error: error, stack: stack);
        _errorReported = true;
        _stream.addError(error, stack);
      }
    } finally {
      if (_cause == null) _stop(KafkaGroupDeliveryStopReason.revoked);
      await _finish();
    }
  }

  Future<void> _finish() async {
    if (_finishing) return;
    _finishing = true;
    _buffer = null;
    final cleanupError = await _dataClosed;
    final cause = _cause!;
    if (!_errorReported && cause.error != null) {
      // A processing deadline may have fired inside a listener/acknowledgement.
      // Emit only after the pump/data close settle, never reentrantly in a hook.
      _errorReported = true;
      _stream.addError(cause.error!, cause.stackTrace);
    }
    // A paused listener may not receive done until resume/cancel. Do not wait
    // for controller.close: its onCancel waits on this independent barrier.
    unawaited(_stream.close());
    _finished.complete(KafkaGroupDeliveryTermination(
        reason: cause.reason,
        error: cause.error,
        stackTrace: cause.stackTrace,
        cleanupError: cleanupError));
  }
}
