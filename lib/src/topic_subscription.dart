import 'dart:async';

import 'api/response/api1_fetch_response.dart';
import 'close_wait_group.dart';
import 'codec/record_batch.dart';
import 'reliability.dart';

/// One read-uncommitted record from an explicitly selected topic partition.
///
/// Offsets increase within each partition; there is no cross-partition order.
/// Delivery is not a commit or an acknowledgement of listener processing.
class KafkaTopicRecord {
  final String topic;
  final int partition;
  final KafkaRecord record;

  const KafkaTopicRecord(this.topic, this.partition, this.record);

  int get offset => record.offset;
}

/// Package-internal, single-listener polling engine.
///
/// Cancellation stops admission, but waits for an admitted fetch to settle. It
/// never closes a shared client or attempts to cancel its transport requests.
class TopicSubscriptionController {
  final String topic;
  final Map<int, int> _cursors;
  final Future<FetchResponse> Function(int partition, int offset) _fetch;
  final bool Function(Object error) _isRetryable;
  final bool Function() _isClientClosed;
  final void Function() _onStart;
  final void Function() _onFinish;
  final Duration pollInterval;
  final Duration retryDelay;
  final CloseWaitGroup _waits = CloseWaitGroup();
  final Completer<void> _finished = Completer<void>();
  late final StreamController<KafkaTopicRecord> _controller;
  Completer<void>? _resumed;
  bool _started = false;
  bool _stopped = false;

  TopicSubscriptionController({
    required this.topic,
    required Map<int, int> startOffsets,
    required Future<FetchResponse> Function(int partition, int offset) fetch,
    required bool Function(Object error) isRetryable,
    required bool Function() isClientClosed,
    required void Function() onStart,
    required void Function() onFinish,
    required this.pollInterval,
    required this.retryDelay,
  })  : _cursors = Map<int, int>.of(startOffsets),
        _fetch = fetch,
        _isRetryable = isRetryable,
        _isClientClosed = isClientClosed,
        _onStart = onStart,
        _onFinish = onFinish {
    if (_cursors.isEmpty ||
        pollInterval <= Duration.zero ||
        retryDelay <= Duration.zero) {
      throw ArgumentError(
          'Partitions and positive polling delays are required');
    }
    _controller = StreamController<KafkaTopicRecord>(
      onListen: _listen,
      onResume: _resume,
      onCancel: stop,
    );
  }

  Stream<KafkaTopicRecord> get stream => _controller.stream;

  bool get _active => !_stopped && !_isClientClosed();

  void _listen() {
    if (_stopped) return;
    _started = true;
    // Defer the pump so listen() returns before data or errors can be emitted.
    unawaited(Future<void>(_run));
  }

  void _resume() {
    final resumed = _resumed;
    _resumed = null;
    if (resumed != null && !resumed.isCompleted) resumed.complete();
  }

  Future<void> _ready() async {
    while (_active && _controller.isPaused) {
      final resumed = _resumed ??= Completer<void>();
      await resumed.future;
    }
  }

  /// Stops this stream and waits for admitted fetch work, not listener callbacks.
  Future<void> stop() {
    _stopped = true;
    _waits.close();
    _resume();
    if (!_started) {
      unawaited(_controller.close());
      if (!_finished.isCompleted) _finished.complete();
    }
    return _finished.future;
  }

  Future<void> _run() async {
    var registered = false;
    try {
      if (_stopped) return;
      if (_isClientClosed()) {
        // A stream created while open can be listened to after client close.
        // Report that misuse without registering or admitting any fetch work.
        _controller.addError(StateError('Kafka client is closed'));
        return;
      }
      _onStart();
      registered = true;
      final partitions = _cursors.keys.toList(growable: false);
      var index = 0;
      while (_active) {
        do {
          await _ready();
          // A resumed wait can complete before a queued re-pause. Check again
          // synchronously at admission, with no await before the fetch call.
        } while (_active && _controller.isPaused);
        if (!_active) break;
        final partition = partitions[index];
        final cursor = _cursors[partition]!;
        FetchResponse response;
        try {
          // Never race this future against cancellation: its owner must settle it.
          response = await _fetch(partition, cursor);
        } catch (error, stack) {
          if (!_active) break;
          if (!_isRetryable(error)) {
            Error.throwWithStackTrace(error, stack);
          }
          // Preserve this cursor but let other selected partitions make progress.
          index = (index + 1) % partitions.length;
          await _waits.wait(retryDelay);
          continue;
        }
        if (!_active) break;
        if (response.throttleTimeMs < 0) {
          throw const FormatException('Negative broker throttle duration');
        }
        if (response.topics.length != 1 ||
            response.topics.single.name != topic ||
            response.topics.single.partitions.length != 1 ||
            response.topics.single.partitions.single.partition != partition) {
          throw const FormatException(
              'Unexpected subscription fetch partition');
        }
        final result = response.topics.single.partitions.single;
        if (result.errorCode != 0) {
          throw KafkaBrokerException(result.errorCode, 'Fetch');
        }
        // Validate the entire response before admitting any of its records.
        int? previous;
        for (final record in result.records) {
          if (record.offset < cursor ||
              (previous != null && record.offset <= previous)) {
            throw const FormatException('Non-increasing subscription offsets');
          }
          previous = record.offset;
        }
        final end = result.lastBatchOffset ?? previous;
        if (end != null &&
            (end < cursor || (previous != null && end < previous))) {
          throw const FormatException(
              'Inconsistent subscription batch progress');
        }
        if (end == 9223372036854775807) {
          throw const FormatException(
              'Subscription next offset overflows int64');
        }
        for (final record in result.records) {
          do {
            await _ready();
          } while (_active && _controller.isPaused);
          if (!_active) break;
          assert(!_controller.isPaused,
              'Record admission must not occur while the stream is paused');
          _controller.add(KafkaTopicRecord(topic, partition, record));
          // One event turn per record lets pause/cancel affect admission. Async
          // listener callbacks are not awaited and do not imply acknowledgments.
          await _waits.wait(Duration.zero);
        }
        if (!_active) break;
        if (end != null) _cursors[partition] = end + 1;
        index = (index + 1) % partitions.length;
        // A high watermark alone never advances a cursor. Empty polls back off.
        final throttle = Duration(milliseconds: response.throttleTimeMs);
        final delay =
            end == null && pollInterval > throttle ? pollInterval : throttle;
        if (delay > Duration.zero) await _waits.wait(delay);
      }
    } catch (error, stack) {
      if (_active) _controller.addError(error, stack);
    } finally {
      _stopped = true;
      _waits.close();
      _resume();
      try {
        if (registered) _onFinish();
      } finally {
        // close() waits for a paused listener and calls onCancel. Waiting here
        // would deadlock with onCancel's wait for this pump to finish.
        unawaited(_controller.close());
        if (!_finished.isCompleted) _finished.complete();
      }
    }
  }
}
