import 'dart:async';
import 'dart:io' show SocketException, TlsException;

import 'broker/broker.dart';
import 'classic_group_session.dart';
import 'close_wait_group.dart';
import 'group_delivery.dart';
import 'group_offsets.dart';
import 'group_processing.dart';
import 'group_session.dart';
import 'kafka_client_base.dart';
import 'list_offsets.dart';
import 'monotonic_retry_delay.dart';
import 'reliability.dart';
import 'security/security.dart';

/// This runner owns one serial application Future at a time. Draining means that
/// exact returned Future settled, not that detached work or outside effects were
/// cancelled or fenced. A callback must return all work belonging to its record.
enum KafkaGroupConsumerState {
  idle,
  joining,
  loadingPositions,
  processing,
  draining,
  backoff,
  closed
}

enum KafkaGroupConsumerStopReason { cancelled, rejoinLimit, failure }

/// Final evidence after the callback/data/commit/position barriers and the
/// bounded membership cleanup attempt settle. A membership cleanup timeout can
/// leave underlying shutdown unconfirmed; inspect every cleanup result. Socket
/// closure cannot prove broker departure or undo effects. No hard overall deadline.
final class KafkaGroupConsumerTermination {
  final KafkaGroupConsumerStopReason reason;
  final int generationsStarted;
  final Object? error;
  final StackTrace? stackTrace;

  /// A startup/position operation failed while explicit stop was in progress.
  /// Kept as evidence without claiming cancellation caused that failure.
  final Object? interruptedOperationError;
  final List<Object> cleanupErrors;
  final KafkaGroupTermination? lastSession;
  final KafkaGroupDeliveryTermination? lastProcessing;
  final KafkaGroupCommitResult? lastCommit;
  KafkaGroupConsumerTermination({
    required this.reason,
    required this.generationsStarted,
    this.error,
    this.stackTrace,
    this.interruptedOperationError,
    List<Object> cleanupErrors = const [],
    this.lastSession,
    this.lastProcessing,
    this.lastCommit,
  }) : cleanupErrors = List.unmodifiable(cleanupErrors);
}

/// Opt-in serial processing with fresh committed positions and bounded rejoin.
///
/// A replacement generation is allowed only after a stable processing cycle
/// loses membership through Heartbeat REBALANCE_IN_PROGRESS (27), its callback
/// Future settles successfully, and all owned cleanup succeeds. The default-off
/// [KafkaGroupConsumer.new] coordinator-loss policy also permits narrowly typed
/// stable heartbeat failures, using the same rejoin budget. Startup, lookup,
/// Fetch, processing, commit and cleanup failures remain terminal. An unknown
/// commit cannot be hidden by an earlier heartbeat rejection.
///
/// This is not an exactly-once consumer or broker/external-effect fencing lease.
/// Existing KafkaGroupSession, raw delivery and manual commit APIs are unchanged.
class KafkaGroupConsumer {
  final GroupConsumerController _controller;
  KafkaGroupConsumer._(this._controller);

  /// [commitAfterProcessing] explicitly chooses whether each successful record
  /// is committed after acknowledgement. False never commits and can replay
  /// acknowledged work on rejoin because positions are freshly read from Kafka.
  ///
  /// Each assigned partition uses its stored next offset. A successful missing
  /// value uses [missingOffsets] first, then [missingOffsetPosition] if provided.
  /// The default null preserves numeric-only fallback and terminal absence.
  /// The boundary option covers any assigned partition lacking a numeric entry,
  /// including newly assigned partitions. Earliest can replay retained records;
  /// latest skips records below the exclusive read-uncommitted high watermark.
  /// It never handles lookup failures, known position-floor conflicts or
  /// out-of-range Fetch. Boundary starts establish no committed-position floor.
  /// Lookup and Fetch are separate reads; retention can race either one.
  ///
  /// [tlsFetchRecovery] explicitly enables bounded high-level Fetch recovery
  /// on each generation's dedicated data client. It requires [tls] and explicit
  /// [allowTlsBootstrapFallback]. Control, position lookup and commit clients
  /// retain their existing behavior; no membership-changing replay is added.
  /// The recovery window bounds phase admission, not a hard overall deadline.
  /// Revocation still discards late data and closes the generation's data client;
  /// recovery neither extends authority nor changes the allowed rejoin reasons.
  ///
  /// [rejoinOnCoordinatorLoss] defaults to false. When enabled, a failed stable
  /// heartbeat may also replace the generation for broker errors 15/16 or an
  /// observed KafkaConnectionException, SocketException or TimeoutException.
  /// Explicit TLS/handshake failures are excluded. Some established transport
  /// failures are normalized to KafkaConnectionException before this boundary.
  /// Replacement shares [maxRejoins] and [rejoinDelay] with heartbeat error 27
  /// and still requires successful callback, commit and owned cleanup drains.
  /// Every replacement under this flag, including error 27, also rejects missing
  /// or lower fresh broker positions for overlapping partitions compared with
  /// the previous generation's broker-observed starts and confirmed commits.
  /// This comparison never substitutes local offsets or retries the lookup;
  /// explicit fallback starts and uncommitted progress do not establish a floor.
  ///
  /// [requireStableOffsets] opts each generation's position read into exact
  /// OffsetFetch v7 with require_stable=true. The default remains v5. Unsupported
  /// versions fail without downgrade. Unstable-offset errors fail immediately
  /// unless [stableOffsetRetry] explicitly enables bounded error-88 retries.
  /// That policy requires [requireStableOffsets] and applies independently to
  /// each owned generation committed-position read, not any subsequent boundary
  /// lookup. Closing the owned positions client
  /// cancels pending retries; a retry never extends generation authority.
  /// Existing transport/coordinator retries and explicit missing-offset fallbacks
  /// remain unchanged, as do rejoin eligibility and previous-position floors.
  factory KafkaGroupConsumer({
    required List<Broker> brokers,
    required String groupId,
    required Set<String> topics,
    required bool commitAfterProcessing,
    Map<String, Map<int, int>> missingOffsets = const {},
    KafkaOffsetPosition? missingOffsetPosition,
    int maxRejoins = 3,
    bool rejoinOnCoordinatorLoss = false,
    bool requireStableOffsets = false,
    KafkaStableOffsetRetry? stableOffsetRetry,
    Duration rejoinDelay = const Duration(milliseconds: 250),
    Duration processingTimeout = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
    int maxWaitMs = 100,
    int maxBytes = 1048576,
    int maxBufferedRecords = 10000,
    int maxBufferedPayloadBytes = 16 * 1024 * 1024,
    KafkaTlsConfig? tls,
    KafkaSaslPlain? sasl,
    bool allowTlsBootstrapFallback = false,
    KafkaTlsFetchRecovery? tlsFetchRecovery,
    Duration sessionTimeout = const Duration(seconds: 10),
    Duration rebalanceTimeout = const Duration(seconds: 20),
    Duration heartbeatInterval = const Duration(seconds: 2),
    Duration localExpiry = const Duration(seconds: 8),
    Duration startupTimeout = const Duration(seconds: 60),
    Duration leaveTimeout = const Duration(seconds: 3),
    Duration requestTimeout = const Duration(seconds: 35),
    Duration connectionTimeout = const Duration(seconds: 5),
    int maxFrameBytes = 16 * 1024 * 1024,
    int maxConnections = 8,
    int maxPendingRequestBytes = 32 * 1024 * 1024,
    int maxBufferedResponseBytes = 32 * 1024 * 1024,
    int maxReadRetries = 2,
    Duration retryBaseDelay = const Duration(milliseconds: 50),
  }) {
    if (stableOffsetRetry != null && !requireStableOffsets) {
      throw ArgumentError(
          'stableOffsetRetry requires requireStableOffsets=true');
    }
    final frozenBrokers = List<Broker>.unmodifiable(brokers);
    final frozenTopics = Set<String>.unmodifiable(topics);
    final fallbacks =
        GroupConsumerController.freezeFallbacks(missingOffsets, frozenTopics);
    GroupConsumerController.validate(maxRejoins, rejoinDelay);
    GroupProcessingController.validate(1, processingTimeout);
    if (pollInterval < const Duration(milliseconds: 1) ||
        pollInterval > const Duration(minutes: 1) ||
        maxWaitMs < 0 ||
        maxWaitMs > 0x7fffffff ||
        maxBytes < 1 ||
        maxBytes > 0x7fffffff ||
        maxBufferedRecords < 1 ||
        maxBufferedRecords > 0x7fffffff ||
        maxBufferedPayloadBytes < 1 ||
        maxBufferedPayloadBytes > 0x7fffffff) {
      throw ArgumentError('Invalid group consumer delivery bounds');
    }
    KafkaClient createPositionClient() => KafkaClient(
        brokers: frozenBrokers,
        tls: tls,
        sasl: sasl,
        allowTlsBootstrapFallback: allowTlsBootstrapFallback,
        requestTimeout: requestTimeout,
        connectionTimeout: connectionTimeout,
        maxFrameBytes: maxFrameBytes,
        maxConnections: maxConnections,
        maxInFlightRequests: 4,
        maxPendingRequestBytes: maxPendingRequestBytes,
        maxBufferedResponseBytes: maxBufferedResponseBytes,
        maxReadRetries: maxReadRetries,
        retryBaseDelay: retryBaseDelay);
    GroupConsumerCycle createCycle() => _NativeConsumerCycle(
        KafkaGroupSession(
            brokers: frozenBrokers,
            groupId: groupId,
            topics: frozenTopics,
            tls: tls,
            sasl: sasl,
            allowTlsBootstrapFallback: allowTlsBootstrapFallback,
            tlsFetchRecovery: tlsFetchRecovery,
            sessionTimeout: sessionTimeout,
            rebalanceTimeout: rebalanceTimeout,
            heartbeatInterval: heartbeatInterval,
            localExpiry: localExpiry,
            startupTimeout: startupTimeout,
            leaveTimeout: leaveTimeout,
            requestTimeout: requestTimeout,
            connectionTimeout: connectionTimeout,
            maxFrameBytes: maxFrameBytes,
            maxConnections: maxConnections,
            maxPendingRequestBytes: maxPendingRequestBytes,
            maxBufferedResponseBytes: maxBufferedResponseBytes,
            maxReadRetries: maxReadRetries,
            retryBaseDelay: retryBaseDelay),
        createPositionClient,
        requireStableOffsets: requireStableOffsets,
        stableOffsetRetry: stableOffsetRetry,
        processingTimeout: processingTimeout,
        pollInterval: pollInterval,
        maxWaitMs: maxWaitMs,
        maxBytes: maxBytes,
        maxBufferedRecords: maxBufferedRecords,
        maxBufferedPayloadBytes: maxBufferedPayloadBytes);
    // Validate all native session/client settings now, without network activity.
    final initial = createCycle();
    return KafkaGroupConsumer._(GroupConsumerController(
        initial: initial,
        createCycle: createCycle,
        commitAfterProcessing: commitAfterProcessing,
        missingOffsets: fallbacks,
        missingOffsetPosition: missingOffsetPosition,
        maxRejoins: maxRejoins,
        rejoinOnCoordinatorLoss: rejoinOnCoordinatorLoss,
        rejoinDelay: rejoinDelay));
  }

  KafkaGroupConsumerState get state => _controller.state;
  int get generationsStarted => _controller.generationsStarted;
  KafkaGroupAssignment? get assignment => _controller.assignment;
  bool isCurrent(KafkaGroupAssignment token) => _controller.isCurrent(token);
  Future<KafkaGroupConsumerTermination> get closed => _controller.closed;

  /// Starts once and returns [closed]. Calls [process] directly from the original
  /// synchronous record callback, then waits its returned Future. No next record
  /// or generation starts while that Future or its explicit commit is pending.
  /// Detached/unreturned Futures are outside this guarantee.
  Future<KafkaGroupConsumerTermination> run(
          FutureOr<void> Function(KafkaGroupRecord record) process) =>
      _controller.run(process);

  /// Synchronously stops new callbacks, joins and commits, then starts cleanup.
  /// Await [closed] outside the processing callback to observe draining. Awaiting
  /// closed inside that same callback would wait on itself. Idempotent.
  void stop() => _controller.stop();
}

/// Internal owned-cycle seam for deterministic lifecycle tests.
abstract class GroupConsumerCycle {
  Future<KafkaGroupAssignment> start();
  bool isCurrent(KafkaGroupAssignment token);
  Future<KafkaGroupTermination> get revoked;
  Future<KafkaGroupTermination> close();
  Future<Map<int, KafkaCommittedOffset?>> readPositions(
      String topic, Set<int> partitions);
  Future<int> resolveMissingOffset(
      String topic, int partition, KafkaOffsetPosition position);
  Future<void> closePositions();
  KafkaGroupProcessing processing(
      KafkaGroupAssignment token, Map<String, Map<int, int>> positions);
}

class _NativeConsumerCycle implements GroupConsumerCycle {
  final KafkaGroupSession session;
  final KafkaClient Function() createPositions;
  final bool requireStableOffsets;
  final KafkaStableOffsetRetry? stableOffsetRetry;
  final Duration processingTimeout, pollInterval;
  final int maxWaitMs, maxBytes, maxBufferedRecords, maxBufferedPayloadBytes;
  KafkaClient? _positions;
  Future<void>? _positionsClosed;
  bool _positionsClosing = false;
  _NativeConsumerCycle(this.session, this.createPositions,
      {required this.requireStableOffsets,
      required this.stableOffsetRetry,
      required this.processingTimeout,
      required this.pollInterval,
      required this.maxWaitMs,
      required this.maxBytes,
      required this.maxBufferedRecords,
      required this.maxBufferedPayloadBytes});
  @override
  Future<KafkaGroupAssignment> start() => session.start();
  @override
  bool isCurrent(KafkaGroupAssignment token) => session.isCurrent(token);
  @override
  Future<KafkaGroupTermination> get revoked => session.revoked;
  @override
  Future<KafkaGroupTermination> close() => session.close();
  @override
  Future<Map<int, KafkaCommittedOffset?>> readPositions(
      String topic, Set<int> partitions) {
    if (_positionsClosing) {
      return Future.error(StateError('Position client is closed'));
    }
    return (_positions ??= createPositions()).committedOffsets(
        session.groupId, topic,
        partitions: partitions,
        requireStable: requireStableOffsets,
        stableOffsetRetry: stableOffsetRetry);
  }

  @override
  Future<int> resolveMissingOffset(
      String topic, int partition, KafkaOffsetPosition position) async {
    if (_positionsClosing) throw StateError('Position client is closed');
    final response = await (_positions ??= createPositions())
        .listOffsets(topic, partition: partition, position: position);
    return response.topics.single.partitions.single.offset;
  }

  @override
  Future<void> closePositions() {
    _positionsClosing = true;
    return _positionsClosed ??= _positions?.close() ?? Future<void>.value();
  }

  @override
  KafkaGroupProcessing processing(
          KafkaGroupAssignment token, Map<String, Map<int, int>> positions) =>
      session.subscribeProcessing(
          assignment: token,
          startOffsets: positions,
          maxPendingRecords: 1,
          processingTimeout: processingTimeout,
          pollInterval: pollInterval,
          maxWaitMs: maxWaitMs,
          maxBytes: maxBytes,
          maxBufferedRecords: maxBufferedRecords,
          maxBufferedPayloadBytes: maxBufferedPayloadBytes);
}

class _ConsumerStopped implements Exception {}

/// Internal serial-lifecycle owner. No user-created session/processing handle is
/// exposed, so every admitted callback and commit has an owned settling Future.
class GroupConsumerController {
  final GroupConsumerCycle initial;
  final GroupConsumerCycle Function() createCycle;
  final bool commitAfterProcessing;
  final Map<String, Map<int, int>> missingOffsets;
  final KafkaOffsetPosition? missingOffsetPosition;
  final int maxRejoins;
  final bool rejoinOnCoordinatorLoss;
  Map<String, Map<int, int>> _previousPositionFloors = const {};
  Map<String, Map<int, int>> _observedPositions = {};
  final Duration rejoinDelay;
  final _finished = Completer<KafkaGroupConsumerTermination>();
  final _waits = CloseWaitGroup();
  final _clock = Stopwatch()..start();
  GroupConsumerCycle? _cycle;
  KafkaGroupAssignment? _assignment;
  KafkaGroupConsumerState _state = KafkaGroupConsumerState.idle;
  bool _started = false, _stopping = false;
  int _generations = 0;
  Future<Object?>? _positionsClosed;
  Future<KafkaGroupTermination?>? _sessionClosed;
  KafkaGroupProcessing? _processing;
  StreamSubscription<KafkaGroupRecord>? _listener;
  Future<void>? _work;
  bool _working = false;
  Object? _error;
  Object? _interruptedOperationError;
  StackTrace? _stack;
  final List<Object> _cleanupErrors = [];
  KafkaGroupTermination? _lastSession;
  KafkaGroupDeliveryTermination? _lastProcessing;
  KafkaGroupCommitResult? _lastCommit;

  GroupConsumerController(
      {required this.initial,
      required this.createCycle,
      required this.commitAfterProcessing,
      required this.missingOffsets,
      this.missingOffsetPosition,
      required this.maxRejoins,
      this.rejoinOnCoordinatorLoss = false,
      required this.rejoinDelay}) {
    validate(maxRejoins, rejoinDelay);
  }

  static void validate(int count, Duration delay) {
    if (count < 0 ||
        count > 100 ||
        delay < const Duration(milliseconds: 1) ||
        delay > const Duration(minutes: 1)) {
      throw ArgumentError('Invalid bounded group rejoin policy');
    }
  }

  static Map<String, Map<int, int>> freezeFallbacks(
      Map<String, Map<int, int>> input, Set<String> topics) {
    final result = <String, Map<int, int>>{};
    for (final topic in input.entries) {
      if (!topics.contains(topic.key)) {
        throw ArgumentError('Fallback topic is not subscribed');
      }
      for (final p in topic.value.entries) {
        if (p.key < 0 ||
            p.key > 0x7fffffff ||
            p.value < 0 ||
            p.value > 9223372036854775807) {
          throw ArgumentError('Invalid missing-offset fallback');
        }
      }
      result[topic.key] = Map.unmodifiable(topic.value);
    }
    return Map.unmodifiable(result);
  }

  KafkaGroupConsumerState get state => _state;
  int get generationsStarted => _generations;
  KafkaGroupAssignment? get assignment {
    final token = _assignment;
    return token != null && isCurrent(token) ? token : null;
  }

  bool isCurrent(KafkaGroupAssignment token) =>
      !_stopping &&
      identical(token, _assignment) &&
      (_cycle?.isCurrent(token) ?? false);
  Future<KafkaGroupConsumerTermination> get closed => _finished.future;

  Future<KafkaGroupConsumerTermination> run(
      FutureOr<void> Function(KafkaGroupRecord) process) {
    if (_started || _stopping) {
      return Future.error(StateError('Consumer can run only once'));
    }
    _started = true;
    unawaited(_run(process));
    return closed;
  }

  void stop() {
    if (_stopping || _finished.isCompleted) return;
    _stopping = true;
    _waits.close();
    if (_cycle != null) {
      // close revokes synchronously; its result is owned by the run finalizer.
      unawaited(_closeSession());
      unawaited(_closePositions());
    } else if (!_started) {
      _state = KafkaGroupConsumerState.draining;
      unawaited(_closeIdle());
    }
  }

  Future<void> _closeIdle() async {
    _cycle = initial;
    final closingSession = _closeSession();
    final closingPositions = _closePositions();
    _lastSession = await closingSession;
    final positionError = await closingPositions;
    if (positionError != null) _cleanupErrors.add(positionError);
    if (_lastSession?.cleanupError != null) {
      _cleanupErrors.add(_lastSession!.cleanupError!);
    }
    _cycle = null;
    _finish(_cleanupErrors.isEmpty
        ? KafkaGroupConsumerStopReason.cancelled
        : KafkaGroupConsumerStopReason.failure);
  }

  void _check(KafkaGroupAssignment token) {
    if (_stopping) throw _ConsumerStopped();
    if (!isCurrent(token)) {
      throw StateError('Consumer generation is no longer current');
    }
  }

  void _fail(Object error, [StackTrace? stack]) {
    _error ??= error;
    _stack ??= stack;
    final cycle = _cycle;
    if (cycle != null) {
      unawaited(_closeSession());
      unawaited(_closePositions());
    }
  }

  Future<KafkaGroupTermination?> _closeSession() => _sessionClosed ??=
          Future<KafkaGroupTermination>.sync(() => _cycle!.close())
              .then<KafkaGroupTermination?>((value) => value,
                  onError: (Object error, StackTrace _) {
        _cleanupErrors.add(error);
        return null;
      });

  Future<Object?> _closePositions() =>
      _positionsClosed ??= Future<void>.sync(() => _cycle!.closePositions())
          .then<Object?>((_) => null,
              onError: (Object error, StackTrace _) => error);

  Future<Map<String, Map<int, int>>> _loadPositions(
      KafkaGroupAssignment token) async {
    final result = <String, Map<int, int>>{};
    final unresolved = <String, List<int>>{};
    final names = token.partitions.keys.toList()..sort();
    for (final topic in names) {
      final ids = Set<int>.unmodifiable(token.partitions[topic]!);
      if (ids.isEmpty) continue;
      _check(token);
      final stored = await _cycle!.readPositions(topic, ids);
      _check(token);
      if (stored.length != ids.length || !ids.containsAll(stored.keys)) {
        throw const FormatException(
            'Position response does not match assignment');
      }
      final offsets = <int, int>{};
      for (final id in ids) {
        final value = stored[id];
        final brokerNext = value?.nextOffset;
        final floor = _previousPositionFloors[topic]?[id];
        if (rejoinOnCoordinatorLoss &&
            floor != null &&
            (brokerNext == null || brokerNext < floor)) {
          throw StateError(brokerNext == null
              ? 'Missing fresh broker position for $topic:$id; previous floor $floor'
              : 'Lower fresh broker position for $topic:$id: $brokerNext < previous floor $floor');
        }
        final next = brokerNext ?? missingOffsets[topic]?[id];
        if (next == null) {
          if (missingOffsetPosition != null) {
            (unresolved[topic] ??= []).add(id);
            continue;
          }
          throw StateError(
              'No committed offset or explicit fallback for $topic:$id');
        }
        if (next < 0 || next > 9223372036854775807) {
          throw const FormatException('Invalid starting position');
        }
        offsets[id] = next;
        if (rejoinOnCoordinatorLoss && brokerNext != null) {
          (_observedPositions[topic] ??= {})[id] = brokerNext;
        }
      }
      result[topic] = offsets;
    }
    // Snapshot every exact committed response and validate all raw floors before
    // adding boundary awaits. A boundary is a fallback, never committed evidence.
    for (final topic in unresolved.entries) {
      for (final id in topic.value) {
        _check(token);
        final next = await _cycle!
            .resolveMissingOffset(topic.key, id, missingOffsetPosition!);
        _check(token);
        if (next < 0 || next > 9223372036854775807) {
          throw const FormatException('Invalid resolved starting position');
        }
        result[topic.key]![id] = next;
      }
    }
    return Map.unmodifiable(result.map((topic, offsets) =>
        MapEntry(topic, Map<int, int>.unmodifiable(offsets))));
  }

  Future<void> _handle(KafkaGroupRecord record,
      FutureOr<void> Function(KafkaGroupRecord) process) async {
    _working = true;
    try {
      _check(record.assignment);
      // Future.sync calls application code now, inside original admission.
      await Future<void>.sync(() => process(record));
      if (!isCurrent(record.assignment)) return;
      _processing!.acknowledge(record);
      if (commitAfterProcessing && isCurrent(record.assignment)) {
        final result =
            _lastCommit = await _processing!.commitAcknowledged(record.topic);
        if (result.error != null ||
            result.partitions.values
                .any((p) => p.outcome != KafkaGroupCommitOutcome.confirmed)) {
          _fail(result.error ?? StateError('Commit was not confirmed'),
              result.stackTrace);
        }
      }
    } catch (error, stack) {
      _fail(error, stack);
    } finally {
      _working = false;
      if (isCurrent(record.assignment) && _error == null) _listener?.resume();
    }
  }

  Future<void> _run(FutureOr<void> Function(KafkaGroupRecord) process) async {
    var cycle = initial;
    var rejoins = 0;
    var reason = KafkaGroupConsumerStopReason.failure;
    try {
      while (!_stopping) {
        _cycle = cycle;
        _positionsClosed = null;
        _sessionClosed = null;
        _processing = null;
        _listener = null;
        _work = null;
        _lastSession = null;
        _lastProcessing = null;
        _lastCommit = null;
        _assignment = null;
        _observedPositions = {};
        var processingStarted = false;
        final ownedCycle = cycle;
        unawaited(ownedCycle.revoked.then<void>((_) {
          if (identical(_cycle, ownedCycle)) unawaited(_closePositions());
        }, onError: (Object error, StackTrace stack) {
          if (identical(_cycle, ownedCycle)) _fail(error, stack);
        }));
        try {
          _state = KafkaGroupConsumerState.joining;
          _generations++;
          final token = _assignment = await cycle.start();
          _check(token);
          _state = KafkaGroupConsumerState.loadingPositions;
          final positions = await _loadPositions(token);
          final positionError = await _closePositions();
          if (positionError != null) throw positionError;
          _check(token);
          _processing = cycle.processing(token, positions);
          _state = KafkaGroupConsumerState.processing;
          _listener = _processing!.records.listen((record) {
            if (_stopping || _error != null) return;
            if (_working) {
              _fail(StateError('Concurrent consumer callback'));
              return;
            }
            _listener!.pause();
            _work = _handle(record, process);
          }, onError: (Object error, StackTrace stack) => _fail(error, stack));
          processingStarted = true;
          await cycle.revoked;
        } catch (error, stack) {
          if (error is! _ConsumerStopped) {
            if (_stopping) {
              _interruptedOperationError ??= error;
            } else {
              _error ??= error;
              _stack ??= stack;
            }
          }
        } finally {
          _state = KafkaGroupConsumerState.draining;
          // Initiate every owned cleanup before awaiting any of them or app work.
          final processingStop = _processing?.cancel();
          final sessionStop = _closeSession();
          final positionsStop = _closePositions();
          try {
            _lastProcessing = await processingStop;
          } catch (error) {
            _cleanupErrors.add(error);
          }
          try {
            _lastSession = await sessionStop;
          } catch (error) {
            _cleanupErrors.add(error);
          }
          final positionError = await positionsStop;
          if (positionError != null) _cleanupErrors.add(positionError);
          await _work;
          try {
            await _listener?.cancel();
          } catch (error) {
            _cleanupErrors.add(error);
          }
          if (_lastProcessing?.error != null) _error ??= _lastProcessing!.error;
          if (_lastProcessing?.cleanupError != null) {
            _cleanupErrors.add(_lastProcessing!.cleanupError!);
          }
          if (_lastSession?.cleanupError != null) {
            _cleanupErrors.add(_lastSession!.cleanupError!);
          }
        }
        if (_error != null || _cleanupErrors.isNotEmpty) break;
        if (_stopping) {
          reason = KafkaGroupConsumerStopReason.cancelled;
          break;
        }
        final termination = _lastSession;
        final failure = termination?.error;
        final rebalance =
            termination?.reason == KafkaGroupStopReason.brokerError &&
                failure is KafkaBrokerException &&
                failure.errorCode == 27 &&
                (failure.operation == 'Heartbeat' ||
                    failure.operation == 'heartbeat');
        final coordinatorLoss = rejoinOnCoordinatorLoss &&
            termination?.stableHeartbeatFailure != null &&
            _isCoordinatorLoss(termination!);
        final eligible = processingStarted &&
            (rebalance || coordinatorLoss) &&
            _lastProcessing?.reason == KafkaGroupDeliveryStopReason.revoked;
        if (!eligible) {
          _error = failure ??
              StateError('Generation ended without eligible rebalance');
          break;
        }
        if (rejoins >= maxRejoins) {
          reason = KafkaGroupConsumerStopReason.rejoinLimit;
          break;
        }
        if (rejoinOnCoordinatorLoss) {
          // All callback/commit and cleanup barriers settled successfully above.
          // The ledger retains every confirmation, including late responses after
          // revocation. Keep this assignment only, never an accumulating history.
          final floors = <String, Map<int, int>>{
            for (final topic in _observedPositions.entries)
              topic.key: Map.of(topic.value)
          };
          final confirmed = _processing!.confirmedNextOffsets;
          for (final topic in confirmed.entries) {
            for (final partition in topic.value.entries) {
              final offsets = floors[topic.key] ??= {};
              final previous = offsets[partition.key];
              if (previous == null || partition.value > previous) {
                offsets[partition.key] = partition.value;
              }
            }
          }
          _previousPositionFloors = floors;
        }
        _state = KafkaGroupConsumerState.backoff;
        await waitForMonotonicRetryDeadline(
            deadline: _clock.elapsed + rejoinDelay,
            elapsed: () => _clock.elapsed,
            check: () {
              if (_stopping) throw _ConsumerStopped();
            },
            wait: _waits.wait);
        if (_stopping) {
          reason = KafkaGroupConsumerStopReason.cancelled;
          break;
        }
        cycle = createCycle();
        rejoins++;
      }
      if (_stopping && _error == null && _cleanupErrors.isEmpty) {
        reason = KafkaGroupConsumerStopReason.cancelled;
      }
    } catch (error, stack) {
      if (error is _ConsumerStopped) {
        reason = KafkaGroupConsumerStopReason.cancelled;
      } else {
        _error ??= error;
        _stack ??= stack;
      }
    } finally {
      _cycle = null;
      _assignment = null;
      _waits.close();
      _finish(reason);
    }
  }

  bool _isCoordinatorLoss(KafkaGroupTermination termination) {
    final error = termination.error;
    final evidence = termination.stableHeartbeatFailure;
    if (evidence == null ||
        !identical(evidence.error, error) ||
        !identical(evidence.stackTrace, termination.stackTrace)) {
      return false;
    }
    if (error is TlsException) return false;
    if (termination.reason == KafkaGroupStopReason.brokerError) {
      return error is KafkaBrokerException &&
          (error.errorCode == 15 || error.errorCode == 16);
    }
    return termination.reason == KafkaGroupStopReason.failure &&
        (error is KafkaConnectionException ||
            error is SocketException ||
            error is TimeoutException);
  }

  void _finish(KafkaGroupConsumerStopReason reason) {
    _state = KafkaGroupConsumerState.closed;
    if (!_finished.isCompleted) {
      _finished.complete(KafkaGroupConsumerTermination(
          reason: reason,
          generationsStarted: _generations,
          error: _error,
          stackTrace: _stack,
          interruptedOperationError: _interruptedOperationError,
          cleanupErrors: _cleanupErrors,
          lastSession: _lastSession,
          lastProcessing: _lastProcessing,
          lastCommit: _lastCommit));
    }
  }
}
