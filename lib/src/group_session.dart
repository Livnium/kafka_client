import 'dart:convert';

import 'broker/broker.dart';
import 'classic_group_session.dart';
import 'group_delivery.dart';
import 'group_processing.dart';
import 'kafka_client_base.dart';
import 'native_classic_group_driver.dart';
import 'reliability.dart';
import 'security/security.dart';

/// Experimental, single-generation classic-group membership management.
///
/// It owns a separate control client and can create one bounded [subscribe]
/// delivery channel using a separate data client. Optional [subscribeProcessing]
/// adds explicit acknowledgements and generation commits, never automatic commits
/// or rejoining. A rebalance, rejection or uncertain operation ends
/// the session permanently. Create another session only after handling revoked
/// application work yourself.
class KafkaGroupSession {
  final String groupId;
  final Set<String> topics;
  final ClassicGroupSessionController _controller;
  final KafkaClient Function() _createDataClient;
  final NativeClassicGroupDriver _driver;
  bool _deliveryCreated = false;

  KafkaGroupSession._(this.groupId, this.topics, this._controller,
      this._createDataClient, this._driver);

  /// Creates an idle session; [start] explicitly begins network activity.
  ///
  /// All broker endpoints use the same TLS/SASL settings. A dedicated owned
  /// client isolates control traffic from application Fetch/Produce admission.
  /// Its fixed four-request admission budget is not shared with other clients.
  ///
  /// [tlsFetchRecovery] is a separate opt-in for high-level Fetch on the
  /// generation's dedicated data client only. It requires [tls] and explicit
  /// [allowTlsBootstrapFallback]; control traffic and commits do not gain
  /// recovery or replay. Certificate and hostname validation remain strict.
  /// Its finite window bounds admission of recovery phases, not an overall hard
  /// deadline. Revocation still invalidates the generation, discards late data
  /// and closes the data client; recovery never extends membership authority.
  ///
  /// [localExpiry] is a conservative local deadline shorter than the requested
  /// broker [sessionTimeout]. It is measured from heartbeat admission, not
  /// receipt. Neither this deadline nor a guard proves current broker ownership.
  /// [requestTimeout] must exceed [rebalanceTimeout] for blocking Join/Sync;
  /// [startupTimeout] bounds the complete attempt independently. No timeout
  /// authorizes replay of a membership-changing request.
  factory KafkaGroupSession({
    required List<Broker> brokers,
    required String groupId,
    required Set<String> topics,
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
    void string(String value, String label) {
      final bytes = utf8.encode(value);
      if (value.isEmpty ||
          bytes.length > 32767 ||
          utf8.decode(bytes) != value) {
        throw ArgumentError('Invalid $label');
      }
    }

    void milliseconds(Duration value, String label) {
      if (value.inMilliseconds < 1 ||
          value.inMilliseconds > 2147483647 ||
          value.inMicroseconds != value.inMilliseconds * 1000) {
        throw ArgumentError(
            '$label must be a positive INT32 millisecond value');
      }
    }

    string(groupId, 'groupId');
    if (topics.isEmpty) throw ArgumentError('At least one topic is required');
    final frozenTopics = Set<String>.unmodifiable(topics);
    for (final topic in frozenTopics) {
      string(topic, 'topic');
    }
    milliseconds(sessionTimeout, 'sessionTimeout');
    milliseconds(rebalanceTimeout, 'rebalanceTimeout');
    if (heartbeatInterval <= Duration.zero ||
        localExpiry <= heartbeatInterval ||
        localExpiry >= sessionTimeout ||
        startupTimeout <= Duration.zero ||
        leaveTimeout <= Duration.zero ||
        requestTimeout <= rebalanceTimeout) {
      throw ArgumentError('Invalid group session timing budget');
    }
    // Validate the lazy data client's policy before any network activity.
    if (tlsFetchRecovery != null &&
        (tls == null || !allowTlsBootstrapFallback)) {
      throw ArgumentError('TLS Fetch recovery requires TLS bootstrap fallback');
    }
    final frozenBrokers = List<Broker>.unmodifiable(brokers);
    KafkaClient createClient({KafkaTlsFetchRecovery? fetchRecovery}) =>
        KafkaClient(
            brokers: frozenBrokers,
            tls: tls,
            sasl: sasl,
            allowTlsBootstrapFallback: allowTlsBootstrapFallback,
            tlsFetchRecovery: fetchRecovery,
            requestTimeout: requestTimeout,
            connectionTimeout: connectionTimeout,
            maxFrameBytes: maxFrameBytes,
            maxConnections: maxConnections,
            maxInFlightRequests: 4,
            maxPendingRequestBytes: maxPendingRequestBytes,
            maxBufferedResponseBytes: maxBufferedResponseBytes,
            maxReadRetries: maxReadRetries,
            retryBaseDelay: retryBaseDelay);
    final driver = NativeClassicGroupDriver(
        client: createClient(),
        groupId: groupId,
        topics: frozenTopics,
        sessionTimeoutMs: sessionTimeout.inMilliseconds,
        rebalanceTimeoutMs: rebalanceTimeout.inMilliseconds);
    return KafkaGroupSession._(
        groupId,
        frozenTopics,
        ClassicGroupSessionController(
            driver: driver,
            heartbeatInterval: heartbeatInterval,
            localExpiry: localExpiry,
            startupTimeout: startupTimeout,
            leaveTimeout: leaveTimeout),
        () => createClient(fetchRecovery: tlsFetchRecovery),
        driver);
  }

  KafkaGroupSessionState get state => _controller.state;

  /// The locally current assignment, if any. It is not a broker ownership lock.
  KafkaGroupAssignment? get assignment => _controller.assignment;

  /// Completes when local authority is invalidated, before transport cleanup.
  /// The initial snapshot does not yet establish the final Leave outcome.
  Future<KafkaGroupTermination> get revoked => _controller.revoked;

  /// Completes after the bounded cleanup attempt settles.
  /// Inspect cleanupError: a failure or timeout leaves shutdown unconfirmed.
  Future<KafkaGroupTermination> get closed => _controller.closed;

  /// Joins once, syncs a validated eager range assignment, then confirms it with
  /// a timely heartbeat before exposing the assignment. No automatic rejoin.
  Future<KafkaGroupAssignment> start() => _controller.start();

  /// True only for this session's exact live token at the instant checked.
  ///
  /// This cannot fence Fetch, offset commits or external effects. Ownership can
  /// be lost immediately afterward. Do not combine this check with an async
  /// callback and assume that callback becomes cancellable or safe to finish.
  bool isCurrent(KafkaGroupAssignment assignment) =>
      _controller.isCurrent(assignment);

  /// Throws when [assignment] is not locally current. Same limits as [isCurrent].
  void assertCurrent(KafkaGroupAssignment assignment) =>
      _controller.assertCurrent(assignment);

  /// Creates this session's sole single-generation record delivery channel.
  ///
  /// [assignment] must be this session's exact live token from [start]. Provide
  /// inclusive [startOffsets] for exactly its nonempty assigned topic/partition
  /// sets. An empty assignment requires an empty map and issues no Fetch.
  /// Offsets are copied; no earliest/latest lookup or committed-offset lookup is
  /// implicit. No network polling starts until listening to the returned stream.
  ///
  /// The data client uses the same frozen broker list and TLS/SASL configuration as control,
  /// with separate transport/admission budgets. One Fetch at a time is admitted;
  /// a terminal Fetch error ends delivery and membership after the underlying
  /// read-safe client retries. No outer indefinite retry or automatic rejoin.
  ///
  /// A paused listener retains at most one validated response. Record count and
  /// key/value/header payload bounds are enforced AFTER decoding and before any
  /// record is delivered. These are not whole-heap limits; [maxBytes] is a soft
  /// broker request bound and the client's wire/frame caps still apply.
  /// Cancellation/failure closes membership. Session revocation discards buffered
  /// and late responses. Await the delivery's stopped barrier for data cleanup
  /// and [closed] separately for membership cleanup. Neither drains application
  /// async work or fences external effects. No commits or acknowledgements.
  KafkaGroupDelivery subscribe({
    required KafkaGroupAssignment assignment,
    required Map<String, Map<int, int>> startOffsets,
    Duration pollInterval = const Duration(milliseconds: 100),
    int maxWaitMs = 100,
    int maxBytes = 1048576,
    int maxBufferedRecords = 10000,
    int maxBufferedPayloadBytes = 16 * 1024 * 1024,
  }) =>
      _subscribe(
              assignment: assignment,
              startOffsets: startOffsets,
              pollInterval: pollInterval,
              maxWaitMs: maxWaitMs,
              maxBytes: maxBytes,
              maxBufferedRecords: maxBufferedRecords,
              maxBufferedPayloadBytes: maxBufferedPayloadBytes)
          .delivery;

  /// Opt-in acknowledgement ledger and explicit generation-aware commits.
  /// Shares [subscribe]'s admission, Fetch and lifetime boundaries. Capacity
  /// counts all unretired work, including acknowledged entries behind a gap.
  /// The oldest unfinished record's original admission time determines the
  /// processing deadline, independent of heartbeats or later acknowledgements.
  /// This session can create either one raw or one processing delivery, not both.
  KafkaGroupProcessing subscribeProcessing({
    required KafkaGroupAssignment assignment,
    required Map<String, Map<int, int>> startOffsets,
    Duration pollInterval = const Duration(milliseconds: 100),
    int maxWaitMs = 100,
    int maxBytes = 1048576,
    int maxBufferedRecords = 10000,
    int maxBufferedPayloadBytes = 16 * 1024 * 1024,
    int maxPendingRecords = 1000,
    Duration processingTimeout = const Duration(seconds: 30),
  }) {
    _controller.assertCurrent(assignment);
    GroupProcessingController.validate(maxPendingRecords, processingTimeout);
    _driver.requireMemberCommitSupport();
    final processing = GroupProcessingController(
        session: _controller,
        assignment: assignment,
        maxPendingRecords: maxPendingRecords,
        processingTimeout: processingTimeout,
        commit: (topic, offsets, cancellation) => _driver.commit(topic, offsets,
            generationId: assignment.generationId,
            memberId: assignment.memberId,
            cancellationToken: cancellation));
    final delivery = _subscribe(
        assignment: assignment,
        startOffsets: startOffsets,
        pollInterval: pollInterval,
        maxWaitMs: maxWaitMs,
        maxBytes: maxBytes,
        maxBufferedRecords: maxBufferedRecords,
        maxBufferedPayloadBytes: maxBufferedPayloadBytes,
        processing: processing);
    processing.bind(delivery);
    return processing.processing;
  }

  GroupDeliveryController _subscribe({
    required KafkaGroupAssignment assignment,
    required Map<String, Map<int, int>> startOffsets,
    required Duration pollInterval,
    required int maxWaitMs,
    required int maxBytes,
    required int maxBufferedRecords,
    required int maxBufferedPayloadBytes,
    GroupProcessingController? processing,
  }) {
    _controller.assertCurrent(assignment);
    if (_deliveryCreated) throw StateError('Session permits only one delivery');
    GroupDeliveryController.validate(assignment, startOffsets, pollInterval,
        maxBufferedRecords, maxBufferedPayloadBytes);
    if (maxWaitMs < 0 ||
        maxWaitMs > 0x7fffffff ||
        maxBytes < 1 ||
        maxBytes > 0x7fffffff) {
      throw ArgumentError('Invalid group delivery Fetch bounds');
    }
    final data = _createDataClient();
    final controller = GroupDeliveryController(
      session: _controller,
      assignment: assignment,
      startOffsets: startOffsets,
      fetch: (topic, partition, offset) => data.fetch(topic,
          partition: partition,
          offset: offset,
          maxWaitMs: maxWaitMs,
          maxBytes: maxBytes),
      closeData: data.close,
      pollInterval: pollInterval,
      maxBufferedRecords: maxBufferedRecords,
      maxBufferedPayloadBytes: maxBufferedPayloadBytes,
      waitForProcessingCapacity: processing?.waitForCapacity,
      admitProcessing: processing?.admit,
      stopProcessing: processing?.stop,
    );
    _deliveryCreated = true;
    return controller;
  }

  /// Immediately revokes local validity, then performs bounded owned cleanup.
  ///
  /// Leave is attempted at most once only from an eligible stable state. A lost
  /// response is not confirmed departure. No application callback is drained.
  /// If subscribed, await the delivery stopped barrier for data-client cleanup;
  /// this future reports only membership/control-client cleanup.
  Future<KafkaGroupTermination> close() => _controller.close();
}
