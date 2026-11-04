import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'api/abstract/kafka_request.dart';
import 'api/request/api0_produce_request.dart';
import 'api/request/api1_fetch_request.dart';
import 'api/request/api18_api_vesions_request.dart';
import 'api/request/api3_metadata_request.dart';
import 'api/response/api0_produce_response.dart';
import 'api/response/api1_fetch_response.dart';
import 'api/response/api18_api_versions_response.dart';
import 'api/response/api3_metadata_response.dart';
import 'broker/broker.dart';
import 'codec/byte_reader.dart';
import 'codec/record_batch.dart';
import 'security/security.dart';
import 'security/sasl_requests.dart';
import 'reliability.dart';
import 'close_wait_group.dart';
import 'monotonic_retry_delay.dart';
import 'topic_subscription.dart';
import 'transport.dart';
import 'group_offsets.dart';
import 'group_offsets_protocol.dart';

/// Experimental native Kafka client with validated TLS and optional PLAIN SASL.
/// TLS is never downgraded and SASL credentials are never sent without TLS.
class KafkaClient {
  /// Immutable bootstrap endpoints; discovered leader endpoints share security settings.
  final List<Broker> brokers;

  /// Per-request deadline, including admission to an authenticated connection.
  final Duration requestTimeout;

  /// TCP/TLS connection establishment deadline.
  final Duration connectionTimeout;

  /// Maximum request/response frame body bytes, excluding the outer length.
  final int maxFrameBytes;

  /// Null retains plaintext compatibility; non-null always requires valid TLS.
  final KafkaTlsConfig? tls;

  /// Opt in to trying the next distinct configured bootstrap endpoint after a
  /// TLS handshake failure during metadata discovery, including certificate or
  /// hostname rejection. Validation stays strict for every peer; TLS is required.
  /// Local CA/context failures remain terminal. Low-level failures may be
  /// sanitized but never traverse bootstraps. Leader TLS failures stay terminal
  /// unless high-level Fetch separately opts in with [tlsFetchRecovery].
  /// Each metadata traversal is bounded; this is not an overall deadline.
  final bool allowTlsBootstrapFallback;

  /// Optional operation-local TLS/stale-leader recovery for high-level Fetch.
  /// Requires TLS and [allowTlsBootstrapFallback]. Null preserves the ordinary
  /// read-retry policy and terminal leader TLS failures. Enabled Fetch uses this
  /// policy's total-round budget, not [maxReadRetries] or [retryBaseDelay].
  /// Produce and low-level sends do not gain recovery or replay.
  final KafkaTlsFetchRecovery? tlsFetchRecovery;

  /// Optional PLAIN authentication; TLS is mandatory when configured.
  final KafkaSaslPlain? sasl;

  /// Maximum admitted application requests; canceled unsettled dispatches keep a slot.
  final int maxInFlightRequests;

  /// Maximum simultaneous live or connecting broker endpoints.
  final int maxConnections;

  /// Aggregate serialized request-byte budget before socket flush completes.
  final int maxPendingRequestBytes;

  /// Aggregate receive-buffer budget across all connections, not decoded heap.
  final int maxBufferedResponseBytes;

  /// Additional attempts (0–10) only for read-safe transient failures.
  final int maxReadRetries;

  /// Full-jitter exponential retry base; each delay is capped at one second.
  final Duration retryBaseDelay;
  final Map<String, Future<_Connection>> _connections = {};
  final Map<String, _Connection> _liveConnections = {};
  final Map<RawSocket, Completer<RawSecureSocket>> _tlsHandshakes = {};
  final Map<int, _Pending> _pending = {};
  final Map<_Connection, Future<ApiVersionsResponse>> _versions = {};
  final Expando<bool> _sentRequests = Expando<bool>('sent Kafka requests');
  final Random _jitter = Random();
  final CloseWaitGroup _retryWaits = CloseWaitGroup();
  final Set<TopicSubscriptionController> _subscriptions = {};
  final Set<_StableOffsetScope> _stableScopes = {};
  int _stableSequence = 0;
  bool _closed = false;
  Future<void>? _closeFuture;
  final Map<String, int> _connectionLeases = {};
  int _connectionSequence = 0;
  int _bootstrapTlsFallbackCount = 0;
  int _userInFlight = 0,
      _admittedCount = 0,
      _queuedBytes = 0,
      _bufferedBytes = 0;

  /// Listened topic streams whose pumps have not finished, including paused ones.
  /// Unlistened streams do not start work and are not counted.
  int get activeSubscriptionCount => _subscriptions.length;

  /// Metadata advances to another distinct configured endpoint after TLS
  /// handshake failure, not physical connections or successful recoveries.
  /// Concurrent traversals count independently, even with shared connections.
  int get bootstrapTlsFallbackCount => _bootstrapTlsFallbackCount;

  /// Application requests whose caller-facing waits have not completed.
  /// Canceled requests may still retain admission until dispatch settles.
  int get pendingRequestCount => _userInFlight;

  /// Application admission slots held, including canceled unsettled dispatches.
  /// This is the exact count governed by [maxInFlightRequests].
  int get admittedRequestCount => _admittedCount;

  /// Number of live or connecting endpoints.
  int get connectionCount => _connections.length;

  /// Currently reserved serialized wire bytes.
  int get queuedRequestBytes => _queuedBytes;

  /// Currently retained raw response bytes.
  int get bufferedResponseBytes => _bufferedBytes;

  /// Creates a client without connecting until its first operation.
  KafkaClient(
      {required List<Broker> brokers,
      this.requestTimeout = const Duration(seconds: 10),
      this.connectionTimeout = const Duration(seconds: 5),
      this.maxFrameBytes = 16 * 1024 * 1024,
      this.tls,
      this.allowTlsBootstrapFallback = false,
      this.tlsFetchRecovery,
      this.sasl,
      this.maxInFlightRequests = 100,
      this.maxConnections = 8,
      this.maxPendingRequestBytes = 32 * 1024 * 1024,
      this.maxBufferedResponseBytes = 32 * 1024 * 1024,
      this.maxReadRetries = 2,
      this.retryBaseDelay = const Duration(milliseconds: 50)})
      : brokers = List.unmodifiable(brokers) {
    if (brokers.isEmpty ||
        requestTimeout <= Duration.zero ||
        connectionTimeout <= Duration.zero ||
        maxFrameBytes < 4 ||
        maxInFlightRequests < 1 ||
        maxConnections < 1 ||
        maxPendingRequestBytes < 4 ||
        maxBufferedResponseBytes < 4 ||
        maxReadRetries < 0 ||
        maxReadRetries > 10 ||
        retryBaseDelay < Duration.zero ||
        retryBaseDelay > const Duration(seconds: 1)) {
      throw ArgumentError(
          'Invalid Kafka client brokers, timeouts or resource limits');
    }
    if (allowTlsBootstrapFallback && tls == null) {
      throw ArgumentError('TLS bootstrap fallback requires TLS');
    }
    if (tlsFetchRecovery != null &&
        (tls == null || !allowTlsBootstrapFallback)) {
      throw ArgumentError('TLS Fetch recovery requires TLS bootstrap fallback');
    }
    if (sasl != null && tls == null) {
      throw ArgumentError(
          'SASL PLAIN requires TLS; plaintext credentials are prohibited');
    }
  }

  /// Low-level single-use request. No automatic retries or leader discovery.
  /// `acks=0` means local flush only. Canceling cannot retract sent records.
  Future<dynamic> send(KafkaRequest request,
          {Broker? broker, KafkaCancellationToken? cancellationToken}) =>
      _enqueue(request,
          broker: broker ?? brokers.first,
          cancellationToken: cancellationToken);

  Future<dynamic> _enqueue(KafkaRequest request,
      {required Broker broker,
      _Connection? connection,
      bool internal = false,
      KafkaCancellationToken? cancellationToken,
      _StableOffsetScope? stable}) {
    if (_closed) return Future.error(StateError('KafkaClient is closed'));
    if (tls == null &&
        (request is SaslHandshakeRequest ||
            request is SaslAuthenticateRequest)) {
      return Future.error(ArgumentError('SASL requests require TLS'));
    }
    if (cancellationToken?.isCancelled == true) {
      return Future.error(KafkaCancelledException());
    }
    if (_sentRequests[request] == true ||
        _pending.containsKey(request.correlationId)) {
      return Future.error(
          StateError('Kafka requests are single-use; create a new request'));
    }
    if (!internal && _admittedCount >= maxInFlightRequests) {
      return Future.error(KafkaResourceLimitException('in-flight requests'));
    }
    List<int> bytes;
    try {
      bytes = request.toBytes();
      if (bytes.length < 4 ||
          KafkaBytesReader.fromBytes(bytes).readInt32() != bytes.length - 4) {
        throw FormatException('Invalid request frame length');
      }
      if (bytes.length - 4 > maxFrameBytes) {
        throw KafkaResourceLimitException('request frame bytes');
      }
      if (_queuedBytes + bytes.length > maxPendingRequestBytes) {
        throw KafkaResourceLimitException('queued request bytes');
      }
    } catch (e, st) {
      return Future.error(e, st);
    }
    _sentRequests[request] = true;
    stable?.check();
    final p = _Pending(request, broker, bytes, internal)..stable = stable;
    p.connection = connection;
    _pending[request.correlationId] = p;
    _queuedBytes += bytes.length;
    if (!internal) _userInFlight++;
    p.timer = Timer(requestTimeout, () {
      _completeError(p,
          TimeoutException('Kafka request deadline exceeded', requestTimeout));
      final c = p.connection;
      if (c != null) _disconnect(c, KafkaConnectionException());
    });
    p.removeCancellation = cancellationToken?.onCancel(() {
      final connection = p.connection;
      _completeError(p, KafkaCancelledException());
      // Canceling a stalled flush must not leave retained bytes or an unbounded
      // serialization tail. Other requests on this connection fail explicitly.
      if (connection != null) {
        _disconnect(connection, KafkaConnectionException());
      }
    });
    if (!internal) _admittedCount++;
    final dispatch = _dispatch(p).whenComplete(() {
      p.dispatchSettled = true;
      _releaseAdmission(p);
    });
    stable?.dispatches.add(dispatch);
    return p.completer.future;
  }

  Future<void> _dispatch(_Pending p) async {
    try {
      final c = p.connection ?? await _connect(p.broker);
      if (!_isPending(p) || _closed) return;
      p.connection = c;
      if (c.closed) throw KafkaConnectionException();
      // Only one write/flush per connection at a time. Queued bytes are budgeted.
      c.writes++;
      final write = c.flush.then((_) async {
        if (!_isPending(p) || _closed) return;
        if (c.closed) throw KafkaConnectionException();
        final bytes = p.bytes;
        if (bytes == null) return;
        p.writing = true;
        try {
          p.stable?.check();
          c.socket.add(bytes);
          await c.socket.flush();
        } finally {
          p.writing = false;
        }
        _releaseBytes(p);
        if (p.request?.expectsResponse == false && _isPending(p)) {
          _complete(p, null);
        }
      });
      // Keep the serialization tail non-erroring; errors still reach this request.
      c.flush = write.then<void>((_) {}, onError: (Object _, StackTrace __) {});
      try {
        await write;
      } finally {
        c.writes--;
      }
    } catch (e, st) {
      _completeError(p, e, st);
      final c = p.connection;
      if (c != null) _disconnect(c, KafkaConnectionException());
    }
  }

  Future<_Connection> _connect(Broker broker, {_StableOffsetScope? stable}) {
    stable?.check();
    if (_closed) return Future.error(StateError('KafkaClient is closed'));
    final key = stable == null
        ? _brokerKey(broker)
        : '${_brokerKey(broker)}#stable${stable.id}';
    final live = _liveConnections[key];
    if (live != null) live.lastUsed = ++_connectionSequence;
    final current = _connections[key];
    if (current != null) return current;
    if (_connections.length >= maxConnections && !_evictIdleConnection()) {
      return Future.error(KafkaResourceLimitException('connections'));
    }
    late Future<_Connection> attempt;
    attempt = _open(broker, key, stable: stable).catchError((Object e) {
      if (identical(_connections[key], attempt)) _connections.remove(key);
      throw e;
    });
    _connections[key] = attempt;
    return attempt;
  }

  String _brokerKey(Broker broker) => '${broker.host}:${broker.port}';

  // Authentication, negotiated operations and unfinished writes retain their
  // endpoint. Only genuinely idle established sockets may be replaced.
  bool _evictIdleConnection() {
    _Connection? oldest;
    for (final c in _liveConnections.values) {
      if (!c.established ||
          c.closed ||
          c.writes != 0 ||
          (_connectionLeases[c.key] ?? 0) != 0 ||
          _pending.values.any((p) =>
              identical(p.connection, c) || _brokerKey(p.broker) == c.key)) {
        continue;
      }
      if (oldest == null || c.lastUsed < oldest.lastUsed) oldest = c;
    }
    if (oldest == null) return false;
    _disconnect(oldest, KafkaConnectionException());
    return true;
  }

  Future<_Connection> _open(Broker broker, String key,
      {_StableOffsetScope? stable}) async {
    final socket = stable == null
        ? await _openTransport(broker)
        : await _openStableTransport(broker, stable);
    try {
      stable?.check();
    } catch (_) {
      socket.destroy();
      rethrow;
    }
    if (_closed) {
      socket.destroy();
      throw StateError('KafkaClient is closed');
    }
    final c = _Connection(key, socket)..lastUsed = ++_connectionSequence;
    _liveConnections[key] = c;
    stable?.onStop(
        () => _disconnect(c, stable.error ?? KafkaConnectionException()));
    socket.stream.listen((bytes) => _receive(c, bytes),
        onError: (Object _, StackTrace st) =>
            _disconnect(c, KafkaConnectionException(), st),
        onDone: () => _disconnect(c, KafkaConnectionException()),
        cancelOnError: true);
    try {
      if (sasl != null) {
        final handshake = await _enqueue(SaslHandshakeRequest(),
            broker: broker,
            connection: c,
            internal: true,
            cancellationToken: stable?.token,
            stable: stable) as SaslHandshakeResponse;
        stable?.check();
        handshake.ensureSuccess();
        final authenticated = await _enqueue(
            SaslAuthenticateRequest(sasl!.authBytes),
            broker: broker,
            connection: c,
            internal: true,
            cancellationToken: stable?.token,
            stable: stable) as SaslAuthenticateResponse;
        stable?.check();
        authenticated.ensureSuccess();
      }
      if (c.closed || _closed) throw KafkaConnectionException();
      c.established = true;
      return c;
    } catch (e, st) {
      _disconnect(c, e, st);
      rethrow;
    }
  }

  void _forceCloseRaw(RawSocket raw) {
    try {
      raw.close().then<void>((_) {}, onError: (Object _, StackTrace __) {});
    } catch (_) {/* Already closed during concurrent teardown. */}
  }

  Future<KafkaTransport> _openTransport(Broker broker) async {
    if (tls == null) {
      final socket = await Socket.connect(broker.host, broker.port,
          timeout: connectionTimeout);
      socket.setOption(SocketOption.tcpNoDelay, true);
      return SocketKafkaTransport(socket);
    }
    // Own the raw handle throughout the TLS handshake. SecureSocket.connect's
    // timeout does not reliably bound a peer stalled after TCP establishment.
    final clock = Stopwatch()..start();
    final context = tls!.createSecurityContext();
    final raw = await RawSocket.connect(broker.host, broker.port,
        timeout: connectionTimeout);
    raw.setOption(SocketOption.tcpNoDelay, true);
    if (_closed) {
      _forceCloseRaw(raw);
      throw StateError('KafkaClient is closed');
    }
    final canceledHandshake = Completer<RawSecureSocket>();
    _tlsHandshakes[raw] = canceledHandshake;
    try {
      final remaining = connectionTimeout - clock.elapsed;
      if (remaining <= Duration.zero) {
        throw TimeoutException(
            'TLS connection deadline exceeded', connectionTimeout);
      }
      late RawSecureSocket secure;
      try {
        final handshake =
            RawSecureSocket.secure(raw, host: broker.host, context: context);
        secure = await Future.any<RawSecureSocket>([
          handshake,
          canceledHandshake.future,
        ]).timeout(remaining, onTimeout: () {
          _forceCloseRaw(raw);
          throw TimeoutException(
              'TLS connection deadline exceeded', connectionTimeout);
        });
      } on HandshakeException {
        if (!allowTlsBootstrapFallback) rethrow;
        // Mark only this transport handshake stage, never local CA loading,
        // authentication, protocol parsing, deadlines or resource failures.
        // Do not retain the native message, certificate, OS error or cause.
        throw _BootstrapTlsHandshakeException();
      }
      if (_closed) {
        secure.close();
        throw StateError('KafkaClient is closed');
      }
      return RawTlsKafkaTransport(secure,
          closeUnderlying: () => _forceCloseRaw(raw));
    } catch (_) {
      _forceCloseRaw(raw);
      rethrow;
    } finally {
      _tlsHandshakes.remove(raw);
    }
  }

  // Scoped connections use this parent's resource accounting, but never share
  // their deadline or cancellation with unrelated operations.
  Future<KafkaTransport> _openStableTransport(
      Broker broker, _StableOffsetScope scope) async {
    scope.check();
    final clock = Stopwatch()..start();
    if (tls == null) {
      ConnectionTask<Socket>? task;
      Socket? acquired;
      var abandoned = false;
      final starting = Socket.startConnect(broker.host, broker.port);
      starting.then((value) {
        task = value;
        value.socket.then((socket) {
          acquired = socket;
          if (abandoned) socket.destroy();
        }, onError: (Object _, StackTrace __) {});
        if (abandoned) value.cancel();
      }, onError: (Object _, StackTrace __) {});
      final remove = scope.onStop(() {
        abandoned = true;
        task?.cancel();
      });
      try {
        task = await scope.wait(starting, limit: connectionTimeout, abort: () {
          abandoned = true;
          task?.cancel();
        });
        final socket = await scope.wait(task!.socket,
            limit: connectionTimeout - clock.elapsed, abort: () {
          abandoned = true;
          task?.cancel();
        });
        if (abandoned) {
          socket.destroy();
          scope.check();
        }
        socket.setOption(SocketOption.tcpNoDelay, true);
        return SocketKafkaTransport(socket);
      } catch (_) {
        abandoned = true;
        task?.cancel();
        acquired?.destroy();
        rethrow;
      } finally {
        remove();
      }
    }
    final context = tls!.createSecurityContext();
    scope.check();
    final starting = RawSocket.startConnect(broker.host, broker.port);
    ConnectionTask<RawSocket>? task;
    RawSocket? raw;
    RawSecureSocket? acquiredSecure;
    var abandoned = false;
    final remove = scope.onStop(() {
      abandoned = true;
      task?.cancel();
      if (raw != null) _forceCloseRaw(raw!);
    });
    // startConnect can finish DNS/task creation after cancellation. Observe its
    // result and cancel that task rather than allowing a late connection.
    starting.then((value) {
      task = value;
      value.socket.then((socket) {
        raw = socket;
        if (abandoned) _forceCloseRaw(socket);
      }, onError: (Object _, StackTrace __) {});
      if (abandoned) value.cancel();
    }, onError: (Object _, StackTrace __) {});
    try {
      task = await scope.wait(starting, limit: connectionTimeout, abort: () {
        abandoned = true;
        task?.cancel();
      });
      raw = await scope.wait(task!.socket,
          limit: connectionTimeout - clock.elapsed, abort: () {
        abandoned = true;
        task?.cancel();
      });
      scope.check();
      raw!.setOption(SocketOption.tcpNoDelay, true);
      final handshake =
          RawSecureSocket.secure(raw!, host: broker.host, context: context);
      handshake.then((socket) {
        acquiredSecure = socket;
        if (abandoned || scope.error != null) socket.close();
      }, onError: (Object _, StackTrace __) {});
      final secure = await scope.wait(handshake,
          limit: connectionTimeout - clock.elapsed,
          abort: () => _forceCloseRaw(raw!));
      scope.check();
      return RawTlsKafkaTransport(secure,
          closeUnderlying: () => _forceCloseRaw(raw!));
    } catch (_) {
      abandoned = true;
      task?.cancel();
      if (raw != null) _forceCloseRaw(raw!);
      acquiredSecure?.close();
      rethrow;
    } finally {
      remove();
    }
  }

  Future<Map<int, KafkaCommittedOffset?>> _stableCommittedOffsets(
      GroupOffsetFetchRequest query,
      KafkaStableOffsetRetry policy,
      KafkaCancellationToken? cancellation,
      Stopwatch clock) async {
    final scope =
        _StableOffsetScope(++_stableSequence, policy, cancellation, clock);
    _stableScopes.add(scope);
    var succeeded = false;
    try {
      final bootstraps = <String, Broker>{
        for (final broker in brokers) _brokerKey(broker): broker,
      }.values.toList();
      for (var attempt = 0; attempt < policy.maxAttempts; attempt++) {
        scope.check();
        try {
          // Each round admits one discovery, avoiding a nested bootstrap/retry
          // budget. Rotate distinct bootstraps on the next permitted round.
          final coordinator = await _groupCoordinatorOnce(query.groupId,
              stable: scope,
              onlyBootstrap: bootstraps[attempt % bootstraps.length]);
          scope.check();
          final response = await _sendChecked(
              coordinator,
              9,
              7,
              GroupOffsetFetchRequest(
                  query.groupId, query.topic, query.partitions,
                  requireStable: true),
              stable: scope) as GroupOffsetFetchResponse;
          scope.check();
          final errors = <int>[
            if (response.errorCode != 0) response.errorCode,
            for (final topic in response.topics)
              for (final partition in topic.partitions)
                if (partition.errorCode != 0) partition.errorCode,
          ];
          // Terminal partition errors always beat retryable group errors.
          final terminal =
              errors.where((code) => ![14, 15, 16, 88].contains(code));
          if (terminal.isNotEmpty) {
            throw KafkaBrokerException(
                terminal.first, 'fetch committed offsets');
          }
          // A group error may legally omit topic results. When supplied, the
          // shape must still match; malformed partial results cannot be retried.
          if (response.errorCode == 0 || response.topics.isNotEmpty) {
            if (response.topics.length != 1 ||
                response.topics.single.name != query.topic ||
                response.topics.single.partitions.length !=
                    query.partitions.length ||
                response.topics.single.partitions
                        .map((p) => p.partitionIndex)
                        .toSet()
                        .length !=
                    query.partitions.length ||
                response.topics.single.partitions
                    .any((p) => !query.partitions.contains(p.partitionIndex))) {
              throw FormatException(
                  'OffsetFetch response does not match request');
            }
          }
          if (errors.isNotEmpty) {
            throw KafkaBrokerException(errors.first, 'fetch committed offsets');
          }
          final result = <int, KafkaCommittedOffset?>{
            for (final p in response.topics.single.partitions)
              p.partitionIndex: p.committedOffset == -1
                  ? null
                  : KafkaCommittedOffset(
                      nextOffset: p.committedOffset,
                      leaderEpoch: p.committedLeaderEpoch,
                      metadata: p.metadata),
          };
          scope.check();
          succeeded = true;
          return Map.unmodifiable(result);
        } catch (error) {
          scope.check();
          final unstable = error is KafkaBrokerException &&
              error.errorCode == 88 &&
              error.operation == 'fetch committed offsets';
          if ((!unstable && !_groupReadRetryable(error)) ||
              attempt + 1 >= policy.maxAttempts) {
            rethrow;
          }
          await scope.delay(policy.retryDelay);
        }
      }
      throw StateError('Stable offset round budget exhausted');
    } finally {
      try {
        scope.dispose();
        await Future.wait(scope.dispatches);
      } finally {
        _stableScopes.remove(scope);
        scope.finished.complete();
      }
      final expired = clock.elapsed >= policy.timeout;
      clock.stop();
      // Cleanup is part of the operation: a response received before the
      // deadline cannot turn into success after its caller's lifetime ended.
      if (succeeded) {
        if (_closed) throw StateError('KafkaClient is closed');
        if (cancellation?.isCancelled == true) throw KafkaCancelledException();
        if (expired) {
          throw TimeoutException(
              'Stable offset deadline exceeded', policy.timeout);
        }
      }
    }
  }

  void _receive(_Connection c, List<int> bytes) {
    if (c.closed) return;
    if (_bufferedBytes + bytes.length > maxBufferedResponseBytes) {
      _disconnect(c, KafkaResourceLimitException('buffered response bytes'));
      return;
    }
    c.buffer.addAll(bytes);
    _bufferedBytes += bytes.length;
    var offset = 0;
    try {
      while (c.buffer.length - offset >= 4) {
        final size =
            KafkaBytesReader.fromBytes(c.buffer.sublist(offset, offset + 4))
                .readInt32();
        if (size < 4 || size > maxFrameBytes) {
          throw FormatException('Invalid response frame size: $size');
        }
        if (c.buffer.length - offset - 4 < size) break;
        final frame = c.buffer.sublist(offset + 4, offset + 4 + size);
        offset += size + 4;
        final id = KafkaBytesReader.fromBytes(frame).readInt32();
        final p = _pending[id];
        if (p == null) continue;
        if (!identical(p.connection, c)) {
          throw FormatException('Correlation ID belongs to another connection');
        }
        try {
          _complete(p, p.request!.parseResponse(frame));
        } catch (e, st) {
          _completeError(p, e, st);
        }
      }
      if (offset > 0) {
        c.buffer.removeRange(0, offset);
        _bufferedBytes -= offset;
      }
    } catch (e, st) {
      _disconnect(c, e, st);
    }
  }

  bool _isPending(_Pending p) => identical(_pending[p.correlationId], p);
  void _releaseBytes(_Pending p) {
    final bytes = p.bytes;
    if (bytes != null) {
      _queuedBytes -= bytes.length;
      p.bytes = null;
    }
  }

  void _releaseAdmission(_Pending p) {
    if (!p.internal &&
        p.finished &&
        p.dispatchSettled &&
        !p.admissionReleased) {
      p.admissionReleased = true;
      _admittedCount--;
    }
  }

  void _finish(_Pending p) {
    _pending.remove(p.correlationId);
    p.timer?.cancel();
    p.removeCancellation?.call();
    _releaseBytes(p);
    p.request =
        null; // Release caller payloads even while a connect is finishing.
    if (!p.internal) _userInFlight--;
    p.finished = true;
    _releaseAdmission(p);
  }

  void _complete(_Pending p, dynamic value) {
    if (!_isPending(p)) return;
    _finish(p);
    p.completer.complete(value);
  }

  void _completeError(_Pending p, Object error, [StackTrace? st]) {
    if (!_isPending(p)) return;
    _finish(p);
    p.completer.completeError(error, st);
  }

  void _disconnect(_Connection c, Object error, [StackTrace? st]) {
    if (c.closed) return;
    c.closed = true;
    c.socket.destroy();
    _bufferedBytes -= c.buffer.length;
    c.buffer.clear();
    if (identical(_liveConnections[c.key], c)) {
      _liveConnections.remove(c.key);
      _connections.remove(c.key);
    }
    _versions.remove(c);
    for (final p in List<_Pending>.of(_pending.values)) {
      if (identical(p.connection, c)) _completeError(p, error, st);
    }
  }

  bool _retryable(Object e) =>
      e is SocketException ||
      e is TimeoutException ||
      e is KafkaConnectionException ||
      (e is KafkaBrokerException && [5, 6, 9].contains(e.errorCode));
  Future<T> _safeRetry<T>(Future<T> Function() operation) async {
    for (var attempt = 0;; attempt++) {
      if (_closed) throw StateError('KafkaClient is closed');
      try {
        return await operation();
      } catch (e) {
        if (!_retryable(e) || attempt >= maxReadRetries || _closed) rethrow;
        final cap = min(1000, retryBaseDelay.inMilliseconds * (1 << attempt));
        final delay =
            Duration(milliseconds: cap == 0 ? 0 : _jitter.nextInt(cap + 1));
        await _retryWaits.wait(delay);
      }
    }
  }

  /// Validate the version this implementation actually encodes, never choose an
  /// advertised version without its matching codec. Kafka 4 may advertise
  /// Produce 0 for compatibility but rejects it; we explicitly require v3.
  Future<dynamic> _sendChecked(
      Broker broker, int key, int version, KafkaRequest request,
      {_TlsFetchRecoveryState? recovery, _StableOffsetScope? stable}) async {
    stable?.check();
    recovery?.check(_closed);
    // Keep the endpoint leased through admission, and bind the actual request
    // to the socket whose versions were validated. Never silently reconnect.
    final endpoint = stable == null
        ? _brokerKey(broker)
        : '${_brokerKey(broker)}#stable${stable.id}';
    _connectionLeases[endpoint] = (_connectionLeases[endpoint] ?? 0) + 1;
    try {
      final connection = await _requireVersion(broker, key, version,
          recovery: recovery, stable: stable);
      recovery?.check(_closed);
      stable?.check();
      final result = await _enqueue(request,
          broker: broker,
          connection: connection,
          cancellationToken: stable?.token,
          stable: stable);
      stable?.check();
      return result;
    } finally {
      final remaining = _connectionLeases[endpoint]! - 1;
      if (remaining == 0) {
        _connectionLeases.remove(endpoint);
      } else {
        _connectionLeases[endpoint] = remaining;
      }
    }
  }

  Future<_Connection> _requireVersion(Broker broker, int key, int version,
      {_TlsFetchRecoveryState? recovery, _StableOffsetScope? stable}) async {
    stable?.check();
    final c = await _connect(broker, stable: stable);
    stable?.check();
    // Connection establishment includes authentication. Do not admit version
    // negotiation after that phase has consumed this Fetch's polling window.
    recovery?.check(_closed);
    final versionsFuture = _versions.putIfAbsent(c, () async {
      final result = await _enqueue(ApiVersionsRequest(),
          broker: broker,
          connection: c,
          cancellationToken: stable?.token,
          stable: stable) as ApiVersionsResponse;
      if (result.errorCode != 0) {
        throw StateError('ApiVersions error ${result.errorCode}');
      }
      result.validateSupportedVersions();
      return result;
    });
    late ApiVersionsResponse response;
    try {
      response = await versionsFuture;
    } catch (_) {
      if (identical(_versions[c], versionsFuture)) _versions.remove(c);
      rethrow;
    }
    stable?.check();
    if (!response.apiVersions.any((a) =>
        a.apiKey == key &&
        a.minVersion <= version &&
        a.maxVersion >= version)) {
      throw UnsupportedError(
          'Broker does not support implemented API $key version $version');
    }
    return c;
  }

  /// Read-only metadata retries transport failures within configured limits.
  /// Topic and partition error fields are returned unchanged, not converted
  /// into retryable exceptions. Callers must inspect those fields themselves.
  /// Selected topics are copied before I/O and retained across read retries.
  Future<MetadataResponse> metadata([Set<String>? topics]) async {
    if (_closed) throw StateError('KafkaClient is closed');
    final snapshot = topics == null ? null : Set<String>.unmodifiable(topics);
    return _safeRetry(() => _metadataOnce(snapshot));
  }

  bool _groupReadRetryable(Object error) =>
      error is SocketException ||
      error is TimeoutException ||
      error is KafkaConnectionException ||
      (error is KafkaBrokerException && [14, 15, 16].contains(error.errorCode));

  Future<T> _safeGroupReadRetry<T>(Future<T> Function() operation) async {
    for (var attempt = 0;; attempt++) {
      if (_closed) throw StateError('KafkaClient is closed');
      try {
        return await operation();
      } catch (error) {
        if (!_groupReadRetryable(error) ||
            attempt >= maxReadRetries ||
            _closed) {
          rethrow;
        }
        final cap = min(1000, retryBaseDelay.inMilliseconds * (1 << attempt));
        await _retryWaits.wait(
            Duration(milliseconds: cap == 0 ? 0 : _jitter.nextInt(cap + 1)));
      }
    }
  }

  Future<Broker> _groupCoordinatorOnce(String groupId,
      {_StableOffsetScope? stable, Broker? onlyBootstrap}) async {
    Object? last;
    final seen = <String>{};
    for (final bootstrap in onlyBootstrap == null ? brokers : [onlyBootstrap]) {
      stable?.check();
      if (!seen.add(_brokerKey(bootstrap))) continue;
      if (_closed) throw StateError('KafkaClient is closed');
      try {
        final response = await _sendChecked(
            bootstrap, 10, 2, FindCoordinatorRequest(groupId),
            stable: stable) as FindCoordinatorResponse;
        if (response.errorCode != 0) {
          throw KafkaBrokerException(response.errorCode, 'find coordinator');
        }
        if (response.nodeId < 0 ||
            response.host.isEmpty ||
            response.port <= 0 ||
            response.port > 65535) {
          throw FormatException('Invalid group coordinator endpoint');
        }
        return Broker(response.host, response.port);
      } catch (error) {
        // Existing TLS fallback options apply to Metadata/Fetch only. Every
        // group-discovery certificate/handshake rejection or authentication
        // rejection remains terminal. Transport deadlines can still retry.
        if (!_groupReadRetryable(error)) rethrow;
        last = error;
      }
    }
    throw last ?? KafkaConnectionException();
  }

  /// Discovers a consumer group's coordinator without joining the group.
  ///
  /// Uses FindCoordinator v2. Transport and coordinator errors 14/15/16 can
  /// retry within [maxReadRetries]; certificate/handshake and authentication
  /// rejections stay terminal. Transport deadlines can retry,
  /// including when Metadata/Fetch TLS recovery options are enabled. Discovery
  /// grants no ownership and does not create group membership or heartbeats.
  Future<Broker> findGroupCoordinator(String groupId) async {
    if (_closed) throw StateError('KafkaClient is closed');
    final query = FindCoordinatorRequest(groupId); // Validate before I/O.
    return _safeGroupReadRetry(() => _groupCoordinatorOnce(query.groupId));
  }

  /// Reads broker-stored next offsets for one explicitly selected topic.
  ///
  /// A null map value means a successful response with no stored offset. Missing
  /// or unexpected partitions and broker errors never become null. Results and
  /// input snapshots are immutable. An unknown group can have no stored offsets.
  /// This method never selects earliest/latest offsets or resets a position.
  ///
  /// Defaults to OffsetFetch v5. Explicit [requireStable] selects exact v7
  /// with RequireStable=true and requires coordinator support; it never falls
  /// back to v5. Broker stability errors remain errors, not missing offsets.
  /// Fresh coordinator discovery is used on each existing bounded read retry.
  /// Stored epochs are diagnostic; log-truncation recovery is not implemented.
  /// With null [stableOffsetRetry], existing per-phase deadlines apply.
  /// Opting in requires [requireStable], replaces ordinary read retries, and
  /// applies one absolute deadline including connection/authentication/discovery.
  /// Error 88 retries only this read. Scoped connections use the same aggregate
  /// budgets, so concurrent ordinary traffic can require an extra connection.
  /// [cancellationToken] requires [stableOffsetRetry]; cancellation cannot undo
  /// bytes already sent. No commits, group joins or offset resets are performed.
  Future<Map<int, KafkaCommittedOffset?>> committedOffsets(
      String groupId, String topic,
      {required Set<int> partitions,
      bool requireStable = false,
      KafkaStableOffsetRetry? stableOffsetRetry,
      KafkaCancellationToken? cancellationToken}) async {
    final operationClock =
        stableOffsetRetry == null ? null : (Stopwatch()..start());
    if (_closed) throw StateError('KafkaClient is closed');
    if (stableOffsetRetry != null && !requireStable) {
      throw ArgumentError('Stable offset retry requires requireStable=true');
    }
    if (cancellationToken != null && stableOffsetRetry == null) {
      throw ArgumentError(
          'Committed offset cancellation requires stableOffsetRetry');
    }
    final query = GroupOffsetFetchRequest(groupId, topic, partitions,
        requireStable: requireStable);
    if (stableOffsetRetry != null) {
      return _stableCommittedOffsets(
          query, stableOffsetRetry, cancellationToken, operationClock!);
    }
    return _safeGroupReadRetry(() async {
      final coordinator = await _groupCoordinatorOnce(query.groupId);
      final response = await _sendChecked(
          coordinator,
          9,
          query.apiVersion,
          GroupOffsetFetchRequest(query.groupId, query.topic, query.partitions,
              requireStable: query.requireStable)) as GroupOffsetFetchResponse;
      if (response.errorCode != 0) {
        throw KafkaBrokerException(
            response.errorCode, 'fetch committed offsets');
      }
      if (response.topics.length != 1 ||
          response.topics.single.name != query.topic ||
          response.topics.single.partitions.length != query.partitions.length) {
        throw FormatException('OffsetFetch response does not match request');
      }
      final result = <int, KafkaCommittedOffset?>{};
      final errors = <int>[];
      for (final partition in response.topics.single.partitions) {
        final index = partition.partitionIndex;
        if (!query.partitions.contains(index) || result.containsKey(index)) {
          throw FormatException(
              'OffsetFetch response has unexpected partition');
        }
        if (partition.errorCode != 0) errors.add(partition.errorCode);
        result[index] = partition.committedOffset == -1
            ? null
            : KafkaCommittedOffset(
                nextOffset: partition.committedOffset,
                leaderEpoch: partition.committedLeaderEpoch,
                metadata: partition.metadata);
      }
      if (errors.isNotEmpty) {
        // Never hide a terminal partition rejection behind a retryable one.
        final code = errors.firstWhere((code) => ![14, 15, 16].contains(code),
            orElse: () => errors.first);
        throw KafkaBrokerException(code, 'fetch committed offsets');
      }
      return Map<int, KafkaCommittedOffset?>.unmodifiable(result);
    });
  }

  /// Explicitly stores processed next offsets for one manually assigned topic.
  ///
  /// Use a dedicated group ID and external single-writer coordination per
  /// partition. This method does not join a group, fence writers, assign
  /// partitions, heartbeat, or auto-commit. Concurrent writers are last-write-
  /// wins and can rewind each other's progress. Await each intended commit
  /// before issuing a later one; no monotonicity or cross-client order is added.
  /// Commit only after application processing succeeds, never from delivery
  /// alone. Application effects and offsets are not committed atomically.
  ///
  /// Uses OffsetCommit v6 with generation -1, empty member ID and unknown leader
  /// epoch. Only pre-send coordinator discovery may retry. The commit itself
  /// is dispatched once, including when its broker response reports coordinator
  /// errors. [KafkaOffsetCommitException] preserves partial acknowledgements.
  /// Timeout, disconnect, malformed response or closure can leave the commit
  /// outcome unknown; they do not authorize replay or imply rollback. Readback
  /// cannot prove the original request's outcome with concurrent writers.
  ///
  /// Inputs are copied and validated before I/O. Wire-valid [metadata] can still
  /// exceed the broker's configured metadata limit. Existing per-phase deadlines
  /// apply; Future.timeout does not cancel a commit already in progress.
  Future<void> commitOffsets(String groupId, String topic,
      {required Map<int, int> nextOffsets, String? metadata}) async {
    if (_closed) throw StateError('KafkaClient is closed');
    final request = GroupOffsetCommitRequest(groupId, topic, nextOffsets,
        metadata: metadata);
    final coordinator =
        await _safeGroupReadRetry(() => _groupCoordinatorOnce(request.groupId));
    final response = await _sendChecked(coordinator, 8, 6, request)
        as GroupOffsetCommitResponse;
    if (response.topics.length != 1 ||
        response.topics.single.name != request.topic ||
        response.topics.single.partitions.length !=
            request.nextOffsets.length) {
      throw FormatException('OffsetCommit response does not match request');
    }
    final errors = <int, int>{};
    for (final partition in response.topics.single.partitions) {
      if (!request.nextOffsets.containsKey(partition.partitionIndex) ||
          errors.containsKey(partition.partitionIndex)) {
        throw FormatException('OffsetCommit response has unexpected partition');
      }
      errors[partition.partitionIndex] = partition.errorCode;
    }
    if (errors.values.any((code) => code != 0)) {
      throw KafkaOffsetCommitException(errors);
    }
  }

  Future<MetadataResponse> _metadataOnce(Set<String>? topics,
      {_TlsFetchRecoveryState? recovery}) async {
    Object? last;
    // Carried exclusions must not become a retryable transport-only tail on a
    // later round, even when this traversal encounters no new TLS rejection.
    var hadTlsFailure = recovery?.excluded.isNotEmpty ?? false;
    var advanceAfterTls = false;
    final seen = <String>{};
    for (final broker in brokers) {
      final endpoint = _brokerKey(broker);
      if (recovery?.excluded.contains(endpoint) == true) continue;
      if (allowTlsBootstrapFallback && !seen.add(endpoint)) continue;
      recovery?.check(_closed);
      if (allowTlsBootstrapFallback && _closed) {
        throw StateError('KafkaClient is closed');
      }
      if (advanceAfterTls) {
        _bootstrapTlsFallbackCount++;
        advanceAfterTls = false;
      }
      try {
        return await _sendChecked(broker, 3, 0, MetadataRequest(topics),
            recovery: recovery) as MetadataResponse;
      } catch (e) {
        if (allowTlsBootstrapFallback && _closed) {
          throw StateError('KafkaClient is closed');
        }
        if (allowTlsBootstrapFallback && e is _BootstrapTlsHandshakeException) {
          recovery?.excluded.add(endpoint);
          hadTlsFailure = true;
          advanceAfterTls = true;
        } else if (!_retryable(e)) {
          rethrow;
        }
        last = e;
      }
    }
    // A trailing transport failure must not make outer read retries revisit
    // TLS-failing peers. Terminal non-transport errors already escaped above.
    if (hadTlsFailure) {
      throw HandshakeException('Kafka bootstrap TLS discovery failed');
    }
    throw last ?? KafkaConnectionException();
  }

  Future<Broker> _leader(String topic, int partition,
      {_TlsFetchRecoveryState? recovery}) async {
    final m = await _metadataOnce({topic}, recovery: recovery);
    // Routing identities must be unique. Never pick whichever duplicate was
    // listed first, even if only one duplicate advertises a usable endpoint.
    // Raw metadata() keeps the original response available for diagnostics.
    final topics = m.topics.where((t) => t.name == topic).toList();
    if (topics.length > 1) {
      throw FormatException('Ambiguous topic metadata');
    }
    for (final t in topics) {
      if (t.errorCode != 0) {
        throw KafkaBrokerException(t.errorCode, 'metadata');
      }
      final partitions =
          t.partitions.where((p) => p.partitionIndex == partition).toList();
      if (partitions.length > 1) {
        throw FormatException('Ambiguous partition metadata');
      }
      for (final p in partitions) {
        // Kafka 4.0.2 Metadata v0 can report REPLICA_NOT_AVAILABLE when a
        // follower endpoint is absent while still supplying a live leader
        // (KafkaApis / KRaftMetadataCache). Preserve the raw metadata error;
        // only routing may use this narrowly qualified leader.
        if ((p.errorCode != 0 && p.errorCode != 9) || p.leaderId < 0) {
          throw KafkaBrokerException(
              p.errorCode == 0 ? 5 : p.errorCode, 'metadata');
        }
        final leaders = m.brokers.where((b) => b.nodeId == p.leaderId).toList();
        if (leaders.length > 1) {
          throw FormatException('Ambiguous leader metadata');
        }
        for (final b in leaders) {
          if (b.host.isNotEmpty && b.port > 0 && b.port <= 65535) {
            final broker = Broker(b.host, b.port);
            if (recovery?.excluded.contains(_brokerKey(broker)) == true) {
              throw _TlsFetchLeaderUnavailable();
            }
            return broker;
          }
        }
        if (p.errorCode == 9) throw KafkaBrokerException(9, 'metadata');
      }
    }
    throw StateError('No leader for $topic partition $partition');
  }

  /// Produce to the discovered partition leader. No automatic retries: an
  /// uncertain acknowledgement can mean the broker stored the records already.
  ///
  /// [acks] defaults to 1 (leader acknowledgement); -1 requests in-sync replica
  /// acknowledgements and 0 returns null after local socket flush, without proof
  /// of broker receipt or persistence. [timeoutMs] is the broker acknowledgement
  /// timeout, not a client or overall-operation deadline. Only the pre-send
  /// leader lookup may be retried.
  ///
  /// Invalid scalar bounds or empty records fail before leader discovery.
  /// Partition and timeout use nonnegative signed INT32 bounds.
  /// The record list is copied before discovery; later caller mutations do not
  /// change the batch. Each KafkaRecord already owns immutable payload copies.
  /// This high-level method has no cancellation token or overall deadline.
  /// Applying Future.timeout does not cancel its underlying work.
  Future<ProduceResponse?> produce(String topic, List<KafkaRecord> records,
      {int partition = 0, int acks = 1, int timeoutMs = 5000}) async {
    if (records.isEmpty || partition < 0) {
      throw ArgumentError('Nonempty records and partition >= 0 required');
    }
    if (_closed) throw StateError('KafkaClient is closed');
    if (partition > 0x7fffffff ||
        ![-1, 0, 1].contains(acks) ||
        timeoutMs < 0 ||
        timeoutMs > 0x7fffffff) {
      throw ArgumentError(
          'Invalid produce records, partition, acknowledgments, or timeout');
    }
    final recordSnapshot = List<KafkaRecord>.unmodifiable(records);
    final broker = await _safeRetry(() => _leader(topic, partition));
    final result = await _sendChecked(
        broker,
        0,
        3,
        ProduceRequest(acks: acks, timeoutMs: timeoutMs, topicData: [
          TopicData(name: topic, partitions: [
            PartitionData(index: partition, records: recordSnapshot)
          ])
        ])) as ProduceResponse?;
    if (result != null) {
      if (result.responses.length != 1 ||
          result.responses.single.name != topic ||
          result.responses.single.partitionResponses.length != 1 ||
          result.responses.single.partitionResponses.single.index !=
              partition) {
        throw FormatException(
            'Produce response does not match requested partition');
      }
      for (final t in result.responses) {
        for (final p in t.partitionResponses) {
          if (p.errorCode != 0) {
            throw KafkaBrokerException(p.errorCode, 'produce');
          }
          // This is a broker acknowledgement for a nonempty batch. The -1
          // unavailable-offset sentinel is valid on errors, not this success.
          if (p.baseOffset < 0) {
            throw FormatException('Invalid successful Produce base offset');
          }
        }
      }
      if (result.throttleTimeMs < 0) {
        throw FormatException('Invalid successful Produce throttle duration');
      }
    }
    return result;
  }

  /// Fetch manually assigned partition at an explicit offset; no consumer groups.
  ///
  /// An open client rejects invalid scalar bounds with an ArgumentError Future
  /// before discovery; a closed client continues to report StateError first.
  /// Partition/wait are nonnegative INT32, bytes positive INT32, offset nonnegative.
  ///
  /// [offset] is inclusive and defaults to literal zero. Calls are stateless:
  /// no cursor, partition assignment, earliest/latest lookup or offset commits
  /// are maintained. Track each partition's position from processed absolute
  /// record offsets; an empty response does not itself advance that position.
  /// Retention and out-of-range recovery are application-managed.
  ///
  /// [maxWaitMs] is a broker wait, not the client's request deadline. [maxBytes]
  /// is a requested soft limit: Kafka may return an oversized first batch.
  /// Wire-buffer limits remain separately enforced. This high-level method has
  /// no cancellation token or overall deadline; Future.timeout does not cancel
  /// its underlying work. [tlsFetchRecovery], when configured, instead bounds
  /// total rounds and admission of further phases with a monotonic window.
  /// An already admitted phase may finish after that window under the existing
  /// connection/request deadlines. Close interrupts waits and stops recovery.
  Future<FetchResponse> fetch(String topic,
      {int partition = 0,
      int offset = 0,
      int maxWaitMs = 100,
      int maxBytes = 1048576}) {
    if (_closed) return Future.error(StateError('KafkaClient is closed'));
    if (partition < 0 ||
        partition > 0x7fffffff ||
        offset < 0 ||
        maxWaitMs < 0 ||
        maxWaitMs > 0x7fffffff ||
        maxBytes <= 0 ||
        maxBytes > 0x7fffffff) {
      // Preserve asynchronous error delivery without starting discovery or TLS.
      return Future.error(ArgumentError(
          'Invalid fetch partition, offset, wait, or byte limit'));
    }
    final policy = tlsFetchRecovery;
    if (policy != null) {
      return _fetchWithTlsRecovery(topic,
          partition: partition,
          offset: offset,
          maxWaitMs: maxWaitMs,
          maxBytes: maxBytes,
          policy: policy);
    }
    return _safeRetry(() => _fetchOnce(topic,
        partition: partition,
        offset: offset,
        maxWaitMs: maxWaitMs,
        maxBytes: maxBytes));
  }

  Future<FetchResponse> _fetchWithTlsRecovery(String topic,
      {required int partition,
      required int offset,
      required int maxWaitMs,
      required int maxBytes,
      required KafkaTlsFetchRecovery policy}) async {
    final recovery = _TlsFetchRecoveryState(policy);
    try {
      for (var attempt = 0; attempt < policy.maxAttempts; attempt++) {
        recovery.check(_closed);
        try {
          return await _fetchOnce(topic,
              partition: partition,
              offset: offset,
              maxWaitMs: maxWaitMs,
              maxBytes: maxBytes,
              recovery: recovery);
        } catch (e) {
          if (_closed) throw StateError('KafkaClient is closed');
          // Metadata exhaustion, local TLS configuration, authentication,
          // protocol and resource failures remain immediately terminal.
          if (e is! _TlsFetchLeaderUnavailable && !_retryable(e)) rethrow;
          if (brokers.every(
              (broker) => recovery.excluded.contains(_brokerKey(broker)))) {
            throw HandshakeException('Kafka bootstrap TLS discovery failed');
          }
          recovery.check(_closed);
          if (attempt + 1 >= policy.maxAttempts) {
            if (recovery.excluded.isNotEmpty) {
              throw HandshakeException('Kafka Fetch TLS recovery exhausted');
            }
            rethrow;
          }
          await _delayFetchRecovery(recovery);
        }
      }
      throw StateError('Invalid TLS Fetch recovery attempt budget');
    } on _TlsFetchRecoveryWindowExpired {
      if (_closed) throw StateError('KafkaClient is closed');
      if (recovery.excluded.isNotEmpty) {
        throw HandshakeException('Kafka Fetch TLS recovery window expired');
      }
      throw TimeoutException(
          'Kafka Fetch TLS recovery window expired', policy.window);
    }
  }

  Future<void> _delayFetchRecovery(_TlsFetchRecoveryState recovery) async {
    recovery.check(_closed);
    final retryAt = recovery.clock.elapsed + recovery.policy.retryDelay;
    final deadline =
        retryAt < recovery.policy.window ? retryAt : recovery.policy.window;
    await waitForMonotonicRetryDeadline(
      deadline: deadline,
      elapsed: () => recovery.clock.elapsed,
      check: () => recovery.check(_closed),
      wait: _retryWaits.wait,
    );
  }

  Future<FetchResponse> _fetchOnce(String topic,
      {required int partition,
      required int offset,
      required int maxWaitMs,
      required int maxBytes,
      _TlsFetchRecoveryState? recovery}) async {
    final broker = await _leader(topic, partition, recovery: recovery);
    recovery?.check(_closed);
    late FetchResponse result;
    try {
      result = await _sendChecked(
          broker,
          1,
          4,
          FetchRequest(
              -1,
              maxWaitMs,
              1,
              [
                FetchTopic(topic, [FetchPartition(partition, offset, maxBytes)])
              ],
              maxBytes: maxBytes),
          recovery: recovery) as FetchResponse;
    } on _BootstrapTlsHandshakeException {
      if (recovery == null) rethrow;
      recovery.excluded.add(_brokerKey(broker));
      throw _TlsFetchLeaderUnavailable();
    }
    if (result.topics.length != 1 ||
        result.topics.single.name != topic ||
        result.topics.single.partitions.length != 1 ||
        result.topics.single.partitions.single.partition != partition) {
      throw FormatException(
          'Fetch response does not match requested partition');
    }
    for (final t in result.topics) {
      for (final p in t.partitions) {
        if (p.errorCode != 0) {
          throw KafkaBrokerException(p.errorCode, 'fetch');
        }
        // Brokers can return an entire batch spanning the requested offset.
        p.records.removeWhere((record) => record.offset < offset);
      }
    }
    return result;
  }

  /// Continually reads one topic's explicitly assigned partitions and offsets.
  ///
  /// This is an uncoordinated, single-listener stream, not a consumer group.
  /// [startOffsets] is copied and fixes the selected partitions for this stream;
  /// new topic partitions, earliest/latest lookup and commits are not automatic.
  /// Each event identifies its topic, partition and absolute record offset.
  /// Ordering is per partition; there is no cross-partition ordering guarantee.
  ///
  /// No work starts until listening. Pause stops further requests and events;
  /// an already admitted Fetch may finish and retain one decoded response.
  /// Cancellation stops only this stream and awaits admitted work settling under
  /// existing per-phase deadlines. It does not close this shared client or
  /// promise immediate network cancellation. Client close stops all streams.
  ///
  /// Read-safe transient failures are retried with [retryDelay] while active.
  /// TLS/authentication/protocol/resource and out-of-range failures remain
  /// terminal after the existing Fetch policy. Empty replies never cause a jump
  /// to the high watermark; only validated batch/record offsets advance progress.
  /// Each stream admits one Fetch at a time. Wire limits are not decoded-heap caps.
  ///
  /// Delivery is not processing acknowledgment or a durable checkpoint. Persist
  /// offsets only after successful processing and explicitly supply them on a
  /// restart. Replays may occur across restarts; no exactly-once promise is made.
  /// Use pause/resume or asyncMap to await asynchronous processing; an async
  /// listen callback alone does not apply backpressure to a Dart stream.
  ///
  /// Configuration is checked synchronously before any I/O. Poll/retry delays
  /// must be between one millisecond and one minute. Other scalar bounds match
  /// [fetch]. Valid calls on a closed client produce a StateError stream.
  Stream<KafkaTopicRecord> subscribe(String topic,
      {required Map<int, int> startOffsets,
      Duration pollInterval = const Duration(milliseconds: 100),
      Duration retryDelay = const Duration(milliseconds: 500),
      int maxWaitMs = 100,
      int maxBytes = 1048576}) {
    if (topic.isEmpty ||
        startOffsets.isEmpty ||
        startOffsets.entries.any((entry) =>
            entry.key < 0 || entry.key > 0x7fffffff || entry.value < 0) ||
        pollInterval < const Duration(milliseconds: 1) ||
        pollInterval > const Duration(minutes: 1) ||
        retryDelay < const Duration(milliseconds: 1) ||
        retryDelay > const Duration(minutes: 1) ||
        maxWaitMs < 0 ||
        maxWaitMs > 0x7fffffff ||
        maxBytes <= 0 ||
        maxBytes > 0x7fffffff) {
      throw ArgumentError('Invalid topic subscription configuration');
    }
    if (_closed) {
      return Stream.error(StateError('KafkaClient is closed'));
    }
    late TopicSubscriptionController subscription;
    subscription = TopicSubscriptionController(
        topic: topic,
        startOffsets: Map<int, int>.unmodifiable(startOffsets),
        fetch: (partition, offset) => fetch(topic,
            partition: partition,
            offset: offset,
            maxWaitMs: maxWaitMs,
            maxBytes: maxBytes),
        isRetryable: _retryable,
        isClientClosed: () => _closed,
        onStart: () => _subscriptions.add(subscription),
        onFinish: () => _subscriptions.remove(subscription),
        pollInterval: pollInterval,
        retryDelay: retryDelay);
    return subscription.stream;
  }

  /// Cancel local waits, close live sockets, and await bounded pending connects.
  /// This permanently closes the client; create a new client to resume. Await
  /// intended operations first: close cancels pending work rather than draining
  /// delivery acknowledgements, and pending Produce delivery may be ambiguous.
  Future<void> close() => _closeFuture ??= _close();

  Future<void> _close() async {
    _closed = true;
    final stoppingStableReads = _stableScopes.toList();
    for (final scope in stoppingStableReads) {
      scope.stop(StateError('KafkaClient is closed'));
    }
    final stoppingSubscriptions =
        _subscriptions.toList().map((stream) => stream.stop()).toList();
    _retryWaits.close();
    for (final entry in _tlsHandshakes.entries.toList()) {
      _forceCloseRaw(entry.key);
      if (!entry.value.isCompleted) {
        entry.value.completeError(StateError('KafkaClient is closed'));
      }
    }
    _tlsHandshakes.clear();
    for (final p in List<_Pending>.of(_pending.values)) {
      _completeError(p, StateError('KafkaClient closed'));
    }
    for (final c in List<_Connection>.of(_liveConnections.values)) {
      _disconnect(c, StateError('KafkaClient closed'));
    }
    final connections = List<Future<_Connection>>.of(_connections.values);
    _connections.clear();
    await Future.wait<void>([
      ...stoppingSubscriptions,
      ...stoppingStableReads.map((scope) => scope.finished.future),
      ...connections.map((f) async {
        try {
          final c = await f;
          _disconnect(c, StateError('KafkaClient closed'));
        } catch (_) {}
      }),
    ]);
    _subscriptions.clear();
    _versions.clear();
  }
}

// Private stage marker with fixed text and no original error/cause retention.
class _BootstrapTlsHandshakeException extends HandshakeException {
  _BootstrapTlsHandshakeException() : super('Kafka TLS handshake failed');
}

// Private control-flow markers never escape the enabled high-level Fetch.
class _TlsFetchLeaderUnavailable implements Exception {}

class _TlsFetchRecoveryWindowExpired implements Exception {}

class _TlsFetchRecoveryState {
  final KafkaTlsFetchRecovery policy;
  final Stopwatch clock = Stopwatch()..start();
  final Set<String> excluded = {};
  _TlsFetchRecoveryState(this.policy);

  void check(bool closed) {
    if (closed) throw StateError('KafkaClient is closed');
    if (clock.elapsed >= policy.window) {
      throw _TlsFetchRecoveryWindowExpired();
    }
  }
}

// One clock and one cancellation lifetime for every phase of a stable read.
class _StableOffsetScope {
  final int id;
  final KafkaStableOffsetRetry policy;
  final Stopwatch clock;
  final KafkaCancellationToken token = KafkaCancellationToken();
  final Completer<void> _stopped = Completer<void>();
  final Set<void Function()> _cleanup = {};
  final List<Future<void>> dispatches = [];
  final Completer<void> finished = Completer<void>();
  Timer? _timer;
  void Function()? _removeCancellation;
  Object? error;

  _StableOffsetScope(
      this.id, this.policy, KafkaCancellationToken? external, this.clock) {
    _armDeadline();
    _removeCancellation =
        external?.onCancel(() => stop(KafkaCancelledException()));
  }

  void _armDeadline() {
    final remaining = policy.timeout - clock.elapsed;
    if (remaining <= Duration.zero) {
      stop(_timeout());
      return;
    }
    _timer = Timer(
        Duration(microseconds: remaining.inMicroseconds + 999), _armDeadline);
  }

  TimeoutException _timeout() =>
      TimeoutException('Stable offset deadline exceeded', policy.timeout);

  void check() {
    if (error != null) throw error!;
    if (clock.elapsed >= policy.timeout) {
      stop(_timeout());
      throw error!;
    }
  }

  void Function() onStop(void Function() cleanup) {
    if (_stopped.isCompleted) {
      cleanup();
      return () {};
    }
    _cleanup.add(cleanup);
    return () => _cleanup.remove(cleanup);
  }

  void stop(Object reason) {
    if (_stopped.isCompleted) return;
    error = reason;
    _timer?.cancel();
    _stopped.complete();
    // Teardown is best-effort for every owned resource. A failed callback
    // must not replace the original timeout/cancellation or strand siblings.
    try {
      token.cancel();
    } catch (_) {}
    for (final cleanup in _cleanup.toList()) {
      try {
        cleanup();
      } catch (_) {}
    }
    _cleanup.clear();
  }

  Future<T> wait<T>(Future<T> future,
      {Duration? limit, void Function()? abort}) async {
    Timer? timer;
    final phaseExpired = Completer<T>();
    final phaseClock = Stopwatch()..start();
    try {
      check();
      if (limit != null) {
        if (limit <= Duration.zero) {
          throw TimeoutException('Kafka connection deadline exceeded');
        }
        void armPhase() {
          final remaining = limit - phaseClock.elapsed;
          if (remaining <= Duration.zero) {
            phaseExpired.completeError(
                TimeoutException('Kafka connection deadline exceeded'));
          } else {
            timer = Timer(
                Duration(microseconds: remaining.inMicroseconds + 999),
                armPhase);
          }
        }

        armPhase();
      }
      final value = await Future.any<T>([
        future,
        _stopped.future.then<T>((_) => throw error!),
        if (limit != null) phaseExpired.future,
      ]);
      check();
      if (limit != null && phaseClock.elapsed >= limit) {
        throw TimeoutException('Kafka connection deadline exceeded');
      }
      return value;
    } catch (_) {
      abort?.call();
      rethrow;
    } finally {
      timer?.cancel();
    }
  }

  Future<void> delay(Duration duration) async {
    final until = clock.elapsed + duration;
    await waitForMonotonicRetryDeadline(
        deadline: until,
        elapsed: () => clock.elapsed,
        check: check,
        wait: (remaining) async {
          final done = Completer<void>();
          final timer = Timer(remaining, done.complete);
          try {
            await wait(done.future);
          } finally {
            timer.cancel();
          }
        });
  }

  void dispose() {
    stop(KafkaCancelledException());
    _removeCancellation?.call();
  }
}

class _Connection {
  final String key;
  final KafkaTransport socket;
  final List<int> buffer = [];
  Future<void> flush = Future.value();
  bool closed = false;
  bool established = false;
  int writes = 0;
  int lastUsed = 0;
  _Connection(this.key, this.socket);
}

class _Pending {
  KafkaRequest? request;
  final int correlationId;
  final Broker broker;
  List<int>? bytes;
  final bool internal;
  final Completer<dynamic> completer = Completer<dynamic>();
  _Connection? connection;
  _StableOffsetScope? stable;
  bool writing = false;
  bool finished = false;
  bool dispatchSettled = false;
  bool admissionReleased = false;
  Timer? timer;
  void Function()? removeCancellation;
  _Pending(KafkaRequest this.request, this.broker, this.bytes, this.internal)
      : correlationId = request.correlationId;
}
