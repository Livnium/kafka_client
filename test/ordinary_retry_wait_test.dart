import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

// Own every socket, and implement only the protocol versions used below.
class _RetryPeer {
  final ServerSocket server;
  final sockets = <Socket>[];
  int metadataCalls = 0;
  int produceCalls = 0;
  int metadataFailures = 100;
  int metadataError = 6;
  bool disconnectMetadata = false;
  bool disconnectProduce = false;
  bool oversizedMetadata = false;
  int produceError = 0;

  _RetryPeer._(this.server);

  static Future<_RetryPeer> start() async {
    final peer =
        _RetryPeer._(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));
    peer.server.listen(peer._accept);
    return peer;
  }

  void _accept(Socket socket) {
    sockets.add(socket);
    final buffer = <int>[];
    socket.listen((data) {
      buffer.addAll(data);
      while (buffer.length >= 4) {
        final length = KafkaBytesReader.fromBytes(buffer).readInt32();
        if (buffer.length < length + 4) return;
        final reader =
            KafkaBytesReader.fromBytes(buffer.sublist(4, length + 4));
        buffer.removeRange(0, length + 4);
        final key = reader.readInt16();
        reader.readInt16();
        final response = KafkaBytesBuilder()..addInt32(reader.readInt32());
        if (key == 18) {
          response
            ..addInt16(0)
            ..addInt32(2);
          for (final api in [
            [3, 0],
            [0, 3]
          ]) {
            response
              ..addInt16(api[0])
              ..addInt16(api[1])
              ..addInt16(api[1]);
          }
        } else if (key == 3) {
          metadataCalls++;
          if (disconnectMetadata) {
            socket.destroy();
            return;
          }
          if (oversizedMetadata) {
            socket.add(KafkaRequest.frame(List.filled(300, 0)));
            return;
          }
          final error = metadataCalls <= metadataFailures ? metadataError : 0;
          response
            ..addInt32(1)
            ..addInt32(1)
            ..addString('127.0.0.1')
            ..addInt32(server.port)
            ..addInt32(1)
            ..addInt16(error)
            ..addString('topic')
            ..addInt32(1)
            ..addInt16(0)
            ..addInt32(0)
            ..addInt32(1)
            ..addInt32(1)
            ..addInt32(1)
            ..addInt32(1)
            ..addInt32(1);
        } else if (key == 0) {
          produceCalls++;
          if (disconnectProduce) {
            socket.destroy();
            return;
          }
          response
            ..addInt32(1)
            ..addString('topic')
            ..addInt32(1)
            ..addInt32(0)
            ..addInt16(produceError)
            ..addInt64(0)
            ..addInt64(-1)
            ..addInt32(0);
        } else {
          throw StateError('Unexpected API key $key');
        }
        socket.add(KafkaRequest.frame(response.takeBytes()));
      }
    }, onError: (Object _) {});
  }

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
  }
}

// A deliberately controllable timer lets tests force close and timer callbacks
// into the same event turn without depending on scheduler or jitter luck.
class _HeldTimer implements Timer {
  bool _active = true;
  @override
  bool get isActive => _active;
  @override
  int get tick => 0;
  @override
  void cancel() => _active = false;
}

void main() {
  late _RetryPeer peer;
  late KafkaClient client;

  setUp(() async {
    peer = await _RetryPeer.start();
  });
  tearDown(() async {
    await client.close();
    await peer.close();
  });

  KafkaClient make(
      {int retries = 2, int baseMs = 125, int buffered = 33554432}) {
    return KafkaClient(
      brokers: [Broker('127.0.0.1', peer.server.port)],
      maxReadRetries: retries,
      retryBaseDelay: Duration(milliseconds: baseMs),
      // Keep request/connection deadlines outside the backoff timer range.
      requestTimeout: const Duration(seconds: 10),
      connectionTimeout: const Duration(seconds: 10),
      maxBufferedResponseBytes: buffered,
    );
  }

  Future<ProduceResponse?> produce() => client.produce('topic', [
        KafkaRecord(value: [1])
      ]);

  // Socket.connect creates its own short Happy Eyeballs timer. Establish the
  // connection and negotiate versions OUTSIDE any timer-interception zone.
  // Intercepted operations below reuse this socket; they never reconnect.
  Future<void> warmConnection() async {
    await client.metadata({'topic'});
    expect(peer.sockets, hasLength(1));
    peer.metadataCalls = 0;
  }

  Future<T> captureDelays<T>(
      Future<T> Function() action, List<Duration> delays) {
    return runZoned(action, zoneSpecification: ZoneSpecification(
        createTimer: (self, parent, zone, duration, callback) {
      if (duration <= const Duration(seconds: 1)) {
        delays.add(duration);
        return parent.createTimer(zone, Duration.zero, callback);
      }
      return parent.createTimer(zone, duration, callback);
    }));
  }

  for (final retries in [0, 1, 5, 10]) {
    test('ordinary retry budget $retries keeps exponential full-jitter bounds',
        () async {
      client = make(retries: retries);
      await warmConnection();
      final delays = <Duration>[];
      await expectLater(
          captureDelays(produce, delays), throwsA(isA<KafkaBrokerException>()));
      expect(peer.metadataCalls, retries + 1);
      expect(peer.produceCalls, 0);
      expect(delays, hasLength(retries));
      for (var attempt = 0; attempt < delays.length; attempt++) {
        final cap = min(1000, 125 * (1 << attempt));
        expect(delays[attempt].inMicroseconds, inInclusiveRange(0, cap * 1000),
            reason: 'attempt $attempt');
      }
    });
  }

  test('zero base delay stays zero through the complete retry budget',
      () async {
    client = make(retries: 10, baseMs: 0);
    await warmConnection();
    final delays = <Duration>[];
    await expectLater(
        captureDelays(produce, delays), throwsA(isA<KafkaBrokerException>()));
    expect(delays, List.filled(10, Duration.zero));
    expect(peer.metadataCalls, 11);
  });

  for (final code in [5, 6, 9, 3, 29]) {
    test('metadata routing error $code preserves retry classification',
        () async {
      peer.metadataError = code;
      client = make();
      await warmConnection();
      final delays = <Duration>[];
      await expectLater(
          captureDelays(produce, delays),
          throwsA(isA<KafkaBrokerException>()
              .having((error) => error.errorCode, 'errorCode', code)));
      final retryable = [5, 6, 9].contains(code);
      expect(peer.metadataCalls, retryable ? 3 : 1);
      expect(delays, hasLength(retryable ? 2 : 0));
      expect(peer.produceCalls, 0);
    });
  }

  test('raw metadata broker errors remain diagnostics without retries',
      () async {
    client = make();
    final result = await client.metadata({'topic'});
    expect(result.topics.single.errorCode, 6);
    expect(peer.metadataCalls, 1);
  });

  test('metadata disconnect remains a bounded retryable transport error',
      () async {
    peer.disconnectMetadata = true;
    client = make(baseMs: 10);
    // Reconnecting paths use real bounded delays, never socket timer capture.
    await expectLater(
        client.metadata({'topic'}), throwsA(isA<KafkaConnectionException>()));
    expect(peer.metadataCalls, 3);
  });

  test('response resource limit is terminal for ordinary metadata retries',
      () async {
    peer.oversizedMetadata = true;
    client = make(buffered: 256);
    await expectLater(client.metadata({'topic'}),
        throwsA(isA<KafkaResourceLimitException>()));
    expect(peer.metadataCalls, 1);
  });

  for (final outcome in ['success', 'brokerError', 'disconnect']) {
    test('pre-send metadata retry allows one Produce only: $outcome', () async {
      peer.metadataFailures = 1;
      peer.produceError = outcome == 'brokerError' ? 6 : 0;
      peer.disconnectProduce = outcome == 'disconnect';
      client = make();
      await warmConnection();
      final delays = <Duration>[];
      final result = captureDelays(produce, delays);
      if (outcome == 'success') {
        expect(await result, isNotNull);
      } else {
        await expectLater(
            result,
            throwsA(outcome == 'brokerError'
                ? isA<KafkaBrokerException>()
                : isA<KafkaConnectionException>()));
      }
      expect(peer.metadataCalls, 2);
      expect(peer.produceCalls, 1);
      expect(delays, hasLength(1));
    });
  }

  test('ordinary retry timer creation failure propagates without replay',
      () async {
    client = make(retries: 10);
    await warmConnection();
    final failure = StateError('synthetic ordinary retry timer failure');
    var backoffTimers = 0;
    final result = runZoned(produce, zoneSpecification: ZoneSpecification(
        createTimer: (self, parent, zone, duration, callback) {
      if (duration <= const Duration(seconds: 1)) {
        backoffTimers++;
        throw failure;
      }
      return parent.createTimer(zone, duration, callback);
    }));
    await expectLater(result, throwsA(same(failure)));
    expect(backoffTimers, 1);
    expect(peer.metadataCalls, 1);
    expect(peer.produceCalls, 0);
    expect(client.pendingRequestCount, 0);
    await client.close();
  });

  test('close cancels every concurrent ordinary retry without Produce replay',
      () async {
    const operations = 12;
    client = make(retries: 10, baseMs: 1000);
    await warmConnection();
    final waiting = Completer<void>();
    final timers = <_HeldTimer>[];
    final results =
        runZoned(() => [for (var i = 0; i < operations; i++) produce()],
            zoneSpecification: ZoneSpecification(
                createTimer: (self, parent, zone, duration, callback) {
      if (duration <= const Duration(seconds: 1)) {
        final timer = _HeldTimer();
        timers.add(timer);
        if (timers.length == operations) waiting.complete();
        return timer;
      }
      return parent.createTimer(zone, duration, callback);
    }));
    // Observe every operation's error before releasing any close waiter.
    final checked = [
      for (final result in results) expectLater(result, throwsStateError)
    ];
    await waiting.future.timeout(const Duration(seconds: 2));
    expect(peer.metadataCalls, operations);
    expect(timers, hasLength(operations));
    expect(timers.every((timer) => timer.isActive), isTrue);
    await Future.wait([client.close(), ...checked])
        .timeout(const Duration(seconds: 2));
    expect(peer.metadataCalls, operations);
    expect(peer.produceCalls, 0);
    expect(timers.every((timer) => !timer.isActive), isTrue);
    expect(client.pendingRequestCount, 0);
    expect(client.admittedRequestCount, 0);
    expect(client.connectionCount, 0);
    expect(client.queuedRequestBytes, 0);
    expect(client.bufferedResponseBytes, 0);
  });

  for (final order in ['closeOnly', 'closeThenTimer', 'timerThenClose']) {
    test('close interrupts ordinary retry without replay: $order', () async {
      client = make(retries: 10, baseMs: 1000);
      await warmConnection();
      final waiting = Completer<void>();
      late void Function() fire;
      late _HeldTimer heldTimer;
      final result = runZoned(produce, zoneSpecification: ZoneSpecification(
          createTimer: (self, parent, zone, duration, callback) {
        if (duration <= const Duration(seconds: 1)) {
          fire = callback;
          waiting.complete();
          return heldTimer = _HeldTimer();
        }
        return parent.createTimer(zone, duration, callback);
      }));
      // Install the error listener before either close or timer can complete.
      final checked = expectLater(result, throwsStateError);
      await waiting.future.timeout(const Duration(seconds: 2));
      expect(peer.metadataCalls, 1);
      Future<void> closing;
      if (order == 'timerThenClose') {
        fire();
        closing = client.close();
      } else {
        closing = client.close();
        if (order == 'closeThenTimer') fire();
      }
      await Future.wait([closing, checked]).timeout(const Duration(seconds: 2));
      expect(peer.metadataCalls, 1);
      expect(peer.produceCalls, 0);
      expect(client.pendingRequestCount, 0);
      expect(heldTimer.isActive, isFalse,
          reason: 'Close must cancel the ordinary retry timer');
      // A repeated close must remain safe after either winner.
      await client.close();
    });
  }
}
