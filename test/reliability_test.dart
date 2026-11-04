import 'dart:async';
import 'dart:io';
import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:kafka_client/src/security/sasl_requests.dart';
import 'package:test/test.dart';

class Probe extends KafkaRequest {
  final int extraBytes;
  Probe([this.extraBytes = 0]);
  @override
  List<int> toBytes() => KafkaRequest.frame((KafkaBytesBuilder()
        ..addInt32(correlationId)
        ..addRaw(List.filled(extraBytes, 0)))
      .takeBytes());
  @override
  int parseResponse(List<int> bytes) =>
      KafkaBytesReader.fromBytes(bytes).readInt32();
}

void main() {
  late ServerSocket server;
  late KafkaClient client;
  final sockets = <Socket>[];
  setUp(() async {
    server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  });
  tearDown(() async {
    await client.close();
    for (final s in sockets) {
      s.destroy();
    }
    sockets.clear();
    await server.close();
  });
  KafkaClient make(
          {int inFlight = 100,
          int queued = 33554432,
          int buffered = 33554432,
          int connections = 8,
          int retries = 2}) =>
      KafkaClient(
          brokers: [Broker('127.0.0.1', server.port)],
          maxInFlightRequests: inFlight,
          maxPendingRequestBytes: queued,
          maxBufferedResponseBytes: buffered,
          maxConnections: connections,
          maxReadRetries: retries,
          requestTimeout: Duration(milliseconds: 120),
          retryBaseDelay: Duration.zero);
  void serve(void Function(Socket, List<int>) action) {
    server.listen((s) {
      sockets.add(s);
      final b = <int>[];
      s.listen((d) {
        b.addAll(d);
        while (b.length >= 4) {
          final n = KafkaBytesReader.fromBytes(b).readInt32();
          if (b.length < n + 4) break;
          final frame = b.sublist(4, n + 4);
          b.removeRange(0, n + 4);
          action(s, frame);
        }
      }, onError: (Object _) {});
    });
  }

  test('in-flight admission rejects instead of unbounded queue', () async {
    client = make(inFlight: 1);
    serve((s, b) {});
    final token = KafkaCancellationToken();
    final first = expectLater(client.send(Probe(), cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    await expectLater(
        client.send(Probe()), throwsA(isA<KafkaResourceLimitException>()));
    expect(client.pendingRequestCount, 1);
    token.cancel();
    await first;
    expect(client.pendingRequestCount, 0);
    expect(client.queuedRequestBytes, 0);
  });
  test('canceled pending connect retains bounded dispatch admission', () async {
    client = make(inFlight: 1);
    serve((s, b) {});
    final token = KafkaCancellationToken();
    final pending = expectLater(client.send(Probe(), cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    token.cancel();
    // Cancellation releases payload bytes, but the asynchronous connect has not
    // settled. It cannot be used to create an unbounded abandoned dispatch list.
    await expectLater(
        client.send(Probe()), throwsA(isA<KafkaResourceLimitException>()));
    await pending;
    expect(client.queuedRequestBytes, 0);
  });
  test('queued byte admission is bounded before connect', () async {
    client = make(queued: 8);
    serve((s, b) {});
    final token = KafkaCancellationToken();
    final first = expectLater(client.send(Probe(), cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    await expectLater(
        client.send(Probe()), throwsA(isA<KafkaResourceLimitException>()));
    token.cancel();
    await first;
    expect(client.queuedRequestBytes, 0);
  });
  test('receive-byte budget disconnects without retaining buffer', () async {
    client = make(buffered: 16);
    serve((s, b) {
      s.add((KafkaBytesBuilder()
            ..addInt32(100)
            ..addRaw(List.filled(32, 0)))
          .takeBytes());
    });
    await expectLater(
        client.send(Probe()), throwsA(isA<KafkaResourceLimitException>()));
    expect(client.bufferedResponseBytes, 0);
    expect(client.pendingRequestCount, 0);
    expect(client.queuedRequestBytes, 0);
  });
  test('connection limit includes established connections', () async {
    client = make(connections: 1);
    final received = Completer<void>();
    serve((s, b) {
      if (!received.isCompleted) received.complete();
    });
    final token = KafkaCancellationToken();
    final first = expectLater(client.send(Probe(), cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    await received.future;
    await expectLater(
        client.send(Probe(), broker: Broker('localhost', server.port)),
        throwsA(isA<KafkaResourceLimitException>()));
    expect(sockets.length, 1);
    token.cancel();
    await first;
  });
  test('pre-cancel does not connect or consume request object', () async {
    client = make();
    serve((s, b) {
      s.add(KafkaRequest.frame(b));
    });
    final token = KafkaCancellationToken()..cancel();
    final request = Probe();
    await expectLater(client.send(request, cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    expect(client.connectionCount, 0);
    expect(await client.send(request), request.correlationId);
  });
  test('cancel assigned request retires connection and new request reconnects',
      () async {
    client = make();
    final received = Completer<void>();
    var first = true;
    serve((socket, bytes) {
      if (first) {
        first = false;
        received.complete();
      } else {
        socket.add(KafkaRequest.frame(bytes));
      }
    });
    final token = KafkaCancellationToken();
    final pending = expectLater(client.send(Probe(), cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    await received.future;
    token.cancel();
    await pending;
    final next = Probe();
    expect(await client.send(next), next.correlationId);
    expect(sockets.length, 2);
  });
  test('throwing token listener cannot prevent request cancellation', () async {
    client = make();
    serve((s, b) {});
    final token = KafkaCancellationToken();
    token.onCancel(() => throw StateError('listener'));
    final pending = expectLater(client.send(Probe(), cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    expect(token.cancel, throwsStateError);
    await pending;
    expect(client.pendingRequestCount, 0);
  });
  test('request timeout releases all counters and retires stalled socket',
      () async {
    client = make();
    serve((s, b) {});
    await expectLater(client.send(Probe()), throwsA(isA<TimeoutException>()));
    expect(client.pendingRequestCount, 0);
    expect(client.queuedRequestBytes, 0);
    expect(client.bufferedResponseBytes, 0);
    expect(client.connectionCount, 0);
  });
  test('SASL configuration and raw auth sends cannot use plaintext', () async {
    client = make();
    serve((s, b) {
      fail('Authentication bytes must never reach plaintext socket');
    });
    expect(
        () => KafkaClient(
            brokers: [Broker('localhost', server.port)],
            sasl: KafkaSaslPlain(username: 'synthetic', password: 'test-only')),
        throwsArgumentError);
    await expectLater(client.send(SaslAuthenticateRequest([0, 1, 0, 2])),
        throwsArgumentError);
    expect(client.connectionCount, 0);
  });
  test('cancel during blocked flush retires socket and releases bytes',
      () async {
    client = make();
    final accepted = Completer<void>();
    // Intentionally do not subscribe to incoming bytes: TCP backpressure stalls
    // a payload substantially larger than the local socket send buffer.
    server.listen((socket) {
      sockets.add(socket);
      accepted.complete();
    });
    final token = KafkaCancellationToken();
    final pending = expectLater(
        client.send(Probe(8 * 1024 * 1024), cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    await accepted.future;
    await Future<void>.delayed(Duration(milliseconds: 20));
    expect(client.queuedRequestBytes, greaterThan(0));
    token.cancel();
    await pending;
    expect(client.queuedRequestBytes, 0);
    expect(client.connectionCount, 0);
    expect(client.pendingRequestCount, 0);
  });
  test('TLS handshake is bounded when peer never answers', () async {
    client = KafkaClient(
        brokers: [Broker('127.0.0.1', server.port)],
        tls: KafkaTlsConfig(),
        connectionTimeout: Duration(milliseconds: 60),
        requestTimeout: Duration(seconds: 2));
    server.listen((socket) {
      sockets.add(socket);
    });
    final watch = Stopwatch()..start();
    await expectLater(client.send(Probe()), throwsA(isA<TimeoutException>()));
    expect(watch.elapsed, lessThan(Duration(seconds: 1)));
    expect(client.pendingRequestCount, 0);
    expect(client.connectionCount, 0);
  });
  test('close actively interrupts an in-progress TLS handshake', () async {
    client = KafkaClient(
        brokers: [Broker('127.0.0.1', server.port)],
        tls: KafkaTlsConfig(),
        connectionTimeout: Duration(seconds: 2));
    final accepted = Completer<void>();
    server.listen((socket) {
      sockets.add(socket);
      accepted.complete();
    });
    final pending =
        expectLater(client.send(Probe()), throwsA(isA<StateError>()));
    await accepted.future;
    await Future<void>.delayed(Duration(milliseconds: 20));
    final watch = Stopwatch()..start();
    await client.close();
    await pending;
    expect(watch.elapsed, lessThan(Duration(seconds: 1)));
    expect(client.connectionCount, 0);
    expect(client.pendingRequestCount, 0);
  });
  test('metadata safely reconnects after transient disconnect', () async {
    client = make();
    var attempts = 0;
    serve((s, b) {
      final r = KafkaBytesReader.fromBytes(b);
      final key = r.readInt16();
      r.readInt16();
      final id = r.readInt32();
      if (key == 18) {
        attempts++;
        if (attempts == 1) {
          s.destroy();
          return;
        }
        s.add(KafkaRequest.frame((KafkaBytesBuilder()
              ..addInt32(id)
              ..addInt16(0)
              ..addInt32(1)
              ..addInt16(3)
              ..addInt16(0)
              ..addInt16(0))
            .takeBytes()));
      } else {
        s.add(KafkaRequest.frame((KafkaBytesBuilder()
              ..addInt32(id)
              ..addInt32(0)
              ..addInt32(0))
            .takeBytes()));
      }
    });
    expect((await client.metadata()).topics, isEmpty);
    expect(attempts, 2);
    expect(sockets.length, 2);
  });
  test('metadata retry budget terminates repeated disconnects', () async {
    client = make(retries: 2);
    var attempts = 0;
    serve((s, b) {
      attempts++;
      s.destroy();
    });
    await expectLater(
        client.metadata(), throwsA(isA<KafkaConnectionException>()));
    expect(attempts, 3);
    expect(client.pendingRequestCount, 0);
  });
  test('close interrupts retry delay instead of waiting full backoff',
      () async {
    client = KafkaClient(
        brokers: [Broker('127.0.0.1', server.port)],
        maxReadRetries: 10,
        retryBaseDelay: Duration(seconds: 1));
    final seen = Completer<void>();
    serve((s, b) {
      s.destroy();
      if (!seen.isCompleted) seen.complete();
    });
    final pending = expectLater(client.metadata(),
        throwsA(anyOf(isA<StateError>(), isA<KafkaConnectionException>())));
    await seen.future;
    await client.close();
    await pending;
    expect(client.connectionCount, 0);
  });
}
