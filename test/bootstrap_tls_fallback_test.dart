import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';

// Owned loopback peers read a complete ClientHello before graceful FIN. No
// certificates, bypass callbacks, Kafka broker or external service is involved.
class _Peer {
  final ServerSocket server;
  final bool stall;
  final Future<void>? release;
  final sockets = <Socket>[];
  final tasks = <Future<void>>[];
  final errors = <Object>[];
  final hello = Completer<void>();
  late StreamSubscription<Socket> subscription;
  int accepts = 0;
  int hellos = 0;
  int fins = 0;
  _Peer(this.server, this.stall, this.release) {
    subscription = server.listen((socket) {
      accepts++;
      sockets.add(socket);
      tasks.add(_serve(socket));
    }, onError: (Object e) => errors.add(e));
  }
  Broker get broker => Broker('127.0.0.1', server.port);
  static Future<_Peer> bind(
          {bool stall = false, Future<void>? release}) async =>
      _Peer(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0), stall,
          release);
  Future<void> _serve(Socket socket) async {
    final bytes = <int>[];
    var readHello = false;
    try {
      await for (final chunk in socket) {
        if (readHello) continue;
        bytes.addAll(chunk);
        if (bytes.length > 65540) throw StateError('Oversized ClientHello');
        if (bytes.length < 5) continue;
        final size = (bytes[3] << 8) | bytes[4];
        if (bytes.length < 5 + size) continue;
        if (size < 4 ||
            bytes[0] != 22 ||
            bytes[1] != 3 ||
            bytes[5] != 1 ||
            ((bytes[6] << 16) | (bytes[7] << 8) | bytes[8]) + 4 != size) {
          throw StateError('Expected complete TLS ClientHello');
        }
        readHello = true;
        hellos++;
        if (!hello.isCompleted) hello.complete();
        if (stall) continue;
        if (release != null) await release;
        await socket.flush();
        await socket.close();
        fins++;
      }
    } catch (e) {
      errors.add(e);
    } finally {
      socket.destroy();
    }
  }

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
    await subscription.cancel();
    await Future.wait(tasks).timeout(const Duration(seconds: 2));
    expect(errors, isEmpty);
  }
}

Future<Object?> _error(Future<dynamic> future) =>
    future.then<Object?>((_) => null, onError: (Object e, StackTrace _) => e);

Future<void> _settled(KafkaClient client) async {
  final clock = Stopwatch()..start();
  while ((client.admittedRequestCount != 0 || client.connectionCount != 0) &&
      clock.elapsed < const Duration(seconds: 2)) {
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  expect(client.pendingRequestCount, 0);
  expect(client.admittedRequestCount, 0);
  expect(client.connectionCount, 0);
  expect(client.queuedRequestBytes, 0);
  expect(client.bufferedResponseBytes, 0);
}

void main() {
  test('TLS bootstrap fallback cannot imply plaintext configuration', () {
    expect(
        () => KafkaClient(
              brokers: [Broker('localhost', 1)],
              allowTlsBootstrapFallback: true,
            ),
        throwsArgumentError);
  });

  late List<_Peer> peers;
  late List<KafkaClient> clients;
  setUp(() {
    peers = [];
    clients = [];
  });
  tearDown(() async {
    for (final client in clients) {
      await client.close().timeout(const Duration(seconds: 3));
      await _settled(client);
    }
    for (final peer in peers) {
      await peer.close();
    }
  });
  Future<_Peer> peer({bool stall = false, Future<void>? release}) async {
    final p = await _Peer.bind(stall: stall, release: release);
    peers.add(p);
    return p;
  }

  KafkaClient client(List<Broker> brokers,
      {bool enabled = true,
      KafkaTlsConfig? tls,
      Duration timeout = const Duration(seconds: 1)}) {
    final c = KafkaClient(
        brokers: brokers,
        tls: tls ?? KafkaTlsConfig(),
        allowTlsBootstrapFallback: enabled,
        maxReadRetries: 3,
        maxConnections: 1,
        retryBaseDelay: Duration.zero,
        connectionTimeout: timeout,
        requestTimeout: const Duration(seconds: 2));
    clients.add(c);
    return c;
  }

  test('explicit false preserves native EOF and never advances', () async {
    final a = await peer();
    final b = await peer();
    final c = client([a.broker, b.broker], enabled: false);
    final error = await _error(c.metadata());
    expect(error, isA<HandshakeException>());
    expect(error.runtimeType, HandshakeException);
    expect(c.bootstrapTlsFallbackCount, 0);
    expect(a.accepts, 1);
    expect(b.accepts, 0);
    await _settled(c);
  });

  test('all EOF deduplicates endpoints and bounds traversal despite retries',
      () async {
    final a = await peer();
    final b = await peer();
    final c = client([a.broker, a.broker, b.broker, a.broker, b.broker]);
    final error = await _error(c.metadata());
    expect(error, isA<HandshakeException>());
    expect((error as HandshakeException).message,
        'Kafka bootstrap TLS discovery failed');
    expect(error.osError, isNull);
    expect(c.bootstrapTlsFallbackCount, 1);
    expect(a.accepts, 1);
    expect(b.accepts, 1);
    expect(a.hellos, 1);
    expect(b.hellos, 1);
    await _settled(c);
  });

  for (final tlsFirst in [true, false]) {
    for (final timeout in [true, false]) {
      test(
          'mixed EOF and ${timeout ? "timeout" : "refusal"}, TLS first=$tlsFirst never restarts',
          () async {
        final a = await peer();
        _Peer? stalled;
        late Broker transport;
        if (timeout) {
          stalled = await peer(stall: true);
          transport = stalled.broker;
        } else {
          final refused =
              await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
          transport = Broker('127.0.0.1', refused.port);
          await refused.close();
        }
        final c = client(
            tlsFirst ? [a.broker, transport] : [transport, a.broker],
            timeout: const Duration(milliseconds: 250));
        final error = await _error(c.metadata());
        expect(error, isA<HandshakeException>());
        expect((error as HandshakeException).message,
            'Kafka bootstrap TLS discovery failed');
        expect(error.osError, isNull);
        expect(a.accepts, 1);
        if (stalled != null) expect(stalled.accepts, 1);
        expect(c.bootstrapTlsFallbackCount, tlsFirst ? 1 : 0);
        await _settled(c);
      });
    }
  }

  test('invalid local CA is terminal before any peer', () async {
    final a = await peer();
    final b = await peer();
    final c = client([a.broker, b.broker],
        tls: KafkaTlsConfig(
            trustedCertificates: [1, 2, 3], withTrustedRoots: false));
    expect(await _error(c.metadata()), isNotNull);
    expect(a.accepts, 0);
    expect(b.accepts, 0);
    expect(c.bootstrapTlsFallbackCount, 0);
    await _settled(c);
  });

  test('close during stalled TLS never attempts next endpoint', () async {
    final a = await peer(stall: true);
    final b = await peer();
    final c = client([a.broker, b.broker]);
    final result = _error(c.metadata());
    await a.hello.future.timeout(const Duration(seconds: 2));
    await c.close();
    expect(await result, isA<StateError>());
    expect(b.accepts, 0);
    expect(c.bootstrapTlsFallbackCount, 0);
    await _settled(c);
  });

  test('low-level EOF is sanitized but never traverses', () async {
    final a = await peer();
    final b = await peer();
    final c = client([a.broker, b.broker]);
    final error = await _error(c.send(MetadataRequest(null)));
    expect(error, isA<HandshakeException>());
    expect((error as HandshakeException).message, 'Kafka TLS handshake failed');
    expect(error.osError, isNull);
    expect(b.accepts, 0);
    expect(c.bootstrapTlsFallbackCount, 0);
    await _settled(c);
  });

  test('cancel during stalled low-level TLS never attempts next endpoint',
      () async {
    final a = await peer(stall: true);
    final b = await peer();
    final c = client([a.broker, b.broker]);
    final token = KafkaCancellationToken();
    final result =
        _error(c.send(MetadataRequest(null), cancellationToken: token));
    await a.hello.future.timeout(const Duration(seconds: 2));
    token.cancel();
    expect(await result, isA<KafkaCancelledException>());
    await _settled(c);
    expect(b.accepts, 0);
    expect(c.bootstrapTlsFallbackCount, 0);
  });

  test(
      'concurrent metadata counts traversals independently with one connection',
      () async {
    final release = Completer<void>();
    addTearDown(() {
      if (!release.isCompleted) release.complete();
    });
    final a = await peer(release: release.future);
    final b = await peer();
    final c = client([a.broker, a.broker, b.broker]);
    final first = _error(c.metadata());
    final second = _error(c.metadata());
    await a.hello.future.timeout(const Duration(seconds: 2));
    release.complete();
    expect(await first, isA<HandshakeException>());
    expect(await second, isA<HandshakeException>());
    expect(a.accepts, 1);
    expect(b.accepts, 1);
    expect(c.bootstrapTlsFallbackCount, 2);
    await _settled(c);
  });
}
