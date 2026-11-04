import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

// Owned local peers, real strict TLS, bounded synthetic Kafka codecs. This is
// a send-count oracle, not a Kafka durability/storage test.
class _Peer {
  final ServerSocket server;
  final SecurityContext? context;
  final String name;
  final String mode;
  final List<String> order;
  final sockets = <Socket>[];
  final tasks = <Future<void>>[];
  final errors = <Object>[];
  final keys = <int>[];
  int accepts = 0;
  int tlsConnections = 0;
  int tlsRejections = 0;
  _Peer? leader;
  late StreamSubscription<Socket> subscription;
  _Peer(this.server, this.context, this.name, this.mode, this.order) {
    subscription = server.listen((socket) {
      accepts++;
      order.add(name);
      sockets.add(socket);
      tasks.add(_serve(socket));
    }, onError: (Object e) => errors.add(e));
  }
  Broker get broker => Broker('localhost', server.port);
  static Future<_Peer> bind(String dir, String cert, String name, String mode,
      List<String> order) async {
    SecurityContext? context;
    if (cert != 'eof') {
      context = SecurityContext()
        ..useCertificateChain('$dir/$cert.pem')
        ..usePrivateKey('$dir/$cert.key');
    }
    return _Peer(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0),
        context, name, mode, order);
  }

  Future<void> _serve(Socket raw) async {
    Socket socket = raw;
    try {
      if (context == null) {
        final hello = <int>[];
        var sentFin = false;
        await for (final chunk in raw) {
          if (sentFin) continue;
          hello.addAll(chunk);
          if (hello.length > 65540) throw StateError('Oversized hello');
          if (hello.length >= 5 &&
              hello.length >= 5 + ((hello[3] << 8) | hello[4])) {
            if (hello[0] != 22 || hello[5] != 1) {
              throw StateError('Expected ClientHello');
            }
            sentFin = true;
            await raw.flush();
            await raw.close();
          }
        }
        return;
      }
      try {
        socket = await SecureSocket.secureServer(raw, context!)
            .timeout(const Duration(seconds: 3));
        sockets.add(socket);
        tlsConnections++;
      } on HandshakeException {
        tlsRejections++;
        return; // Expected certificate rejection, no application stream.
      }
      final buffer = <int>[];
      await for (final chunk in socket) {
        buffer.addAll(chunk);
        if (buffer.length > 65540) throw StateError('Oversized request');
        while (buffer.length >= 4) {
          final size = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (size < 8 || size > 65536) {
            throw StateError('Invalid request size');
          }
          if (buffer.length < size + 4) break;
          final reader =
              KafkaBytesReader.fromBytes(buffer.sublist(4, size + 4));
          buffer.removeRange(0, size + 4);
          final key = reader.readInt16();
          reader.readInt16();
          final id = reader.readInt32();
          keys.add(key);
          if (mode == 'dropProduce' && key == 0) {
            socket.destroy();
            return;
          }
          final b = KafkaBytesBuilder()..addInt32(id);
          if (key == 17) {
            b
              ..addInt16(mode == 'mechanism' ? 33 : 0)
              ..addInt32(1)
              ..addString(mode == 'mechanism' ? 'SCRAM-SHA-256' : 'PLAIN');
          } else if (key == 36) {
            b
              ..addInt16(mode == 'auth' ? 58 : 0)
              ..addString(mode == 'auth'
                  ? Platform.environment['KAFKA_BOOTSTRAP_TLS_PASSWORD']
                  : null)
              ..addBytes([]);
          } else if (key == 18) {
            if (mode == 'resource') {
              // A valid but incomplete frame must exceed the receive budget
              // before any payload parser runs, regardless of TLS chunking.
              socket.add((KafkaBytesBuilder()
                    ..addInt32(4096)
                    ..addRaw(List<int>.filled(256, 0)))
                  .takeBytes());
              continue;
            }
            if (mode != 'malformed') {
              b
                ..addInt16(0)
                ..addInt32(mode == 'unsupported' ? 0 : 2);
              if (mode != 'unsupported') {
                for (final pair in [
                  [3, 0],
                  [0, 3]
                ]) {
                  b
                    ..addInt16(pair[0])
                    ..addInt16(pair[1])
                    ..addInt16(pair[1]);
                }
              }
            }
          } else if (key == 3) {
            b
              ..addInt32(1)
              ..addInt32(7)
              ..addString('localhost')
              ..addInt32((leader ?? this).server.port)
              ..addInt32(1)
              ..addInt16(0)
              ..addString('topic')
              ..addInt32(1)
              ..addInt16(0)
              ..addInt32(0)
              ..addInt32(7)
              ..addInt32(1)
              ..addInt32(7)
              ..addInt32(1)
              ..addInt32(7);
          } else if (key == 0) {
            b
              ..addInt32(1)
              ..addString('topic')
              ..addInt32(1)
              ..addInt32(0)
              ..addInt16(0)
              ..addInt64(42)
              ..addInt64(-1)
              ..addInt32(0);
          } else {
            throw StateError('Unexpected application API');
          }
          socket.add(KafkaRequest.frame(b.takeBytes()));
          await socket.flush();
        }
      }
    } catch (e) {
      errors.add(e);
    } finally {
      socket.destroy();
      raw.destroy();
    }
  }

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
    await subscription.cancel();
    await Future.wait(tasks).timeout(const Duration(seconds: 4));
    expect(errors, isEmpty);
  }
}

Future<Object?> _error(Future<dynamic> future) =>
    future.then<Object?>((_) => null, onError: (Object e, StackTrace _) => e);

void main() {
  final env = Platform.environment;
  final dir = env['KAFKA_BOOTSTRAP_TLS_FIXTURE_DIR'];
  final configured = dir != null &&
      env['KAFKA_BOOTSTRAP_TLS_USERNAME'] != null &&
      env['KAFKA_BOOTSTRAP_TLS_PASSWORD'] != null;
  group('validated bootstrap TLS fallback', () {
    late List<_Peer> peers;
    late List<KafkaClient> clients;
    late List<String> order;
    setUp(() {
      peers = [];
      clients = [];
      order = [];
    });
    tearDown(() async {
      for (final c in clients) {
        await c.close().timeout(const Duration(seconds: 4));
        expect(c.connectionCount, 0);
        expect(c.pendingRequestCount, 0);
        expect(c.admittedRequestCount, 0);
        expect(c.queuedRequestBytes, 0);
        expect(c.bufferedResponseBytes, 0);
      }
      for (final p in peers) {
        await p.close();
      }
    });
    Future<_Peer> peer(String cert, String name,
        [String mode = 'normal']) async {
      final p = await _Peer.bind(dir!, cert, name, mode, order);
      peers.add(p);
      return p;
    }

    KafkaClient client(List<_Peer> endpoints,
        {bool sasl = true, bool enabled = true, bool resource = false}) {
      final c = KafkaClient(
          brokers: endpoints.map((p) => p.broker).toList(),
          tls: KafkaTlsConfig(
              trustedCertificates: File('$dir/ca.pem').readAsBytesSync(),
              withTrustedRoots: false),
          sasl: sasl
              ? KafkaSaslPlain(
                  username: env['KAFKA_BOOTSTRAP_TLS_USERNAME']!,
                  password: env['KAFKA_BOOTSTRAP_TLS_PASSWORD']!)
              : null,
          allowTlsBootstrapFallback: enabled,
          maxReadRetries: 3,
          retryBaseDelay: Duration.zero,
          maxConnections: 1,
          maxBufferedResponseBytes: resource ? 128 : 65536,
          connectionTimeout: const Duration(seconds: 2),
          requestTimeout: const Duration(seconds: 2));
      clients.add(c);
      return c;
    }

    for (final badCert in ['eof', 'untrusted', 'wronghost', 'expired']) {
      test('$badCert then valid strictly validates before SASL or Kafka',
          () async {
        final bad = await peer(badCert, 'bad');
        final good = await peer('valid', 'good');
        final c = client([bad, good]);
        final result = await c.metadata({'topic'});
        expect(result.topics.single.name, 'topic');
        expect(order, ['bad', 'good']);
        expect(bad.keys, isEmpty);
        expect(good.keys, [17, 36, 18, 3]);
        expect(good.tlsConnections, 1);
        expect(c.bootstrapTlsFallbackCount, 1);
      });
      test('$badCert default-off never reaches next valid peer', () async {
        final bad = await peer(badCert, 'bad');
        final good = await peer('valid', 'good');
        final c = client([bad, good], enabled: false);
        expect(await _error(c.metadata()), isA<HandshakeException>());
        expect(order, ['bad']);
        expect(bad.keys, isEmpty);
        expect(good.keys, isEmpty);
        expect(c.bootstrapTlsFallbackCount, 0);
      });
    }
    test('all invalid bounded once, sanitized, no outer read retries',
        () async {
      final bad = <_Peer>[];
      for (final cert in ['untrusted', 'wronghost', 'expired']) {
        bad.add(await peer(cert, cert));
      }
      final c = client([...bad, bad.first]);
      final e = await _error(c.metadata());
      expect(e, isA<HandshakeException>());
      expect((e as HandshakeException).message,
          'Kafka bootstrap TLS discovery failed');
      expect(e.osError, isNull);
      expect(order, ['untrusted', 'wronghost', 'expired']);
      for (final p in bad) {
        expect(p.keys, isEmpty);
      }
      expect(c.bootstrapTlsFallbackCount, 2);
      expect(c.connectionCount, 0);
      expect(c.pendingRequestCount, 0);
      expect(c.admittedRequestCount, 0);
    });
    for (final earlierTls in [false, true]) {
      for (final mode in [
        'auth',
        'mechanism',
        'unsupported',
        'malformed',
        'resource'
      ]) {
        test('$mode terminal, earlier TLS failure=$earlierTls', () async {
          final bad = earlierTls ? await peer('untrusted', 'bad') : null;
          final terminal = await peer('valid', 'terminal', mode);
          final untouched = await peer('valid', 'untouched');
          final c = client([if (bad != null) bad, terminal, untouched],
              resource: mode == 'resource');
          final e = await _error(c.metadata());
          if (mode == 'auth' || mode == 'mechanism') {
            expect(e, isA<KafkaAuthenticationException>());
            expect(terminal.keys, mode == 'auth' ? [17, 36] : [17]);
          } else if (mode == 'unsupported') {
            expect(e, isA<UnsupportedError>());
          } else if (mode == 'malformed') {
            expect(e, isA<FormatException>());
          } else {
            expect(e, isA<KafkaResourceLimitException>());
          }
          expect(e.toString(),
              isNot(contains(env['KAFKA_BOOTSTRAP_TLS_PASSWORD']!)));
          expect(e.toString(),
              isNot(contains(env['KAFKA_BOOTSTRAP_TLS_USERNAME']!)));
          expect(order, [if (earlierTls) 'bad', 'terminal']);
          expect(untouched.keys, isEmpty);
          expect(c.bootstrapTlsFallbackCount, earlierTls ? 1 : 0);
        });
      }
    }
    for (final ambiguous in [false, true]) {
      test('pre-send metadata fallback Produce once, ambiguous=$ambiguous',
          () async {
        final bad = await peer('untrusted', 'bad');
        final good =
            await peer('valid', 'good', ambiguous ? 'dropProduce' : 'normal');
        final c = client([bad, good]);
        final result = c.produce('topic', [
          KafkaRecord(value: [1])
        ]);
        if (ambiguous) {
          expect(await _error(result), isNotNull);
        } else {
          expect(
              (await result)!
                  .responses
                  .single
                  .partitionResponses
                  .single
                  .baseOffset,
              42);
        }
        expect(good.keys.where((key) => key == 0).length, 1);
        expect(bad.keys, isEmpty);
        expect(order, ['bad', 'good']);
        expect(c.bootstrapTlsFallbackCount, 1);
      });
    }
    test(
        'discovered leader TLS failure never uses another bootstrap for Produce',
        () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final leader = await peer('untrusted', 'leader');
      final arbitrary = await peer('valid', 'arbitrary');
      bootstrap.leader = leader;
      final c = client([bootstrap, arbitrary]);
      expect(
          await _error(c.produce('topic', [
            KafkaRecord(value: [1])
          ])),
          isA<HandshakeException>());
      expect(order, ['bootstrap', 'leader']);
      expect(bootstrap.keys, [17, 36, 18, 3]);
      expect(leader.keys, isEmpty);
      expect(arbitrary.keys, isEmpty);
      expect(c.bootstrapTlsFallbackCount, 0);
    });
  },
      skip: configured
          ? false
          : 'Run the disposable bootstrap TLS fixture runner',
      timeout: const Timeout(Duration(seconds: 20)));
}
