import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

// Count payload access without exposing or retaining copies in diagnostics.
class _ObservedSasl implements KafkaSaslPlain {
  final KafkaSaslPlain delegate;
  int reads = 0;
  _ObservedSasl(this.delegate);
  @override
  Uint8List get authBytes {
    reads++;
    return delegate.authBytes;
  }
}

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
  _Peer? coordinator;
  int clientHellos = 0;
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
          if (hello.length >= 6 &&
              hello.length >= 5 + ((hello[3] << 8) | hello[4])) {
            if (hello[0] != 22 || hello[5] != 1) {
              throw StateError('Expected ClientHello');
            }
            clientHellos++;
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
          final version = reader.readInt16();
          final id = reader.readInt32();
          keys.add(key);
          if (mode == 'dropCommit' && key == 8) {
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
            b
              ..addInt16(0)
              ..addInt32(3);
            for (final pair in [
              [10, 2],
              [9, 5],
              [8, 6]
            ]) {
              b
                ..addInt16(pair[0])
                ..addInt16(pair[1])
                ..addInt16(pair[1]);
            }
          } else if (key == 10) {
            expect(version, 2);
            b
              ..addInt32(0)
              ..addInt16(0)
              ..addString(null)
              ..addInt32(7)
              ..addString('localhost')
              ..addInt32((coordinator ?? this).server.port);
          } else if (key == 9) {
            expect(version, 5);
            b
              ..addInt32(0)
              ..addInt32(1)
              ..addString('topic')
              ..addInt32(1)
              ..addInt32(0)
              ..addInt64(42)
              ..addInt32(3)
              ..addString('saved')
              ..addInt16(0)
              ..addInt16(0);
          } else if (key == 8) {
            expect(version, 6);
            b
              ..addInt32(0)
              ..addInt32(1)
              ..addString('topic')
              ..addInt32(1)
              ..addInt32(0)
              ..addInt16(0);
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
  group('manual group TLS boundaries', () {
    late List<_Peer> peers;
    late List<KafkaClient> clients;
    late List<String> order;
    late Map<KafkaClient, _ObservedSasl> credentials;
    setUp(() {
      peers = [];
      clients = [];
      order = [];
      credentials = {};
    });
    tearDown(() async {
      // Always release every owned resource, even if an earlier assertion fails.
      final failures = <Object>[];
      for (final c in clients) {
        try {
          await c.close().timeout(const Duration(seconds: 4));
          expect(c.connectionCount, 0);
          expect(c.pendingRequestCount, 0);
          expect(c.admittedRequestCount, 0);
          expect(c.queuedRequestBytes, 0);
          expect(c.bufferedResponseBytes, 0);
        } catch (e) {
          failures.add(e);
        }
      }
      for (final p in peers) {
        try {
          await p.close();
        } catch (e) {
          failures.add(e);
        }
      }
      expect(failures, isEmpty);
    });
    Future<_Peer> peer(String cert, String name,
        [String mode = 'normal']) async {
      final p = await _Peer.bind(dir!, cert, name, mode, order);
      peers.add(p);
      return p;
    }

    KafkaClient client(List<_Peer> endpoints) {
      final observed = _ObservedSasl(KafkaSaslPlain(
          username: env['KAFKA_BOOTSTRAP_TLS_USERNAME']!,
          password: env['KAFKA_BOOTSTRAP_TLS_PASSWORD']!));
      final c = KafkaClient(
          brokers: endpoints.map((p) => p.broker).toList(),
          tls: KafkaTlsConfig(
              trustedCertificates: File('$dir/ca.pem').readAsBytesSync(),
              withTrustedRoots: false),
          sasl: observed,
          allowTlsBootstrapFallback: true,
          tlsFetchRecovery: KafkaTlsFetchRecovery(
              maxAttempts: 5,
              window: const Duration(seconds: 5),
              retryDelay: Duration.zero),
          maxReadRetries: 3,
          retryBaseDelay: Duration.zero,
          maxConnections: 4,
          connectionTimeout: const Duration(seconds: 2),
          requestTimeout: const Duration(seconds: 2));
      clients.add(c);
      credentials[c] = observed;
      return c;
    }

    Future<dynamic> operation(KafkaClient c, String op) => op == 'fetch'
        ? c.committedOffsets('group', 'topic', partitions: {0})
        : c.commitOffsets('group', 'topic',
            nextOffsets: {0: 42}, metadata: 'saved');

    for (final op in ['fetch', 'commit']) {
      test('$op validated distinct coordinator succeeds', () async {
        final bootstrap = await peer('valid', 'bootstrap');
        final coordinator = await peer('valid', 'coordinator');
        final unused = await peer('valid', 'unused');
        bootstrap.coordinator = coordinator;
        final c = client([bootstrap, unused]);
        final result = await operation(c, op);
        if (op == 'fetch') {
          expect(result[0].nextOffset, 42);
          expect(result[0].leaderEpoch, 3);
          expect(result[0].metadata, 'saved');
        }
        expect(order, ['bootstrap', 'coordinator']);
        expect(bootstrap.keys, [17, 36, 18, 10]);
        expect(coordinator.keys, [17, 36, 18, op == 'fetch' ? 9 : 8]);
        expect(bootstrap.tlsConnections, 1);
        expect(coordinator.tlsConnections, 1);
        expect(credentials[c]!.reads, 2);
        expect(unused.accepts, 0);
        expect(c.bootstrapTlsFallbackCount, 0);
      });
      for (final stage in ['bootstrap', 'coordinator']) {
        for (final defect in [
          'eof',
          'untrusted',
          'wronghost',
          'expired',
          'auth',
          'mechanism'
        ]) {
          test('$op $stage $defect terminal with both recovery flags',
              () async {
            final auth = defect == 'auth' || defect == 'mechanism';
            final target = await peer(
                auth ? 'valid' : defect, 'target', auth ? defect : 'normal');
            final bootstrap = stage == 'bootstrap'
                ? target
                : await peer('valid', 'bootstrap');
            bootstrap.coordinator = target;
            final unused = await peer('valid', 'unused');
            final c = client([bootstrap, unused]);
            final error = await _error(operation(c, op));
            expect(error, isNotNull);
            if (!auth) {
              expect(error, isA<HandshakeException>());
            } else {
              expect(error, isA<KafkaAuthenticationException>());
              expect(
                  error
                      .toString()
                      .contains(env['KAFKA_BOOTSTRAP_TLS_PASSWORD']!),
                  isFalse);
              expect(
                  error
                      .toString()
                      .contains(env['KAFKA_BOOTSTRAP_TLS_USERNAME']!),
                  isFalse);
            }
            // Wait for peer-side handshake rejection to settle without timers.
            await c.close();
            await Future.wait(target.tasks).timeout(const Duration(seconds: 4));
            await target.close();
            peers.remove(target);
            expect(order,
                stage == 'bootstrap' ? ['target'] : ['bootstrap', 'target']);
            expect(target.accepts, 1);
            expect(target.keys,
                auth ? (defect == 'auth' ? [17, 36] : [17]) : isEmpty);
            if (defect == 'eof') {
              expect(target.clientHellos, 1);
            } else if (!auth) {
              expect(target.tlsRejections, 1);
              expect(target.tlsConnections, 0);
            } else {
              expect(target.tlsConnections, 1);
            }
            if (stage == 'coordinator') {
              expect(bootstrap.keys, [17, 36, 18, 10]);
            }
            expect(credentials[c]!.reads,
                (stage == 'coordinator' ? 1 : 0) + (defect == 'auth' ? 1 : 0));
            expect(unused.accepts, 0);
            expect(unused.keys, isEmpty);
            expect(c.bootstrapTlsFallbackCount, 0);
          });
        }
      }
    }
    test('ambiguous commit is never replayed with both recovery flags',
        () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final coordinator = await peer('valid', 'coordinator', 'dropCommit');
      final unused = await peer('valid', 'unused');
      bootstrap.coordinator = coordinator;
      final c = client([bootstrap, unused]);
      expect(await _error(operation(c, 'commit')), isNotNull);
      expect(order, ['bootstrap', 'coordinator']);
      expect(bootstrap.keys, [17, 36, 18, 10]);
      expect(coordinator.keys, [17, 36, 18, 8]);
      expect(credentials[c]!.reads, 2);
      expect(unused.accepts, 0);
      expect(c.bootstrapTlsFallbackCount, 0);
    });
  },
      skip: configured
          ? false
          : 'Run the disposable group TLS boundary fixture runner',
      timeout: const Timeout(Duration(seconds: 20)));
}
