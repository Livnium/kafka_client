import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

// Owned loopback peers with real strict TLS and bounded synthetic Kafka codecs.
// This suite proves routing/admission/send counts, not Kafka storage semantics.
class _Peer {
  final ServerSocket server;
  final SecurityContext? context;
  final String certificate;
  final String name;
  final List<String> order;
  String mode;
  final sockets = <Socket>[];
  final tasks = <Future<void>>[];
  final errors = <Object>[];
  final keys = <int>[];
  final metadataByTopic = <String, int>{};
  final routes = <String, List<_Peer>>{};
  final accepted = Completer<void>();
  final metadataSeen = Completer<void>();
  final metadataTwiceSeen = Completer<void>();
  final versionsSeen = Completer<void>();
  final authenticationSeen = Completer<void>();
  Completer<void>? handshakeGate;
  Completer<void>? versionsGate;
  Completer<void>? authenticationGate;
  int accepts = 0;
  int tlsConnections = 0;
  int tlsRejections = 0;
  int fetches = 0;
  int produces = 0;
  int metadataCalls = 0;
  int transientFetches = 0;
  Duration handshakeDelay = Duration.zero;
  Duration versionsDelay = Duration.zero;
  Duration metadataDelay = Duration.zero;
  Duration fetchDelay = Duration.zero;
  List<_Peer> leaders = [];
  bool closing = false;
  late StreamSubscription<Socket> subscription;

  _Peer(this.server, this.context, this.certificate, this.name, this.mode,
      this.order) {
    subscription = server.listen((socket) {
      accepts++;
      order.add(name);
      sockets.add(socket);
      if (!accepted.isCompleted) accepted.complete();
      tasks.add(_serve(socket));
    }, onError: (Object e) => errors.add(e));
  }

  Broker get broker => Broker('localhost', server.port);

  static Future<_Peer> bind(String dir, String cert, String name, String mode,
      List<String> order) async {
    SecurityContext? context;
    if (cert != 'eof' && cert != 'stall') {
      context = SecurityContext()
        ..useCertificateChain('$dir/$cert.pem')
        ..usePrivateKey('$dir/$cert.key');
    }
    return _Peer(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0),
        context, cert, name, mode, order);
  }

  Future<void> _serve(Socket raw) async {
    Socket socket = raw;
    try {
      if (context == null) {
        final hello = <int>[];
        var sentFin = false;
        await for (final chunk in raw) {
          if (certificate == 'stall' || sentFin) continue;
          hello.addAll(chunk);
          if (hello.length > 65540) throw StateError('Oversized hello');
          if (hello.length >= 6 &&
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
        await Future<void>.delayed(handshakeDelay);
        final gate = handshakeGate;
        if (gate != null) await gate.future;
        if (closing) return;
        socket = await SecureSocket.secureServer(raw, context!)
            .timeout(const Duration(seconds: 3));
        sockets.add(socket);
        tlsConnections++;
      } on HandshakeException {
        tlsRejections++;
        return;
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
          reader.readString(); // Client ID, not retained.
          keys.add(key);
          if ((mode == 'dropVersions' && key == 18) ||
              (mode == 'dropProduce' && key == 0) ||
              (mode == 'dropLaterMetadata' && key == 3 && metadataCalls > 0)) {
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
            if (!authenticationSeen.isCompleted) authenticationSeen.complete();
            final gate = authenticationGate;
            if (gate != null) await gate.future;
            b
              ..addInt16(mode == 'auth' ? 58 : 0)
              ..addString(mode == 'auth'
                  ? Platform.environment['KAFKA_BOOTSTRAP_TLS_PASSWORD']
                  : null)
              ..addBytes([]);
          } else if (key == 18) {
            if (!versionsSeen.isCompleted) versionsSeen.complete();
            final gate = versionsGate;
            if (gate != null) await gate.future;
            await Future<void>.delayed(versionsDelay);
            if (mode == 'resource') {
              socket.add((KafkaBytesBuilder()
                    ..addInt32(4096)
                    ..addRaw(List<int>.filled(256, 0)))
                  .takeBytes());
              await socket.flush();
              continue;
            }
            if (mode != 'malformed') {
              b
                ..addInt16(0)
                ..addInt32(mode == 'unsupported' ? 0 : 3);
              if (mode != 'unsupported') {
                for (final pair in [
                  [3, 0],
                  [0, 3],
                  [1, 4]
                ]) {
                  b
                    ..addInt16(pair[0])
                    ..addInt16(pair[1])
                    ..addInt16(pair[1]);
                }
              }
            }
          } else if (key == 3) {
            final count = reader.readInt32();
            final topic = count == 1 ? reader.readString()! : 'topic';
            final topicCall =
                metadataByTopic.update(topic, (n) => n + 1, ifAbsent: () => 1);
            metadataCalls++;
            if (!metadataSeen.isCompleted) metadataSeen.complete();
            if (metadataCalls >= 2 && !metadataTwiceSeen.isCompleted) {
              metadataTwiceSeen.complete();
            }
            final sequence = routes[topic] ?? leaders;
            final leader = sequence.isEmpty
                ? this
                : sequence[topicCall > sequence.length
                    ? sequence.length - 1
                    : topicCall - 1];
            await Future<void>.delayed(metadataDelay);
            b
              ..addInt32(1)
              ..addInt32(7)
              ..addString('localhost')
              ..addInt32(leader.server.port)
              ..addInt32(1)
              ..addInt16(0)
              ..addString(topic)
              ..addInt32(1)
              ..addInt16(0)
              ..addInt32(0)
              ..addInt32(7)
              ..addInt32(1)
              ..addInt32(7)
              ..addInt32(1)
              ..addInt32(7);
          } else if (key == 1) {
            fetches++;
            // Fetch v4 fields before the requested topic.
            reader.readInt32();
            reader.readInt32();
            reader.readInt32();
            reader.readInt32();
            reader.readInt8();
            reader.readInt32();
            final topic = reader.readString()!;
            await Future<void>.delayed(fetchDelay);
            b
              ..addInt32(0)
              ..addInt32(1)
              ..addString(topic)
              ..addInt32(1)
              ..addInt32(0)
              ..addInt16(fetches <= transientFetches ? 6 : 0)
              ..addInt64(0)
              ..addInt64(0)
              ..addInt32(-1)
              ..addBytes([]);
          } else if (key == 0) {
            produces++;
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
          if (closing) return;
          socket.add(KafkaRequest.frame(b.takeBytes()));
          await socket.flush();
        }
      }
    } catch (e) {
      if (!closing) errors.add(e);
    } finally {
      socket.destroy();
      raw.destroy();
    }
  }

  Future<void> close() async {
    closing = true;
    // Failed assertions must never strand an owned peer behind a test gate.
    for (final gate in [handshakeGate, versionsGate, authenticationGate]) {
      if (gate != null && !gate.isCompleted) gate.complete();
    }
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
  group('bounded TLS Fetch recovery', () {
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

    KafkaTlsFetchRecovery policy(
            {int attempts = 5,
            Duration window = const Duration(seconds: 5),
            Duration delay = Duration.zero}) =>
        KafkaTlsFetchRecovery(
            maxAttempts: attempts, window: window, retryDelay: delay);

    KafkaTlsConfig trust() => KafkaTlsConfig(
        trustedCertificates: File('$dir/ca.pem').readAsBytesSync(),
        withTrustedRoots: false);

    KafkaClient client(List<_Peer> endpoints,
        {KafkaTlsFetchRecovery? recovery,
        bool enabled = true,
        bool resource = false,
        int maxConnections = 4,
        int maxReadRetries = 10,
        KafkaTlsConfig? tlsOverride}) {
      final c = KafkaClient(
          brokers: endpoints.map((p) => p.broker).toList(),
          tls: tlsOverride ?? trust(),
          sasl: KafkaSaslPlain(
              username: env['KAFKA_BOOTSTRAP_TLS_USERNAME']!,
              password: env['KAFKA_BOOTSTRAP_TLS_PASSWORD']!),
          allowTlsBootstrapFallback: enabled,
          tlsFetchRecovery: recovery,
          maxReadRetries: maxReadRetries,
          retryBaseDelay: Duration.zero,
          maxConnections: maxConnections,
          maxBufferedResponseBytes: resource ? 128 : 65536,
          connectionTimeout: const Duration(seconds: 2),
          requestTimeout: const Duration(seconds: 2));
      clients.add(c);
      return c;
    }

    void recoveryFailure(Object? e,
        [String message = 'Kafka Fetch TLS recovery exhausted']) {
      expect(e, isA<HandshakeException>());
      expect((e as HandshakeException).message, message);
      expect(e.osError, isNull);
      expect(
          e.toString(), isNot(contains(env['KAFKA_BOOTSTRAP_TLS_PASSWORD']!)));
      expect(
          e.toString(), isNot(contains(env['KAFKA_BOOTSTRAP_TLS_USERNAME']!)));
      for (final p in peers) {
        expect(e.toString(), isNot(contains(p.server.port.toString())));
      }
    }

    void fetched(FetchResponse response, [String topic = 'topic']) {
      expect(response.topics.single.name, topic);
      expect(response.topics.single.partitions.single.records, isEmpty);
    }

    test('invalid security combinations fail before network', () async {
      final good = await peer('valid', 'good');
      expect(
          () => KafkaClient(brokers: [good.broker], tlsFetchRecovery: policy()),
          throwsArgumentError);
      expect(
          () => KafkaClient(
              brokers: [good.broker], tls: trust(), tlsFetchRecovery: policy()),
          throwsArgumentError);
      expect(
          () => KafkaClient(
              brokers: [good.broker],
              allowTlsBootstrapFallback: true,
              tlsFetchRecovery: policy()),
          throwsArgumentError);
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(good.accepts, 0);
    });

    for (final fallback in [false, true]) {
      test('null recovery preserves leader failure, fallback=$fallback',
          () async {
        final bootstrap = await peer('valid', 'bootstrap');
        final bad = await peer('untrusted', 'bad');
        final good = await peer('valid', 'good');
        bootstrap.leaders = [bad, good];
        final c = client([bootstrap], enabled: fallback);
        expect(await _error(c.fetch('topic')), isA<HandshakeException>());
        expect(bootstrap.metadataCalls, 1);
        expect(bad.accepts, 1);
        expect(good.accepts, 0);
      });
    }

    for (final cert in ['eof', 'untrusted', 'wronghost', 'expired']) {
      test('$cert leader rejected once then validated replacement', () async {
        final bootstrap = await peer('valid', 'bootstrap');
        final bad = await peer(cert, 'bad');
        final good = await peer('valid', 'good');
        bootstrap.leaders = [bad, good];
        final c = client([bootstrap],
            recovery: policy(), maxConnections: 1, maxReadRetries: 0);
        fetched(await c.fetch('topic'));
        expect(bootstrap.metadataCalls, 2);
        expect(bad.accepts, 1);
        expect(bad.keys, isEmpty);
        expect(good.keys, [17, 36, 18, 1]);
        expect(good.tlsConnections, 1);
        expect(good.fetches, 1);
      });
    }

    test('rejected bootstrap later advertised as leader is not retried',
        () async {
      final bad = await peer('untrusted', 'bad');
      final bootstrap = await peer('valid', 'bootstrap');
      final good = await peer('valid', 'good');
      bootstrap.leaders = [bad, good];
      final c = client([bad, bootstrap], recovery: policy());
      fetched(await c.fetch('topic'));
      expect(order, ['bad', 'bootstrap', 'good']);
      expect(bad.accepts, 1);
      expect(bad.keys, isEmpty);
      expect(bootstrap.metadataCalls, 2);
      expect(good.fetches, 1);
    });

    test('repeated stale discovered leader is skipped until replacement',
        () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final bad = await peer('wronghost', 'bad');
      final good = await peer('valid', 'good');
      bootstrap.leaders = [bad, bad, bad, good];
      final c = client([bootstrap], recovery: policy(attempts: 4));
      fetched(await c.fetch('topic'));
      expect(bootstrap.metadataCalls, 4);
      expect(bad.accepts, 1);
      expect(bad.keys, isEmpty);
      expect(good.fetches, 1);
    });

    test('all invalid bootstraps terminate without outer retries', () async {
      final bad = <_Peer>[];
      for (final cert in ['untrusted', 'wronghost', 'expired']) {
        bad.add(await peer(cert, cert));
      }
      final c = client([...bad, bad.first], recovery: policy());
      recoveryFailure(await _error(c.fetch('topic')),
          'Kafka bootstrap TLS discovery failed');
      expect(order, ['untrusted', 'wronghost', 'expired']);
      for (final p in bad) {
        expect(p.accepts, 1);
        expect(p.keys, isEmpty);
      }
    });

    test('TLS rejection plus transport exhaustion is terminal', () async {
      final bad = await peer('untrusted', 'bad');
      final transport = await peer('valid', 'transport', 'dropVersions');
      final c = client([bad, transport], recovery: policy());
      recoveryFailure(await _error(c.fetch('topic')),
          'Kafka bootstrap TLS discovery failed');
      expect(bad.accepts, 1);
      expect(transport.accepts, 1);
      expect(transport.keys, [17, 36, 18]);
    });

    test(
        'earlier leader rejection survives later metadata transport exhaustion',
        () async {
      final bootstrap = await peer('valid', 'bootstrap', 'dropLaterMetadata');
      final bad = await peer('expired', 'bad');
      bootstrap.leaders = [bad];
      final c = client([bootstrap], recovery: policy());
      recoveryFailure(await _error(c.fetch('topic')),
          'Kafka bootstrap TLS discovery failed');
      expect(bootstrap.keys.where((k) => k == 3).length, 2);
      expect(bootstrap.accepts, 1);
      expect(bad.accepts, 1);
    });

    test('later metadata TLS rejection joins the same exclusion set', () async {
      final bootstrap = await peer('valid', 'bootstrap', 'dropLaterMetadata');
      final badLeader = await peer('untrusted', 'badLeader');
      final badBootstrap = await peer('wronghost', 'badBootstrap');
      final replacement = await peer('valid', 'replacement');
      final good = await peer('valid', 'good');
      bootstrap.leaders = [badLeader];
      replacement.leaders = [badBootstrap, good];
      final c = client([bootstrap, badBootstrap, replacement],
          recovery: policy(attempts: 3));
      fetched(await c.fetch('topic'));
      expect(bootstrap.metadataCalls, 1);
      expect(bootstrap.keys.where((k) => k == 3).length, 3);
      expect(replacement.metadataCalls, 2);
      expect(badLeader.accepts, 1);
      expect(badBootstrap.accepts, 1);
      expect(badLeader.keys, isEmpty);
      expect(badBootstrap.keys, isEmpty);
      expect(good.fetches, 1);
    });

    for (final attempts in [1, 3]) {
      test('forever stale consumes exactly $attempts total rounds', () async {
        final bootstrap = await peer('valid', 'bootstrap');
        final bad = await peer('untrusted', 'bad');
        bootstrap.leaders = [bad];
        final c = client([bootstrap], recovery: policy(attempts: attempts));
        recoveryFailure(await _error(c.fetch('topic')));
        expect(bootstrap.metadataCalls, attempts);
        expect(bad.accepts, 1);
        expect(bad.keys, isEmpty);
      });
    }

    test('alternating rejected leaders share exclusion set and round budget',
        () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final first = await peer('untrusted', 'first');
      final second = await peer('wronghost', 'second');
      final good = await peer('valid', 'good');
      bootstrap.leaders = [first, second, first, second, good];
      final c = client([bootstrap], recovery: policy(attempts: 4));
      recoveryFailure(await _error(c.fetch('topic')));
      expect(bootstrap.metadataCalls, 4);
      expect(first.accepts, 1);
      expect(second.accepts, 1);
      expect(good.accepts, 0);
    });

    for (final earlierTls in [false, true]) {
      test('ordinary transient shares total budget, earlier TLS=$earlierTls',
          () async {
        final bootstrap = await peer('valid', 'bootstrap');
        final bad = await peer('untrusted', 'bad');
        final good = await peer('valid', 'good');
        good.transientFetches = 20;
        bootstrap.leaders = [if (earlierTls) bad, good];
        final c = client([bootstrap], recovery: policy(attempts: 3));
        final e = await _error(c.fetch('topic'));
        if (earlierTls) {
          recoveryFailure(e);
        } else {
          expect(
              e,
              isA<KafkaBrokerException>()
                  .having((e) => e.errorCode, 'code', 6));
        }
        expect(bootstrap.metadataCalls, 3);
        expect(good.fetches, earlierTls ? 2 : 3);
        expect(bad.accepts, earlierTls ? 1 : 0);
      });
    }

    test('transient Fetch after TLS recovery can succeed within shared budget',
        () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final bad = await peer('untrusted', 'bad');
      final good = await peer('valid', 'good');
      good.transientFetches = 1;
      bootstrap.leaders = [bad, good];
      final c = client([bootstrap], recovery: policy(attempts: 3));
      fetched(await c.fetch('topic'));
      expect(bootstrap.metadataCalls, 3);
      expect(bad.accepts, 1);
      expect(good.fetches, 2);
    });

    for (final earlierTls in [false, true]) {
      test('window checked after metadata versions, earlier TLS=$earlierTls',
          () async {
        final bad = await peer('untrusted', 'bad');
        final bootstrap = await peer('valid', 'bootstrap');
        bootstrap.versionsDelay = const Duration(milliseconds: 350);
        final c = client([if (earlierTls) bad, bootstrap],
            recovery: policy(window: const Duration(milliseconds: 200)));
        final e = await _error(c.fetch('topic'));
        if (earlierTls) {
          recoveryFailure(e, 'Kafka Fetch TLS recovery window expired');
        } else {
          expect(e, isA<TimeoutException>());
        }
        expect(bootstrap.keys, [17, 36, 18]);
        expect(bootstrap.metadataCalls, 0);
        expect(bootstrap.fetches, 0);
      });
    }

    test('expired rejected handshake cannot start next metadata endpoint',
        () async {
      final bad = await peer('untrusted', 'bad');
      final untouched = await peer('valid', 'untouched');
      bad.handshakeDelay = const Duration(milliseconds: 350);
      final c = client([bad, untouched],
          recovery: policy(window: const Duration(milliseconds: 200)));
      recoveryFailure(await _error(c.fetch('topic')),
          'Kafka Fetch TLS recovery window expired');
      expect(bad.accepts, 1);
      expect(bad.keys, isEmpty);
      expect(untouched.accepts, 0);
    });

    test('window checked after leader versions before Fetch enqueue', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final leader = await peer('valid', 'leader');
      leader.versionsDelay = const Duration(milliseconds: 400);
      bootstrap.leaders = [leader];
      final c = client([bootstrap],
          recovery: policy(window: const Duration(milliseconds: 250)));
      expect(await _error(c.fetch('topic')), isA<TimeoutException>());
      expect(bootstrap.metadataCalls, 1);
      expect(leader.keys, [17, 36, 18]);
      expect(leader.fetches, 0);
    });

    test('expired metadata response cannot start leader connection', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final leader = await peer('valid', 'leader');
      bootstrap.metadataDelay = const Duration(milliseconds: 350);
      bootstrap.leaders = [leader];
      final c = client([bootstrap],
          recovery: policy(window: const Duration(milliseconds: 200)));
      expect(await _error(c.fetch('topic')), isA<TimeoutException>());
      expect(bootstrap.metadataCalls, 1);
      expect(leader.accepts, 0);
    });

    test('retry delay cannot admit another round after window', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final bad = await peer('untrusted', 'bad');
      final good = await peer('valid', 'good');
      bootstrap.leaders = [bad, good];
      final c = client([bootstrap],
          recovery: policy(
              window: const Duration(milliseconds: 250),
              delay: const Duration(milliseconds: 500)));
      recoveryFailure(await _error(c.fetch('topic')),
          'Kafka Fetch TLS recovery window expired');
      expect(bootstrap.metadataCalls, 1);
      expect(bad.accepts, 1);
      expect(good.accepts, 0);
    });

    test('admitted Fetch may finish after polling window', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      bootstrap.fetchDelay = const Duration(milliseconds: 600);
      final c = client([bootstrap],
          recovery: policy(window: const Duration(milliseconds: 400)));
      final clock = Stopwatch()..start();
      fetched(await c.fetch('topic'));
      expect(clock.elapsed, greaterThan(const Duration(milliseconds: 400)));
      expect(bootstrap.fetches, 1);
    });

    test('close interrupts recovery delay without another round', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final bad = await peer('untrusted', 'bad');
      bootstrap.leaders = [bad];
      final c = client([bootstrap],
          recovery: policy(delay: const Duration(seconds: 1)));
      final failure = _error(c.fetch('topic'));
      await bad.accepted.future.timeout(const Duration(seconds: 2));
      await Future<void>.delayed(const Duration(milliseconds: 100));
      final clock = Stopwatch()..start();
      await c.close();
      expect(await failure, isA<StateError>());
      expect(clock.elapsed, lessThan(const Duration(milliseconds: 700)));
      expect(bootstrap.metadataCalls, 1);
      expect(bad.accepts, 1);
    });

    test('close interrupts connecting recovery peer without retry', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final stalled = await peer('stall', 'stalled');
      bootstrap.leaders = [stalled];
      final c = client([bootstrap], recovery: policy());
      final failure = _error(c.fetch('topic'));
      await stalled.accepted.future.timeout(const Duration(seconds: 2));
      final clock = Stopwatch()..start();
      await c.close();
      expect(await failure, isA<StateError>());
      expect(clock.elapsed, lessThan(const Duration(seconds: 1)));
      expect(stalled.accepts, 1);
      expect(bootstrap.metadataCalls, 1);
    });

    for (final stage in ['bootstrap', 'leader']) {
      for (final mode in [
        'auth',
        'mechanism',
        'unsupported',
        'malformed',
        'resource'
      ]) {
        test('$mode terminal after TLS rejection at $stage', () async {
          final bad = await peer('untrusted', 'bad');
          final bootstrap = await peer('valid', 'bootstrap');
          final terminal = await peer('valid', 'terminal', mode);
          final untouched = await peer('valid', 'untouched');
          bootstrap.leaders = [bad, terminal, untouched];
          final c = client(
              stage == 'bootstrap'
                  ? [bad, terminal, untouched]
                  : [bootstrap, untouched],
              recovery: policy(),
              resource: mode == 'resource');
          final e = await _error(c.fetch('topic'));
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
          expect(bad.accepts, 1);
          expect(bad.keys, isEmpty);
          expect(terminal.accepts, 1);
          expect(untouched.accepts, 0);
        });
      }
    }

    test('invalid local CA remains terminal before any network', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final untouched = await peer('valid', 'untouched');
      final c = client([bootstrap, untouched],
          recovery: policy(),
          tlsOverride: KafkaTlsConfig(
              trustedCertificates: [1, 2, 3], withTrustedRoots: false));
      final e = await _error(c.fetch('topic'));
      expect(e, isNotNull);
      expect(e.toString(), isNot(contains('Kafka Fetch TLS recovery')));
      expect(bootstrap.accepts, 0);
      expect(untouched.accepts, 0);
    });

    test('new calls get independent rejection sets', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final bad = await peer('untrusted', 'bad');
      final good = await peer('valid', 'good');
      bootstrap.leaders = [bad, good, bad, good];
      final c = client([bootstrap], recovery: policy(attempts: 2));
      fetched(await c.fetch('topic'));
      fetched(await c.fetch('topic'));
      expect(bad.accepts, 2);
      expect(bootstrap.metadataCalls, 4);
      expect(good.fetches, 2);
    });

    test('concurrent calls each retain their own round budget', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final first = await peer('untrusted', 'first');
      final second = await peer('wronghost', 'second');
      final good = await peer('valid', 'good');
      bootstrap.routes['one'] = [first, good];
      bootstrap.routes['two'] = [second, good];
      final c =
          client([bootstrap], recovery: policy(attempts: 2), maxConnections: 4);
      final results = await Future.wait([c.fetch('one'), c.fetch('two')]);
      fetched(results[0], 'one');
      fetched(results[1], 'two');
      expect(bootstrap.metadataByTopic, {'one': 2, 'two': 2});
      expect(first.accepts, 1);
      expect(second.accepts, 1);
      expect(good.fetches, 2);
    });

    test(
        'concurrent calls share rejected handshake but retain independent recovery',
        () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final bad = await peer('untrusted', 'bad');
      final good = await peer('valid', 'good');
      bad.handshakeGate = Completer<void>();
      bootstrap.routes['one'] = [bad, good];
      bootstrap.routes['two'] = [bad, good];
      final c = client([bootstrap], recovery: policy(attempts: 2));
      final results = Future.wait([c.fetch('one'), c.fetch('two')]);
      await bootstrap.metadataTwiceSeen.future
          .timeout(const Duration(seconds: 2));
      // This same-socket response is serialized after both leader replies.
      // The next event turn drains both callers' metadata continuations before
      // releasing the single shared TLS connection's rejected handshake.
      await c.send(MetadataRequest({'barrier'}), broker: bootstrap.broker);
      await Future<void>.delayed(Duration.zero);
      await bad.accepted.future.timeout(const Duration(seconds: 2));
      expect(bad.accepts, 1);
      bad.handshakeGate!.complete();
      final completed = await results;
      fetched(completed[0], 'one');
      fetched(completed[1], 'two');
      expect(bootstrap.metadataByTopic, {'one': 2, 'two': 2, 'barrier': 1});
      expect(bad.accepts, 1);
      expect(bad.keys, isEmpty);
      expect(good.tlsConnections, 1);
      expect(good.keys.where((key) => key == 18).length, 1);
      expect(good.fetches, 2);
    });

    test('shared versions future preserves each Fetch admission window',
        () async {
      final bootstrap = await peer('valid', 'bootstrap');
      bootstrap.versionsGate = Completer<void>();
      const window = Duration(milliseconds: 300);
      final c = client([bootstrap], recovery: policy(window: window));
      var earlierFinished = false;
      final earlier = _error(c.fetch('one')).then((e) {
        earlierFinished = true;
        return e;
      });
      await bootstrap.versionsSeen.future.timeout(const Duration(seconds: 2));
      // Crossing the admission window must not cancel already admitted work.
      // Keep the shared exchange pending until the later caller has joined it.
      await Future<void>.delayed(window + const Duration(milliseconds: 50));
      expect(earlierFinished, isFalse);
      final later = c.fetch('two');
      await Future<void>.delayed(Duration.zero);
      bootstrap.versionsGate!.complete();
      expect(
          await earlier,
          isA<TimeoutException>().having((e) => e.message, 'message',
              'Kafka Fetch TLS recovery window expired'));
      fetched(await later, 'two');
      expect(bootstrap.accepts, 1);
      expect(bootstrap.keys, [17, 36, 18, 3, 1]);
      expect(bootstrap.metadataByTopic, {'two': 1});
      expect(bootstrap.fetches, 1);
    });

    test('admitted SASL may finish after window but admits no version or data',
        () async {
      final bootstrap = await peer('valid', 'bootstrap');
      bootstrap.authenticationGate = Completer<void>();
      const window = Duration(milliseconds: 300);
      final c = client([bootstrap], recovery: policy(window: window));
      var finished = false;
      final failure = _error(c.fetch('topic')).then((e) {
        finished = true;
        return e;
      });
      await bootstrap.authenticationSeen.future
          .timeout(const Duration(seconds: 2));
      await Future<void>.delayed(window + const Duration(milliseconds: 50));
      // The policy is an admission window, not an overall cancellation timer:
      // an admitted authentication phase keeps its existing request deadline.
      expect(finished, isFalse);
      expect(bootstrap.keys, [17, 36]);
      bootstrap.authenticationGate!.complete();
      expect(
          await failure,
          isA<TimeoutException>().having((e) => e.message, 'message',
              'Kafka Fetch TLS recovery window expired'));
      expect(bootstrap.keys, [17, 36]);
      expect(bootstrap.versionsSeen.isCompleted, isFalse);
      expect(bootstrap.metadataCalls, 0);
      expect(bootstrap.fetches, 0);
      expect(bootstrap.accepts, 1);
      expect(bootstrap.tlsConnections, 1);
    });

    test('metadata ignores Fetch recovery budget and window', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      bootstrap.versionsDelay = const Duration(milliseconds: 100);
      final c = client([bootstrap],
          recovery:
              policy(attempts: 1, window: const Duration(microseconds: 1)));
      expect((await c.metadata({'topic'})).topics.single.name, 'topic');
      expect(bootstrap.metadataCalls, 1);
    });

    test('Produce ignores Fetch recovery window', () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final c = client([bootstrap],
          recovery:
              policy(attempts: 1, window: const Duration(microseconds: 1)));
      final result = await c.produce('topic', [
        KafkaRecord(value: [1])
      ]);
      expect(result!.responses.single.partitionResponses.single.baseOffset, 42);
      expect(bootstrap.keys.where((k) => k == 0).length, 1);
    });

    test('ambiguous Produce is sent once with recovery configured', () async {
      final bootstrap = await peer('valid', 'bootstrap', 'dropProduce');
      final c = client([bootstrap], recovery: policy());
      expect(
          await _error(c.produce('topic', [
            KafkaRecord(value: [1])
          ])),
          isA<KafkaConnectionException>());
      expect(bootstrap.metadataCalls, 1);
      expect(bootstrap.keys.where((k) => k == 0).length, 1);
    });

    test('Produce leader rejection never redirects with recovery configured',
        () async {
      final bootstrap = await peer('valid', 'bootstrap');
      final bad = await peer('untrusted', 'bad');
      final good = await peer('valid', 'good');
      bootstrap.leaders = [bad, good];
      final c = client([bootstrap, good], recovery: policy());
      expect(
          await _error(c.produce('topic', [
            KafkaRecord(value: [1])
          ])),
          isA<HandshakeException>());
      expect(bootstrap.metadataCalls, 1);
      expect(bad.accepts, 1);
      expect(good.accepts, 0);
    });

    test('low-level Fetch remains single attempt without discovery', () async {
      final bad = await peer('untrusted', 'bad');
      final good = await peer('valid', 'good');
      final c = client([bad, good], recovery: policy());
      expect(
          await _error(c.send(FetchRequest(-1, 100, 1, [
            FetchTopic('topic', [FetchPartition(0, 0, 1024)])
          ]))),
          isA<HandshakeException>());
      expect(bad.accepts, 1);
      expect(bad.keys, isEmpty);
      expect(good.accepts, 0);
    });
  },
      skip: configured ? false : 'Run the disposable TLS Fetch recovery runner',
      timeout: const Timeout(Duration(seconds: 15)));
}
