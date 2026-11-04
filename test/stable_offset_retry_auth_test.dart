import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

// Owned loopback protocol peers establish routing/replay behavior, not broker
// persistence, group ownership, transactional stability or security guarantees.
class _Request {
  final Socket socket;
  final int key, version, id;
  final KafkaBytesReader body;
  _Request(this.socket, this.key, this.version, this.id, this.body);
  void reply(KafkaBytesBuilder bytes) =>
      socket.add(KafkaRequest.frame(bytes.takeBytes()));
  KafkaBytesBuilder response() => KafkaBytesBuilder()..addInt32(id);
}

class _Peer {
  final ServerSocket server;
  final sockets = <Socket>[];
  final counts = <int, int>{};
  final versions = <int, int>{10: 2, 9: 7, 8: 6};
  late void Function(_Request) handle;
  void Function(_Request)? handleVersions;
  _Peer._(this.server);
  int count(int key) => counts[key] ?? 0;
  static Future<_Peer> start(SecurityContext context) async {
    final peer =
        _Peer._(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));
    peer.handle = (r) {
      if (r.key == 10) {
        _coordinator(r, peer.server.port);
      } else if (r.key == 9) {
        _fetched(r);
      } else if (r.key == 8) {
        _committed(r);
      } else {
        fail('Unexpected API ${r.key}');
      }
    };
    peer.server.listen((raw) async {
      peer.sockets.add(raw);
      late SecureSocket socket;
      try {
        socket = await SecureSocket.secureServer(raw, context);
      } catch (_) {
        raw.destroy();
        return;
      }
      peer.sockets.add(socket);
      final buffer = <int>[];
      socket.listen((chunk) {
        buffer.addAll(chunk);
        while (buffer.length >= 4) {
          final size = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (buffer.length < size + 4) break;
          final body = KafkaBytesReader.fromBytes(buffer.sublist(4, size + 4));
          buffer.removeRange(0, size + 4);
          final key = body.readInt16();
          final version = body.readInt16();
          final id = body.readInt32();
          body.readString();
          final request = _Request(socket, key, version, id, body);
          peer.counts.update(key, (n) => n + 1, ifAbsent: () => 1);
          if (key == 18) {
            if (peer.handleVersions != null) {
              peer.handleVersions!(request);
              continue;
            }
            final b = request.response()
              ..addInt16(0)
              ..addInt32(peer.versions.length);
            for (final entry in peer.versions.entries) {
              b
                ..addInt16(entry.key)
                ..addInt16(entry.value)
                ..addInt16(entry.value);
            }
            request.reply(b);
          } else {
            peer.handle(request);
          }
        }
      }, onError: (Object _) {});
    });
    return peer;
  }

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
  }
}

void _coordinator(_Request r, int port,
    {int error = 0, int nodeId = 1, String host = '127.0.0.1'}) {
  expect(r.version, 2);
  r.reply(r.response()
    ..addInt32(0)
    ..addInt16(error)
    ..addString(null)
    ..addInt32(nodeId)
    ..addString(host)
    ..addInt32(port));
}

void _fetched(_Request r,
    {String topic = 'topic',
    List<int> indices = const [0],
    List<int>? errors,
    List<int>? offsets,
    int groupError = 0,
    int topicCount = 1}) {
  expect(r.version, 7);
  expect(r.body.readInt8(), 0); // Header tags.
  expect(r.body.readInt8(), 2);
  expect(r.body.readRaw(1), utf8.encode('g'));
  expect(r.body.readInt8(), 2);
  expect(r.body.readInt8(), 6);
  expect(r.body.readRaw(5), utf8.encode('topic'));
  final count = r.body.readInt8() - 1;
  for (var i = 0; i < count; i++) {
    r.body.readInt32();
  }
  expect(r.body.readInt8(), 0);
  expect(r.body.readBoolean(), isTrue);
  expect(r.body.readInt8(), 0);
  expect(r.body.isEOF, isTrue);
  final b = r.response()
    ..addInt8(0)
    ..addInt32(0)
    ..addInt8(topicCount + 1);
  for (var t = 0; t < topicCount; t++) {
    b
      ..addInt8(utf8.encode(topic).length + 1)
      ..addRaw(utf8.encode(topic))
      ..addInt8(indices.length + 1);
    for (var i = 0; i < indices.length; i++) {
      b
        ..addInt32(indices[i])
        ..addInt64(offsets?[i] ?? 42)
        ..addInt32(3)
        ..addInt8(0)
        ..addInt16(errors?[i] ?? 0)
        ..addInt8(0);
    }
    b.addInt8(0);
  }
  b
    ..addInt16(groupError)
    ..addInt8(0);
  r.reply(b);
}

void _committed(_Request r,
    {String topic = 'topic',
    List<int> indices = const [0],
    List<int>? errors,
    int topicCount = 1}) {
  expect(r.version, 6);
  final b = r.response()
    ..addInt32(0)
    ..addInt32(topicCount);
  for (var t = 0; t < topicCount; t++) {
    b
      ..addString(topic)
      ..addInt32(indices.length);
    for (var i = 0; i < indices.length; i++) {
      b
        ..addInt32(indices[i])
        ..addInt16(errors?[i] ?? 0);
    }
  }
  r.reply(b);
}

void main() {
  late Directory certificates;
  late SecurityContext context;
  late _Peer peer;
  late KafkaClient client;
  setUpAll(() async {
    certificates = await Directory.systemTemp.createTemp('stable-offset-auth-');
    // macOS requires serverAuth EKU even for this explicitly trusted, owned
    // loopback certificate. Keep platform validation enabled in the client.
    final result = await Process.run('openssl', [
      'req',
      '-x509',
      '-newkey',
      'rsa:2048',
      '-nodes',
      '-keyout',
      '${certificates.path}/key.pem',
      '-out',
      '${certificates.path}/cert.pem',
      '-days',
      '1',
      '-subj',
      '/CN=localhost',
      '-addext',
      'subjectAltName=DNS:localhost',
      '-addext',
      'extendedKeyUsage=serverAuth'
    ]);
    if (result.exitCode != 0) {
      throw StateError('Owned TLS certificate generation failed');
    }
    context = SecurityContext()
      ..useCertificateChain('${certificates.path}/cert.pem')
      ..usePrivateKey('${certificates.path}/key.pem');
  });
  tearDownAll(() async => certificates.delete(recursive: true));
  setUp(() async {
    peer = await _Peer.start(context);
    client = KafkaClient(
        brokers: [Broker('localhost', peer.server.port)],
        tls: KafkaTlsConfig(
            trustedCertificates:
                File('${certificates.path}/cert.pem').readAsBytesSync(),
            withTrustedRoots: false),
        sasl: KafkaSaslPlain(
            username: 'fixture-user', password: 'fixture-secret'),
        requestTimeout: const Duration(seconds: 3));
  });
  tearDown(() async {
    await client.close();
    expect(client.connectionCount, 0);
    expect(client.admittedRequestCount, 0);
    expect(client.pendingRequestCount, 0);
    expect(client.queuedRequestBytes, 0);
    await peer.close();
  });
  for (final failure in ['untrusted certificate', 'wrong hostname']) {
    test('TLS rejects $failure before SASL', () async {
      await client.close();
      client = KafkaClient(
          brokers: [
            Broker(failure == 'wrong hostname' ? '127.0.0.1' : 'localhost',
                peer.server.port)
          ],
          tls: failure == 'wrong hostname'
              ? KafkaTlsConfig(
                  trustedCertificates:
                      File('${certificates.path}/cert.pem').readAsBytesSync(),
                  withTrustedRoots: false)
              : KafkaTlsConfig(),
          sasl: KafkaSaslPlain(
              username: 'fixture-user', password: 'fixture-secret'),
          requestTimeout: const Duration(seconds: 3));
      await expectLater(client.committedOffsets('g', 'topic', partitions: {0}),
          throwsA(isA<TlsException>()));
      for (final key in [17, 36, 18, 10, 9]) {
        expect(peer.count(key), 0);
      }
    });
  }
  for (final key in [17, 36]) {
    for (final stop in ['deadline', 'cancel', 'close']) {
      test('$stop during SASL API $key admits no later phase', () async {
        final entered = Completer<void>();
        peer.handle = (r) {
          if (r.key == key) {
            entered.complete();
            return;
          }
          if (r.key == 17) {
            r.reply(r.response()
              ..addInt16(0)
              ..addInt32(1)
              ..addString('PLAIN'));
          } else {
            fail('Unexpected key ${r.key}');
          }
        };
        final token = KafkaCancellationToken();
        final pending = client.committedOffsets('g', 'topic',
            partitions: {0},
            requireStable: true,
            cancellationToken: token,
            stableOffsetRetry: KafkaStableOffsetRetry(
                timeout: stop == 'deadline'
                    ? const Duration(milliseconds: 150)
                    : const Duration(seconds: 2)));
        final expectation = expectLater(
            pending,
            throwsA(stop == 'deadline'
                ? isA<TimeoutException>()
                : stop == 'cancel'
                    ? isA<KafkaCancelledException>()
                    : isA<StateError>()));
        await entered.future;
        if (stop == 'cancel') token.cancel();
        if (stop == 'close') await client.close();
        await expectation;
        expect(peer.count(18), 0);
        expect(peer.count(10), 0);
        expect(peer.count(9), 0);
        expect(client.connectionCount, 0);
        expect(client.admittedRequestCount, 0);
      });
    }
  }
}
