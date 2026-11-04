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
  static Future<_Peer> start() async {
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
    peer.server.listen((socket) {
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
  late _Peer bootstrap;
  late KafkaClient client;
  setUp(() async {
    bootstrap = await _Peer.start();
    client = KafkaClient(
        brokers: [Broker('127.0.0.1', bootstrap.server.port)],
        maxReadRetries: 2,
        retryBaseDelay: Duration.zero,
        requestTimeout: const Duration(milliseconds: 200));
  });
  tearDown(() async {
    await client.close();
    expect(client.pendingRequestCount, 0);
    expect(client.admittedRequestCount, 0);
    expect(client.connectionCount, 0);
    await bootstrap.close();
  });
  Future<Map<int, KafkaCommittedOffset?>> read(
          {Set<int> partitions = const {0}}) =>
      client.committedOffsets('g', 'topic',
          partitions: partitions, requireStable: true);
  test('explicit stable request encodes true and returns exact next offset',
      () async {
    expect((await read())[0]!.nextOffset, 42);
    expect(bootstrap.count(9), 1);
    expect(bootstrap.count(8), 0);
  });
  test('missing remains null with no second request or local replacement',
      () async {
    bootstrap.handle = (r) => r.key == 10
        ? _coordinator(r, bootstrap.server.port)
        : _fetched(r, offsets: [-1]);
    expect((await read())[0], isNull);
    expect(bootstrap.count(9), 1);
    expect(bootstrap.count(8), 0);
  });
  for (final version in [5, 6, 8]) {
    test('unsupported exact v7 (only v$version) rejected before dispatch',
        () async {
      bootstrap.versions[9] = version;
      await expectLater(read(), throwsA(isA<UnsupportedError>()));
      expect(bootstrap.count(9), 0);
      expect(bootstrap.count(8), 0);
    });
  }
  for (final code in [23, 29, 30, 88]) {
    for (final group in [false, true]) {
      test('error $code group=$group is terminal, never null/fallback/commit',
          () async {
        bootstrap.handle = (r) => r.key == 10
            ? _coordinator(r, bootstrap.server.port)
            : _fetched(r,
                groupError: group ? code : 0, errors: [group ? 0 : code]);
        await expectLater(
            read(),
            throwsA(isA<KafkaBrokerException>()
                .having((e) => e.errorCode, 'error', code)));
        expect(bootstrap.count(9), 1);
        expect(bootstrap.count(10), 1);
        expect(bootstrap.count(8), 0);
      });
    }
  }
  for (final scenario in [
    'topic',
    'missing',
    'extra',
    'duplicate',
    'unexpected'
  ]) {
    test('exact response matching: $scenario', () async {
      bootstrap.handle = (r) {
        if (r.key == 10) {
          _coordinator(r, bootstrap.server.port);
          return;
        }
        _fetched(r,
            topic: scenario == 'topic' ? 'wrong' : 'topic',
            indices: switch (scenario) {
              'missing' => [0],
              'extra' => [0, 1, 2],
              'duplicate' => [0, 0],
              'unexpected' => [0, 2],
              _ => [0, 1]
            });
      };
      await expectLater(read(partitions: {0, 1}), throwsFormatException);
      expect(bootstrap.count(9), 1);
      expect(bootstrap.count(8), 0);
    });
  }
  test('stable flag retained across existing coordinator-error retry',
      () async {
    bootstrap.handle = (r) {
      if (r.key == 10) {
        _coordinator(r, bootstrap.server.port);
        return;
      }
      _fetched(r, groupError: bootstrap.count(9) == 1 ? 16 : 0);
    };
    expect((await read())[0]!.nextOffset, 42);
    expect(bootstrap.count(9), 2);
    expect(bootstrap.count(10), 2);
    expect(bootstrap.count(8), 0);
  });
  test('timeout retains existing read budget without fallback or commit replay',
      () async {
    bootstrap.handle = (r) {
      if (r.key == 10) {
        _coordinator(r, bootstrap.server.port);
      } else {
        expect(r.version, 7);
      }
    };
    await expectLater(read(), throwsA(isA<TimeoutException>()));
    expect(bootstrap.count(9), 3);
    expect(bootstrap.count(8), 0);
  });
  test('missing OffsetFetch API rejects before request', () async {
    bootstrap.versions.remove(9);
    await expectLater(read(), throwsA(isA<UnsupportedError>()));
    expect(bootstrap.count(9), 0);
    expect(bootstrap.count(8), 0);
  });
  for (final codes in [
    [16, 88],
    [88, 16]
  ]) {
    test('unstable88 wins over coordinator retry error $codes', () async {
      bootstrap.handle = (r) => r.key == 10
          ? _coordinator(r, bootstrap.server.port)
          : _fetched(r, indices: [0, 1], errors: codes);
      await expectLater(
          read(partitions: {0, 1}),
          throwsA(isA<KafkaBrokerException>()
              .having((e) => e.errorCode, 'error', 88)));
      expect(bootstrap.count(9), 1);
      expect(bootstrap.count(10), 1);
      expect(bootstrap.count(8), 0);
    });
  }
  test('new coordinator capability checked after existing read retry',
      () async {
    final moved = await _Peer.start();
    moved.versions[9] = 5;
    try {
      bootstrap.handle = (r) {
        if (r.key == 10) {
          _coordinator(
              r,
              bootstrap.count(10) == 1
                  ? bootstrap.server.port
                  : moved.server.port);
          return;
        }
        _fetched(r, groupError: 16);
      };
      await expectLater(read(), throwsA(isA<UnsupportedError>()));
      expect(bootstrap.count(9), 1);
      expect(bootstrap.count(10), 2);
      expect(moved.count(18), 1);
      expect(moved.count(9), 0);
      expect(moved.count(8), 0);
    } finally {
      await moved.close();
    }
  });
}
