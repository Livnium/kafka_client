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
  late _Peer peer;
  late KafkaClient client;
  setUp(() async {
    peer = await _Peer.start();
    client = KafkaClient(
        brokers: [Broker('127.0.0.1', peer.server.port)],
        requestTimeout: const Duration(seconds: 3),
        maxReadRetries: 10);
  });
  tearDown(() async {
    await client.close();
    expect(client.pendingRequestCount, 0);
    expect(client.admittedRequestCount, 0);
    expect(client.connectionCount, 0);
    expect(client.queuedRequestBytes, 0);
    expect(client.bufferedResponseBytes, 0);
    await peer.close();
  });
  Future<Map<int, KafkaCommittedOffset?>> read(
          {int attempts = 3,
          Duration timeout = const Duration(seconds: 2),
          Duration delay = Duration.zero,
          KafkaCancellationToken? token,
          Set<int> partitions = const {0}}) =>
      client.committedOffsets('g', 'topic',
          partitions: partitions,
          requireStable: true,
          cancellationToken: token,
          stableOffsetRetry: KafkaStableOffsetRetry(
              maxAttempts: attempts, timeout: timeout, retryDelay: delay));
  test('88 retries exact v7 and returns stable result', () async {
    peer.handle = (r) => r.key == 10
        ? _coordinator(r, peer.server.port)
        : _fetched(r, errors: [peer.count(9) < 3 ? 88 : 0]);
    expect((await read())[0]!.nextOffset, 42);
    expect(peer.count(9), 3);
    expect(peer.count(10), 3);
    expect(peer.count(8), 0);
    expect(client.connectionCount, 0);
    expect(client.admittedRequestCount, 0);
  });
  test('attempts replace maxReadRetries without multiplication', () async {
    peer.handle = (r) => r.key == 10
        ? _coordinator(r, peer.server.port)
        : _fetched(r, errors: [88]);
    await expectLater(
        read(attempts: 2),
        throwsA(isA<KafkaBrokerException>()
            .having((e) => e.errorCode, 'code', 88)));
    expect(peer.count(9), 2);
    expect(peer.count(10), 2);
  });
  test('coordinator 88 does not acquire retry privilege', () async {
    peer.handle = (r) => _coordinator(r, peer.server.port, error: 88);
    await expectLater(read(), throwsA(isA<KafkaBrokerException>()));
    expect(peer.count(10), 1);
    expect(peer.count(9), 0);
  });
  for (final groupError in [0, 16, 88]) {
    test('terminal partition beats group $groupError and partition 88',
        () async {
      peer.handle = (r) => r.key == 10
          ? _coordinator(r, peer.server.port)
          : _fetched(r,
              groupError: groupError, indices: [0, 1], errors: [88, 29]);
      await expectLater(
          read(partitions: {0, 1}),
          throwsA(isA<KafkaBrokerException>()
              .having((e) => e.errorCode, 'code', 29)));
      expect(peer.count(9), 1);
    });
  }
  test('null remains absent and immutable', () async {
    peer.handle = (r) => r.key == 10
        ? _coordinator(r, peer.server.port)
        : _fetched(r, offsets: [-1]);
    final result = await read();
    expect(result[0], isNull);
    expect(() => result.clear(), throwsUnsupportedError);
  });
  test('invalid opt in rejects before network', () async {
    await expectLater(
        client.committedOffsets('g', 'topic',
            partitions: {0}, stableOffsetRetry: KafkaStableOffsetRetry()),
        throwsArgumentError);
    expect(peer.sockets, isEmpty);
    for (final attempts in [0, 21]) {
      expect(() => KafkaStableOffsetRetry(maxAttempts: attempts),
          throwsArgumentError);
    }
    expect(() => KafkaStableOffsetRetry(timeout: Duration.zero),
        throwsArgumentError);
  });
  for (final stall in [18, 10, 9]) {
    test('absolute deadline interrupts stalled API $stall', () async {
      if (stall == 18) peer.handleVersions = (_) {};
      peer.handle = (r) {
        if (r.key == stall) return;
        if (r.key == 10) _coordinator(r, peer.server.port);
      };
      final clock = Stopwatch()..start();
      await expectLater(read(timeout: const Duration(milliseconds: 80)),
          throwsA(isA<TimeoutException>()));
      expect(clock.elapsed, lessThan(const Duration(seconds: 1)));
      expect(client.connectionCount, 0);
      expect(client.admittedRequestCount, 0);
      expect(peer.count(9), stall == 9 ? 1 : 0);
    });
    test('cancellation interrupts stalled API $stall', () async {
      final token = KafkaCancellationToken();
      final entered = Completer<void>();
      if (stall == 18) peer.handleVersions = (_) => entered.complete();
      peer.handle = (r) {
        if (r.key == stall) {
          entered.complete();
          return;
        }
        if (r.key == 10) _coordinator(r, peer.server.port);
      };
      final pending = read(token: token);
      final expectation =
          expectLater(pending, throwsA(isA<KafkaCancelledException>()));
      await entered.future;
      token.cancel();
      await expectation;
      expect(client.connectionCount, 0);
      expect(client.admittedRequestCount, 0);
    });
  }
  test('close interrupts long retry delay', () async {
    final fetched = Completer<void>();
    peer.handle = (r) {
      if (r.key == 10) {
        _coordinator(r, peer.server.port);
        return;
      }
      _fetched(r, errors: [88]);
      fetched.complete();
    };
    final pending = read(delay: const Duration(seconds: 1));
    final expectation = expectLater(pending, throwsStateError);
    await fetched.future;
    await Future<void>.delayed(const Duration(milliseconds: 20));
    await client.close();
    await expectation;
    expect(peer.count(9), 1);
  });
  test('deadline in backoff admits no second discovery', () async {
    peer.handle = (r) => r.key == 10
        ? _coordinator(r, peer.server.port)
        : _fetched(r, errors: [88]);
    await expectLater(
        read(
            timeout: const Duration(milliseconds: 60),
            delay: const Duration(seconds: 1)),
        throwsA(isA<TimeoutException>()));
    expect(peer.count(10), 1);
    expect(peer.count(9), 1);
  });
  test('ordinary connection remains alive after scoped deadline', () async {
    await client.findGroupCoordinator('g');
    final ordinary = peer.sockets.single;
    peer.handle = (r) {
      if (r.key == 10) _coordinator(r, peer.server.port);
    };
    await expectLater(read(timeout: const Duration(milliseconds: 60)),
        throwsA(isA<TimeoutException>()));
    expect(client.connectionCount, 1);
    expect((await client.findGroupCoordinator('g')).port, peer.server.port);
    expect(peer.sockets.first, same(ordinary));
    expect(peer.sockets.length, 2);
  });
  test(
      'scoped read respects parent connection limit without disturbing busy traffic',
      () async {
    await client.close();
    client = KafkaClient(
        brokers: [Broker('127.0.0.1', peer.server.port)], maxConnections: 1);
    final entered = Completer<void>();
    _Request? held;
    peer.handle = (r) {
      held = r;
      entered.complete();
    };
    final ordinary = client.findGroupCoordinator('g');
    await entered.future;
    await expectLater(read(), throwsA(isA<KafkaResourceLimitException>()));
    expect(peer.sockets.length, 1);
    _coordinator(held!, peer.server.port);
    await ordinary;
  });
  test('canceled token does not connect', () async {
    final token = KafkaCancellationToken()..cancel();
    await expectLater(
        read(token: token), throwsA(isA<KafkaCancelledException>()));
    expect(peer.sockets, isEmpty);
  });
  for (final stop in ['deadline', 'cancel', 'close']) {
    test('$stop interrupts owned TLS handshake', () async {
      await client.close();
      final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final sockets = <Socket>[];
      final hello = Completer<void>();
      server.listen((socket) {
        sockets.add(socket);
        socket.listen((bytes) {
          if (!hello.isCompleted) hello.complete();
        }, onError: (Object _) {});
      });
      client = KafkaClient(
          brokers: [Broker('127.0.0.1', server.port)],
          tls: KafkaTlsConfig(),
          connectionTimeout: const Duration(seconds: 5));
      final token = KafkaCancellationToken();
      final pending = read(
          token: token,
          timeout: stop == 'deadline'
              ? const Duration(milliseconds: 100)
              : const Duration(seconds: 2));
      final expectation = expectLater(
          pending,
          throwsA(stop == 'deadline'
              ? isA<TimeoutException>()
              : stop == 'cancel'
                  ? isA<KafkaCancelledException>()
                  : isA<StateError>()));
      await hello.future;
      if (stop == 'cancel') token.cancel();
      if (stop == 'close') await client.close();
      await expectation;
      expect(client.connectionCount, 0);
      for (final socket in sockets) {
        socket.destroy();
      }
      await server.close();
    });
  }
  test(
      'one deadline spans ApiVersions and coordinator without late OffsetFetch',
      () async {
    final lateReply = Completer<void>();
    peer.handleVersions = (r) {
      Timer(const Duration(milliseconds: 100), () {
        final b = r.response()
          ..addInt16(0)
          ..addInt32(peer.versions.length);
        for (final entry in peer.versions.entries) {
          b
            ..addInt16(entry.key)
            ..addInt16(entry.value)
            ..addInt16(entry.value);
        }
        r.reply(b);
      });
    };
    peer.handle = (r) {
      expect(r.key, 10);
      Timer(const Duration(milliseconds: 180), () {
        try {
          _coordinator(r, peer.server.port);
        } finally {
          lateReply.complete();
        }
      });
    };
    await expectLater(read(timeout: const Duration(milliseconds: 230)),
        throwsA(isA<TimeoutException>()));
    await lateReply.future;
    expect(peer.count(18), 1);
    expect(peer.count(10), 1);
    expect(peer.count(9), 0);
    expect(client.connectionCount, 0);
    expect(client.pendingRequestCount, 0);
    expect(client.admittedRequestCount, 0);
  });
  for (final terminal in [3, 29]) {
    for (final errors in [
      [terminal, 88],
      [88, terminal]
    ]) {
      test('terminal $terminal takes precedence in order $errors', () async {
        peer.handle = (r) => r.key == 10
            ? _coordinator(r, peer.server.port)
            : _fetched(r, groupError: 88, indices: [0, 1], errors: errors);
        await expectLater(
            read(partitions: {0, 1}),
            throwsA(isA<KafkaBrokerException>()
                .having((e) => e.errorCode, 'code', terminal)));
        expect(peer.count(9), 1);
        expect(peer.count(10), 1);
      });
    }
  }
  for (final errors in [
    [88, 16],
    [16, 88]
  ]) {
    test('88 and16 only order $errors retries then succeeds', () async {
      peer.handle = (r) => r.key == 10
          ? _coordinator(r, peer.server.port)
          : _fetched(r,
              indices: [0, 1], errors: peer.count(9) == 1 ? errors : [0, 0]);
      expect((await read(partitions: {0, 1})).length, 2);
      expect(peer.count(9), 2);
      expect(peer.count(10), 2);
    });
  }
  test('complete bounds and cancellation without policy fail before I/O',
      () async {
    for (final timeout in [
      Duration.zero,
      const Duration(microseconds: -1),
      const Duration(minutes: 1, microseconds: 1)
    ]) {
      expect(
          () => KafkaStableOffsetRetry(timeout: timeout), throwsArgumentError);
    }
    for (final delay in [
      const Duration(microseconds: -1),
      const Duration(seconds: 1, microseconds: 1)
    ]) {
      expect(
          () => KafkaStableOffsetRetry(retryDelay: delay), throwsArgumentError);
    }
    expect(
        () => KafkaStableOffsetRetry(
            maxAttempts: 1,
            timeout: const Duration(microseconds: 1),
            retryDelay: Duration.zero),
        returnsNormally);
    expect(
        () => KafkaStableOffsetRetry(
            maxAttempts: 20,
            timeout: const Duration(minutes: 1),
            retryDelay: const Duration(seconds: 1)),
        returnsNormally);
    await expectLater(
        client.committedOffsets('g', 'topic',
            partitions: {0},
            requireStable: true,
            cancellationToken: KafkaCancellationToken()),
        throwsArgumentError);
    expect(peer.sockets, isEmpty);
  });
}
