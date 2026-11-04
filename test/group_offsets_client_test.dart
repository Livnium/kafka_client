import 'dart:async';
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
  final versions = <int, int>{10: 2, 9: 5, 8: 6};
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
  expect(r.version, 5);
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
        ..addInt64(offsets?[i] ?? 42)
        ..addInt32(3)
        ..addString('saved')
        ..addInt16(errors?[i] ?? 0);
    }
  }
  b.addInt16(groupError);
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
  late _Peer bootstrap;
  final peers = <_Peer>[];
  final clients = <KafkaClient>[];
  Future<_Peer> peer() async {
    final p = await _Peer.start();
    peers.add(p);
    return p;
  }

  KafkaClient client({int retries = 2, int timeoutMs = 2000, int delayMs = 0}) {
    final c = KafkaClient(
        brokers: [Broker('127.0.0.1', bootstrap.server.port)],
        maxReadRetries: retries,
        retryBaseDelay: Duration(milliseconds: delayMs),
        requestTimeout: Duration(milliseconds: timeoutMs));
    clients.add(c);
    return c;
  }

  void settled(KafkaClient c) {
    expect(c.pendingRequestCount, 0);
    expect(c.admittedRequestCount, 0);
    expect(c.connectionCount, 0);
    expect(c.queuedRequestBytes, 0);
    expect(c.bufferedResponseBytes, 0);
  }

  setUp(() async {
    bootstrap = await peer();
  });
  tearDown(() async {
    for (final c in clients) {
      await c.close();
      settled(c);
    }
    for (final p in peers) {
      await p.close();
    }
    clients.clear();
    peers.clear();
  });

  test('invalid inputs fail asynchronously before connecting', () async {
    final c = client();
    final long = List.filled(11000, '界').join();
    final invalid = <Future<dynamic> Function()>[
      () => c.findGroupCoordinator(''),
      () => c.findGroupCoordinator(long),
      () => c.committedOffsets('', 'topic', partitions: {0}),
      () => c.committedOffsets(long, 'topic', partitions: {0}),
      () => c.committedOffsets('g', '', partitions: {0}),
      () => c.committedOffsets('g', long, partitions: {0}),
      () => c.committedOffsets('g', 'topic', partitions: {}),
      () => c.committedOffsets('g', 'topic', partitions: {-1}),
      () => c.committedOffsets('g', 'topic', partitions: {2147483648}),
      () => c.commitOffsets('', 'topic', nextOffsets: {0: 1}),
      () => c.commitOffsets(long, 'topic', nextOffsets: {0: 1}),
      () => c.commitOffsets('g', '', nextOffsets: {0: 1}),
      () => c.commitOffsets('g', long, nextOffsets: {0: 1}),
      () => c.commitOffsets('g', 'topic', nextOffsets: {}),
      () => c.commitOffsets('g', 'topic', nextOffsets: {-1: 0}),
      () => c.commitOffsets('g', 'topic', nextOffsets: {2147483648: 0}),
      () => c.commitOffsets('g', 'topic', nextOffsets: {0: -1}),
      () => c.commitOffsets('g', 'topic', nextOffsets: {0: 1}, metadata: long),
    ];
    for (final action in invalid) {
      late Future<dynamic> result;
      expect(() {
        result = action();
      }, returnsNormally);
      await expectLater(result, throwsArgumentError);
    }
    expect(bootstrap.sockets, isEmpty);
    expect(bootstrap.counts, isEmpty);
  });

  test('closed client wins over invalid inputs without connection', () async {
    final c = client();
    await c.close();
    await expectLater(c.findGroupCoordinator(''), throwsStateError);
    await expectLater(
        c.committedOffsets('', '', partitions: {}), throwsStateError);
    await expectLater(
        c.commitOffsets('', '', nextOffsets: {}), throwsStateError);
    expect(bootstrap.sockets, isEmpty);
  });

  test('routes to coordinator and preserves absent offset and immutable result',
      () async {
    final target = await peer();
    bootstrap.handle = (r) => _coordinator(r, target.server.port);
    target.handle = (r) => _fetched(r, indices: [0, 1], offsets: [42, -1]);
    final result =
        await client().committedOffsets('g', 'topic', partitions: {0, 1});
    expect(result[0]!.nextOffset, 42);
    expect(result[0]!.leaderEpoch, 3);
    expect(result[0]!.metadata, 'saved');
    expect(result.containsKey(1), isTrue);
    expect(result[1], isNull);
    expect(() => result[2] = null, throwsUnsupportedError);
    expect(bootstrap.counts, {18: 1, 10: 1});
    expect(target.counts, {18: 1, 9: 1});
  });

  for (final committing in [false, true]) {
    test(
        'snapshots caller collection before discovery await (commit=$committing)',
        () async {
      final seen = Completer<_Request>();
      bootstrap.handle = (r) {
        if (r.key == 10) {
          seen.complete(r);
          return;
        }
        final b = r.body;
        expect(b.readString(), 'g');
        if (committing) {
          expect(b.readInt32(), -1);
          expect(b.readString(), '');
        }
        expect(b.readInt32(), 1);
        expect(b.readString(), 'topic');
        expect(b.readInt32(), 1);
        expect(b.readInt32(), 0);
        if (committing) {
          expect(b.readInt64(), 8);
          expect(b.readInt32(), -1);
          expect(b.readString(), 'meta');
          _committed(r);
        } else {
          _fetched(r);
        }
        expect(b.isEOF, isTrue);
      };
      final c = client();
      final indices = <int>{0};
      final offsets = <int, int>{0: 8};
      final operation = committing
          ? c.commitOffsets('g', 'topic',
              nextOffsets: offsets, metadata: 'meta')
          : c.committedOffsets('g', 'topic', partitions: indices);
      indices
        ..clear()
        ..add(3);
      offsets
        ..clear()
        ..[3] = 99;
      _coordinator(await seen.future, bootstrap.server.port);
      await operation;
      expect(bootstrap.count(10), 1);
      expect(bootstrap.count(committing ? 8 : 9), 1);
    });
  }

  for (final key in [10, 9, 8]) {
    test('unsupported API $key is checked on actual target connection',
        () async {
      final target = key == 10 ? bootstrap : await peer();
      target.versions.remove(key);
      if (key != 10) {
        bootstrap.handle = (r) => _coordinator(r, target.server.port);
      }
      final c = client();
      final Future<dynamic> result = key == 10
          ? c.findGroupCoordinator('g')
          : key == 9
              ? c.committedOffsets('g', 'topic', partitions: {0})
              : c.commitOffsets('g', 'topic', nextOffsets: {0: 1});
      await expectLater(result, throwsA(isA<UnsupportedError>()));
      expect(target.count(key), 0);
      expect(target.count(18), 1);
      if (key != 10) expect(bootstrap.count(10), 1);
    });
  }

  for (final committing in [false, true]) {
    for (final shape in [
      'duplicate',
      'missing',
      'unexpected',
      'wrongTopic',
      'duplicateTopic'
    ]) {
      test('rejects $shape response identities (commit=$committing)', () async {
        bootstrap.handle = (r) {
          if (r.key == 10) {
            _coordinator(r, bootstrap.server.port);
            return;
          }
          final indices = shape == 'duplicate'
              ? [0, 0]
              : shape == 'missing'
                  ? [0]
                  : shape == 'unexpected'
                      ? [0, 2]
                      : [0, 1];
          final topic = shape == 'wrongTopic' ? 'other' : 'topic';
          final count = shape == 'duplicateTopic' ? 2 : 1;
          if (committing) {
            _committed(r, topic: topic, indices: indices, topicCount: count);
          } else {
            _fetched(r, topic: topic, indices: indices, topicCount: count);
          }
        };
        final c = client(retries: 3);
        await expectLater(
            committing
                ? c.commitOffsets('g', 'topic', nextOffsets: {0: 1, 1: 2})
                : c.committedOffsets('g', 'topic', partitions: {0, 1}),
            throwsFormatException);
        expect(bootstrap.count(10), 1);
        expect(bootstrap.count(committing ? 8 : 9), 1);
      });
    }
  }

  for (final phase in ['discovery', 'group', 'partition']) {
    for (final code in [14, 15, 16, 22, 29, 30, 31]) {
      test('$phase error $code has bounded retry classification', () async {
        bootstrap.handle = (r) {
          if (r.key == 10) {
            _coordinator(r, bootstrap.server.port,
                error: phase == 'discovery' ? code : 0);
          } else {
            _fetched(r,
                groupError: phase == 'group' ? code : 0,
                errors: [phase == 'partition' ? code : 0]);
          }
        };
        final c = client();
        await expectLater(
            c.committedOffsets('g', 'topic', partitions: {0}),
            throwsA(isA<KafkaBrokerException>()
                .having((e) => e.errorCode, 'code', code)));
        final attempts = [14, 15, 16].contains(code) ? 3 : 1;
        expect(bootstrap.count(10), attempts);
        expect(bootstrap.count(9), phase == 'discovery' ? 0 : attempts);
      });
    }
  }

  test('terminal partition errors win over earlier retryable errors', () async {
    bootstrap.handle = (r) => r.key == 10
        ? _coordinator(r, bootstrap.server.port)
        : _fetched(r, indices: [0, 1], errors: [16, 30]);
    await expectLater(
        client().committedOffsets('g', 'topic', partitions: {0, 1}),
        throwsA(isA<KafkaBrokerException>()
            .having((e) => e.errorCode, 'code', 30)));
    expect(bootstrap.count(10), 1);
    expect(bootstrap.count(9), 1);
  });

  for (final failure in ['stale', 'disconnect', 'timeout']) {
    test('read $failure rediscovers and routes to replacement coordinator',
        () async {
      final stale = await peer();
      final replacement = await peer();
      bootstrap.handle = (r) => _coordinator(
          r,
          bootstrap.count(10) == 1
              ? stale.server.port
              : replacement.server.port);
      stale.handle = (r) {
        if (failure == 'stale') _fetched(r, errors: [16]);
        if (failure == 'disconnect') r.socket.destroy();
      };
      final result = await client(timeoutMs: 150)
          .committedOffsets('g', 'topic', partitions: {0});
      expect(result[0]!.nextOffset, 42);
      expect(bootstrap.count(10), 2);
      expect(stale.count(9), 1);
      expect(replacement.count(9), 1);
    });
  }

  for (final failure in [
    'disconnect',
    'timeout',
    'malformed',
    'coordinator',
    'partial'
  ]) {
    test(
        'commit $failure never replays after dispatch with read retries enabled',
        () async {
      bootstrap.handle = (r) {
        if (r.key == 10) {
          _coordinator(r, bootstrap.server.port);
          return;
        }
        if (failure == 'disconnect') {
          r.socket.destroy();
          return;
        }
        if (failure == 'timeout') return;
        if (failure == 'malformed') {
          r.reply(r.response()..addInt8(0));
          return;
        }
        _committed(r,
            indices: [0, 1], errors: failure == 'partial' ? [0, 30] : [16, 16]);
      };
      final c = client(retries: 5, timeoutMs: 150);
      final expected = failure == 'disconnect'
          ? isA<KafkaConnectionException>()
          : failure == 'timeout'
              ? isA<TimeoutException>()
              : failure == 'malformed'
                  ? isA<FormatException>()
                  : isA<KafkaOffsetCommitException>();
      await expectLater(
          c.commitOffsets('g', 'topic', nextOffsets: {0: 1, 1: 2}),
          throwsA(expected));
      expect(bootstrap.count(10), 1);
      expect(bootstrap.count(8), 1);
    });
  }

  test('partial commit exception keeps complete immutable acknowledged map',
      () async {
    bootstrap.handle = (r) => r.key == 10
        ? _coordinator(r, bootstrap.server.port)
        : _committed(r, indices: [0, 1], errors: [0, 30]);
    try {
      await client().commitOffsets('g', 'topic', nextOffsets: {0: 1, 1: 2});
      fail('Expected partial acknowledgement');
    } on KafkaOffsetCommitException catch (e) {
      expect(e.partitionErrors, {0: 0, 1: 30});
      expect(e.successfulPartitions, {0});
      expect(() => e.partitionErrors[1] = 0, throwsUnsupportedError);
      expect(() => e.successfulPartitions.add(1), throwsUnsupportedError);
    }
    expect(bootstrap.count(8), 1);
  });

  test('pre-send discovery retries still dispatch exactly one commit',
      () async {
    bootstrap.handle = (r) => r.key == 10
        ? _coordinator(r, bootstrap.server.port,
            error: bootstrap.count(10) == 1 ? 15 : 0)
        : _committed(r);
    await client().commitOffsets('g', 'topic', nextOffsets: {0: 1});
    expect(bootstrap.count(10), 2);
    expect(bootstrap.count(8), 1);
  });

  test('close interrupts stalled discovery and settles resource counters',
      () async {
    final received = Completer<void>();
    bootstrap.handle = (_) => received.complete();
    final c = client();
    final checked = expectLater(
        c.commitOffsets('g', 'topic', nextOffsets: {0: 1}), throwsStateError);
    await received.future;
    await c.close();
    await checked;
    settled(c);
    expect(bootstrap.count(10), 1);
    expect(bootstrap.count(8), 0);
  });

  test('close interrupts held group retry backoff without another request',
      () async {
    final c = client(delayMs: 1000, timeoutMs: 10000);
    await c.findGroupCoordinator(
        'g'); // Warm transport outside timer interception.
    bootstrap.handle = (r) => _coordinator(r, bootstrap.server.port, error: 15);
    final waiting = Completer<void>();
    final timers = <_HeldTimer>[];
    final result =
        runZoned(() => c.committedOffsets('g', 'topic', partitions: {0}),
            zoneSpecification: ZoneSpecification(
                createTimer: (self, parent, zone, duration, callback) {
      if (duration <= const Duration(seconds: 1)) {
        final timer = _HeldTimer();
        timers.add(timer);
        waiting.complete();
        return timer;
      }
      return parent.createTimer(zone, duration, callback);
    }));
    final checked = expectLater(result, throwsStateError);
    await waiting.future.timeout(const Duration(seconds: 2));
    await c.close();
    await checked;
    settled(c);
    expect(timers.single.isActive, isFalse);
    expect(bootstrap.count(10), 2);
    expect(bootstrap.count(9), 0);
  });
  for (final failure in ['disconnect', 'timeout']) {
    test('direct discovery $failure exhausts exactly the read budget',
        () async {
      bootstrap.handle = (r) {
        if (failure == 'disconnect') r.socket.destroy();
      };
      final c = client(timeoutMs: 100);
      await expectLater(
          c.findGroupCoordinator('g'),
          throwsA(failure == 'disconnect'
              ? isA<KafkaConnectionException>()
              : isA<TimeoutException>()));
      expect(bootstrap.count(10), 3);
      expect(bootstrap.count(8), 0);
      expect(bootstrap.count(9), 0);
    });
  }

  test('malformed read response is terminal rather than retried', () async {
    bootstrap.handle = (r) {
      if (r.key == 10) {
        _coordinator(r, bootstrap.server.port);
      } else {
        r.reply(r.response()..addInt8(0));
      }
    };
    await expectLater(client().committedOffsets('g', 'topic', partitions: {0}),
        throwsFormatException);
    expect(bootstrap.count(10), 1);
    expect(bootstrap.count(9), 1);
  });

  test('close after commit dispatch preserves no-replay and settles counters',
      () async {
    final dispatched = Completer<void>();
    bootstrap.handle = (r) {
      if (r.key == 10) {
        _coordinator(r, bootstrap.server.port);
      } else {
        dispatched.complete();
      }
    };
    final c = client(retries: 10);
    final checked = expectLater(
        c.commitOffsets('g', 'topic', nextOffsets: {0: 9}), throwsStateError);
    await dispatched.future;
    await c.close();
    await checked;
    settled(c);
    expect(bootstrap.count(10), 1);
    expect(bootstrap.count(8), 1);
  });

  test('duplicate bootstraps are visited once each per discovery round',
      () async {
    final other = await peer();
    final traversal = <String>[];
    bootstrap.handle = (r) {
      traversal.add('first');
      _coordinator(r, bootstrap.server.port, error: 15);
    };
    other.handle = (r) {
      traversal.add('second');
      _coordinator(r, other.server.port, error: 15);
    };
    final firstBroker = Broker('127.0.0.1', bootstrap.server.port);
    final secondBroker = Broker('127.0.0.1', other.server.port);
    final c = KafkaClient(
      brokers: [
        firstBroker,
        firstBroker,
        secondBroker,
        firstBroker,
        secondBroker
      ],
      maxReadRetries: 2,
      retryBaseDelay: Duration.zero,
    );
    clients.add(c);
    await expectLater(
        c.findGroupCoordinator('g'),
        throwsA(isA<KafkaBrokerException>()
            .having((e) => e.errorCode, 'code', 15)));
    expect(
        traversal, ['first', 'second', 'first', 'second', 'first', 'second']);
    expect(bootstrap.counts, {18: 1, 10: 3});
    expect(other.counts, {18: 1, 10: 3});
  });

  for (final invalid in [
    'node',
    'host',
    'zeroPort',
    'negativePort',
    'largePort'
  ]) {
    test(
        'successful coordinator discovery rejects invalid $invalid before target I/O',
        () async {
      final target = await peer();
      bootstrap.handle = (r) => _coordinator(
          r,
          invalid == 'zeroPort'
              ? 0
              : invalid == 'negativePort'
                  ? -1
                  : invalid == 'largePort'
                      ? 65536
                      : target.server.port,
          nodeId: invalid == 'node' ? -1 : 1,
          host: invalid == 'host' ? '' : '127.0.0.1');
      final c = client(retries: 5);
      await expectLater(c.committedOffsets('g', 'topic', partitions: {0}),
          throwsFormatException);
      expect(bootstrap.counts, {18: 1, 10: 1});
      expect(target.sockets, isEmpty);
      expect(target.counts, isEmpty);
    });
  }

  for (final committing in [false, true]) {
    test(
        'close during coordinator negotiation admits no operation (commit=$committing)',
        () async {
      final target = await peer();
      final negotiating = Completer<void>();
      target.handleVersions = (_) => negotiating.complete();
      bootstrap.handle = (r) => _coordinator(r, target.server.port);
      final c = client(retries: 5);
      final checked = expectLater(
          committing
              ? c.commitOffsets('g', 'topic', nextOffsets: {0: 12})
              : c.committedOffsets('g', 'topic', partitions: {0}),
          throwsStateError);
      await negotiating.future.timeout(const Duration(seconds: 2));
      expect(bootstrap.counts, {18: 1, 10: 1});
      expect(target.counts, {18: 1});
      await c.close();
      await checked;
      settled(c);
      expect(bootstrap.counts, {18: 1, 10: 1});
      expect(target.counts, {18: 1});
    });
  }
}
