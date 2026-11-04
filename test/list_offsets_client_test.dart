import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

import 'support/list_offsets_peer.dart';

class _HeldTimer implements Timer {
  bool _active = true;
  @override
  bool get isActive => _active;
  @override
  int get tick => 0;
  @override
  void cancel() => _active = false;
}

Future<void> until(bool Function() ready) async {
  final clock = Stopwatch()..start();
  while (!ready()) {
    if (clock.elapsed > const Duration(seconds: 5)) {
      throw StateError('Timed out waiting for loopback peer');
    }
    await Future<void>.delayed(const Duration(milliseconds: 1));
  }
}

KafkaClient clientFor(ListOffsetsPeer peer, {int retries = 0}) => KafkaClient(
      brokers: [Broker('127.0.0.1', peer.server.port)],
      maxReadRetries: retries,
      retryBaseDelay: Duration.zero,
      requestTimeout: const Duration(milliseconds: 500),
      connectionTimeout: const Duration(milliseconds: 500),
    );

Future<void> cleanup(KafkaClient client, ListOffsetsPeer peer) async {
  await client.close();
  await until(() => client.admittedRequestCount == 0);
  expect(client.pendingRequestCount, 0);
  expect(client.queuedRequestBytes, 0);
  expect(client.bufferedResponseBytes, 0);
  expect(peer.errors, isEmpty);
  await peer.close();
}

void main() {
  for (final position in KafkaOffsetPosition.values) {
    for (final boundary in [0, 37, 9223372036854775807]) {
      test('explicit $position preserves broker boundary $boundary', () async {
        final peer = await ListOffsetsPeer.start();
        peer
          ..topic = 'tπ'
          ..partition = 3
          ..onListOffsets = (request, _) =>
              offsetResponseBytes(request.id, throttle: 17, topics: [
                offsetTopicBytes(name: 'tπ', partitions: [
                  offsetPartitionBytes(partition: 3, offset: boundary),
                ]),
              ]);
        final client = clientFor(peer);
        try {
          final response =
              await client.listOffsets('tπ', partition: 3, position: position);
          expect(response.throttleTimeMs, 17);
          expect(response.topics.single.partitions.single.offset, boundary);
          final request = peer.requests.last;
          expect(request.key, 2);
          expect(request.version, 2);
          expect(request.clientId, 'dart_kafka');
          expect(request.body, [
            ...offsetI32(-1),
            0,
            ...offsetI32(1),
            ...offsetString('tπ'),
            ...offsetI32(1),
            ...offsetI32(3),
            ...offsetI64(position == KafkaOffsetPosition.earliest ? -2 : -1),
          ]);
          expect(peer.requests.map((request) => request.key), [18, 3, 2]);
          expect(() => response.topics.clear(), throwsUnsupportedError);
        } finally {
          await cleanup(client, peer);
        }
      });
    }
  }

  test('invalid inputs fail asynchronously before any connection', () async {
    final peer = await ListOffsetsPeer.start();
    final client = clientFor(peer);
    try {
      for (final topic in ['', String.fromCharCode(0xd800), 'π' * 16384]) {
        await expectLater(
            client.listOffsets(topic, position: KafkaOffsetPosition.earliest),
            throwsArgumentError);
      }
      for (final partition in [-1, 2147483648]) {
        await expectLater(
            client.listOffsets('topic',
                partition: partition, position: KafkaOffsetPosition.latest),
            throwsArgumentError);
      }
      expect(client.connectionCount, 0);
      expect(peer.requests, isEmpty);
      await client.close();
      await expectLater(
          client.listOffsets('',
              partition: -1, position: KafkaOffsetPosition.earliest),
          throwsStateError);
      expect(peer.requests, isEmpty);
    } finally {
      await cleanup(client, peer);
    }
  });

  final invalidResponses = <String, List<List<int>>>{
    'missing topics': [],
    'wrong topic': [offsetTopicBytes(name: 'other')],
    'duplicate topic': [offsetTopicBytes(), offsetTopicBytes()],
    'missing partition': [offsetTopicBytes(partitions: [])],
    'wrong partition': [
      offsetTopicBytes(partitions: [offsetPartitionBytes(partition: 1)])
    ],
    'duplicate partition': [
      offsetTopicBytes(
          partitions: [offsetPartitionBytes(), offsetPartitionBytes()])
    ],
    'unavailable successful offset': [
      offsetTopicBytes(partitions: [offsetPartitionBytes(offset: -1)])
    ],
    'invalid boundary timestamp': [
      offsetTopicBytes(partitions: [offsetPartitionBytes(timestamp: 100)])
    ],
  };
  for (final invalid in invalidResponses.entries) {
    test('${invalid.key} fails without retry, reset or fallback', () async {
      final peer = await ListOffsetsPeer.start();
      peer.onListOffsets = (request, _) =>
          offsetResponseBytes(request.id, throttle: 100, topics: invalid.value);
      final client = clientFor(peer, retries: 2);
      try {
        await expectLater(
            client.listOffsets('topic', position: KafkaOffsetPosition.earliest),
            throwsFormatException);
        expect(peer.requests.map((request) => request.key), [18, 3, 2]);
      } finally {
        await cleanup(client, peer);
      }
    });
  }

  for (final error in [-1, 1, 3, 29, 35, 78, 88, 12345]) {
    test('broker error $error surfaces without reset, commit or retry',
        () async {
      final peer = await ListOffsetsPeer.start();
      peer.onListOffsets = (request, _) =>
          offsetResponseBytes(request.id, topics: [
            offsetTopicBytes(partitions: [
              offsetPartitionBytes(error: error, timestamp: 100, offset: -1),
            ]),
          ]);
      final client = clientFor(peer, retries: 2);
      try {
        await expectLater(
            client.listOffsets('topic', position: KafkaOffsetPosition.latest),
            throwsA(isA<KafkaBrokerException>()
                .having((e) => e.errorCode, 'errorCode', error)
                .having((e) => e.operation, 'operation', 'list offsets')));
        expect(peer.requests.map((request) => request.key), [18, 3, 2]);
      } finally {
        await cleanup(client, peer);
      }
    });
  }

  for (final error in [5, 6, 9]) {
    test('transient broker error $error retries with fresh leader discovery',
        () async {
      final peer = await ListOffsetsPeer.start();
      var lookups = 0;
      peer.onListOffsets =
          (request, _) => offsetResponseBytes(request.id, topics: [
                offsetTopicBytes(partitions: [
                  offsetPartitionBytes(
                      error: lookups++ == 0 ? error : 0,
                      offset: lookups == 1 ? -1 : 51),
                ]),
              ]);
      final client = clientFor(peer, retries: 1);
      try {
        final result = await client.listOffsets('topic',
            position: KafkaOffsetPosition.latest);
        expect(result.topics.single.partitions.single.offset, 51);
        expect(peer.requests.map((request) => request.key), [18, 3, 2, 3, 2]);
      } finally {
        await cleanup(client, peer);
      }
    });
    test('transient broker error $error respects finite retry budget',
        () async {
      final peer = await ListOffsetsPeer.start();
      peer.onListOffsets =
          (request, _) => offsetResponseBytes(request.id, topics: [
                offsetTopicBytes(partitions: [
                  offsetPartitionBytes(error: error, offset: -1),
                ]),
              ]);
      final client = clientFor(peer, retries: 2);
      try {
        await expectLater(
            client.listOffsets('topic', position: KafkaOffsetPosition.earliest),
            throwsA(isA<KafkaBrokerException>()
                .having((e) => e.errorCode, 'code', error)));
        expect(peer.requests.where((r) => r.key == 2), hasLength(3));
        expect(peer.requests.where((r) => r.key == 3), hasLength(3));
      } finally {
        await cleanup(client, peer);
      }
    });
  }

  test('transport disconnect retries the read with a fresh connection',
      () async {
    final peer = await ListOffsetsPeer.start();
    var calls = 0;
    peer.onListOffsets = (request, socket) {
      if (calls++ == 0) {
        socket.destroy();
        return null;
      }
      return offsetResponseBytes(request.id);
    };
    final client = clientFor(peer, retries: 1);
    try {
      final response = await client.listOffsets('topic',
          position: KafkaOffsetPosition.earliest);
      expect(response.topics.single.partitions.single.offset, 42);
      expect(peer.requests.map((r) => r.key), [18, 3, 2, 18, 3, 2]);
    } finally {
      await cleanup(client, peer);
    }
  });

  for (final recover in [false, true]) {
    test(
        'request deadline ${recover ? 'retries safely' : 'exhausts its budget'}',
        () async {
      final peer = await ListOffsetsPeer.start();
      var calls = 0;
      peer.onListOffsets = (request, _) =>
          calls++ == 0 || !recover ? null : offsetResponseBytes(request.id);
      final client = KafkaClient(
          brokers: [Broker('127.0.0.1', peer.server.port)],
          requestTimeout: const Duration(milliseconds: 100),
          retryBaseDelay: Duration.zero,
          maxReadRetries: 1);
      try {
        final result =
            client.listOffsets('topic', position: KafkaOffsetPosition.latest);
        if (recover) {
          expect((await result).topics.single.partitions.single.offset, 42);
        } else {
          await expectLater(result, throwsA(isA<TimeoutException>()));
        }
        expect(peer.requests.where((r) => r.key == 2), hasLength(2));
        expect(peer.requests.where((r) => r.key == 3), hasLength(2));
      } finally {
        await cleanup(client, peer);
      }
    });
  }

  test('close interrupts held retry backoff without another discovery',
      () async {
    final peer = await ListOffsetsPeer.start();
    peer.onListOffsets =
        (request, _) => offsetResponseBytes(request.id, topics: [
              offsetTopicBytes(
                  partitions: [offsetPartitionBytes(error: 6, offset: -1)]),
            ]);
    final client = KafkaClient(
        brokers: [Broker('127.0.0.1', peer.server.port)],
        requestTimeout: const Duration(seconds: 3),
        connectionTimeout: const Duration(seconds: 2),
        retryBaseDelay: const Duration(seconds: 1),
        maxReadRetries: 2);
    final held = <_HeldTimer>[];
    final waiting = Completer<void>();
    try {
      final result = runZoned(
          () =>
              client.listOffsets('topic', position: KafkaOffsetPosition.latest),
          zoneSpecification: ZoneSpecification(
              createTimer: (self, parent, zone, duration, callback) {
        if (duration <= const Duration(seconds: 1) &&
            peer.requests.any((r) => r.key == 2)) {
          final timer = _HeldTimer();
          held.add(timer);
          if (!waiting.isCompleted) waiting.complete();
          return timer;
        }
        return parent.createTimer(zone, duration, callback);
      }));
      final rejected = expectLater(result, throwsStateError);
      await waiting.future.timeout(const Duration(seconds: 5));
      expect(held, hasLength(1));
      expect(held.single.isActive, isTrue);
      await client.close();
      await rejected;
      expect(held.single.isActive, isFalse);
      expect(peer.requests.map((r) => r.key), [18, 3, 2]);
    } finally {
      await cleanup(client, peer);
    }
  });

  test('Fetch TLS recovery does not replay failed ListOffsets discovery',
      () async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final sockets = <Socket>[];
    final tasks = <Future<void>>[];
    final errors = <Object>[];
    var hellos = 0;
    server.listen((socket) {
      sockets.add(socket);
      tasks.add(() async {
        final bytes = <int>[];
        var finished = false;
        try {
          await for (final data in socket) {
            if (finished) continue;
            bytes.addAll(data);
            if (bytes.length < 5) continue;
            final length = (bytes[3] << 8) | bytes[4];
            if (bytes.length < 5 + length) continue;
            if (length < 4 || bytes[0] != 22 || bytes[5] != 1) {
              throw StateError('Expected complete TLS ClientHello');
            }
            hellos++;
            finished = true;
            await socket.flush();
            await socket.close();
          }
        } catch (error) {
          errors.add(error);
        } finally {
          socket.destroy();
        }
      }());
    });
    final client = KafkaClient(
        brokers: [Broker('127.0.0.1', server.port)],
        tls: KafkaTlsConfig(),
        allowTlsBootstrapFallback: true,
        tlsFetchRecovery: KafkaTlsFetchRecovery(maxAttempts: 5),
        maxReadRetries: 3,
        retryBaseDelay: Duration.zero);
    try {
      await expectLater(
          client.listOffsets('topic', position: KafkaOffsetPosition.latest),
          throwsA(isA<HandshakeException>()));
      expect(hellos, 1);
      expect(sockets, hasLength(1));
      expect(client.bootstrapTlsFallbackCount, 0);
    } finally {
      await client.close();
      for (final socket in sockets) {
        socket.destroy();
      }
      await server.close();
      await Future.wait(tasks);
      expect(errors, isEmpty);
    }
  });

  for (final stage in ['metadata', 'lookup']) {
    test('close during $stage stops the request without later retries',
        () async {
      final peer = await ListOffsetsPeer.start();
      if (stage == 'metadata') {
        peer.onMetadata = (_, __) => null;
      } else {
        peer.onListOffsets = (_, __) => null;
      }
      final client = clientFor(peer, retries: 2);
      try {
        final result =
            client.listOffsets('topic', position: KafkaOffsetPosition.latest);
        final rejected = expectLater(result, throwsStateError);
        await until(() =>
            peer.requests.any((r) => r.key == (stage == 'metadata' ? 3 : 2)));
        await client.close();
        await rejected;
        expect(peer.requests.where((r) => r.key == 3), hasLength(1));
        expect(peer.requests.where((r) => r.key == 2),
            hasLength(stage == 'metadata' ? 0 : 1));
      } finally {
        await cleanup(client, peer);
      }
    });
  }

  test('leader refresh routes retry to the newly discovered leader', () async {
    final bootstrap = await ListOffsetsPeer.start();
    final first = await ListOffsetsPeer.start();
    final second = await ListOffsetsPeer.start();
    bootstrap.leaderPort = first.server.port;
    first.onListOffsets = (request, _) {
      bootstrap.leaderPort = second.server.port;
      return offsetResponseBytes(request.id, topics: [
        offsetTopicBytes(
            partitions: [offsetPartitionBytes(error: 6, offset: -1)]),
      ]);
    };
    final client = clientFor(bootstrap, retries: 1);
    try {
      final response = await client.listOffsets('topic',
          position: KafkaOffsetPosition.latest);
      expect(response.topics.single.partitions.single.offset, 42);
      expect(bootstrap.requests.map((r) => r.key), [18, 3, 3]);
      expect(first.requests.map((r) => r.key), [18, 2]);
      expect(second.requests.map((r) => r.key), [18, 2]);
    } finally {
      await cleanup(client, bootstrap);
      expect(first.errors, isEmpty);
      expect(second.errors, isEmpty);
      await first.close();
      await second.close();
    }
  });
}
