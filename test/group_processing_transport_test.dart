import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:kafka_client/src/codec/record_batch.dart';
import 'package:test/test.dart';

int get _addressInUseErrorCode => Platform.isWindows
    ? 10048
    : Platform.isMacOS || Platform.isIOS
        ? 48
        : 98;

Future<void> _expectPortReleased(int port,
    {Duration budget = const Duration(seconds: 3)}) async {
  final elapsed = Stopwatch()..start();
  while (true) {
    late final ServerSocket rebound;
    try {
      rebound = await ServerSocket.bind(InternetAddress.loopbackIPv4, port);
    } on SocketException catch (error) {
      if (error.osError?.errorCode != _addressInUseErrorCode ||
          elapsed.elapsed >= budget) {
        rethrow;
      }
      // Destroying a Socket initiates native close asynchronously. Also, a
      // released ephemeral port can briefly be reused by another parallel test.
      // Still require a real, exclusive bind within one monotonic budget.
      await Future<void>.delayed(const Duration(milliseconds: 5));
      if (elapsed.elapsed >= budget) rethrow;
      continue;
    }
    await rebound.close();
    return;
  }
}

final class _BindFailures extends IOOverrides {
  final Object failure;
  int failures;
  int attempts = 0;
  _BindFailures(this.failure, this.failures);

  @override
  Future<ServerSocket> serverSocketBind(address, int port,
      {int backlog = 0, bool v6Only = false, bool shared = false}) {
    attempts++;
    expect(shared, isFalse);
    if (attempts <= failures) return Future<ServerSocket>.error(failure);
    return super.serverSocketBind(address, port,
        backlog: backlog, v6Only: v6Only, shared: shared);
  }
}

// An owned loopback Kafka-shaped peer. Receipt is a send-count oracle, never
// evidence of durable Kafka storage, rollback, or broker ownership.
class _Commit {
  final Socket socket;
  final int id;
  final String? group, member, topic;
  final int generation;
  final Map<int, int> offsets;
  _Commit(this.socket, this.id, this.group, this.member, this.topic,
      this.generation, this.offsets);

  void reply(
      {List<int> partitions = const [0, 1],
      List<int> codes = const [0, 0],
      String topic = 'topic',
      int throttle = 0}) {
    final b = KafkaBytesBuilder()
      ..addInt32(id)
      ..addInt32(throttle)
      ..addInt32(1)
      ..addString(topic)
      ..addInt32(partitions.length);
    for (var i = 0; i < partitions.length; i++) {
      b
        ..addInt32(partitions[i])
        ..addInt16(codes[i]);
    }
    socket.add(KafkaRequest.frame(b.takeBytes()));
  }
}

class _Peer {
  final ServerSocket server;
  final sockets = <Socket>{};
  final tasks = <Future<void>>[];
  final errors = <Object>[];
  final keys = <int>[];
  final commits = <_Commit>[];
  final commitReceived = Completer<_Commit>();
  late final StreamSubscription<Socket> subscription;
  void Function(_Commit)? onCommit;
  bool rejectHeartbeat = false;
  int accepted = 0;
  _Peer(this.server) {
    subscription = server.listen((socket) {
      accepted++;
      sockets.add(socket);
      tasks.add(_serve(socket));
    }, onError: (Object e) => errors.add(e));
  }
  static Future<_Peer> start() async =>
      _Peer(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));
  Broker get broker => Broker('127.0.0.1', server.port);

  Future<void> _serve(Socket socket) async {
    final buffer = <int>[];
    try {
      await for (final chunk in socket) {
        buffer.addAll(chunk);
        if (buffer.length > 1024 * 1024) throw StateError('Oversized request');
        while (buffer.length >= 4) {
          final size = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (size < 8 || size > 1024 * 1024) throw StateError('Invalid frame');
          if (buffer.length < size + 4) break;
          final r = KafkaBytesReader.fromBytes(buffer.sublist(4, size + 4));
          buffer.removeRange(0, size + 4);
          final key = r.readInt16(),
              version = r.readInt16(),
              id = r.readInt32();
          r.readString();
          keys.add(key);
          final b = KafkaBytesBuilder()..addInt32(id);
          switch (key) {
            case 18:
              expect(version, 0);
              const versions = {
                1: 4,
                3: 0,
                8: 6,
                10: 2,
                11: 4,
                12: 2,
                13: 2,
                14: 2
              };
              b
                ..addInt16(0)
                ..addInt32(versions.length);
              for (final v in versions.entries) {
                b
                  ..addInt16(v.key)
                  ..addInt16(v.value)
                  ..addInt16(v.value);
              }
            case 10:
              b
                ..addInt32(0)
                ..addInt16(0)
                ..addString(null)
                ..addInt32(1)
                ..addString('127.0.0.1')
                ..addInt32(server.port);
            case 11:
              expect(version, 4);
              b
                ..addInt32(0)
                ..addInt16(0)
                ..addInt32(37)
                ..addString('range')
                ..addString('other-leader')
                ..addString('owned-member')
                ..addInt32(0);
            case 14:
              expect(version, 2);
              b
                ..addInt32(0)
                ..addInt16(0)
                ..addBytes(ConsumerGroupAssignment({
                  'topic': [0, 1]
                }).toBytes());
            case 12:
              expect(r.readString(), 'owned-group');
              expect(r.readInt32(), 37);
              expect(r.readString(), 'owned-member');
              b
                ..addInt32(0)
                ..addInt16(rejectHeartbeat ? 27 : 0);
            case 13:
              b
                ..addInt32(0)
                ..addInt16(0);
            case 3:
              b
                ..addInt32(1)
                ..addInt32(1)
                ..addString('127.0.0.1')
                ..addInt32(server.port)
                ..addInt32(1)
                ..addInt16(0)
                ..addString('topic')
                ..addInt32(2);
              for (final p in [0, 1]) {
                b
                  ..addInt16(0)
                  ..addInt32(p)
                  ..addInt32(1)
                  ..addInt32(1)
                  ..addInt32(1)
                  ..addInt32(1)
                  ..addInt32(1);
              }
            case 1:
              expect(version, 4);
              r.readInt32();
              r.readInt32();
              r.readInt32();
              r.readInt32();
              r.readInt8();
              expect(r.readInt32(), 1);
              expect(r.readString(), 'topic');
              expect(r.readInt32(), 1);
              final p = r.readInt32(), offset = r.readInt64();
              r.readInt32();
              expect(r.isEOF, isTrue);
              b
                ..addInt32(0)
                ..addInt32(1)
                ..addString('topic')
                ..addInt32(1)
                ..addInt32(p)
                ..addInt16(0)
                ..addInt64(100)
                ..addInt64(-1)
                ..addInt32(-1)
                ..addBytes(offset == 0
                    ? encodeRecordBatch([
                        KafkaRecord(value: [p])
                      ])
                    : []);
            case 8:
              expect(version, 6);
              final group = r.readString(),
                  generation = r.readInt32(),
                  member = r.readString();
              expect(r.readInt32(), 1);
              final topic = r.readString(), n = r.readInt32();
              final offsets = <int, int>{};
              for (var i = 0; i < n; i++) {
                offsets[r.readInt32()] = r.readInt64();
                expect(r.readInt32(), -1);
                expect(r.readString(), isNull);
              }
              expect(r.isEOF, isTrue);
              final commit = _Commit(
                  socket, id, group, member, topic, generation, offsets);
              commits.add(commit);
              if (!commitReceived.isCompleted) commitReceived.complete(commit);
              onCommit?.call(commit);
              continue;
            default:
              throw StateError('Unexpected API $key');
          }
          socket.add(KafkaRequest.frame(b.takeBytes()));
          await socket.flush();
        }
      }
    } on SocketException {
      // Client cancellation may reset an owned connection.
    } catch (e) {
      errors.add(e);
    } finally {
      socket.destroy();
      sockets.remove(socket);
    }
  }

  Future<void> close(
      {Duration servingTimeout = const Duration(seconds: 3)}) async {
    final port = server.port;
    // Cancelling the subscription closes the listener. Keep its endpoint
    // reserved while owned serving work drains, without accepting new work.
    subscription.pause();
    try {
      for (final socket in sockets.toList()) {
        socket.destroy();
      }
      await Future.wait(tasks).timeout(servingTimeout);
      expect(sockets, isEmpty);
      expect(errors, isEmpty);
    } finally {
      try {
        await server.close();
      } finally {
        await subscription.cancel();
      }
    }
    // Verify the owned listening endpoint was released, not merely unreferenced.
    await _expectPortReleased(port);
  }
}

void main() {
  late _Peer peer;
  late KafkaGroupSession session;
  KafkaGroupProcessing? processing;
  StreamSubscription<KafkaGroupRecord>? listener;
  final records = <KafkaGroupRecord>[];
  final errors = <Object>[];
  Future<void> start() async {
    final token = await session.start();
    processing = session.subscribeProcessing(
        assignment: token,
        startOffsets: {
          'topic': {0: 0, 1: 0}
        },
        pollInterval: const Duration(milliseconds: 10));
    final received = Completer<void>();
    listener = processing!.records.listen((record) {
      records.add(record);
      processing!.acknowledge(record);
      if (records.length == 2) received.complete();
    }, onError: errors.add);
    await received.future.timeout(const Duration(seconds: 3));
    expect(processing!.acknowledgedNextOffsets, {
      'topic': {0: 1, 1: 1}
    });
  }

  void exactOnce() {
    expect(peer.commits, hasLength(1));
    final c = peer.commits.single;
    expect(c.group, 'owned-group');
    expect(c.generation, 37);
    expect(c.member, 'owned-member');
    expect(c.topic, 'topic');
    expect(c.offsets, {0: 1, 1: 1});
    expect(peer.keys.where((k) => k == 11), hasLength(1));
    expect(peer.keys.where((k) => k == 14), hasLength(1));
  }

  Future<void> terminal() async {
    final stopped =
        await processing!.stopped.timeout(const Duration(seconds: 3));
    final closed = await session.closed.timeout(const Duration(seconds: 3));
    expect(stopped.cleanupError, isNull);
    expect(closed.cleanupError, isNull);
    expect(session.isCurrent(session.assignment ?? records.first.assignment),
        isFalse);
    await expectLater(
        processing!.commitAcknowledged('topic'), throwsStateError);
    exactOnce();
  }

  setUp(() async {
    peer = await _Peer.start();
    processing = null;
    listener = null;
    records.clear();
    errors.clear();
    session = KafkaGroupSession(
        brokers: [peer.broker],
        groupId: 'owned-group',
        topics: {'topic'},
        heartbeatInterval: const Duration(milliseconds: 100),
        localExpiry: const Duration(seconds: 3),
        sessionTimeout: const Duration(seconds: 5),
        rebalanceTimeout: const Duration(milliseconds: 100),
        requestTimeout: const Duration(seconds: 1),
        leaveTimeout: const Duration(milliseconds: 300),
        maxReadRetries: 5,
        retryBaseDelay: Duration.zero);
  });
  tearDown(() async {
    await processing?.cancel().timeout(const Duration(seconds: 3));
    await listener?.cancel();
    await session.close().timeout(const Duration(seconds: 3));
    await peer.close();
  });

  test('port release probe waits for transient contention and closes its bind',
      () async {
    final held = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final port = held.port;
    await held.close();
    final busy = SocketException('injected address already in use',
        osError:
            OSError('injected address already in use', _addressInUseErrorCode));
    final binds = _BindFailures(busy, 2);
    await IOOverrides.runWithIOOverrides(
        () => _expectPortReleased(port), binds);
    expect(binds.attempts, 3);
    // The successful attempt above used the real default exclusive bind.
    // Its verification listener must itself have been closed.
    final rebound = await ServerSocket.bind(InternetAddress.loopbackIPv4, port);
    await rebound.close();
  });

  test('port release probe does not reset its budget after repeated contention',
      () async {
    for (final failures in [1, 1 << 30]) {
      final held = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final port = held.port;
      await held.close();
      final busy = SocketException('injected address already in use',
          osError: OSError(
              'injected address already in use', _addressInUseErrorCode));
      final binds = _BindFailures(busy, failures);
      try {
        await expectLater(
            IOOverrides.runWithIOOverrides(
                    () => _expectPortReleased(port,
                        budget: Duration(milliseconds: failures == 1 ? 1 : 30)),
                    binds)
                .timeout(const Duration(seconds: 1)),
            throwsA(same(busy)));
        // Scheduler suspension may consume the entire sustained-failure budget
        // on its first delay. It must still fail with the original bind error.
        if (failures == 1) expect(binds.attempts, 1);
      } finally {
        // A broken retry budget must not leave an endless synthetic failure loop.
        binds.failures = 0;
      }
    }
  });

  test(
      'port release probe fails if a listener remains bound through its budget',
      () async {
    final held = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    try {
      await expectLater(
          _expectPortReleased(held.port,
                  budget: const Duration(milliseconds: 30))
              .timeout(const Duration(seconds: 1)),
          throwsA(isA<SocketException>()));
    } finally {
      await held.close();
    }
  });

  for (final failure in [
    const SocketException('injected non-address-in-use error',
        osError: OSError('injected permission error', 13)),
    StateError('injected non-socket error'),
  ]) {
    test('port release probe propagates $failure without retry', () async {
      var attempts = 0;
      final probe = IOOverrides.runZoned(
        () => _expectPortReleased(12345),
        serverSocketBind: (address, port,
            {int backlog = 0, bool v6Only = false, bool shared = false}) {
          attempts++;
          expect(shared, isFalse);
          throw failure;
        },
      );
      await expectLater(probe, throwsA(same(failure)));
      expect(attempts, 1);
    });
  }

  test('peer keeps its listener reserved while owned serving work drains',
      () async {
    final owned = await _Peer.start();
    final port = owned.server.port;
    final serving = Completer<void>();
    owned.tasks.add(serving.future);
    final closing = owned.close();
    final closed = expectLater(closing, completes);
    ServerSocket? unexpected;
    try {
      expect(owned.subscription.isPaused, isTrue);
      await expectLater(
          ServerSocket.bind(InternetAddress.loopbackIPv4, port).then((socket) {
            unexpected = socket;
            return socket;
          }),
          throwsA(isA<SocketException>()));
    } finally {
      await unexpected?.close();
      serving.complete();
      await closed;
    }
  });

  test('peer closes its listener even if serving work fails or times out',
      () async {
    for (final fail in [true, false]) {
      final owned = await _Peer.start();
      final port = owned.server.port;
      final serving = Completer<void>();
      owned.tasks.add(serving.future);
      final error = StateError('injected serving failure');
      final closed = expectLater(
          owned.close(servingTimeout: const Duration(milliseconds: 30)),
          throwsA(fail ? same(error) : isA<TimeoutException>()));
      if (fail) serving.completeError(error);
      try {
        await closed;
        final rebound =
            await ServerSocket.bind(InternetAddress.loopbackIPv4, port);
        await rebound.close();
      } finally {
        if (!serving.isCompleted) serving.complete();
      }
    }
  });

  test(
      'public managed delivery commits exact generation/member once; confirmed snapshot is not resent',
      () async {
    peer.onCommit = (c) => c.reply();
    await start();
    final result = await processing!.commitAcknowledged('topic');
    expect(result.locallyCurrent, isTrue);
    expect(result.error, isNull);
    expect(result.partitions.values.map((p) => p.outcome),
        everyElement(KafkaGroupCommitOutcome.confirmed));
    expect(processing!.confirmedNextOffsets, {
      'topic': {0: 1, 1: 1}
    });
    expect((await processing!.commitAcknowledged('topic')).partitions, isEmpty);
    exactOnce();
    expect(errors, isEmpty);
  });

  for (final failure in [
    'disconnect',
    'deadline',
    'truncated frame',
    'malformed body',
    'missing partition',
    'duplicate partition',
    'wrong topic'
  ]) {
    test('$failure after accepted managed commit is unknown and never replayed',
        () async {
      peer.onCommit = (c) {
        switch (failure) {
          case 'disconnect':
            c.socket.destroy();
          case 'deadline':
            break;
          case 'truncated frame':
            c.socket.add((KafkaBytesBuilder()
                  ..addInt32(100)
                  ..addInt32(c.id))
                .takeBytes());
            c.socket.flush().then((_) => c.socket.close());
          case 'malformed body':
            c.socket.add(KafkaRequest.frame(
                (KafkaBytesBuilder()..addInt32(c.id)).takeBytes()));
          case 'missing partition':
            c.reply(partitions: [0], codes: [0]);
          case 'duplicate partition':
            c.reply(partitions: [0, 0]);
          case 'wrong topic':
            c.reply(topic: 'other');
        }
      };
      await start();
      final result = await processing!.commitAcknowledged('topic');
      expect(result.error, isNotNull);
      expect(result.locallyCurrent, isFalse);
      expect(result.partitions.keys, [0, 1]);
      expect(result.partitions.values.map((p) => p.outcome),
          everyElement(KafkaGroupCommitOutcome.unknown));
      expect(processing!.confirmedNextOffsets, isEmpty);
      await terminal();
    });
  }
  test('mixed valid response preserves success and rejection without replay',
      () async {
    peer.onCommit = (c) => c.reply(codes: [0, 27]);
    await start();
    final result = await processing!.commitAcknowledged('topic');
    expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.confirmed);
    expect(result.partitions[1]!.outcome, KafkaGroupCommitOutcome.rejected);
    expect(result.partitions[1]!.errorCode, 27);
    expect(processing!.confirmedNextOffsets, {
      'topic': {0: 1}
    });
    await terminal();
  });
  test('same-turn cancellation prevents admitted commit bytes reaching peer',
      () async {
    await start();
    final pending = processing!.commitAcknowledged('topic');
    final stopping = processing!.cancel();
    final result = await pending.timeout(const Duration(seconds: 3));
    final stopped = await stopping.timeout(const Duration(seconds: 3));
    await session.closed.timeout(const Duration(seconds: 3));
    expect(result.locallyCurrent, isFalse);
    // The public result stays conservative even when this owned oracle can
    // establish that this particular request never reached its server.
    expect(result.partitions.values.map((p) => p.outcome),
        everyElement(KafkaGroupCommitOutcome.unknown));
    expect(stopped.cleanupError, isNull);
    expect(peer.commits, isEmpty);
    expect(processing!.confirmedNextOffsets, isEmpty);
  });
  test('out-of-order partition response maps evidence by partition ID',
      () async {
    peer.onCommit = (c) => c.reply(partitions: [1, 0], codes: [27, 0]);
    await start();
    final result = await processing!.commitAcknowledged('topic');
    expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.confirmed);
    expect(result.partitions[1]!.outcome, KafkaGroupCommitOutcome.rejected);
    expect(processing!.confirmedNextOffsets, {
      'topic': {0: 1}
    });
    await terminal();
  });
  test('positive throttle retains confirmed response but ends session',
      () async {
    peer.onCommit = (c) => c.reply(throttle: 1);
    await start();
    final result = await processing!.commitAcknowledged('topic');
    expect(result.throttleTimeMs, 1);
    expect(result.partitions.values.map((p) => p.outcome),
        everyElement(KafkaGroupCommitOutcome.confirmed));
    expect(result.error, isA<UnsupportedError>());
    await terminal();
  });
  for (final reason in [
    'explicit close',
    'heartbeat revocation',
    'processing cancel'
  ]) {
    test('$reason cancels pending commit and settles stopped without replay',
        () async {
      await start();
      final pending = processing!.commitAcknowledged('topic');
      await peer.commitReceived.future.timeout(const Duration(seconds: 3));
      await expectLater(
          processing!.commitAcknowledged('topic'), throwsStateError);
      if (reason == 'explicit close') {
        await session.close();
      } else if (reason == 'heartbeat revocation') {
        peer.rejectHeartbeat = true;
        await session.revoked.timeout(const Duration(seconds: 3));
      } else {
        await processing!.cancel();
      }
      final result = await pending.timeout(const Duration(seconds: 3));
      expect(result.locallyCurrent, isFalse);
      expect(result.partitions.values.map((p) => p.outcome),
          everyElement(KafkaGroupCommitOutcome.unknown));
      expect(processing!.confirmedNextOffsets, isEmpty);
      expect(() => processing!.acknowledge(records.first), throwsStateError);
      await terminal();
    });
  }
}
