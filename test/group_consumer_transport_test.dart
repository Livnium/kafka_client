import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:kafka_client/src/codec/record_batch.dart';
import 'package:test/test.dart';

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
      {List<int> partitions = const [0],
      List<int> codes = const [0],
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
  void Function()? onHeartbeatRejected;
  int accepted = 0;
  int joins = 0, positionReads = 0, rejections = 0;
  bool holdJoin = false, holdPositions = false, emptyAssignment = false;
  String? positionFailure;
  int storedOffset = 0;
  List<int> Function(int offset, int partition)? recordSet;
  int quotaReplies = 0;
  int quotaThrottleMs = 35;
  String? quotaTopic;
  final fetches = <int>[];
  final joinReceived = Completer<void>();
  final positionsReceived = Completer<void>();
  final heartbeatRejected = Completer<void>();
  _Peer(this.server) {
    subscription = server.listen((socket) {
      accepted++;
      sockets.add(socket);
      unawaited(socket.done.then<void>((_) {}, onError: (Object error) {
        if (error is! SocketException) errors.add(error);
      }));
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
                9: 5,
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
              joins++;
              if (!joinReceived.isCompleted) joinReceived.complete();
              if (holdJoin) continue;
              b
                ..addInt32(0)
                ..addInt16(0)
                ..addInt32(36 + joins)
                ..addString('range')
                ..addString('other-leader')
                ..addString('owned-member-$joins')
                ..addInt32(0);
            case 14:
              expect(version, 2);
              b
                ..addInt32(0)
                ..addInt16(0)
                ..addBytes(ConsumerGroupAssignment(emptyAssignment
                        ? {}
                        : {
                            'topic': [0]
                          })
                    .toBytes());
            case 12:
              expect(r.readString(), 'owned-group');
              expect(r.readInt32(), 36 + joins);
              expect(r.readString(), 'owned-member-$joins');
              if (rejectHeartbeat) {
                rejections++;
                if (!heartbeatRejected.isCompleted) {
                  heartbeatRejected.complete();
                }
              }
              b
                ..addInt32(0)
                ..addInt16(rejectHeartbeat ? 27 : 0);
            case 9:
              expect(version, 5);
              expect(r.readString(), 'owned-group');
              expect(r.readInt32(), 1);
              expect(r.readString(), 'topic');
              expect(r.readInt32(), 1);
              expect(r.readInt32(), 0);
              expect(r.isEOF, isTrue);
              positionReads++;
              if (!positionsReceived.isCompleted) positionsReceived.complete();
              if (holdPositions) continue;
              if (positionFailure == 'malformed') break;
              b
                ..addInt32(0)
                ..addInt32(1)
                ..addString('topic')
                ..addInt32(1)
                ..addInt32(0)
                ..addInt64(storedOffset)
                ..addInt32(-1)
                ..addString(null)
                ..addInt16(positionFailure == 'partition error' ? 29 : 0)
                ..addInt16(positionFailure == 'group error' ? 30 : 0);
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
              fetches.add(offset);
              if (quotaReplies > 0) {
                quotaReplies--;
                b
                  ..addInt32(quotaThrottleMs)
                  ..addInt32(quotaTopic == null ? 0 : 1);
                if (quotaTopic != null) {
                  b
                    ..addString(quotaTopic)
                    ..addInt32(0);
                }
                break;
              }
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
                ..addBytes(recordSet?.call(offset, p) ??
                    (offset == storedOffset && storedOffset >= 0
                        ? _batch(offset, p)
                        : []));
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
          if (key == 12 && rejectHeartbeat) onHeartbeatRejected?.call();
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

  Future<void> close() async {
    final port = server.port;
    await subscription.cancel();
    await server.close();
    for (final socket in sockets.toList()) {
      socket.destroy();
    }
    await Future.wait(tasks).timeout(const Duration(seconds: 3));
    expect(sockets, isEmpty);
    expect(errors, isEmpty);
    // Verify the owned listening endpoint was released, not merely unreferenced.
    final rebound = await ServerSocket.bind(InternetAddress.loopbackIPv4, port);
    await rebound.close();
  }
}

List<int> _batch(int offset, int partition) {
  final bytes = encodeRecordBatch([
    KafkaRecord(value: [partition])
  ]);
  // Base offset is outside the CRC-protected payload.
  ByteData.sublistView(bytes).setInt64(0, offset);
  return bytes;
}

Uint8List _progressBatch(int base, int lastDelta,
    {String kind = 'normal', int count = 1}) {
  var bytes = encodeRecordBatch([
    for (var i = 0; i < count; i++) KafkaRecord(value: [i], timestamp: 0)
  ]);
  if (kind == 'empty') bytes = Uint8List.fromList(bytes.sublist(0, 61));
  final data = ByteData.sublistView(bytes);
  data
    ..setInt64(0, base, Endian.big)
    ..setInt32(8, bytes.length - 12, Endian.big)
    ..setInt32(23, lastDelta, Endian.big);
  if (kind == 'control') data.setInt16(21, 0x20, Endian.big);
  if (kind == 'empty') data.setInt32(57, 0, Endian.big);
  data.setUint32(17, crc32c(bytes.sublist(21)), Endian.big);
  return bytes;
}

Future<void> _until(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 3));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('peer condition');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

void main() {
  late _Peer peer;
  KafkaGroupConsumer? consumer;
  final gates = <Completer<void>>[];
  KafkaGroupConsumer make(
          {bool commit = true,
          int maxRejoins = 2,
          Map<String, Map<int, int>> missingOffsets = const {}}) =>
      consumer = KafkaGroupConsumer(
          brokers: [peer.broker],
          groupId: 'owned-group',
          topics: {'topic'},
          commitAfterProcessing: commit,
          missingOffsets: missingOffsets,
          maxRejoins: maxRejoins,
          rejoinDelay: const Duration(milliseconds: 20),
          heartbeatInterval: const Duration(milliseconds: 100),
          localExpiry: const Duration(seconds: 3),
          sessionTimeout: const Duration(seconds: 5),
          rebalanceTimeout: const Duration(milliseconds: 100),
          requestTimeout: const Duration(seconds: 1),
          leaveTimeout: const Duration(milliseconds: 300),
          pollInterval: const Duration(milliseconds: 10),
          maxReadRetries: 0,
          retryBaseDelay: Duration.zero);
  setUp(() async {
    peer = await _Peer.start();
    consumer = null;
    gates.clear();
  });
  tearDown(() async {
    consumer?.stop();
    for (final gate in gates) {
      if (!gate.isCompleted) gate.complete();
    }
    await consumer?.closed.timeout(const Duration(seconds: 4));
    await peer.close();
  });

  test(
      'repeated heartbeat 27 drains each exact callback before fresh Join and positions',
      () async {
    final records = <KafkaGroupRecord>[];
    final run = make(commit: false).run((record) {
      records.add(record);
      final gate = Completer<void>();
      gates.add(gate);
      peer.rejectHeartbeat = true;
      return gate.future;
    });
    for (var generation = 1; generation <= 3; generation++) {
      await _until(
          () => records.length == generation && peer.rejections == generation);
      await Future<void>.delayed(const Duration(milliseconds: 60));
      expect(peer.joins, generation);
      expect(peer.positionReads, generation);
      expect(records.last.offset, (generation - 1) * 10);
      expect(records.last.assignment.generationId, 36 + generation);
      if (generation < 3) {
        peer.storedOffset = generation * 10;
        peer.rejectHeartbeat = false;
      }
      gates.last.complete();
    }
    final result = await run.timeout(const Duration(seconds: 4));
    expect(result.reason, KafkaGroupConsumerStopReason.rejoinLimit);
    expect(result.generationsStarted, 3);
    expect(result.error, isNull);
    expect(result.cleanupErrors, isEmpty);
    expect(peer.joins, 3);
    expect(peer.commits, isEmpty);
    expect(peer.fetches, containsAll([0, 10, 20]));
  });

  test(
      'fresh generation rereads stored position and commits exact member and generation',
      () async {
    peer.onCommit = (c) => c.reply();
    var callbacks = 0;
    final run = make().run((record) {
      callbacks++;
    });
    await _until(() => peer.commits.length == 1 && peer.fetches.contains(1));
    peer.storedOffset = 10;
    peer.rejectHeartbeat = true;
    await peer.heartbeatRejected.future.timeout(const Duration(seconds: 3));
    peer.rejectHeartbeat = false;
    await _until(() => peer.commits.length == 2 && peer.fetches.contains(11));
    consumer!.stop();
    final result = await run.timeout(const Duration(seconds: 4));
    expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
    expect(callbacks, 2);
    expect(peer.positionReads, 2);
    expect(peer.joins, 2);
    for (var i = 0; i < 2; i++) {
      final c = peer.commits[i];
      expect(c.group, 'owned-group');
      expect(c.topic, 'topic');
      expect(c.generation, 37 + i);
      expect(c.member, 'owned-member-${i + 1}');
      expect(c.offsets, {0: i * 10 + 1});
    }
  });

  for (final failure in ['disconnect', 'malformed response']) {
    test(
        'heartbeat 27 with pending commit then $failure is unknown, never rejoins',
        () async {
      final run = make().run((_) {});
      final commit =
          await peer.commitReceived.future.timeout(const Duration(seconds: 3));
      peer.onHeartbeatRejected = () {
        if (failure == 'disconnect') {
          commit.socket.destroy();
        } else {
          commit.socket.add(KafkaRequest.frame(
              (KafkaBytesBuilder()..addInt32(commit.id)).takeBytes()));
        }
      };
      peer.rejectHeartbeat = true;
      await peer.heartbeatRejected.future.timeout(const Duration(seconds: 3));
      final result = await run.timeout(const Duration(seconds: 4));
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.lastCommit!.partitions[0]!.outcome,
          KafkaGroupCommitOutcome.unknown);
      expect(peer.joins, 1);
      expect(peer.commits, hasLength(1));
    });
  }

  for (final stage in ['Join', 'OffsetFetch']) {
    test('stop during pending $stage cancels without data, callback or rejoin',
        () async {
      peer.holdJoin = stage == 'Join';
      peer.holdPositions = stage == 'OffsetFetch';
      var callbacks = 0;
      final run = make().run((_) {
        callbacks++;
      });
      await (stage == 'Join'
              ? peer.joinReceived.future
              : peer.positionsReceived.future)
          .timeout(const Duration(seconds: 3));
      consumer!.stop();
      final result = await run.timeout(const Duration(seconds: 4));
      expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
      expect(peer.joins, 1);
      expect(peer.fetches, isEmpty);
      expect(callbacks, 0);
      expect(peer.commits, isEmpty);
    });
  }

  for (final failure in ['malformed', 'partition error', 'group error']) {
    test('$failure OffsetFetch does not use explicit missing fallback',
        () async {
      peer.positionFailure = failure;
      final result = await make(missingOffsets: {
        'topic': {0: 42}
      }).run((_) {
        fail('No callback after failed positions');
      }).timeout(const Duration(seconds: 4));
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.error, isNotNull);
      expect(peer.positionReads, 1);
      expect(peer.joins, 1);
      expect(peer.fetches, isEmpty);
      expect(peer.commits, isEmpty);
    });
  }

  test('missing committed position uses only explicitly supplied fallback',
      () async {
    peer.storedOffset = -1;
    final run = make(missingOffsets: {
      'topic': {0: 42}
    }).run((_) {
      fail('The peer returns no records at missing positions');
    });
    await _until(() => peer.fetches.isNotEmpty);
    consumer!.stop();
    final result = await run.timeout(const Duration(seconds: 4));
    expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
    expect(peer.fetches, everyElement(42));
    expect(peer.positionReads, 1);
    expect(peer.commits, isEmpty);
  });

  test('missing committed position without fallback fails before any Fetch',
      () async {
    peer.storedOffset = -1;
    final result = await make().run((_) {
      fail('Missing positions cannot admit a callback');
    }).timeout(const Duration(seconds: 4));
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(result.error, isA<StateError>());
    expect(peer.positionReads, 1);
    expect(peer.joins, 1);
    expect(peer.fetches, isEmpty);
    expect(peer.commits, isEmpty);
  });

  test('zero rejoin budget ends after first eligible heartbeat revocation',
      () async {
    final run = make(commit: false, maxRejoins: 0).run((_) {
      peer.rejectHeartbeat = true;
    });
    final result = await run.timeout(const Duration(seconds: 4));
    expect(result.reason, KafkaGroupConsumerStopReason.rejoinLimit);
    expect(result.generationsStarted, 1);
    expect(result.error, isNull);
    expect(peer.joins, 1);
    expect(peer.positionReads, 1);
    expect(peer.commits, isEmpty);
  });

  test('empty assignment never sends OffsetFetch or data fetch', () async {
    peer.emptyAssignment = true;
    final run = make().run((_) {
      fail('Empty assignment callback');
    });
    await _until(() => consumer!.state == KafkaGroupConsumerState.processing);
    consumer!.stop();
    expect((await run.timeout(const Duration(seconds: 4))).reason,
        KafkaGroupConsumerStopReason.cancelled);
    expect(peer.positionReads, 0);
    expect(peer.fetches, isEmpty);
    expect(peer.commits, isEmpty);
  });

  test('stop while callback is held drains it and never sends commit',
      () async {
    final gate = Completer<void>();
    gates.add(gate);
    final admitted = Completer<void>();
    final run = make().run((_) {
      admitted.complete();
      return gate.future;
    });
    await admitted.future.timeout(const Duration(seconds: 3));
    var settled = false;
    unawaited(run.then((_) {
      settled = true;
    }));
    consumer!.stop();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(settled, isFalse);
    expect(peer.commits, isEmpty);
    gate.complete();
    expect((await run.timeout(const Duration(seconds: 4))).reason,
        KafkaGroupConsumerStopReason.cancelled);
    expect(peer.commits, isEmpty);
    expect(peer.joins, 1);
  });

  for (final kind in ['normal', 'control', 'empty', 'inside batch']) {
    test('managed $kind prefix ignores partial tail without skipping offsets',
        () async {
      final prefixEnd = kind == 'inside batch'
          ? 2
          : kind == 'normal'
              ? 0
              : 4;
      peer.storedOffset = kind == 'inside batch' ? 1 : 0;
      final prefix = _progressBatch(0, prefixEnd,
          kind: kind, count: kind == 'inside batch' ? 3 : 1);
      final tailBase = prefixEnd + 1;
      final tail = _progressBatch(tailBase, 9);
      peer.recordSet = (offset, _) => offset == peer.storedOffset
          ? [...prefix, ...tail.take(30)]
          : offset == tailBase
              ? tail
              : [];
      peer.onCommit = (commit) => commit.reply();
      final callbacks = <int>[];
      final run = make().run((record) => callbacks.add(record.offset));
      await _until(() =>
          peer.fetches.contains(tailBase + 10) ||
          consumer!.state == KafkaGroupConsumerState.closed);
      consumer!.stop();
      final result = await run.timeout(const Duration(seconds: 3));
      expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
      expect(result.error, isNull);
      expect(callbacks, [
        if (kind == 'normal') 0,
        if (kind == 'inside batch') ...[1, 2],
        tailBase,
      ]);
      expect(peer.commits.map((c) => c.offsets), [
        for (final offset in callbacks) {0: offset + 1}
      ]);
      expect(
          peer.fetches.take(3), [peer.storedOffset, tailBase, tailBase + 10]);
      expect(peer.joins, 1);
      expect(peer.positionReads, 1);
    });
  }

  for (final failure in [
    'incomplete first',
    'corrupt complete tail',
    'invalid complete tail',
    'unsupported complete tail'
  ]) {
    test('managed $failure delivers no callback or commit', () async {
      final prefix = _progressBatch(0, 0);
      final tail = _progressBatch(1, 0);
      if (failure == 'corrupt complete tail') tail[23] ^= 1;
      if (failure == 'invalid complete tail') {
        ByteData.sublistView(tail).setInt32(57, -1, Endian.big);
        ByteData.sublistView(tail)
            .setUint32(17, crc32c(tail.sublist(21)), Endian.big);
      }
      if (failure == 'unsupported complete tail') {
        ByteData.sublistView(tail).setInt16(21, 1, Endian.big);
        ByteData.sublistView(tail)
            .setUint32(17, crc32c(tail.sublist(21)), Endian.big);
      }
      peer.recordSet = (_, __) => failure == 'incomplete first'
          ? prefix.sublist(0, 30)
          : [...prefix, ...tail];
      var callbacks = 0;
      final result = await make().run((_) {
        callbacks++;
      }).timeout(const Duration(seconds: 3));
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(
          result.error,
          failure == 'unsupported complete tail'
              ? isA<UnsupportedError>()
              : isA<FormatException>());
      expect(callbacks, 0);
      expect(peer.commits, isEmpty);
      expect(peer.fetches, [0]);
      expect(peer.joins, 1);
      expect(peer.positionReads, 1);
    });
  }

  test('native subscription refetches partial batch at its own starting offset',
      () async {
    final prefix = _progressBatch(0, 0), tail = _progressBatch(1, 0);
    peer.recordSet = (offset, _) => offset == 0
        ? [...prefix, ...tail.take(30)]
        : offset == 1
            ? tail
            : [];
    final client = KafkaClient(brokers: [peer.broker], maxReadRetries: 3);
    try {
      final records = await client
          .subscribe('topic', startOffsets: {0: 0})
          .take(2)
          .toList()
          .timeout(const Duration(seconds: 3));
      expect(records.map((record) => record.offset), [0, 1]);
      expect(peer.fetches, [0, 1]);
      expect(peer.commits, isEmpty);
      expect(peer.joins, 0);
    } finally {
      await client.close();
    }
  });

  test('standalone Fetch returns quota diagnostics without waiting or replay',
      () async {
    peer
      ..quotaReplies = 1
      ..quotaThrottleMs = 2147483647;
    final client = KafkaClient(brokers: [peer.broker], maxReadRetries: 3);
    try {
      final response = await client
          .fetch('topic', offset: 7)
          .timeout(const Duration(seconds: 2));
      expect(response.throttleTimeMs, 2147483647);
      expect(response.topics, isEmpty);
      expect(peer.fetches, [7]);
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(peer.fetches, [7]);
      expect(peer.commits, isEmpty);
      expect(peer.joins, 0);
    } finally {
      await client.close();
    }
  });

  for (final throttle in [0, -1]) {
    test('standalone Fetch rejects empty quota reply with throttle $throttle',
        () async {
      peer
        ..quotaReplies = 1
        ..quotaThrottleMs = throttle;
      final client = KafkaClient(brokers: [peer.broker], maxReadRetries: 3);
      try {
        await expectLater(
            client.fetch('topic', offset: 7), throwsFormatException);
        expect(peer.fetches, [7]);
      } finally {
        await client.close();
      }
    });
  }

  for (final topic in ['wrong', 'topic']) {
    test('positive throttle cannot hide nonempty malformed topic $topic',
        () async {
      peer
        ..quotaReplies = 1
        ..quotaTopic = topic;
      final client = KafkaClient(brokers: [peer.broker], maxReadRetries: 3);
      try {
        await expectLater(
            client.fetch('topic', offset: 7), throwsFormatException);
        expect(peer.fetches, [7]);
      } finally {
        await client.close();
      }
    });
  }

  test('native subscription waits and polls unchanged cursor after quota',
      () async {
    peer.quotaReplies = 1;
    final client = KafkaClient(brokers: [peer.broker], maxReadRetries: 3);
    try {
      final records = await client
          .subscribe('topic', startOffsets: {0: 0})
          .take(1)
          .toList()
          .timeout(const Duration(seconds: 3));
      expect(records.single.offset, 0);
      expect(peer.fetches, [0, 0]);
      expect(peer.commits, isEmpty);
      expect(peer.joins, 0);
    } finally {
      await client.close();
    }
  });

  test('managed quota result admits no acknowledgement or commit', () async {
    peer.quotaReplies = 1;
    peer.onCommit = (commit) => commit.reply();
    final callbacks = <int>[];
    final run = make().run((record) => callbacks.add(record.offset));
    await _until(() =>
        peer.fetches.contains(1) ||
        consumer!.state == KafkaGroupConsumerState.closed);
    consumer!.stop();
    final result = await run.timeout(const Duration(seconds: 3));
    expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
    expect(result.error, isNull);
    expect(callbacks, [0]);
    expect(peer.fetches.take(3), [0, 0, 1]);
    expect(peer.commits.map((commit) => commit.offsets), [
      {0: 1}
    ]);
    expect(peer.joins, 1);
    expect(peer.positionReads, 1);
  });

  test('managed stop interrupts maximum quota wait without another Fetch',
      () async {
    peer
      ..quotaReplies = 1
      ..quotaThrottleMs = 2147483647;
    var callbacks = 0;
    final run = make().run((_) {
      callbacks++;
    });
    await _until(() => peer.fetches.isNotEmpty);
    await Future<void>.delayed(const Duration(milliseconds: 10));
    consumer!.stop();
    final result = await run.timeout(const Duration(seconds: 3));
    expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
    expect(callbacks, 0);
    expect(peer.fetches, [0]);
    expect(peer.commits, isEmpty);
    expect(peer.joins, 1);
  });

  test('heartbeat revocation interrupts quota wait without data or commit',
      () async {
    peer
      ..quotaReplies = 1
      ..quotaThrottleMs = 2147483647;
    var callbacks = 0;
    final run = make(maxRejoins: 0).run((_) {
      callbacks++;
    });
    await _until(() => peer.fetches.isNotEmpty);
    peer.rejectHeartbeat = true;
    final result = await run.timeout(const Duration(seconds: 3));
    expect(result.reason, KafkaGroupConsumerStopReason.rejoinLimit);
    expect(callbacks, 0);
    expect(peer.fetches, [0]);
    expect(peer.commits, isEmpty);
    expect(peer.joins, 1);
  });
}
