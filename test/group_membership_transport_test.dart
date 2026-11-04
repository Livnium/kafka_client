import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

// Owned loopback peers verify dispatch and local resource accounting only.
// A peer receiving bytes is not evidence of a broker-side membership transition
// or commit. Timeout, cancellation and disconnect cannot establish rollback.
class _Frame {
  final Socket socket;
  final int key, version, id;
  final KafkaBytesReader body;
  _Frame(this.socket, this.key, this.version, this.id, this.body);

  void reply({int error = 0, int? id}) {
    final b = KafkaBytesBuilder()
      ..addInt32(id ?? this.id)
      ..addInt32(0);
    if (key == 8) {
      b
        ..addInt32(1)
        ..addString('topic')
        ..addInt32(1)
        ..addInt32(0)
        ..addInt16(error);
    } else {
      b.addInt16(error);
      if (key == 11) {
        b
          ..addInt32(7)
          ..addString('range')
          ..addString('member')
          ..addString('member')
          ..addInt32(1)
          ..addString('member')
          ..addBytes(ConsumerGroupSubscription(['topic']).toBytes());
      } else if (key == 14) {
        b.addBytes([]);
      }
    }
    socket.add(KafkaRequest.frame(b.takeBytes()));
  }
}

class _Peer {
  final ServerSocket server;
  final sockets = <Socket>[];
  final frames = <_Frame>[];
  final received = Completer<_Frame>();
  late void Function(_Frame) handle;
  _Peer._(this.server);

  static Future<_Peer> start() async {
    final p = _Peer._(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));
    p.handle = (frame) => frame.reply();
    p.server.listen((socket) {
      p.sockets.add(socket);
      final buffer = <int>[];
      socket.listen((bytes) {
        buffer.addAll(bytes);
        while (buffer.length >= 4) {
          final size = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (buffer.length < size + 4) break;
          final r = KafkaBytesReader.fromBytes(buffer.sublist(4, size + 4));
          buffer.removeRange(0, size + 4);
          final key = r.readInt16();
          final version = r.readInt16();
          final id = r.readInt32();
          r.readString();
          final frame = _Frame(socket, key, version, id, r);
          p.frames.add(frame);
          if (!p.received.isCompleted) p.received.complete(frame);
          p.handle(frame);
        }
      }, onError: (Object _) {});
    });
    return p;
  }

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
  }
}

class _Case {
  final String name;
  final int key, version;
  final KafkaRequest Function() request;
  _Case(this.name, this.key, this.version, this.request);
}

final _cases = <_Case>[
  _Case(
      'JoinGroup v4',
      11,
      4,
      () => JoinGroupRequest('group',
          sessionTimeoutMs: 10000,
          rebalanceTimeoutMs: 30000,
          protocols: [GroupJoinProtocol('range', [])])),
  _Case('SyncGroup v2', 14, 2,
      () => SyncGroupRequest('group', generationId: 7, memberId: 'member')),
  _Case('Heartbeat v2', 12, 2,
      () => HeartbeatRequest('group', generationId: 7, memberId: 'member')),
  _Case('LeaveGroup v2', 13, 2,
      () => LeaveGroupRequest('group', memberId: 'member')),
  _Case(
      'MemberOffsetCommit v6',
      8,
      6,
      () => MemberOffsetCommitRequest('group', 'topic', {0: 42},
          generationId: 7, memberId: 'member')),
];

void _settled(KafkaClient c, {bool disconnected = false}) {
  expect(c.pendingRequestCount, 0);
  expect(c.admittedRequestCount, 0);
  expect(c.queuedRequestBytes, 0);
  expect(c.bufferedResponseBytes, 0);
  if (disconnected) expect(c.connectionCount, 0);
}

void _once(_Peer p, _Case scenario) {
  expect(p.sockets, hasLength(1));
  expect(p.frames, hasLength(1),
      reason: 'No negotiation, discovery, rejoin, fallback or replay');
  expect(p.frames.single.key, scenario.key);
  expect(p.frames.single.version, scenario.version);
}

void main() {
  late _Peer peer;
  final clients = <KafkaClient>[];
  KafkaClient client(
      {int maxFrameBytes = 16777216, int maxPendingRequestBytes = 33554432}) {
    final c = KafkaClient(
        brokers: [Broker('127.0.0.1', peer.server.port)],
        maxReadRetries: 10,
        retryBaseDelay: Duration.zero,
        requestTimeout: const Duration(seconds: 1),
        maxFrameBytes: maxFrameBytes,
        maxPendingRequestBytes: maxPendingRequestBytes);
    clients.add(c);
    return c;
  }

  setUp(() async {
    peer = await _Peer.start();
  });
  tearDown(() async {
    for (final c in clients) {
      await c.close();
      _settled(c, disconnected: true);
    }
    clients.clear();
    await peer.close();
  });

  for (final scenario in _cases) {
    group(scenario.name, () {
      for (final failure in [
        'disconnect',
        'deadline',
        'malformed',
        'wrong ID'
      ]) {
        test('$failure preserves original failure and dispatches once',
            () async {
          peer.handle = (r) {
            switch (failure) {
              case 'disconnect':
                r.socket.destroy();
                break;
              case 'deadline':
                break; // Deliberately withhold the response after receipt.
              case 'malformed':
                r.socket.add(KafkaRequest.frame(
                    (KafkaBytesBuilder()..addInt32(r.id)).takeBytes()));
                break;
              case 'wrong ID':
                r.reply(id: -1); // Unknown IDs are ignored by the dispatcher.
                break;
            }
          };
          final c = client();
          final request = scenario.request();
          final expected = failure == 'disconnect'
              ? isA<KafkaConnectionException>()
              : failure == 'malformed'
                  ? isA<FormatException>()
                  : isA<TimeoutException>().having((e) => e.message,
                      'original deadline', 'Kafka request deadline exceeded');
          final checked = expectLater(c.send(request), throwsA(expected));
          await peer.received.future;
          await checked;
          // No synthetic success, rollback or high-level commit exception is
          // invented: after dispatch the broker-side outcome remains unknown.
          _once(peer, scenario);
          _settled(c, disconnected: failure != 'malformed');
          await expectLater(c.send(request), throwsStateError);
          _once(peer, scenario);
        });
      }

      for (final error in [14, 15, 16, 22, 25, 27, 79]) {
        test('raw broker error $error is returned without rejoin or fallback',
            () async {
          peer.handle = (r) {
            if (scenario.key == 8) {
              expect(r.body.readString(), 'group');
              expect(r.body.readInt32(), 7);
              expect(r.body.readString(), 'member');
            }
            r.reply(error: error);
          };
          final c = client();
          final response = await c.send(scenario.request());
          final int actual = response is GroupOffsetCommitResponse
              ? response.topics.single.partitions.single.errorCode
              : response.errorCode as int;
          expect(actual, error);
          _once(peer, scenario);
          _settled(c);
        });
      }

      test('reuse is rejected while pending and after successful completion',
          () async {
        peer.handle = (_) {};
        final c = client();
        final request = scenario.request();
        final result = c.send(request);
        final frame = await peer.received.future;
        await expectLater(c.send(request), throwsStateError);
        frame.reply();
        await result;
        await expectLater(c.send(request), throwsStateError);
        _once(peer, scenario);
        _settled(c);
      });

      for (final budget in ['frame', 'queued']) {
        test('$budget preflight admits no socket or request', () async {
          final c = client(
              maxFrameBytes: budget == 'frame' ? 4 : 16777216,
              maxPendingRequestBytes: budget == 'queued' ? 4 : 33554432);
          await expectLater(
              c.send(scenario.request()),
              throwsA(isA<KafkaResourceLimitException>().having(
                  (e) => e.resource,
                  'budget',
                  budget == 'frame'
                      ? 'request frame bytes'
                      : 'queued request bytes')));
          expect(peer.sockets, isEmpty);
          expect(peer.frames, isEmpty);
          _settled(c, disconnected: true);
        });
      }
    });
  }

  for (final scenario in _cases.take(2)) {
    for (final cancel in [false, true]) {
      test(
          '${scenario.name} pending ${cancel ? 'cancel' : 'close'} releases counters',
          () async {
        peer.handle = (_) {};
        final c = client();
        final token = KafkaCancellationToken();
        final checked = expectLater(
            c.send(scenario.request(), cancellationToken: token),
            throwsA(
                cancel ? isA<KafkaCancelledException>() : isA<StateError>()));
        await peer.received.future;
        expect(c.pendingRequestCount, 1);
        expect(c.admittedRequestCount, 1);
        if (cancel) {
          token.cancel();
        } else {
          await c.close();
        }
        await checked;
        _settled(c, disconnected: true);
        _once(peer, scenario);
      });
    }
  }
}
