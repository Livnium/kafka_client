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
  final SecurityContext? context;
  final String certificate;
  String mode;
  List<_Peer> leaders = [];
  int metadataCalls = 0;
  int tlsConnections = 0;
  int tlsRejections = 0;
  bool closing = false;
  Completer<void>? fetchGate;
  Completer<void>? metadataGate;
  final metadataSeen = Completer<void>();
  final fetchSeen = Completer<void>();
  final fetchSockets = <Socket>{};
  final controlSockets = <Socket>{};
  final closedFetchSockets = <Socket>{};
  Future<void> Function()? onJoin;

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
  final fetches = <int>[];
  final joinReceived = Completer<void>();
  final positionsReceived = Completer<void>();
  final heartbeatRejected = Completer<void>();
  _Peer(this.server, this.context, this.certificate, this.mode) {
    subscription = server.listen((socket) {
      accepted++;
      sockets.add(socket);
      unawaited(socket.done.then<void>((_) {}, onError: (Object error) {
        if (error is! SocketException) errors.add(error);
      }));
      tasks.add(_serve(socket));
    }, onError: (Object e) => errors.add(e));
  }
  static Future<_Peer> start(String dir, String certificate,
      {String mode = 'normal'}) async {
    final context = certificate == 'eof'
        ? null
        : (SecurityContext()
          ..useCertificateChain('$dir/$certificate.pem')
          ..usePrivateKey('$dir/$certificate.key'));
    return _Peer(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0),
        context, certificate, mode);
  }

  Broker get broker => Broker('localhost', server.port);

  Future<void> _serve(Socket socket) async {
    final raw = socket;
    final buffer = <int>[];
    try {
      if (context == null) {
        await for (final chunk in raw) {
          buffer.addAll(chunk);
          if (buffer.length > 65540) throw StateError('Oversized ClientHello');
          if (buffer.length >= 6 &&
              buffer.length >= 5 + ((buffer[3] << 8) | buffer[4])) {
            expect(buffer[0], 22);
            expect(buffer[5], 1);
            await raw.close();
            return;
          }
        }
        return;
      }
      try {
        socket = await SecureSocket.secureServer(raw, context!)
            .timeout(const Duration(seconds: 3));
        sockets.add(socket);
        unawaited(socket.done.then<void>((_) {}, onError: (Object error) {
          if (error is! SocketException) errors.add(error);
        }));
        tlsConnections++;
      } on HandshakeException {
        tlsRejections++;
        return;
      }

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
          if (mode == 'malformed' && key == 18) {
            socket.add(KafkaRequest.frame(b.takeBytes()));
            continue;
          }
          switch (key) {
            case 17:
              b
                ..addInt16(0)
                ..addInt32(1)
                ..addString('PLAIN');
            case 36:
              b
                ..addInt16(mode == 'auth' ? 58 : 0)
                ..addString(mode == 'auth' ? 'denied' : null)
                ..addBytes([]);
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
                ..addString('localhost')
                ..addInt32(server.port);
            case 11:
              expect(version, 4);
              controlSockets.add(socket);
              joins++;
              await onJoin?.call();
              if (!joinReceived.isCompleted) joinReceived.complete();
              if (mode == 'dropJoin') {
                socket.destroy();
                return;
              }
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
              if (!controlSockets.contains(socket)) metadataCalls++;
              if (!metadataSeen.isCompleted) metadataSeen.complete();
              final leader = controlSockets.contains(socket) || leaders.isEmpty
                  ? this
                  : leaders[metadataCalls > leaders.length
                      ? leaders.length - 1
                      : metadataCalls - 1];
              final gate = metadataGate;
              if (gate != null) await gate.future;
              if (closing) return;
              b
                ..addInt32(1)
                ..addInt32(1)
                ..addString('localhost')
                ..addInt32(leader.server.port)
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
              fetchSockets.add(socket);
              if (!fetchSeen.isCompleted) fetchSeen.complete();
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
                ..addBytes(offset == storedOffset && storedOffset >= 0
                    ? _batch(offset, p)
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
          final gate = key == 1 ? fetchGate : null;
          if (gate != null) {
            final target = socket;
            final response = KafkaRequest.frame(b.takeBytes());
            tasks.add(gate.future.then((_) {
              if (!closing && sockets.contains(target)) target.add(response);
            }));
            continue;
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
      sockets.remove(raw);
      if (fetchSockets.contains(socket)) closedFetchSockets.add(socket);
    }
  }

  Future<void> close() async {
    closing = true;
    for (final gate in [fetchGate, metadataGate]) {
      if (gate != null && !gate.isCompleted) gate.complete();
    }
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
  final env = Platform.environment;
  final dir = env['KAFKA_BOOTSTRAP_TLS_FIXTURE_DIR'];
  final configured = dir != null &&
      env['KAFKA_BOOTSTRAP_TLS_USERNAME'] != null &&
      env['KAFKA_BOOTSTRAP_TLS_PASSWORD'] != null;
  group('managed TLS Fetch recovery', () {
    final peers = <_Peer>[];
    KafkaGroupSession? session;
    KafkaGroupConsumer? consumer;
    KafkaGroupDelivery? delivery;
    StreamSubscription<KafkaGroupRecord>? listener;
    bool stopAfterRecord = false;
    KafkaTlsConfig? tlsOverride;
    final errors = <Object>[];
    final records = <KafkaGroupRecord>[];
    KafkaTlsFetchRecovery policy(
            {int attempts = 3, Duration delay = Duration.zero}) =>
        KafkaTlsFetchRecovery(
            maxAttempts: attempts,
            window: const Duration(seconds: 5),
            retryDelay: delay);
    KafkaTlsConfig trust() => KafkaTlsConfig(
        trustedCertificates: File('$dir/ca.pem').readAsBytesSync(),
        withTrustedRoots: false);
    KafkaSaslPlain sasl() => KafkaSaslPlain(
        username: env['KAFKA_BOOTSTRAP_TLS_USERNAME']!,
        password: env['KAFKA_BOOTSTRAP_TLS_PASSWORD']!);
    Future<_Peer> peer(String cert, {String mode = 'normal'}) async {
      final p = await _Peer.start(dir!, cert, mode: mode);
      peers.add(p);
      return p;
    }

    KafkaGroupSession makeSession(_Peer p,
            {KafkaTlsFetchRecovery? recovery, bool fallback = true}) =>
        session = KafkaGroupSession(
            brokers: [p.broker],
            groupId: 'owned-group',
            topics: {'topic'},
            tls: tlsOverride ?? trust(),
            sasl: sasl(),
            allowTlsBootstrapFallback: fallback,
            tlsFetchRecovery: recovery,
            heartbeatInterval: const Duration(milliseconds: 100),
            localExpiry: const Duration(seconds: 3),
            sessionTimeout: const Duration(seconds: 5),
            rebalanceTimeout: const Duration(milliseconds: 100),
            requestTimeout: const Duration(seconds: 1),
            leaveTimeout: const Duration(milliseconds: 500),
            connectionTimeout: const Duration(seconds: 1),
            maxReadRetries: 0,
            retryBaseDelay: Duration.zero);
    KafkaGroupConsumer makeConsumer(_Peer p,
            {KafkaTlsFetchRecovery? recovery,
            bool commit = false,
            bool fallback = true}) =>
        consumer = KafkaGroupConsumer(
            brokers: [p.broker],
            groupId: 'owned-group',
            topics: {'topic'},
            commitAfterProcessing: commit,
            tls: tlsOverride ?? trust(),
            sasl: sasl(),
            allowTlsBootstrapFallback: fallback,
            tlsFetchRecovery: recovery,
            maxRejoins: 2,
            rejoinDelay: const Duration(milliseconds: 10),
            heartbeatInterval: const Duration(milliseconds: 100),
            localExpiry: const Duration(seconds: 3),
            sessionTimeout: const Duration(seconds: 5),
            rebalanceTimeout: const Duration(milliseconds: 100),
            requestTimeout: const Duration(seconds: 1),
            leaveTimeout: const Duration(milliseconds: 500),
            connectionTimeout: const Duration(seconds: 1),
            pollInterval: const Duration(milliseconds: 10),
            maxReadRetries: 0,
            retryBaseDelay: Duration.zero);
    Future<void> startSession(_Peer p,
        {KafkaTlsFetchRecovery? recovery, bool fallback = true}) async {
      final s = makeSession(p, recovery: recovery, fallback: fallback);
      final token = await s.start();
      delivery = s.subscribe(
          assignment: token,
          startOffsets: {
            'topic': {0: 0}
          },
          pollInterval: const Duration(milliseconds: 10));
      listener = delivery!.records.listen((record) {
        records.add(record);
        if (stopAfterRecord) unawaited(session!.close());
      }, onError: errors.add);
    }

    void rejected(_Peer bad) {
      expect(bad.accepted, 1);
      expect(bad.keys, isEmpty,
          reason: 'No Kafka or SASL application frames on rejected TLS');
      expect(bad.tlsConnections, 0);
    }

    setUp(() {
      stopAfterRecord = false;
      tlsOverride = null;
      peers.clear();
      session = null;
      consumer = null;
      delivery = null;
      listener = null;
      errors.clear();
      records.clear();
    });
    tearDown(() async {
      consumer?.stop();
      await consumer?.closed.timeout(const Duration(seconds: 4));
      await delivery?.cancel().timeout(const Duration(seconds: 4));
      await listener?.cancel();
      await session?.close().timeout(const Duration(seconds: 4));
      for (final p in peers) {
        await p.close();
      }
    });

    for (final api in ['session', 'consumer']) {
      test('$api invalid local CA is terminal before any network', () async {
        final root = await peer('valid');
        tlsOverride = KafkaTlsConfig(
            trustedCertificates: [1, 2, 3], withTrustedRoots: false);
        if (api == 'session') {
          await expectLater(
              makeSession(root, recovery: policy()).start(), throwsA(anything));
          await session!.closed.timeout(const Duration(seconds: 4));
        } else {
          final result = await makeConsumer(root, recovery: policy()).run((_) {
            fail('Invalid local CA cannot deliver');
          }).timeout(const Duration(seconds: 4));
          expect(result.reason, KafkaGroupConsumerStopReason.failure);
          expect(result.error, isNotNull);
        }
        expect(root.accepted, 0);
        expect(root.keys, isEmpty);
      });
      for (final fallback in [false, true]) {
        test(
            '$api null policy preserves terminal leader rejection fallback=$fallback',
            () async {
          final root = await peer('valid'),
              bad = await peer('untrusted'),
              good = await peer('valid');
          root.leaders = [bad, good];
          if (api == 'session') {
            await startSession(root, fallback: fallback);
            final stopped =
                await delivery!.stopped.timeout(const Duration(seconds: 4));
            expect(stopped.error, isNotNull);
            expect(records, isEmpty);
          } else {
            final result =
                await makeConsumer(root, fallback: fallback).run((_) {
              fail('No default recovery callback');
            }).timeout(const Duration(seconds: 4));
            expect(result.reason, KafkaGroupConsumerStopReason.failure);
            expect(result.generationsStarted, 1);
          }
          rejected(bad);
          expect(root.metadataCalls, 1);
          expect(good.accepted, 0);
          expect(root.joins, 1);
          expect(root.commits, isEmpty);
        });
      }
      for (final cert in ['eof', 'untrusted', 'wronghost', 'expired']) {
        test(
            '$api $cert leader rejected once then strictly validated replacement',
            () async {
          final root = await peer('valid'),
              bad = await peer(cert),
              good = await peer('valid');
          root.leaders = [bad, good];
          if (api == 'session') {
            stopAfterRecord = true;
            await startSession(root, recovery: policy());
            await _until(() => records.isNotEmpty);
            await session!.close();
            expect(records.single.assignment.generationId, 37);
            expect(errors, isEmpty);
          } else {
            final result =
                await makeConsumer(root, recovery: policy()).run((r) {
              records.add(r);
              consumer!.stop();
            }).timeout(const Duration(seconds: 4));
            expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
            expect(result.cleanupErrors, isEmpty);
            expect(root.positionReads, 1);
          }
          rejected(bad);
          expect(root.metadataCalls, 2);
          expect(good.fetches.first, 0);
          expect(good.keys, containsAllInOrder([17, 36, 18, 1]));
          expect(records.single.offset, 0);
          expect(root.joins, 1);
          expect(root.commits, isEmpty);
        });
      }
    }

    for (final mode in ['exhaustion', 'auth', 'malformed']) {
      test('$mode after rejected leader is terminal without automatic rejoin',
          () async {
        final root = await peer('valid'), bad = await peer('untrusted');
        final next =
            await peer('valid', mode: mode == 'exhaustion' ? 'normal' : mode);
        root.leaders = mode == 'exhaustion' ? [bad] : [bad, next];
        final result = await makeConsumer(root, recovery: policy()).run((_) {
          fail('Terminal Fetch failure must not deliver');
        }).timeout(const Duration(seconds: 5));
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(result.generationsStarted, 1);
        expect(result.error, isNotNull);
        expect(result.cleanupErrors, isEmpty);
        expect(root.joins, 1);
        expect(root.positionReads, 1);
        expect(root.commits, isEmpty);
        expect(next.fetches, isEmpty);
        rejected(bad);
        expect(root.metadataCalls, mode == 'exhaustion' ? 3 : 2);
      });
    }

    for (final action in ['close', 'heartbeat revoke']) {
      test(
          '$action while recovered Fetch is gated allows no callback or commit',
          () async {
        final root = await peer('valid'),
            bad = await peer('wronghost'),
            good = await peer('valid');
        root.leaders = [bad, good];
        good.fetchGate = Completer<void>();
        await startSession(root, recovery: policy());
        await good.fetchSeen.future.timeout(const Duration(seconds: 4));
        if (action == 'close') {
          await session!.close();
        } else {
          root.rejectHeartbeat = true;
          await session!.revoked.timeout(const Duration(seconds: 3));
        }
        good.fetchGate!.complete();
        final stopped =
            await delivery!.stopped.timeout(const Duration(seconds: 4));
        await session!.closed.timeout(const Duration(seconds: 4));
        expect(stopped.cleanupError, isNull);
        expect(records, isEmpty);
        expect(root.commits, isEmpty);
        expect(root.joins, 1);
        expect(good.fetches, [0]);
        rejected(bad);
      });
    }

    test(
        'stop after TLS rejection with recovery delay admits no later metadata or callback',
        () async {
      final root = await peer('valid'),
          bad = await peer('expired'),
          good = await peer('valid');
      root.leaders = [bad, good];
      final run = makeConsumer(root,
              recovery: policy(delay: const Duration(seconds: 1)))
          .run((_) {
        fail('Stopped recovery cannot callback');
      });
      await _until(() => bad.tlsRejections == 1);
      consumer!.stop();
      final result = await run.timeout(const Duration(seconds: 4));
      expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
      expect(root.metadataCalls, 1);
      expect(good.accepted, 0);
      expect(root.joins, 1);
      expect(root.commits, isEmpty);
    });

    test(
        'heartbeat 27 rejoin cleans old recovery data before fresh generation and positions',
        () async {
      final root = await peer('valid'),
          bad = await peer('untrusted'),
          good = await peer('valid');
      root.leaders = [bad, good, bad, good];
      good.fetchGate = Completer<void>();
      final run = makeConsumer(root, recovery: policy()).run((record) {
        records.add(record);
        consumer!.stop();
      });
      await good.fetchSeen.future.timeout(const Duration(seconds: 4));
      final oldSocket = good.fetchSockets.single;
      var oldClosedBeforeJoinReply = false;
      root.onJoin = () async {
        if (root.joins == 2) {
          // Remote EOF ordering across sockets is not a local cleanup oracle.
          // Gate the new Join reply on observed old data closure.
          await _until(() => good.closedFetchSockets.contains(oldSocket));
          oldClosedBeforeJoinReply = true;
          good.fetchGate!.complete();
          good.fetchGate = null;
        }
      };
      root.storedOffset = 10;
      good.storedOffset = 10;
      root.onHeartbeatRejected = () {
        root.rejectHeartbeat = false;
      };
      root.rejectHeartbeat = true;
      final result = await run.timeout(const Duration(seconds: 5));
      expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
      expect(result.cleanupErrors, isEmpty);
      expect(result.generationsStarted, 2);
      expect(root.joins, 2);
      expect(root.positionReads, 2);
      expect(oldClosedBeforeJoinReply, isTrue);
      expect(good.fetches, [0, 10]);
      expect(records.single.offset, 10);
      expect(records.single.assignment.generationId, 38);
      expect(root.commits, isEmpty);
      expect(bad.accepted, 2);
      expect(bad.keys, isEmpty);
      expect(bad.tlsConnections, 0);
    });

    test('ambiguous Join is sent once and never gains Fetch recovery replay',
        () async {
      final root = await peer('valid', mode: 'dropJoin');
      final result = await makeConsumer(root, recovery: policy()).run((_) {
        fail('Unconfirmed Join cannot deliver');
      }).timeout(const Duration(seconds: 4));
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(root.joins, 1);
      expect(root.keys.where((key) => key == 11), hasLength(1));
      expect(root.positionReads, 0);
      expect(root.metadataCalls, 0);
      expect(root.fetches, isEmpty);
      expect(root.commits, isEmpty);
    });

    test(
        'ambiguous commit after recovered Fetch is sent once and never rejoins',
        () async {
      final root = await peer('valid'),
          bad = await peer('eof'),
          good = await peer('valid');
      root.leaders = [bad, good];
      root.onCommit = (c) {
        c.socket.destroy();
      };
      final result = await makeConsumer(root, recovery: policy(), commit: true)
          .run(records.add)
          .timeout(const Duration(seconds: 5));
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.lastCommit!.partitions[0]!.outcome,
          KafkaGroupCommitOutcome.unknown);
      expect(root.commits, hasLength(1));
      expect(root.commits.single.generation, 37);
      expect(root.commits.single.offsets, {0: 1});
      expect(root.joins, 1);
      expect(records, hasLength(1));
      rejected(bad);
    });
  },
      skip: configured
          ? false
          : 'Run through owned TLS certificate fixture runner');
}
