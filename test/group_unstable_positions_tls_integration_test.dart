import 'dart:async';
import 'dart:io';
import 'dart:convert';
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
  Completer<void>? positionGate;
  bool holdHeartbeats = false;
  int maxOffsetVersion = 7;
  final positionVersions = <int>[];
  final positionSockets = <Socket>{};
  final closedPositionSockets = <Socket>{};
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
  int heartbeatCode = 16;
  bool dropHeartbeat = false;
  int discoveries = 0;
  _Peer? coordinator;
  int generationBase = 36;
  void Function()? onHeartbeatRejected;
  int accepted = 0;
  int joins = 0, positionReads = 0, rejections = 0;
  bool holdJoin = false, holdPositions = false, emptyAssignment = false;
  String? positionFailure;
  int storedOffset = 0;
  List<String?> positionFailures = [];
  int? gatePositionRead;
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
          if (key == 9 && version == 7) expect(r.readInt8(), 0);
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
              final versions = {
                1: 4,
                3: 0,
                8: 6,
                9: maxOffsetVersion,
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
                  ..addInt16(v.key == 9 ? 5 : v.value)
                  ..addInt16(v.value);
              }
            case 10:
              discoveries++;
              b
                ..addInt32(0)
                ..addInt16(0)
                ..addString(null)
                ..addInt32(1)
                ..addString('localhost')
                ..addInt32((coordinator ?? this).server.port);
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
                ..addInt32(generationBase + joins)
                ..addString('range')
                ..addString('other-leader')
                ..addString('owned-member-$generationBase-$joins')
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
              if (holdHeartbeats) continue;
              expect(r.readString(), 'owned-group');
              expect(r.readInt32(), generationBase + joins);
              expect(r.readString(), 'owned-member-$generationBase-$joins');
              if (rejectHeartbeat) {
                rejections++;
                if (!heartbeatRejected.isCompleted) {
                  heartbeatRejected.complete();
                }
              }
              if (rejectHeartbeat && dropHeartbeat) {
                onHeartbeatRejected?.call();
                socket.destroy();
                return;
              }
              b
                ..addInt32(0)
                ..addInt16(rejectHeartbeat ? heartbeatCode : 0);
            case 9:
              expect(version, anyOf(5, 7));
              positionVersions.add(version);
              positionSockets.add(socket);
              if (version == 7) {
                expect(_readCompactString(r), 'owned-group');
                expect(r.readInt8(), 2);
                expect(_readCompactString(r), 'topic');
                expect(r.readInt8(), 2);
                expect(r.readInt32(), 0);
                expect(r.readInt8(), 0);
                expect(r.readInt8(), 1, reason: 'require_stable must be true');
                expect(r.readInt8(), 0);
              } else {
                expect(r.readString(), 'owned-group');
                expect(r.readInt32(), 1);
                expect(r.readString(), 'topic');
                expect(r.readInt32(), 1);
                expect(r.readInt32(), 0);
              }
              expect(r.isEOF, isTrue);
              positionReads++;
              if (positionFailures.isNotEmpty) {
                positionFailure = positionFailures.removeAt(0);
              }
              if (!positionsReceived.isCompleted) positionsReceived.complete();
              if (holdPositions) continue;
              if (positionFailure == 'malformed') break;
              if (version == 7) b.addInt8(0); // Response header tags.
              b.addInt32(0);
              if (version == 7) {
                b.addInt8(positionFailure == 'empty' ? 1 : 2);
              } else {
                b.addInt32(positionFailure == 'empty' ? 0 : 1);
              }
              if (positionFailure != 'empty') {
                if (version == 7) {
                  _compactString(b, 'topic');
                  b.addInt8(2);
                } else {
                  b
                    ..addString('topic')
                    ..addInt32(1);
                }
                b
                  ..addInt32(0)
                  ..addInt64(storedOffset)
                  ..addInt32(-1);
                if (version == 7) {
                  b.addInt8(0);
                } else {
                  b.addString(null);
                }
                b.addInt16(positionFailure == 'partition88'
                    ? 88
                    : positionFailure == 'partition29'
                        ? 29
                        : 0);
                if (version == 7) {
                  b
                    ..addInt8(0)
                    ..addInt8(0);
                }
              }
              b.addInt16(positionFailure == 'group88'
                  ? 88
                  : positionFailure == 'partition88' && mode == 'mixed'
                      ? 30
                      : 0);
              if (version == 7) b.addInt8(0);
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
          final gate = key == 1
              ? fetchGate
              : (key == 9 &&
                      (gatePositionRead == null ||
                          positionReads == gatePositionRead)
                  ? positionGate
                  : null);
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
      if (positionSockets.contains(socket)) closedPositionSockets.add(socket);
    }
  }

  Future<void> close() async {
    closing = true;
    for (final gate in [fetchGate, metadataGate, positionGate]) {
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

String _readCompactString(KafkaBytesReader r) {
  final size = r.readInt8() - 1;
  return utf8.decode(List.generate(size, (_) => r.readInt8()));
}

void _compactString(KafkaBytesBuilder b, String value) {
  final bytes = utf8.encode(value);
  b.addInt8(bytes.length + 1);
  for (final byte in bytes) {
    b.addInt8(byte);
  }
}

void main() {
  final env = Platform.environment;
  final dir = env['KAFKA_BOOTSTRAP_TLS_FIXTURE_DIR'];
  final configured = dir != null &&
      env['KAFKA_BOOTSTRAP_TLS_USERNAME'] != null &&
      env['KAFKA_BOOTSTRAP_TLS_PASSWORD'] != null;
  group('managed unstable positions TLS', () {
    final peers = <_Peer>[];
    KafkaGroupConsumer? consumer;
    final records = <KafkaGroupRecord>[];
    Future<_Peer> peer(String cert, {String mode = 'normal'}) async {
      final p = await _Peer.start(dir!, cert, mode: mode);
      peers.add(p);
      return p;
    }

    KafkaGroupConsumer makeConsumer(
      _Peer p, {
      bool stable = true,
      bool retry = true,
      bool expire = false,
      bool commit = false,
      int attempts = 3,
      Duration timeout = const Duration(seconds: 2),
      Duration delay = const Duration(milliseconds: 20),
      int maxRejoins = 2,
      Map<String, Map<int, int>> missingOffsets = const {},
    }) =>
        consumer = KafkaGroupConsumer(
            brokers: [p.broker],
            groupId: 'owned-group',
            topics: {'topic'},
            commitAfterProcessing: commit,
            missingOffsets: missingOffsets,
            requireStableOffsets: stable,
            stableOffsetRetry: retry
                ? KafkaStableOffsetRetry(
                    maxAttempts: attempts, timeout: timeout, retryDelay: delay)
                : null,
            rejoinOnCoordinatorLoss: true,
            maxRejoins: maxRejoins,
            tls: KafkaTlsConfig(
                trustedCertificates: File('$dir/ca.pem').readAsBytesSync(),
                withTrustedRoots: false),
            sasl: KafkaSaslPlain(
                username: env['KAFKA_BOOTSTRAP_TLS_USERNAME']!,
                password: env['KAFKA_BOOTSTRAP_TLS_PASSWORD']!),
            rejoinDelay: const Duration(milliseconds: 10),
            heartbeatInterval: const Duration(milliseconds: 100),
            localExpiry: Duration(milliseconds: expire ? 500 : 3000),
            sessionTimeout: const Duration(seconds: 5),
            rebalanceTimeout: const Duration(milliseconds: 100),
            requestTimeout: const Duration(seconds: 4),
            leaveTimeout: const Duration(milliseconds: 500),
            connectionTimeout: const Duration(seconds: 1),
            pollInterval: const Duration(milliseconds: 10),
            maxReadRetries: 0,
            retryBaseDelay: Duration.zero);
    setUp(() {
      peers.clear();
      consumer = null;
      records.clear();
    });
    tearDown(() async {
      consumer?.stop();
      await consumer?.closed.timeout(const Duration(seconds: 4));
      for (final p in peers) {
        await p.close();
      }
    });

    test('retry policy without stable opt-in fails before I/O', () async {
      final root = await peer('valid');
      expect(() => makeConsumer(root, stable: false), throwsArgumentError);
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(root.accepted, 0);
    });
    for (final fault in ['group88', 'partition88']) {
      test('$fault then success admits only stable position', () async {
        final root = await peer('valid');
        root.positionFailures = [fault, null];
        root.storedOffset = 12;
        final result = await makeConsumer(root).run((r) {
          records.add(r);
          consumer!.stop();
        }).timeout(const Duration(seconds: 4));
        expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
        expect(root.positionVersions, [7, 7]);
        expect(records.single.offset, 12);
        expect(root.fetches, [12]);
        expect(root.joins, 1);
        expect(root.commits, isEmpty);
      });
      test('$fault without retry remains terminal', () async {
        final root = await peer('valid');
        root.positionFailure = fault;
        final result = await makeConsumer(root, retry: false)
            .run(records.add)
            .timeout(const Duration(seconds: 4));
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(root.positionVersions, [7]);
        expect(root.fetches, isEmpty);
        expect(records, isEmpty);
        expect(root.joins, 1);
        expect(root.commits, isEmpty);
      });
    }
    for (final bound in ['attempts', 'deadline']) {
      test('repeated88 obeys $bound without rejoin or downgrade', () async {
        final root = await peer('valid');
        root.positionFailure = 'partition88';
        final result = await makeConsumer(
          root,
          attempts: bound == 'attempts' ? 3 : 20,
          timeout: bound == 'attempts'
              ? const Duration(seconds: 2)
              : const Duration(milliseconds: 300),
          delay: bound == 'attempts'
              ? const Duration(milliseconds: 10)
              : const Duration(seconds: 1),
        ).run(records.add).timeout(const Duration(seconds: 4));
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(
            root.positionVersions, List.filled(bound == 'attempts' ? 3 : 1, 7));
        expect(root.joins, 1);
        expect(root.fetches, isEmpty);
        expect(root.commits, isEmpty);
        expect(records, isEmpty);
      });
    }
    for (final ending in ['revoke', 'stop', 'expiry']) {
      for (final phase in ['backoff', 'retry']) {
        test('$ending during88 $phase denies late authority', () async {
          final root = await peer('valid');
          root.positionFailures = ['partition88', null];
          if (phase == 'retry') {
            root.gatePositionRead = 2;
            root.positionGate = Completer<void>();
          }
          final run = makeConsumer(root,
                  expire: ending == 'expiry',
                  delay: phase == 'backoff'
                      ? const Duration(seconds: 1)
                      : const Duration(milliseconds: 10))
              .run(records.add);
          await _until(
              () => root.positionReads == (phase == 'backoff' ? 1 : 2));
          if (ending == 'stop') {
            consumer!.stop();
          } else if (ending == 'expiry') {
            root.holdHeartbeats = true;
          } else {
            root.heartbeatCode = 27;
            root.rejectHeartbeat = true;
          }
          final result = await run.timeout(const Duration(seconds: 4));
          await _until(() => root.closedPositionSockets.length == 1);
          root.positionGate?.complete();
          await Future<void>.delayed(const Duration(milliseconds: 80));
          expect(
              result.reason,
              ending == 'stop'
                  ? KafkaGroupConsumerStopReason.cancelled
                  : KafkaGroupConsumerStopReason.failure);
          expect(result.generationsStarted, 1);
          expect(root.positionReads, phase == 'backoff' ? 1 : 2);
          expect(root.positionVersions.every((v) => v == 7), isTrue);
          expect(root.joins, 1);
          expect(root.fetches, isEmpty);
          expect(root.commits, isEmpty);
          expect(records, isEmpty);
        });
      }
    }
    test('deadline during delayed retry denies late successful position',
        () async {
      final root = await peer('valid');
      root.positionFailures = ['partition88', null];
      root.gatePositionRead = 2;
      root.positionGate = Completer<void>();
      final run = makeConsumer(root, timeout: const Duration(milliseconds: 400))
          .run(records.add);
      await _until(() => root.positionReads == 2);
      final result = await run.timeout(const Duration(seconds: 3));
      root.positionGate!.complete();
      await Future<void>.delayed(const Duration(milliseconds: 80));
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(root.positionVersions, [7, 7]);
      expect(root.fetches, isEmpty);
      expect(root.commits, isEmpty);
      expect(root.joins, 1);
      expect(records, isEmpty);
    });
    for (final fresh in [-1, 9, 10, 11]) {
      test('eventual position $fresh preserves previous generation floor',
          () async {
        final root = await peer('valid'),
            next = await peer('valid'),
            data = await peer('valid');
        root.storedOffset = 10;
        root.positionFailures = ['partition88', null];
        next.storedOffset = fresh;
        next.positionFailures = ['group88', null];
        next.generationBase = 80;
        data.storedOffset = fresh;
        final result = await makeConsumer(root, missingOffsets: {
          'topic': {0: 10}
        }).run((r) async {
          records.add(r);
          if (records.length == 1) {
            root.coordinator = next;
            root.rejectHeartbeat = true;
            await _until(
                () => consumer!.state == KafkaGroupConsumerState.draining);
            root.leaders = [data];
          } else {
            consumer!.stop();
          }
        }).timeout(const Duration(seconds: 5));
        expect(root.positionVersions, [7, 7]);
        expect(next.positionVersions, [7, 7]);
        expect(root.joins, 1);
        expect(next.joins, 1);
        expect(root.commits, isEmpty);
        expect(next.commits, isEmpty);
        if (fresh < 10) {
          expect(result.reason, KafkaGroupConsumerStopReason.failure);
          expect(records, hasLength(1));
          expect(data.accepted, 0);
        } else {
          expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
          expect(records, hasLength(2));
          expect(records.last.offset, fresh);
          expect(data.fetches, [fresh]);
        }
      });
    }
    for (final fault in ['unsupported', 'mixed', 'partition29', 'malformed']) {
      test('$fault stays terminal under retry policy', () async {
        final root =
            await peer('valid', mode: fault == 'mixed' ? 'mixed' : 'normal');
        if (fault == 'unsupported') {
          root.maxOffsetVersion = 6;
        } else {
          root.positionFailure = fault == 'mixed' ? 'partition88' : fault;
        }
        final result = await makeConsumer(root)
            .run(records.add)
            .timeout(const Duration(seconds: 4));
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(root.positionVersions, fault == 'unsupported' ? [] : [7]);
        expect(root.fetches, isEmpty);
        expect(root.commits, isEmpty);
        expect(root.joins, 1);
        expect(records, isEmpty);
      });
    }
    for (final defect in ['untrusted', 'wronghost', 'expired']) {
      test('$defect TLS never downgrades with retry enabled', () async {
        final bad = await peer(defect);
        final result = await makeConsumer(bad)
            .run(records.add)
            .timeout(const Duration(seconds: 4));
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(bad.accepted, 1);
        expect(bad.keys, isEmpty);
        expect(bad.tlsConnections, 0);
        expect(records, isEmpty);
      });
    }
    for (final outcome in ['unknown', 'rejected']) {
      test('$outcome commit after88 recovery is never replayed', () async {
        final root = await peer('valid');
        root.positionFailures = ['partition88', null];
        root.onCommit = (c) {
          root.rejectHeartbeat = true;
          if (outcome == 'unknown') {
            c.socket.destroy();
          } else {
            c.reply(codes: [29]);
          }
        };
        final result = await makeConsumer(root, commit: true)
            .run(records.add)
            .timeout(const Duration(seconds: 4));
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(root.positionVersions, [7, 7]);
        expect(root.commits, hasLength(1));
        expect(root.commits.single.offsets, {0: 1});
        expect(root.joins, 1);
        expect(
            result.lastCommit!.partitions[0]!.outcome,
            outcome == 'unknown'
                ? KafkaGroupCommitOutcome.unknown
                : KafkaGroupCommitOutcome.rejected);
        expect(records, hasLength(1));
      });
    }
  },
      skip: configured
          ? false
          : 'Run through owned TLS certificate fixture runner');
}
