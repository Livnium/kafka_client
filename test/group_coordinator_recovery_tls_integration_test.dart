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
  group('managed coordinator recovery TLS', () {
    final peers = <_Peer>[];
    KafkaGroupConsumer? consumer;
    final records = <KafkaGroupRecord>[];
    Future<_Peer> peer(String cert, {String mode = 'normal'}) async {
      final p = await _Peer.start(dir!, cert, mode: mode);
      peers.add(p);
      return p;
    }

    KafkaGroupConsumer makeConsumer(_Peer p,
            {bool enabled = true,
            bool commit = false,
            int maxRejoins = 2,
            Map<String, Map<int, int>> missingOffsets = const {}}) =>
        consumer = KafkaGroupConsumer(
            brokers: [p.broker],
            groupId: 'owned-group',
            topics: {'topic'},
            commitAfterProcessing: commit,
            missingOffsets: missingOffsets,
            rejoinOnCoordinatorLoss: enabled,
            tls: KafkaTlsConfig(
                trustedCertificates: File('$dir/ca.pem').readAsBytesSync(),
                withTrustedRoots: false),
            sasl: KafkaSaslPlain(
                username: env['KAFKA_BOOTSTRAP_TLS_USERNAME']!,
                password: env['KAFKA_BOOTSTRAP_TLS_PASSWORD']!),
            maxRejoins: maxRejoins,
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
    void rejected(_Peer bad) {
      expect(bad.accepted, 1);
      expect(bad.keys, isEmpty,
          reason: 'Rejected TLS peer must receive no SASL or Kafka frames');
      expect(bad.tlsConnections, 0);
    }

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

    for (final fault in ['code15', 'code16', 'disconnect']) {
      for (final enabled in [false, true]) {
        test(
            '$fault stable heartbeat optIn=$enabled drains before fresh generation',
            () async {
          final root = await peer('valid'), next = await peer('valid');
          next.generationBase = 80;
          next.storedOffset = 10;
          final callbackEntered = Completer<void>();
          final callbackRelease = Completer<void>();
          addTearDown(() {
            if (!callbackRelease.isCompleted) callbackRelease.complete();
          });
          var callbackDrained = false;
          var oldDataClosed = false;
          final run =
              makeConsumer(root, enabled: enabled, commit: true).run((r) async {
            records.add(r);
            if (records.length == 1) {
              callbackEntered.complete();
              await callbackRelease.future;
              callbackDrained = true;
            } else {
              consumer!.stop();
            }
          });
          await callbackEntered.future.timeout(const Duration(seconds: 3));
          final oldToken = records.single.assignment;
          final discoveriesBeforeLoss = root.discoveries;
          final oldSocket = root.fetchSockets.single;
          next.onJoin = () async {
            expect(callbackDrained, isTrue);
            await _until(() => root.closedFetchSockets.contains(oldSocket));
            oldDataClosed = true;
          };
          root.coordinator = next;
          root.leaders = [next];
          root.heartbeatCode = fault == 'code15' ? 15 : 16;
          root.dropHeartbeat = fault == 'disconnect';
          root.rejectHeartbeat = true;
          await root.heartbeatRejected.future
              .timeout(const Duration(seconds: 3));
          await Future<void>.delayed(const Duration(milliseconds: 150));
          expect(next.joins, 0,
              reason: 'Replacement must wait for callback drain');
          expect(records, hasLength(1));
          callbackRelease.complete();
          final result = await run.timeout(const Duration(seconds: 5));
          expect(result.cleanupErrors, isEmpty);
          expect(root.joins, 1);
          expect(root.commits, isEmpty,
              reason: 'Revoked callback cannot commit');
          expect(next.commits, isEmpty);
          if (enabled) {
            expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
            expect(result.generationsStarted, 2);
            expect(next.joins, 1);
            expect(root.discoveries, greaterThan(discoveriesBeforeLoss));
            expect(oldDataClosed, isTrue);
            expect(next.positionReads, 1);
            expect(next.fetches.first, 10);
            expect(records, hasLength(2));
            expect(records.last.offset, 10);
            expect(records.last.assignment.generationId, 81);
            expect(identical(oldToken, records.last.assignment), isFalse);
          } else {
            expect(result.reason, KafkaGroupConsumerStopReason.failure);
            expect(result.generationsStarted, 1);
            expect(next.joins, 0);
            expect(next.accepted, 0);
            expect(root.discoveries, discoveriesBeforeLoss);
          }
        });
      }
    }

    for (final defect in [
      'untrusted',
      'wronghost',
      'expired',
      'auth',
      'malformed'
    ]) {
      test('$defect replacement startup is terminal without retry', () async {
        final root = await peer('valid');
        final bad = await peer(
            ['auth', 'malformed'].contains(defect) ? 'valid' : defect,
            mode: ['auth', 'malformed'].contains(defect) ? defect : 'normal');
        final run = makeConsumer(root).run((r) {
          records.add(r);
          root.coordinator = bad;
          root.rejectHeartbeat = true;
        });
        final result = await run.timeout(const Duration(seconds: 5));
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(result.error, isNotNull);
        expect(root.joins, 1);
        expect(bad.joins, 0);
        expect(bad.positionReads, 0);
        expect(bad.fetches, isEmpty);
        expect(root.commits, isEmpty);
        expect(bad.commits, isEmpty);
        expect(records, hasLength(1));
        if (!['auth', 'malformed'].contains(defect)) {
          rejected(bad);
        } else {
          expect(bad.accepted, 1);
          expect(bad.keys.where((k) => k == 36), hasLength(1));
        }
      });
    }

    for (final fault in ['code16', 'disconnect']) {
      test('$fault initial heartbeat is terminal without retry', () async {
        final root = await peer('valid');
        root.rejectHeartbeat = true;
        root.dropHeartbeat = fault == 'disconnect';
        final result = await makeConsumer(root).run((_) {
          fail('Initial heartbeat failure cannot deliver');
        }).timeout(const Duration(seconds: 4));
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(root.joins, 1);
        expect(root.rejections, 1);
        expect(root.positionReads, 0);
        expect(root.fetches, isEmpty);
        expect(root.commits, isEmpty);
      });
    }

    for (final enabled in [true, false]) {
      for (final fresh in (enabled ? [-1, 9, 10, 11] : [-1, 9])) {
        test(
            'replacement position $fresh after stored10 optIn=$enabled uses only fresh authority',
            () async {
          final root = await peer('valid'),
              next = await peer('valid'),
              data = await peer('valid');
          root.storedOffset = 10;
          next.storedOffset = fresh;
          next.generationBase = 80;
          data.storedOffset = fresh < 0 ? 10 : fresh;
          final rejectFresh = enabled && fresh < 10;
          final run = makeConsumer(root, enabled: enabled, missingOffsets: {
            'topic': {0: 10}
          }).run((r) async {
            records.add(r);
            if (records.length == 1) {
              root.coordinator = next;
              root.heartbeatCode = enabled ? 16 : 27;
              root.rejectHeartbeat = true;
              await _until(
                  () => consumer!.state == KafkaGroupConsumerState.draining);
              // Switch only after revocation: otherwise the old generation's
              // next poll could legitimately reach the advertised data peer.
              root.leaders = [data];
            } else {
              consumer!.stop();
            }
          });
          final result = await run.timeout(const Duration(seconds: 5));
          expect(result.cleanupErrors, isEmpty);
          expect(root.joins, 1);
          expect(next.joins, 1);
          expect(root.positionReads, 1);
          expect(next.positionReads, 1,
              reason: 'Fresh position must be read exactly once');
          expect(root.commits, isEmpty);
          expect(next.commits, isEmpty);
          expect(data.commits, isEmpty);
          expect(records.first.offset, 10);
          if (rejectFresh) {
            expect(result.reason, KafkaGroupConsumerStopReason.failure);
            expect(result.error, isNotNull);
            expect(records, hasLength(1));
            expect(data.accepted, 0,
                reason: 'Invalid fresh position fails before data startup');
            expect(data.fetches, isEmpty);
          } else {
            expect(result.reason, KafkaGroupConsumerStopReason.cancelled);
            expect(records, hasLength(2));
            expect(records.last.offset, fresh < 0 ? 10 : fresh);
            expect(data.fetches, [fresh < 0 ? 10 : fresh]);
            expect(records.last.assignment.generationId, 81);
          }
        });
      }
    }

    test('ambiguous Join is sent once without retry', () async {
      final root = await peer('valid', mode: 'dropJoin');
      final result = await makeConsumer(root).run((_) {
        fail('Unconfirmed Join cannot deliver');
      }).timeout(const Duration(seconds: 4));
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(root.joins, 1);
      expect(root.positionReads, 0);
      expect(root.fetches, isEmpty);
    });

    test('ambiguous commit is sent once without retry', () async {
      final root = await peer('valid');
      root.onCommit = (c) => c.socket.destroy();
      final result = await makeConsumer(root, commit: true)
          .run(records.add)
          .timeout(const Duration(seconds: 4));
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.lastCommit!.partitions[0]!.outcome,
          KafkaGroupCommitOutcome.unknown);
      expect(root.commits, hasLength(1));
      expect(root.commits.single.offsets, {0: 1});
      expect(root.joins, 1);
      expect(records, hasLength(1));
    });
  },
      skip: configured
          ? false
          : 'Run through owned TLS certificate fixture runner');
}
