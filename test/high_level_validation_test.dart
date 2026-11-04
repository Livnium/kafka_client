import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

const _int32Max = 0x7fffffff;

typedef _InvalidCall = Future<Object?> Function(KafkaClient client);

Map<String, int> _counters(KafkaClient client) => {
      'connections': client.connectionCount,
      'pending': client.pendingRequestCount,
      'admitted': client.admittedRequestCount,
      'queued bytes': client.queuedRequestBytes,
      'buffered bytes': client.bufferedResponseBytes,
      'bootstrap fallbacks': client.bootstrapTlsFallbackCount,
    };

void main() {
  final invalidCalls = <String, _InvalidCall>{
    'Fetch negative partition': (c) => c.fetch('topic', partition: -1),
    'Fetch negative offset': (c) => c.fetch('topic', offset: -1),
    'Fetch negative maxWaitMs': (c) => c.fetch('topic', maxWaitMs: -1),
    'Fetch zero maxBytes': (c) => c.fetch('topic', maxBytes: 0),
    'Fetch negative maxBytes': (c) => c.fetch('topic', maxBytes: -1),
    'Fetch overflowing partition': (c) =>
        c.fetch('topic', partition: _int32Max + 1),
    'Fetch overflowing maxWaitMs': (c) =>
        c.fetch('topic', maxWaitMs: _int32Max + 1),
    'Fetch overflowing maxBytes': (c) =>
        c.fetch('topic', maxBytes: _int32Max + 1),
    'Produce negative partition': (c) => c.produce(
        'topic',
        [
          KafkaRecord(value: [1])
        ],
        partition: -1),
    'Produce empty records': (c) => c.produce('topic', []),
    'Produce invalid acks': (c) => c.produce(
        'topic',
        [
          KafkaRecord(value: [1])
        ],
        acks: 2),
    'Produce invalid negative acks': (c) => c.produce(
        'topic',
        [
          KafkaRecord(value: [1])
        ],
        acks: -2),
    'Produce negative timeoutMs': (c) => c.produce(
        'topic',
        [
          KafkaRecord(value: [1])
        ],
        timeoutMs: -1),
    'Produce overflowing partition': (c) => c.produce(
        'topic',
        [
          KafkaRecord(value: [1])
        ],
        partition: _int32Max + 1),
    'Produce overflowing timeoutMs': (c) => c.produce(
        'topic',
        [
          KafkaRecord(value: [1])
        ],
        timeoutMs: _int32Max + 1),
  };

  for (final mode in ['plaintext', 'default TLS', 'TLS recovery']) {
    for (final entry in invalidCalls.entries) {
      test('${entry.key} fails asynchronously before I/O with $mode', () async {
        final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
        final sockets = <Socket>[];
        var accepts = 0;
        var receivedBytes = 0;
        final subscription = server.listen((socket) {
          accepts++;
          sockets.add(socket);
          socket.listen((data) => receivedBytes += data.length,
              onError: (Object _) {});
          // An accidental connection must fail promptly, not hang the test.
          socket.destroy();
        });
        final client = KafkaClient(
          brokers: [Broker('127.0.0.1', server.port)],
          // No certificates, certificate fixture generation, or file reads.
          tls: mode == 'plaintext' ? null : KafkaTlsConfig(),
          allowTlsBootstrapFallback: mode == 'TLS recovery',
          tlsFetchRecovery:
              mode == 'TLS recovery' ? KafkaTlsFetchRecovery() : null,
          connectionTimeout: const Duration(milliseconds: 250),
          requestTimeout: const Duration(milliseconds: 250),
        );
        addTearDown(() async {
          await client.close();
          for (final socket in sockets) {
            socket.destroy();
          }
          await server.close();
          await subscription.cancel();
        });

        expect(_counters(client).values, everyElement(0));
        late Future<Object?> result;
        // Passing the call itself to throwsA would also permit a synchronous
        // throw. Capture it separately to protect the public Future contract.
        expect(() => result = entry.value(client), returnsNormally);
        expect(_counters(client).values, everyElement(0));
        await expectLater(result, throwsA(isA<ArgumentError>()));
        await Future<void>.delayed(const Duration(milliseconds: 10));
        expect(accepts, 0);
        expect(receivedBytes, 0);
        expect(_counters(client).values, everyElement(0));
        await client.close();
        await Future<void>.delayed(const Duration(milliseconds: 10));
        expect(accepts, 0, reason: 'Closing must not release deferred work');
        expect(receivedBytes, 0);
        expect(_counters(client).values, everyElement(0));
      });
    }

    test('closed client preserves asynchronous error precedence with $mode',
        () async {
      final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      var accepts = 0;
      final subscription = server.listen((socket) {
        accepts++;
        socket.destroy();
      });
      final client = KafkaClient(
        brokers: [Broker('127.0.0.1', server.port)],
        tls: mode == 'plaintext' ? null : KafkaTlsConfig(),
        allowTlsBootstrapFallback: mode == 'TLS recovery',
        tlsFetchRecovery:
            mode == 'TLS recovery' ? KafkaTlsFetchRecovery() : null,
      );
      addTearDown(() async {
        await client.close();
        await server.close();
        await subscription.cancel();
      });
      await client.close();
      expect(_counters(client).values, everyElement(0));
      final closedCalls = <String, _InvalidCall>{
        ...invalidCalls,
        'Fetch valid': (c) => c.fetch('topic'),
        'Produce valid': (c) => c.produce('topic', [
              KafkaRecord(value: [1])
            ]),
      };
      for (final entry in closedCalls.entries) {
        late Future<Object?> result;
        expect(() => result = entry.value(client), returnsNormally,
            reason: entry.key);
        // These two existing Produce checks have always preceded closure.
        // All new validation preserves the prior closed-client StateError.
        final existingProduceCheck =
            entry.key == 'Produce negative partition' ||
                entry.key == 'Produce empty records';
        await expectLater(
            result,
            throwsA(existingProduceCheck
                ? isA<ArgumentError>()
                : isA<StateError>()),
            reason: entry.key);
        expect(_counters(client).values, everyElement(0), reason: entry.key);
      }
      await client.close();
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(accepts, 0);
      expect(_counters(client).values, everyElement(0));
    });
  }

  for (final acks in [-1, 0, 1]) {
    test('valid scalar boundaries encode exactly with acks=$acks', () async {
      final peer = await _BoundaryPeer.start();
      final client = KafkaClient(
        brokers: [Broker('127.0.0.1', peer.server.port)],
        requestTimeout: const Duration(seconds: 2),
      );
      addTearDown(() async {
        await client.close();
        await peer.close();
      });
      // These are scalar wire values, not buffer allocation sizes.
      final partition = acks == -1 ? _int32Max : 0;
      final maxWaitMs = acks == -1 ? _int32Max : 0;
      final maxBytes = acks == -1 ? _int32Max : 1;
      final timeoutMs = acks == -1 ? _int32Max : 0;
      final offset = acks == -1 ? 0x7fffffffffffffff : 0;
      peer.partition = partition;
      final fetched = await client.fetch('topic',
          partition: partition,
          offset: offset,
          maxWaitMs: maxWaitMs,
          maxBytes: maxBytes);
      expect(fetched.topics.single.partitions.single.records, isEmpty);
      expect(peer.fetches, [
        {
          'replicaId': -1,
          'maxWaitMs': maxWaitMs,
          'minBytes': 1,
          'maxBytes': maxBytes,
          'isolationLevel': 0,
          'partition': partition,
          'offset': offset,
          'partitionMaxBytes': maxBytes,
        }
      ]);
      final produced = await client.produce(
          'topic',
          [
            KafkaRecord(value: [1])
          ],
          partition: partition,
          acks: acks,
          timeoutMs: timeoutMs);
      if (acks == 0) {
        expect(produced, isNull);
      } else {
        expect(produced, isNotNull);
      }
      // acks=0 completes after local flush; wait for the owned peer to receive
      // the request before asserting its count, rather than assuming delivery.
      await peer.produceSeen.future.timeout(const Duration(seconds: 2));
      expect(peer.produces, [
        {'acks': acks, 'timeoutMs': timeoutMs, 'partition': partition}
      ]);
      expect(peer.metadataCalls, greaterThanOrEqualTo(1));
      expect(peer.errors, isEmpty);
      await client.close();
      expect(_counters(client).values, everyElement(0));
      expect(peer.produces, hasLength(1), reason: 'Produce must not replay');
    });
  }
}

/// Minimal plaintext Kafka peer, owned entirely by this test. It advertises only
/// the three protocol versions used here and never contacts a real broker.
class _BoundaryPeer {
  final ServerSocket server;
  final sockets = <Socket>[];
  final errors = <Object>[];
  final fetches = <Map<String, int>>[];
  final produces = <Map<String, int>>[];
  final produceSeen = Completer<void>();
  late final StreamSubscription<Socket> subscription;
  int partition = 0;
  int metadataCalls = 0;

  _BoundaryPeer(this.server) {
    subscription = server.listen(_accept, onError: (Object e) => errors.add(e));
  }

  static Future<_BoundaryPeer> start() async =>
      _BoundaryPeer(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));

  void _accept(Socket socket) {
    sockets.add(socket);
    final buffer = <int>[];
    socket.listen((data) {
      try {
        buffer.addAll(data);
        while (buffer.length >= 4) {
          final length = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (length < 0 || length > 65536) {
            throw StateError('Unexpected request length: $length');
          }
          if (buffer.length < length + 4) break;
          final r = KafkaBytesReader.fromBytes(buffer.sublist(4, length + 4));
          buffer.removeRange(0, length + 4);
          final key = r.readInt16();
          final version = r.readInt16();
          final id = r.readInt32();
          r.readString(); // client ID
          final b = KafkaBytesBuilder()..addInt32(id);
          if (key == 18) {
            b
              ..addInt16(0)
              ..addInt32(3);
            for (final pair in [
              [3, 0],
              [0, 3],
              [1, 4]
            ]) {
              b
                ..addInt16(pair[0])
                ..addInt16(pair[1])
                ..addInt16(pair[1]);
            }
          } else if (key == 3) {
            metadataCalls++;
            expect(version, 0);
            b
              ..addInt32(1)
              ..addInt32(1)
              ..addString('127.0.0.1')
              ..addInt32(server.port)
              ..addInt32(1)
              ..addInt16(0)
              ..addString('topic')
              ..addInt32(1)
              ..addInt16(0)
              ..addInt32(partition)
              ..addInt32(1)
              ..addInt32(1)
              ..addInt32(1)
              ..addInt32(1)
              ..addInt32(1);
          } else if (key == 1) {
            expect(version, 4);
            final values = <String, int>{
              'replicaId': r.readInt32(),
              'maxWaitMs': r.readInt32(),
              'minBytes': r.readInt32(),
              'maxBytes': r.readInt32(),
              'isolationLevel': r.readInt8(),
            };
            expect(r.readInt32(), 1);
            expect(r.readString(), 'topic');
            expect(r.readInt32(), 1);
            values['partition'] = r.readInt32();
            values['offset'] = r.readInt64();
            values['partitionMaxBytes'] = r.readInt32();
            expect(r.isEOF, isTrue);
            fetches.add(values);
            b
              ..addInt32(0)
              ..addInt32(1)
              ..addString('topic')
              ..addInt32(1)
              ..addInt32(partition)
              ..addInt16(0)
              ..addInt64(0)
              ..addInt64(0)
              ..addInt32(-1)
              ..addBytes([]);
          } else if (key == 0) {
            expect(version, 3);
            expect(r.readString(), isNull); // transactional ID
            final acks = r.readInt16();
            final timeoutMs = r.readInt32();
            expect(r.readInt32(), 1);
            expect(r.readString(), 'topic');
            expect(r.readInt32(), 1);
            final index = r.readInt32();
            expect(r.readBytes(), isNotEmpty);
            expect(r.isEOF, isTrue);
            produces.add(
                {'acks': acks, 'timeoutMs': timeoutMs, 'partition': index});
            if (!produceSeen.isCompleted) produceSeen.complete();
            if (acks == 0) continue;
            b
              ..addInt32(1)
              ..addString('topic')
              ..addInt32(1)
              ..addInt32(partition)
              ..addInt16(0)
              ..addInt64(0)
              ..addInt64(-1)
              ..addInt32(0);
          } else {
            throw StateError('Unexpected API key: $key');
          }
          socket.add(KafkaRequest.frame(b.takeBytes()));
        }
      } catch (error) {
        errors.add(error);
        socket.destroy();
      }
    }, onError: (Object error) => errors.add(error));
  }

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
    await subscription.cancel();
  }
}
