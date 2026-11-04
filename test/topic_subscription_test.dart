import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:kafka_client/src/codec/record_batch.dart'
    show encodeRecordBatch, crc32c;
import 'package:test/test.dart';

const _deadline = Duration(seconds: 3);

Future<void> _until(bool Function() predicate) async {
  final end = DateTime.now().add(_deadline);
  while (!predicate()) {
    if (DateTime.now().isAfter(end)) {
      fail('Condition did not settle in $_deadline');
    }
    await Future<void>.delayed(const Duration(milliseconds: 2));
  }
}

Uint8List _batch(int base,
    {int count = 1, int? lastDelta, bool control = false}) {
  final bytes = encodeRecordBatch(
      List.generate(count, (i) => KafkaRecord(value: [i], timestamp: 0)));
  final data = ByteData.sublistView(bytes);
  data.setInt64(0, base, Endian.big);
  if (lastDelta != null) data.setInt32(23, lastDelta, Endian.big);
  if (control) data.setInt16(21, 0x20, Endian.big);
  data.setUint32(17, crc32c(bytes.sublist(21)), Endian.big);
  return bytes;
}

class _Fetch {
  final int partition;
  final int offset;
  final int maxWaitMs;
  final int maxBytes;
  final void Function(List<int> bytes, int error) reply;
  _Fetch(
      this.partition, this.offset, this.maxWaitMs, this.maxBytes, this.reply);
}

// An owned, bounded loopback peer, not a Kafka process. Every request uses the
// real public client, negotiation, routing and Fetch v4 decoding path.
class _Peer {
  final ServerSocket server;
  final sockets = <Socket>[];
  final fetches = <_Fetch>[];
  int metadataCalls = 0;
  int produceCalls = 0;
  void Function(_Fetch)? onFetch;
  _Peer._(this.server);

  static Future<_Peer> start() async {
    final peer =
        _Peer._(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));
    peer.server.listen(peer._accept);
    return peer;
  }

  void _accept(Socket socket) {
    sockets.add(socket);
    final buffer = <int>[];
    socket.listen((bytes) {
      buffer.addAll(bytes);
      while (buffer.length >= 4) {
        final size = KafkaBytesReader.fromBytes(buffer).readInt32();
        if (size < 0 || size > 1024 * 1024) {
          socket.destroy();
          return;
        }
        if (buffer.length < size + 4) return;
        final r = KafkaBytesReader.fromBytes(buffer.sublist(4, size + 4));
        buffer.removeRange(0, size + 4);
        final api = r.readInt16();
        r.readInt16();
        final correlation = r.readInt32();
        r.readString();
        final b = KafkaBytesBuilder()..addInt32(correlation);
        void send(KafkaBytesBuilder body) =>
            socket.add(KafkaRequest.frame(body.takeBytes()));
        if (api == 18) {
          b
            ..addInt16(0)
            ..addInt32(3);
          for (final version in [
            [0, 3],
            [1, 4],
            [3, 0]
          ]) {
            b
              ..addInt16(version[0])
              ..addInt16(version[1])
              ..addInt16(version[1]);
          }
          send(b);
        } else if (api == 3) {
          metadataCalls++;
          b
            ..addInt32(1)
            ..addInt32(1)
            ..addString('127.0.0.1')
            ..addInt32(server.port)
            ..addInt32(1)
            ..addInt16(0)
            ..addString('topic')
            ..addInt32(3);
          // Partition 2 exists, but explicit assignments must not discover it.
          for (var partition = 0; partition < 3; partition++) {
            b
              ..addInt16(0)
              ..addInt32(partition)
              ..addInt32(1)
              ..addInt32(1)
              ..addInt32(1)
              ..addInt32(1)
              ..addInt32(1);
          }
          send(b);
        } else if (api == 1) {
          expect(r.readInt32(), -1);
          final wait = r.readInt32();
          r.readInt32();
          final maxBytes = r.readInt32();
          expect(r.readInt8(), 0);
          expect(r.readInt32(), 1);
          expect(r.readString(), 'topic');
          expect(r.readInt32(), 1);
          final partition = r.readInt32();
          final offset = r.readInt64();
          expect(r.readInt32(), maxBytes);
          expect(r.isEOF, isTrue);
          final request =
              _Fetch(partition, offset, wait, maxBytes, (records, error) {
            final response = KafkaBytesBuilder()
              ..addInt32(correlation)
              ..addInt32(0)
              ..addInt32(1)
              ..addString('topic')
              ..addInt32(1)
              ..addInt32(partition)
              ..addInt16(error)
              ..addInt64(999)
              ..addInt64(999)
              ..addInt32(-1)
              ..addBytes(records);
            send(response);
          });
          fetches.add(request);
          final handler = onFetch;
          if (handler == null) {
            request.reply([], 0);
          } else {
            handler(request);
          }
        } else if (api == 0) {
          produceCalls++;
          b
            ..addInt32(1)
            ..addString('topic')
            ..addInt32(1)
            ..addInt32(0)
            ..addInt16(0)
            ..addInt64(0)
            ..addInt64(-1)
            ..addInt32(0);
          send(b);
        } else {
          fail('Unexpected API $api (no groups or commits are expected)');
        }
      }
    }, onError: (Object _) {});
  }

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
  }
}

void main() {
  group('subscription configuration without I/O', () {
    late KafkaClient client;
    setUp(() {
      client = KafkaClient(brokers: [Broker('127.0.0.1', 1)]);
    });
    tearDown(() async {
      await client.close();
    });

    test('rejects malformed arguments synchronously', () {
      final invalid = <void Function()>[
        () => client.subscribe('', startOffsets: {0: 0}),
        () => client.subscribe('topic', startOffsets: {}),
        () => client.subscribe('topic', startOffsets: {-1: 0}),
        () => client.subscribe('topic', startOffsets: {2147483648: 0}),
        () => client.subscribe('topic', startOffsets: {0: -1}),
        for (final wait in [-1, 2147483648])
          () =>
              client.subscribe('topic', startOffsets: {0: 0}, maxWaitMs: wait),
        for (final bytes in [0, -1, 2147483648])
          () =>
              client.subscribe('topic', startOffsets: {0: 0}, maxBytes: bytes),
        for (final delay in [
          Duration.zero,
          const Duration(microseconds: 999),
          const Duration(milliseconds: -1),
          const Duration(microseconds: 60000001)
        ]) ...[
          () => client.subscribe('topic',
              startOffsets: {0: 0}, pollInterval: delay),
          () => client.subscribe('topic',
              startOffsets: {0: 0}, retryDelay: delay),
        ],
      ];
      for (final action in invalid) {
        expect(action, throwsArgumentError);
      }
      expect(client.activeSubscriptionCount, 0);
    });

    test('accepts inclusive configuration bounds lazily', () {
      client.subscribe('topic',
          startOffsets: {2147483647: 9223372036854775807},
          maxWaitMs: 2147483647,
          maxBytes: 2147483647,
          pollInterval: const Duration(minutes: 1),
          retryDelay: const Duration(minutes: 1));
      client.subscribe('topic',
          startOffsets: {0: 0},
          maxWaitMs: 0,
          maxBytes: 1,
          pollInterval: const Duration(milliseconds: 1),
          retryDelay: const Duration(milliseconds: 1));
      expect(client.activeSubscriptionCount, 0);
    });

    test('closed client validates first, then returns StateError streams',
        () async {
      final beforeClose = client.subscribe('topic', startOffsets: {0: 0});
      await client.close();
      expect(() => client.subscribe('', startOffsets: {}), throwsArgumentError);
      await expectLater(beforeClose, emitsError(isA<StateError>()));
      await expectLater(client.subscribe('topic', startOffsets: {0: 0}),
          emitsError(isA<StateError>()));
      expect(client.activeSubscriptionCount, 0);
    });
  });

  group('subscription public wire path', () {
    late _Peer peer;
    late KafkaClient client;
    setUp(() async {
      peer = await _Peer.start();
      client = KafkaClient(
          brokers: [Broker('127.0.0.1', peer.server.port)],
          maxReadRetries: 0,
          requestTimeout: const Duration(seconds: 2),
          connectionTimeout: const Duration(seconds: 2));
    });
    tearDown(() async {
      await client.close().timeout(_deadline);
      await peer.close();
    });

    test('lazy single-listener stream copies explicit offsets and wire limits',
        () async {
      final offsets = {0: 10};
      final stream = client.subscribe('topic',
          startOffsets: offsets, maxWaitMs: 17, maxBytes: 4096);
      offsets[0] = 400;
      offsets[1] = 900;
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(peer.sockets, isEmpty);
      expect(client.activeSubscriptionCount, 0);
      peer.onFetch = (request) => request.reply(_batch(request.offset), 0);
      final record = await stream.first.timeout(_deadline);
      expect(record.topic, 'topic');
      expect(record.partition, 0);
      expect(record.offset, 10);
      expect(record.record.value, [0]);
      expect(peer.fetches.single.maxWaitMs, 17);
      expect(peer.fetches.single.maxBytes, 4096);
      expect(() => stream.listen((_) {}), throwsStateError);
      await _until(() => client.activeSubscriptionCount == 0);
    });

    test('two explicit partitions advance independently in partition order',
        () async {
      peer.onFetch =
          (request) => request.reply(_batch(request.offset, count: 2), 0);
      final records = await client
          .subscribe('topic',
              startOffsets: {0: 10, 1: 100},
              pollInterval: const Duration(milliseconds: 1))
          .take(6)
          .toList()
          .timeout(_deadline);
      for (final partition in [0, 1]) {
        final actual = records
            .where((r) => r.partition == partition)
            .map((r) => r.offset)
            .toList();
        expect(actual, isNotEmpty);
        final start = partition == 0 ? 10 : 100;
        expect(actual, List.generate(actual.length, (i) => start + i));
      }
      expect(records.every((r) => r.topic == 'topic'), isTrue);
      expect(peer.fetches.every((f) => f.partition == 0 || f.partition == 1),
          isTrue);
      await _until(() => client.activeSubscriptionCount == 0);
    });

    test('pause stops delivery and new fetches; resume keeps buffered order',
        () async {
      peer.onFetch =
          (request) => request.reply(_batch(request.offset, count: 3), 0);
      final received = <int>[];
      late StreamSubscription<KafkaTopicRecord> subscription;
      subscription = client
          .subscribe('topic',
              startOffsets: {0: 0},
              pollInterval: const Duration(milliseconds: 1))
          .listen((record) {
        received.add(record.offset);
        if (received.length == 1) subscription.pause();
      });
      await _until(() => received.length == 1);
      expect(client.activeSubscriptionCount, 1);
      final fetchCount = peer.fetches.length;
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(received, [0]);
      expect(peer.fetches.length, fetchCount);
      subscription.resume();
      await _until(() => received.length >= 3);
      await subscription.cancel().timeout(_deadline);
      expect(received.take(3), [0, 1, 2]);
      expect(client.activeSubscriptionCount, 0);
    });

    test('asyncMap processing backpressure waits before records and new Fetch',
        () async {
      final firstProcessed = Completer<void>();
      final secondProcessed = Completer<void>();
      final started = <int>[];
      final delivered = <int>[];
      peer.onFetch =
          (request) => request.reply(_batch(request.offset, count: 2), 0);
      final result = client
          .subscribe('topic',
              startOffsets: {0: 10},
              pollInterval: const Duration(milliseconds: 1))
          .asyncMap((record) async {
            started.add(record.offset);
            await (record.offset == 10 ? firstProcessed : secondProcessed)
                .future;
            return record;
          })
          .map((record) {
            delivered.add(record.offset);
            return record;
          })
          .take(2)
          .toList();

      await _until(() => started.isNotEmpty);
      // The unfinished Future, rather than a manual pause, owns backpressure.
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(started, [10]);
      expect(delivered, isEmpty);
      expect(peer.fetches, hasLength(1));
      expect(client.activeSubscriptionCount, 1);

      firstProcessed.complete();
      await _until(() => started.length == 2);
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(started, [10, 11]);
      expect(delivered, [10]);
      // Even after the final record in this batch enters asyncMap, the next
      // Fetch cannot begin until its processing Future has completed.
      expect(peer.fetches, hasLength(1));

      secondProcessed.complete();
      expect((await result.timeout(_deadline)).map((r) => r.offset), [10, 11]);
      expect(delivered, [10, 11]);
      await _until(() => client.activeSubscriptionCount == 0);
      expect(peer.fetches, hasLength(1));
      await client.metadata({'topic'}).timeout(_deadline);
      await client.produce('topic', [
        KafkaRecord(value: [1])
      ]).timeout(_deadline);
      expect(peer.produceCalls, 1);
    });

    test('cancel waits for admitted Fetch without closing shared client',
        () async {
      peer.onFetch = (_) {}; // Hold exactly one admitted request.
      final subscription =
          client.subscribe('topic', startOffsets: {0: 0}).listen((_) {
        fail('Cancelled fetch must not deliver records');
      });
      await _until(() => peer.fetches.length == 1);
      var cancelled = false;
      final cancellation = subscription.cancel().then((_) {
        cancelled = true;
      });
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(cancelled, isFalse);
      peer.fetches.single.reply(_batch(0), 0);
      await cancellation.timeout(_deadline);
      expect(client.activeSubscriptionCount, 0);
      await client.metadata({'topic'}).timeout(_deadline);
      await client.produce('topic', [
        KafkaRecord(value: [1])
      ]).timeout(_deadline);
      expect(peer.produceCalls, 1);
      expect(peer.fetches, hasLength(1));
    });

    test('cancelling one stream leaves another active on the shared client',
        () async {
      peer.onFetch = (_) {};
      final first =
          client.subscribe('topic', startOffsets: {0: 10}).listen((_) {});
      final second =
          client.subscribe('topic', startOffsets: {1: 100}).listen((_) {});
      await _until(() => peer.fetches.length == 2);
      expect(client.activeSubscriptionCount, 2);
      final firstCancel = first.cancel();
      peer.fetches.singleWhere((f) => f.partition == 0).reply([], 0);
      await firstCancel.timeout(_deadline);
      expect(client.activeSubscriptionCount, 1);
      peer.fetches.singleWhere((f) => f.partition == 1).reply(_batch(100), 0);
      await _until(() => peer.fetches.length == 3);
      expect(peer.fetches.last.partition, 1);
      expect(peer.fetches.last.offset, 101);
      final secondCancel = second.cancel();
      peer.fetches.last.reply([], 0);
      await secondCancel.timeout(_deadline);
      expect(client.activeSubscriptionCount, 0);
    });

    test('client close settles while listener remains paused', () async {
      peer.onFetch = (request) => request.reply(_batch(0, count: 2), 0);
      late StreamSubscription<KafkaTopicRecord> subscription;
      var received = 0;
      subscription =
          client.subscribe('topic', startOffsets: {0: 0}).listen((_) {
        received++;
        subscription.pause();
      });
      await _until(() => received == 1);
      await client.close().timeout(_deadline);
      expect(client.activeSubscriptionCount, 0);
      expect(received, 1);
      await subscription.cancel().timeout(_deadline);
    });

    test('control and sparse batches advance; empty watermark does not',
        () async {
      peer.onFetch = (request) {
        switch (peer.fetches.length) {
          case 1:
            request.reply(_batch(10, lastDelta: 4, control: true), 0);
          case 2:
            request.reply(_batch(15, lastDelta: 9), 0);
          case 3:
            request.reply([], 0);
          default:
            request.reply(_batch(25), 0);
        }
      };
      final records = await client
          .subscribe('topic',
              startOffsets: {0: 10},
              pollInterval: const Duration(milliseconds: 1))
          .take(2)
          .toList()
          .timeout(_deadline);
      expect(records.map((r) => r.offset), [15, 25]);
      expect(peer.fetches.map((f) => f.offset), [10, 15, 25, 25]);
      await _until(() => client.activeSubscriptionCount == 0);
    });

    test('offset out of range is terminal, never silently reset', () async {
      peer.onFetch = (request) => request.reply([], 1);
      await expectLater(
          client.subscribe('topic', startOffsets: {0: 900}),
          emitsInOrder([
            emitsError(isA<KafkaBrokerException>()
                .having((e) => e.errorCode, 'errorCode', 1)),
            emitsDone
          ])).timeout(_deadline);
      expect(peer.fetches.map((f) => f.offset), [900]);
      expect(client.activeSubscriptionCount, 0);
    });
  });
}
