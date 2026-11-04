import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_client.dart';
import 'package:kafka_client/src/api/abstract/kafka_request.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:kafka_client/src/codec/record_batch.dart';
import 'package:test/test.dart';

class _BrokerFixture {
  final ServerSocket server;
  final List<Socket> sockets = [];
  final List<int> requestKeys = [];
  final List<List<KafkaRecord>> batches = [];
  final Completer<void> metadataSeen = Completer<void>();
  final Completer<void> releaseMetadata = Completer<void>();
  bool closed = false;

  _BrokerFixture(this.server) {
    server.listen((socket) {
      sockets.add(socket);
      final buffer = <int>[];
      socket.listen((bytes) {
        buffer.addAll(bytes);
        while (buffer.length >= 4) {
          final size = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (buffer.length < size + 4) break;
          final reader =
              KafkaBytesReader.fromBytes(buffer.sublist(4, size + 4));
          buffer.removeRange(0, size + 4);
          final key = reader.readInt16();
          reader.readInt16();
          final id = reader.readInt32();
          reader.readString();
          requestKeys.add(key);
          final reply = KafkaBytesBuilder()..addInt32(id);
          if (key == 18) {
            reply
              ..addInt16(0)
              ..addInt32(2);
            for (final entry in const {3: 0, 0: 3}.entries) {
              reply
                ..addInt16(entry.key)
                ..addInt16(entry.value)
                ..addInt16(entry.value);
            }
          } else if (key == 3) {
            reply
              ..addInt32(1)
              ..addInt32(7)
              ..addString('127.0.0.1')
              ..addInt32(server.port)
              ..addInt32(1)
              ..addInt16(0)
              ..addString('topic')
              ..addInt32(1)
              ..addInt16(0)
              ..addInt32(0)
              ..addInt32(7)
              ..addInt32(1)
              ..addInt32(7)
              ..addInt32(1)
              ..addInt32(7);
            metadataSeen.complete();
            unawaited(releaseMetadata.future.then((_) {
              if (!closed) socket.add(KafkaRequest.frame(reply.takeBytes()));
            }));
            continue;
          } else if (key == 0) {
            expect(reader.readString(), isNull);
            expect(reader.readInt16(), 1);
            reader.readInt32();
            expect(reader.readInt32(), 1);
            expect(reader.readString(), 'topic');
            expect(reader.readInt32(), 1);
            expect(reader.readInt32(), 0);
            batches.add(decodeRecordBatches(reader.readBytes()!));
            expect(reader.isEOF, isTrue);
            reply
              ..addInt32(1)
              ..addString('topic')
              ..addInt32(1)
              ..addInt32(0)
              ..addInt16(0)
              ..addInt64(42)
              ..addInt64(-1)
              ..addInt32(0);
          } else {
            fail('Unexpected API $key');
          }
          socket.add(KafkaRequest.frame(reply.takeBytes()));
        }
      }, onError: (Object _) {});
    });
  }

  Future<void> close() async {
    closed = true;
    if (!releaseMetadata.isCompleted) releaseMetadata.complete();
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
  }
}

void main() {
  final mutations = <String, void Function(List<KafkaRecord>)>{
    'replace': (records) => records[0] = KafkaRecord(value: [99]),
    'clear': (records) => records.clear(),
    'append': (records) => records.add(KafkaRecord(value: [99])),
    'reorder': (records) => records.insert(0, records.removeLast()),
  };
  for (final phase in ['immediately after call', 'during metadata discovery']) {
    for (final mutation in mutations.entries) {
      test('Produce snapshots records before ${mutation.key} $phase', () async {
        final fixture = _BrokerFixture(
            await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));
        final client = KafkaClient(
            brokers: [Broker('127.0.0.1', fixture.server.port)],
            requestTimeout: const Duration(seconds: 3));
        addTearDown(() async {
          await client.close();
          await fixture.close();
        });
        final records = [
          KafkaRecord(
              key: [1],
              value: [10],
              timestamp: 1000,
              headers: [
                KafkaRecordHeader('kind', [1]),
                KafkaRecordHeader('kind', null),
              ]),
          KafkaRecord(key: [], value: null, timestamp: 1001),
        ];
        final original = List<KafkaRecord>.of(records);
        final pending = client.produce('topic', records);
        if (phase == 'during metadata discovery') {
          await fixture.metadataSeen.future.timeout(const Duration(seconds: 2));
        }
        mutation.value(records);
        fixture.releaseMetadata.complete();
        final response = await pending;
        expect(response!.responses.single.partitionResponses.single.baseOffset,
            42);
        expect(fixture.requestKeys, [18, 3, 0], reason: 'One Produce only');
        final produced = fixture.batches.single;
        expect(produced.length, original.length);
        for (var i = 0; i < original.length; i++) {
          expect(produced[i].key, original[i].key);
          expect(produced[i].value, original[i].value);
          expect(produced[i].timestamp, original[i].timestamp);
          expect(produced[i].headers.map((h) => h.key),
              original[i].headers.map((h) => h.key));
          expect(produced[i].headers.map((h) => h.value),
              original[i].headers.map((h) => h.value));
        }
        await client.close();
        expect(client.connectionCount, 0);
        expect(client.pendingRequestCount, 0);
        expect(client.admittedRequestCount, 0);
        expect(client.queuedRequestBytes, 0);
        expect(client.bufferedResponseBytes, 0);
      });
    }
  }
}
