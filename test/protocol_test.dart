import 'dart:async';
import 'dart:io';
import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

void main() {
  test('all supported requests include exact size prefix and version', () {
    final requests = <KafkaRequest>[
      ApiVersionsRequest(),
      MetadataRequest({'héllo'}),
      ProduceRequest(acks: 1, timeoutMs: 100, topicData: []),
      FetchRequest(-1, 100, 1, [
        FetchTopic('t', [FetchPartition(0, 0, 1024)])
      ]),
    ];
    for (var i = 0; i < requests.length; i++) {
      final bytes = requests[i].toBytes();
      final r = KafkaBytesReader.fromBytes(bytes);
      expect(r.readInt32(), bytes.length - 4);
      expect(r.readInt16(), [18, 3, 0, 1][i]);
      expect(r.readInt16(), [0, 0, 3, 4][i]);
      expect(r.readInt32(), requests[i].correlationId);
    }
  });
  test('metadata rejects negative counts and trailing bytes', () {
    final b = KafkaBytesBuilder()
      ..addInt32(1)
      ..addInt32(-1);
    expect(
        () => MetadataResponse.fromBytes(b.takeBytes()), throwsFormatException);
    final good = KafkaBytesBuilder()
      ..addInt32(1)
      ..addInt32(0)
      ..addInt32(0)
      ..addInt8(1);
    expect(() => MetadataResponse.fromBytes(good.takeBytes()),
        throwsFormatException);
  });
  test('ApiVersions rejects malformed counts', () {
    final b = KafkaBytesBuilder()
      ..addInt32(1)
      ..addInt16(0)
      ..addInt32(-1);
    expect(() => ApiVersionsResponse.fromBytes(b.takeBytes()),
        throwsFormatException);
  });
  test('negotiation refuses unsupported implemented version before Metadata',
      () async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final sockets = <Socket>[];
    final keys = <int>[];
    server.listen((s) {
      sockets.add(s);
      final buffer = <int>[];
      s.listen((data) {
        buffer.addAll(data);
        if (buffer.length < 4) return;
        final size = KafkaBytesReader.fromBytes(buffer).readInt32();
        if (buffer.length < size + 4) return;
        final r = KafkaBytesReader.fromBytes(buffer.sublist(4));
        keys.add(r.readInt16());
        r.readInt16();
        final id = r.readInt32();
        // Broker only supports Metadata v9+, which this client does not encode.
        s.add(KafkaRequest.frame((KafkaBytesBuilder()
              ..addInt32(id)
              ..addInt16(0)
              ..addInt32(1)
              ..addInt16(3)
              ..addInt16(9)
              ..addInt16(13))
            .takeBytes()));
        buffer.clear();
      });
    });
    final client = KafkaClient(brokers: [Broker('127.0.0.1', server.port)]);
    try {
      await expectLater(client.metadata(), throwsA(isA<UnsupportedError>()));
      expect(keys, [18]);
    } finally {
      await client.close();
      for (final s in sockets) {
        s.destroy();
      }
      await server.close();
    }
  });
  test('failed ApiVersions negotiation retries on a fresh connection',
      () async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final sockets = <Socket>[];
    var attempts = 0;
    server.listen((s) {
      sockets.add(s);
      final buffer = <int>[];
      s.listen((data) {
        buffer.addAll(data);
        while (buffer.length >= 4) {
          final size = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (buffer.length < size + 4) break;
          final r = KafkaBytesReader.fromBytes(buffer.sublist(4, size + 4));
          buffer.removeRange(0, size + 4);
          final key = r.readInt16();
          r.readInt16();
          final id = r.readInt32();
          if (key == 18) {
            attempts++;
            if (attempts == 1) continue;
            s.add(KafkaRequest.frame((KafkaBytesBuilder()
                  ..addInt32(id)
                  ..addInt16(0)
                  ..addInt32(1)
                  ..addInt16(3)
                  ..addInt16(0)
                  ..addInt16(13))
                .takeBytes()));
          } else {
            s.add(KafkaRequest.frame((KafkaBytesBuilder()
                  ..addInt32(id)
                  ..addInt32(0)
                  ..addInt32(0))
                .takeBytes()));
          }
        }
      });
    });
    final client = KafkaClient(
        brokers: [Broker('127.0.0.1', server.port)],
        requestTimeout: Duration(milliseconds: 100),
        maxReadRetries: 0);
    try {
      await expectLater(client.metadata(), throwsA(isA<TimeoutException>()));
      expect((await client.metadata()).topics, isEmpty);
      expect(attempts, 2);
      expect(sockets.length, 2);
    } finally {
      await client.close();
      for (final s in sockets) {
        s.destroy();
      }
      await server.close();
    }
  });
}
