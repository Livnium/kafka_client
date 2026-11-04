import 'dart:io';
import 'package:kafka_client/kafka_client.dart';
import 'package:kafka_client/src/api/abstract/kafka_request.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

void main() {
  for (final malformed in [false, true]) {
    test('leader routing and response matching malformed=$malformed', () async {
      final bootstrap =
          await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final leader = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final sockets = <Socket>[];
      final bootstrapKeys = <int>[], leaderKeys = <int>[];
      void serve(ServerSocket server, List<int> keys) {
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
              keys.add(key);
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
                b
                  ..addInt32(1)
                  ..addInt32(7)
                  ..addString('127.0.0.1')
                  ..addInt32(leader.port)
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
              } else if (key == 0) {
                if (malformed) {
                  b
                    ..addInt32(0)
                    ..addInt32(0);
                } else {
                  b
                    ..addInt32(1)
                    ..addString('topic')
                    ..addInt32(1)
                    ..addInt32(0)
                    ..addInt16(0)
                    ..addInt64(42)
                    ..addInt64(-1)
                    ..addInt32(0);
                }
              }
              s.add(KafkaRequest.frame(b.takeBytes()));
            }
          });
        });
      }

      serve(bootstrap, bootstrapKeys);
      serve(leader, leaderKeys);
      final client =
          KafkaClient(brokers: [Broker('127.0.0.1', bootstrap.port)]);
      try {
        final future = client.produce('topic', [
          KafkaRecord(value: [1])
        ]);
        if (malformed) {
          await expectLater(future, throwsFormatException);
        } else {
          expect(
              (await future)!
                  .responses
                  .single
                  .partitionResponses
                  .single
                  .baseOffset,
              42);
        }
        expect(bootstrapKeys, [18, 3]);
        expect(leaderKeys, [18, 0]);
      } finally {
        await client.close();
        for (final s in sockets) {
          s.destroy();
        }
        await bootstrap.close();
        await leader.close();
      }
    });
  }
}
