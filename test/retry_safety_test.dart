import 'dart:io';
import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

void main() {
  for (final scenario in [
    'fetch6',
    'produceDisconnect',
    'produceResourceLimit',
    'rawMetadata6',
    'metadata9',
    'produce9',
    'metadata9Live',
    'produceMetadata9Live',
    'metadata9NoLeader',
    'metadata9Unlisted',
    'metadata9InvalidEndpoint',
    'topic9'
  ]) {
    final producing = scenario.startsWith('produce');
    final description = {
      'fetch6': 'Fetch leader error refreshes metadata and retries safely',
      'produceDisconnect': 'ambiguous Produce disconnect is never replayed',
      'produceResourceLimit':
          'response resource limit after transmitted Produce never replays',
      'rawMetadata6':
          'raw metadata returns broker errors without retrying them',
      'metadata9': 'Fetch retries transient metadata replica-unavailable error',
      'produce9': 'Produce replica-unavailable response is never replayed',
      'metadata9Live': 'Metadata v0 replica error preserves live Fetch leader',
      'produceMetadata9Live':
          'Metadata v0 replica error preserves live Produce leader',
      'metadata9NoLeader':
          'Replica error without leader exhausts bounded retries',
      'metadata9Unlisted': 'Replica error with unlisted leader never routes',
      'metadata9InvalidEndpoint':
          'Replica error with invalid leader endpoint never routes',
      'topic9': 'Topic replica error is not suppressed by partition routing',
    }[scenario]!;
    test(description, () async {
      final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final sockets = <Socket>[];
      var metadataCalls = 0, produceCalls = 0, fetchCalls = 0;
      server.listen((socket) {
        sockets.add(socket);
        final buffer = <int>[];
        socket.listen((data) {
          buffer.addAll(data);
          while (buffer.length >= 4) {
            final length = KafkaBytesReader.fromBytes(buffer).readInt32();
            if (buffer.length < length + 4) break;
            final r = KafkaBytesReader.fromBytes(buffer.sublist(4, length + 4));
            buffer.removeRange(0, length + 4);
            final key = r.readInt16();
            r.readInt16();
            final id = r.readInt32();
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
              final transientReplicaError =
                  scenario == 'metadata9' && metadataCalls == 1;
              final persistentReplicaError =
                  scenario.startsWith('metadata9') && scenario != 'metadata9' ||
                      scenario == 'produceMetadata9Live';
              final noLeader =
                  transientReplicaError || scenario == 'metadata9NoLeader';
              b
                ..addInt32(1)
                ..addInt32(1)
                ..addString('127.0.0.1')
                ..addInt32(
                    scenario == 'metadata9InvalidEndpoint' ? 0 : server.port)
                ..addInt32(1)
                ..addInt16(scenario == 'topic9' ? 9 : 0)
                ..addString('topic')
                ..addInt32(1)
                ..addInt16(scenario == 'rawMetadata6'
                    ? 6
                    : transientReplicaError || persistentReplicaError
                        ? 9
                        : 0)
                ..addInt32(0)
                ..addInt32(noLeader
                    ? -1
                    : scenario == 'metadata9Unlisted'
                        ? 99
                        : 1)
                ..addInt32(1)
                ..addInt32(1)
                ..addInt32(1)
                ..addInt32(1);
            } else if (key == 0) {
              produceCalls++;
              if (scenario == 'produceDisconnect') {
                socket.destroy();
                continue;
              }
              if (scenario == 'produceResourceLimit') {
                // A complete Produce request has already reached this peer.
                // Overflow the response budget before a response can decode.
                socket.add(KafkaRequest.frame(List.filled(300, 0)));
                continue;
              }
              b
                ..addInt32(1)
                ..addString('topic')
                ..addInt32(1)
                ..addInt32(0)
                ..addInt16(scenario == 'produceMetadata9Live' ? 0 : 9)
                ..addInt64(-1)
                ..addInt64(-1)
                ..addInt32(0);
            } else if (key == 1) {
              fetchCalls++;
              b
                ..addInt32(0)
                ..addInt32(1)
                ..addString('topic')
                ..addInt32(1)
                ..addInt32(0)
                ..addInt16(scenario == 'fetch6' && fetchCalls == 1 ? 6 : 0)
                ..addInt64(0)
                ..addInt64(0)
                ..addInt32(-1)
                ..addBytes([]);
            }
            socket.add(KafkaRequest.frame(b.takeBytes()));
          }
        }, onError: (Object _) {});
      });
      final client = KafkaClient(
          brokers: [Broker('127.0.0.1', server.port)],
          maxBufferedResponseBytes:
              scenario == 'produceResourceLimit' ? 256 : 33554432,
          maxReadRetries: 2,
          retryBaseDelay: Duration.zero);
      try {
        if (scenario == 'metadata9Live' || scenario == 'produceMetadata9Live') {
          final metadata = await client.metadata({'topic'});
          expect(metadata.topics.single.partitions.single.errorCode, 9,
              reason: 'Raw metadata diagnostics must remain intact');
        }
        if (scenario == 'rawMetadata6') {
          final response = await client.metadata({'topic'});
          expect(response.topics.single.partitions.single.errorCode, 6);
          expect(metadataCalls, 1);
          expect(fetchCalls, 0);
          expect(produceCalls, 0);
        } else if ([
          'metadata9NoLeader',
          'metadata9Unlisted',
          'metadata9InvalidEndpoint',
          'topic9'
        ].contains(scenario)) {
          await expectLater(
              client.fetch('topic'),
              throwsA(isA<KafkaBrokerException>()
                  .having((e) => e.errorCode, 'code', 9)));
          expect(metadataCalls, 3);
          expect(fetchCalls, 0);
          expect(produceCalls, 0);
        } else if (scenario == 'produceMetadata9Live') {
          expect(
              await client.produce('topic', [
                KafkaRecord(value: [1])
              ]),
              isNotNull);
          expect(metadataCalls, 2);
          expect(produceCalls, 1);
        } else if (producing) {
          await expectLater(
              client.produce('topic', [
                KafkaRecord(value: [1])
              ]),
              throwsA(scenario == 'produce9'
                  ? isA<KafkaBrokerException>()
                      .having((e) => e.errorCode, 'code', 9)
                  : scenario == 'produceResourceLimit'
                      ? isA<KafkaResourceLimitException>()
                      : isA<KafkaConnectionException>()));
          expect(produceCalls, 1);
          expect(metadataCalls, 1);
        } else {
          expect(
              (await client.fetch('topic'))
                  .topics
                  .single
                  .partitions
                  .single
                  .records,
              isEmpty);
          expect(fetchCalls, scenario == 'fetch6' ? 2 : 1);
          expect(metadataCalls, 2);
        }
      } finally {
        await client.close();
        for (final s in sockets) {
          s.destroy();
        }
        await server.close();
      }
    });
  }
}
