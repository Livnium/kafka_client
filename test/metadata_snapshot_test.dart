import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_client.dart';
import 'package:kafka_client/src/api/abstract/kafka_request.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

void main() {
  for (final phase in ['negotiation', 'retry']) {
    for (final mutation in ['replace', 'clear', 'append']) {
      test('Metadata snapshots topics before $mutation during $phase',
          () async {
        final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
        final sockets = <Socket>[];
        final queries = <List<String>>[];
        final negotiating = Completer<void>();
        final releaseVersions = Completer<void>();
        final retrying = Completer<void>();
        final releaseRetry = Completer<void>();
        var versionCalls = 0;
        var closed = false;
        server.listen((socket) {
          sockets.add(socket);
          final buffer = <int>[];
          socket.listen((bytes) {
            buffer.addAll(bytes);
            while (buffer.length >= 4) {
              final size = KafkaBytesReader.fromBytes(buffer).readInt32();
              if (buffer.length < size + 4) break;
              final r = KafkaBytesReader.fromBytes(buffer.sublist(4, size + 4));
              buffer.removeRange(0, size + 4);
              final key = r.readInt16();
              r.readInt16();
              final id = r.readInt32();
              r.readString();
              if (key == 18) {
                versionCalls++;
                if (!negotiating.isCompleted) negotiating.complete();
                final response = KafkaRequest.frame((KafkaBytesBuilder()
                      ..addInt32(id)
                      ..addInt16(0)
                      ..addInt32(1)
                      ..addInt16(3)
                      ..addInt16(0)
                      ..addInt16(0))
                    .takeBytes());
                unawaited(releaseVersions.future.then((_) {
                  if (!closed) socket.add(response);
                }));
              } else {
                expect(key, 3);
                final count = r.readInt32();
                queries.add([for (var i = 0; i < count; i++) r.readString()!]);
                expect(r.isEOF, isTrue);
                if (phase == 'retry' && queries.length == 1) {
                  retrying.complete();
                  unawaited(releaseRetry.future.then((_) => socket.destroy()));
                } else {
                  socket.add(KafkaRequest.frame((KafkaBytesBuilder()
                        ..addInt32(id)
                        ..addInt32(0)
                        ..addInt32(0))
                      .takeBytes()));
                }
              }
            }
          }, onError: (Object _) {});
        });
        final client = KafkaClient(
            brokers: [Broker('127.0.0.1', server.port)],
            retryBaseDelay: Duration.zero,
            requestTimeout: const Duration(seconds: 3));
        addTearDown(() async {
          closed = true;
          if (!releaseVersions.isCompleted) releaseVersions.complete();
          if (!releaseRetry.isCompleted) releaseRetry.complete();
          await client.close();
          for (final socket in sockets) {
            socket.destroy();
          }
          await server.close();
        });
        final topics = {'original'};
        final pending = client.metadata(topics);
        if (phase == 'negotiation') {
          await negotiating.future.timeout(const Duration(seconds: 2));
        } else {
          releaseVersions.complete();
          await retrying.future.timeout(const Duration(seconds: 2));
        }
        if (mutation == 'replace') {
          topics
            ..clear()
            ..add('replacement');
        } else if (mutation == 'clear') {
          topics.clear();
        } else {
          topics.add('extra');
        }
        if (!releaseVersions.isCompleted) releaseVersions.complete();
        releaseRetry.complete();
        await pending;
        expect(
            queries,
            phase == 'retry'
                ? [
                    ['original'],
                    ['original']
                  ]
                : [
                    ['original']
                  ]);
        expect(versionCalls, phase == 'retry' ? 2 : 1);
        await client.close();
        expect(client.connectionCount, 0);
        expect(client.admittedRequestCount, 0);
        expect(client.pendingRequestCount, 0);
        expect(client.queuedRequestBytes, 0);
        expect(client.bufferedResponseBytes, 0);
      });
    }
  }
}
