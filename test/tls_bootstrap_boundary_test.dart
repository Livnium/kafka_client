import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';

Map<String, int> counters(KafkaClient client) => {
      'connections': client.connectionCount,
      'pending': client.pendingRequestCount,
      'admitted': client.admittedRequestCount,
      'queued_bytes': client.queuedRequestBytes,
      'buffered_bytes': client.bufferedResponseBytes,
    };

Future<void> expectSettled(KafkaClient client) async {
  final clock = Stopwatch()..start();
  while (client.admittedRequestCount != 0 &&
      clock.elapsed < const Duration(seconds: 2)) {
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  expect(counters(client).values, everyElement(0));
}

void main() {
  test(
      'TLS handshake EOF remains fail-closed across bootstrap and read retries',
      () async {
    final first = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final sockets = <Socket>[];
    final peerTasks = <Future<void>>[];
    final peerErrors = <Object>[];
    final subscriptions = <StreamSubscription<Socket>>[];
    ServerSocket? second;
    KafkaClient? client;
    var firstAccepts = 0;
    var secondAccepts = 0;
    var completeClientHellos = 0;
    var gracefulCloses = 0;

    // Register cleanup before creating the second listener or TLS client.
    addTearDown(() async {
      try {
        await client?.close().timeout(const Duration(seconds: 3));
      } finally {
        for (final socket in sockets) {
          socket.destroy();
        }
        await first.close().timeout(const Duration(seconds: 2));
        await second?.close().timeout(const Duration(seconds: 2));
        for (final subscription in subscriptions) {
          await subscription.cancel().timeout(const Duration(seconds: 2));
        }
        await Future.wait(peerTasks).timeout(const Duration(seconds: 2));
      }
    });

    Future<void> closeAfterClientHello(Socket socket) async {
      final bytes = <int>[];
      var sentFin = false;
      try {
        // Keep reading after closing the write side so FIN, rather than an
        // immediate destroy/reset, terminates the unfinished TLS handshake.
        await for (final chunk in socket) {
          if (sentFin) continue;
          bytes.addAll(chunk);
          if (bytes.length > 65540) {
            throw StateError('Unexpected oversized local TLS first record');
          }
          if (bytes.length < 5) continue;
          final recordLength = (bytes[3] << 8) | bytes[4];
          if (bytes.length < 5 + recordLength) continue;
          if (recordLength < 4 ||
              bytes[0] != 22 || // TLS handshake record.
              bytes[1] != 3 ||
              bytes[5] != 1) {
            // ClientHello handshake type.
            throw StateError('Expected local TLS ClientHello first record');
          }
          final helloLength = (bytes[6] << 16) | (bytes[7] << 8) | bytes[8];
          if (helloLength + 4 != recordLength) {
            throw StateError('Expected one complete local ClientHello record');
          }
          completeClientHellos++;
          sentFin = true;
          await socket.flush();
          await socket.close();
          gracefulCloses++;
        }
      } catch (error) {
        // Awaited below; never leak errors from a detached stream handler.
        peerErrors.add(error);
      } finally {
        socket.destroy();
      }
    }

    subscriptions.add(first.listen((socket) {
      firstAccepts++;
      sockets.add(socket);
      peerTasks.add(closeAfterClientHello(socket));
    }, onError: (Object error) => peerErrors.add(error)));
    second = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    subscriptions.add(second.listen((socket) {
      secondAccepts++;
      sockets.add(socket);
      // Any attempt here violates this boundary; no Kafka server is needed.
      socket.destroy();
    }, onError: (Object error) => peerErrors.add(error)));

    final activeClient = KafkaClient(
      brokers: [
        Broker('127.0.0.1', first.port),
        Broker('127.0.0.1', second.port),
      ],
      // No certificate is exchanged and validation is never disabled.
      tls: KafkaTlsConfig(),
      connectionTimeout: const Duration(seconds: 2),
      requestTimeout: const Duration(seconds: 2),
      maxReadRetries: 2,
      retryBaseDelay: const Duration(milliseconds: 10),
      maxConnections: 2,
    );
    client = activeClient;
    final result = await activeClient
        .metadata({'tls-boundary'})
        .then<Object?>((_) => null,
            onError: (Object error, StackTrace stack) => error)
        .timeout(const Duration(seconds: 5));
    expect(result, isA<HandshakeException>());
    await expectSettled(activeClient);
    await activeClient.close().timeout(const Duration(seconds: 3));
    await expectSettled(activeClient);
    await Future.wait(peerTasks).timeout(const Duration(seconds: 2));
    expect(peerErrors, isEmpty);
    expect(completeClientHellos, 1);
    expect(gracefulCloses, 1);
    expect(firstAccepts, 1,
        reason: 'A TLS handshake failure must not enter maxReadRetries');
    expect(secondAccepts, 0,
        reason: 'A TLS handshake failure must not fall through bootstraps');
    final error = result as HandshakeException;
    print('TLS_BOOTSTRAP_BOUNDARY ${jsonEncode({
          'error_type': error.runtimeType.toString(),
          'os_error_present': error.osError != null,
          'first_peer_attempts': firstAccepts,
          'second_peer_attempts': secondAccepts,
          'complete_client_hellos': completeClientHellos,
          'graceful_closes': gracefulCloses,
          'resources': counters(activeClient),
        })}');
  }, timeout: const Timeout(Duration(seconds: 20)));
}
