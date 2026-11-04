import 'dart:io';

import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';

void main() {
  group('managed stable retry construction', () {
    for (final stable in [false, true]) {
      for (final enabled in [false, true]) {
        test('stable=$stable retry=$enabled validates before network I/O',
            () async {
          final server =
              await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
          var connections = 0;
          final subscription = server.listen((socket) {
            connections++;
            socket.destroy();
          });
          KafkaGroupConsumer? consumer;
          addTearDown(() async {
            consumer?.stop();
            await consumer?.closed;
            await subscription.cancel();
            await server.close();
          });
          KafkaGroupConsumer create() => KafkaGroupConsumer(
                brokers: [Broker('127.0.0.1', server.port)],
                groupId: 'construction-only',
                topics: {'topic'},
                commitAfterProcessing: false,
                requireStableOffsets: stable,
                stableOffsetRetry: enabled
                    ? KafkaStableOffsetRetry(
                        maxAttempts: 2,
                        timeout: const Duration(seconds: 1),
                        retryDelay: const Duration(milliseconds: 10))
                    : null,
              );
          if (enabled && !stable) {
            expect(create, throwsArgumentError);
          } else {
            consumer = create();
            expect(consumer.state, KafkaGroupConsumerState.idle);
            expect(consumer.generationsStarted, 0);
            consumer.stop();
            expect((await consumer.closed).reason,
                KafkaGroupConsumerStopReason.cancelled);
          }
          await Future<void>.delayed(const Duration(milliseconds: 20));
          expect(connections, 0);
        });
      }
    }
  });
}
