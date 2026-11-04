import 'dart:async';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/src/group_consumer.dart';
import 'package:test/test.dart';

import 'classic_group_session_test.dart' as session_fixture;
import 'group_consumer_controller_test.dart' as fixture;

// A transport may finish successfully after shutdown. These tests exercise the
// lifecycle check independently of actual socket cancellation, with real session
// identity and no edits to the controller's eligibility or commit barriers.
void main() {
  for (final ending in ['heartbeat27', 'coordinator16', 'stop', 'expiry']) {
    test('$ending rejects a successful late initial position response',
        () async {
      final first = fixture.ConsumerCycle(), next = fixture.ConsumerCycle();
      final pending = Completer<Map<int, KafkaCommittedOffset?>>();
      final closeBarrier = Completer<void>();
      first.read = (_, __) => pending.future;
      first.closePosition = () => closeBarrier.future;
      final controller = GroupConsumerController(
        initial: first,
        createCycle: () => next,
        commitAfterProcessing: true,
        missingOffsets: const {},
        maxRejoins: 2,
        rejoinOnCoordinatorLoss: true,
        rejoinDelay: const Duration(milliseconds: 1),
      );
      var callbacks = 0;
      final run = controller.run((_) {
        callbacks++;
      });
      addTearDown(() async {
        controller.stop();
        if (!pending.isCompleted) pending.complete({0: null});
        if (!closeBarrier.isCompleted) closeBarrier.complete();
        await controller.closed.timeout(const Duration(seconds: 3));
      });
      await fixture.until(() => first.reads.isNotEmpty);
      final token = controller.assignment!;
      if (ending == 'stop') {
        controller.stop();
      } else if (ending == 'expiry') {
        first.clock.advance(session_fixture.lease);
      } else {
        first.reject(code: ending == 'heartbeat27' ? 27 : 16);
      }
      await fixture.until(() => first.positionCloses == 1);
      expect(controller.isCurrent(token), isFalse);
      expect(controller.assignment, isNull);
      expect(next.starts, 0);
      pending.complete(
          {0: const KafkaCommittedOffset(nextOffset: 10, leaderEpoch: -1)});
      await fixture.flush();
      expect(first.loaded, isNull);
      expect(first.fetches, isEmpty);
      expect(first.commits, isEmpty);
      expect(callbacks, 0);
      expect(next.starts, 0);
      closeBarrier.complete();
      final result = await run.timeout(const Duration(seconds: 3));
      expect(
          result.reason,
          ending == 'stop'
              ? KafkaGroupConsumerStopReason.cancelled
              : KafkaGroupConsumerStopReason.failure);
      expect(result.generationsStarted, 1);
      expect(result.cleanupErrors, isEmpty);
      expect(first.positionCloses, 1);
      expect(first.loaded, isNull);
      expect(first.fetches, isEmpty);
      expect(first.commits, isEmpty);
      expect(next.starts, 0);
    });
  }
}
