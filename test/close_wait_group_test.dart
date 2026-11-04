import 'dart:async';

import 'package:kafka_client/src/close_wait_group.dart';
import 'package:test/test.dart';

void main() {
  test('completed delays retain no historical waiters', () async {
    final waits = CloseWaitGroup();
    for (var i = 0; i < 100; i++) {
      final pending = waits.wait(Duration.zero);
      expect(waits.activeCount, 1);
      await pending;
      expect(waits.activeCount, 0);
    }
    waits.close();
    expect(waits.activeCount, 0);
  });

  test('close releases every concurrent waiter and remains idempotent',
      () async {
    final waits = CloseWaitGroup();
    final pending = [
      for (var i = 0; i < 12; i++) waits.wait(const Duration(seconds: 30))
    ];
    expect(waits.activeCount, 12);
    waits.close();
    waits.close();
    await Future.wait(pending).timeout(const Duration(seconds: 1));
    expect(waits.activeCount, 0);
    await waits
        .wait(const Duration(seconds: 30))
        .timeout(const Duration(seconds: 1));
    expect(waits.activeCount, 0);
  });

  test('timer and close callbacks are safe in both synchronous orders',
      () async {
    for (final closeFirst in [false, true]) {
      for (var i = 0; i < 100; i++) {
        final waits = CloseWaitGroup();
        final pending = runZoned(() => waits.wait(Duration.zero),
            zoneSpecification: ZoneSpecification(createTimer: (Zone self,
                ZoneDelegate parent,
                Zone zone,
                Duration duration,
                void Function() callback) {
          return parent.createTimer(zone, duration, () {
            expect(waits.activeCount, 1);
            if (closeFirst) {
              waits.close();
              callback();
            } else {
              callback();
              // Async completer cleanup has not run yet in this same turn.
              expect(waits.activeCount, 1);
              waits.close();
            }
          });
        }));
        await pending;
        expect(waits.activeCount, 0);
      }
    }
  });

  test('timer creation failure unregisters its waiter', () async {
    final waits = CloseWaitGroup();
    final failure = runZoned(() => waits.wait(const Duration(seconds: 1)),
        zoneSpecification: ZoneSpecification(createTimer: (Zone self,
            ZoneDelegate parent,
            Zone zone,
            Duration duration,
            void Function() callback) {
      throw StateError('synthetic timer creation failure');
    }));
    await expectLater(failure, throwsStateError);
    expect(waits.activeCount, 0);
    waits.close();
  });
}
