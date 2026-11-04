import 'dart:async';

import 'package:kafka_client/src/monotonic_retry_delay.dart';
import 'package:test/test.dart';

void main() {
  test('early timer wakeup cannot complete the requested monotonic backoff',
      () async {
    var now = Duration.zero;
    final calls = <Duration>[];
    const deadline = Duration(microseconds: 1500);
    await waitForMonotonicRetryDeadline(
        deadline: deadline,
        elapsed: () => now,
        check: () {},
        wait: (delay) async {
          calls.add(delay);
          now =
              calls.length == 1 ? const Duration(microseconds: 1499) : deadline;
        });
    expect(calls,
        [const Duration(milliseconds: 2), const Duration(milliseconds: 1)]);
    expect(now, deadline);
  });

  test('multiple early wakes preserve a single fixed absolute retry target',
      () async {
    var now = const Duration(microseconds: 4900);
    var calls = 0;
    const target = Duration(milliseconds: 5);
    await waitForMonotonicRetryDeadline(
        deadline: target,
        elapsed: () => now,
        check: () {},
        wait: (delay) async {
          calls++;
          expect(delay, const Duration(milliseconds: 1));
          now += const Duration(microseconds: 10);
        });
    expect(calls, 10);
    expect(now, target);
  });

  test('clipped expiry waits recheck window and never return retry admission',
      () async {
    var now = const Duration(microseconds: 100);
    var calls = 0;
    const window = Duration(milliseconds: 250);
    final failure = TimeoutException('window');
    await expectLater(
        waitForMonotonicRetryDeadline(
            deadline: window,
            elapsed: () => now,
            check: () {
              if (now >= window) throw failure;
            },
            wait: (delay) async {
              calls++;
              now = calls == 1 ? const Duration(microseconds: 249999) : window;
            }),
        throwsA(same(failure)));
    expect(calls, 2);
  });

  test('expired or closed admission is checked before requesting a timer',
      () async {
    var calls = 0;
    final error = StateError('closed');
    await expectLater(
        waitForMonotonicRetryDeadline(
            deadline: const Duration(seconds: 1),
            elapsed: () => Duration.zero,
            check: () => throw error,
            wait: (_) async {
              calls++;
            }),
        throwsA(same(error)));
    expect(calls, 0);
  });

  test('close during an admitted wait prevents later waits and admission',
      () async {
    var closed = false, calls = 0;
    await expectLater(
        waitForMonotonicRetryDeadline(
            deadline: const Duration(seconds: 1),
            elapsed: () => Duration.zero,
            check: () {
              if (closed) throw StateError('closed');
            },
            wait: (_) async {
              calls++;
              closed = true;
            }),
        throwsStateError);
    expect(calls, 1);
  });

  test('zero/already elapsed target still yields once and checks after yield',
      () async {
    for (final target in [Duration.zero, const Duration(microseconds: -1)]) {
      var calls = 0, checks = 0;
      await waitForMonotonicRetryDeadline(
          deadline: target,
          elapsed: () => Duration.zero,
          check: () {
            checks++;
          },
          wait: (delay) async {
            calls++;
            expect(delay, Duration.zero);
          });
      expect(calls, 1);
      expect(checks, 2);
    }
  });

  test('original wait failure propagates without replay or masking', () async {
    final failure = StateError('owned wait failed');
    var calls = 0;
    await expectLater(
        waitForMonotonicRetryDeadline(
            deadline: const Duration(seconds: 1),
            elapsed: () => Duration.zero,
            check: () {},
            wait: (_) async {
              calls++;
              throw failure;
            }),
        throwsA(same(failure)));
    expect(calls, 1);
  });

  test('slow timer wake checks expiry before reporting retry completion',
      () async {
    var now = Duration.zero;
    final expired = TimeoutException('window');
    await expectLater(
        waitForMonotonicRetryDeadline(
            deadline: const Duration(milliseconds: 10),
            elapsed: () => now,
            check: () {
              if (now >= const Duration(milliseconds: 20)) throw expired;
            },
            wait: (_) async {
              now = const Duration(milliseconds: 30);
            }),
        throwsA(same(expired)));
  });
}
