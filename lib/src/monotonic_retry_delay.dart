/// Internal close-aware wait that treats timer wakeups as observations, not as
/// proof a monotonic deadline has elapsed. Dart VM timers have millisecond
/// granularity while Stopwatch elapsed values can contain microseconds.
///
/// [check] enforces the caller's lifetime/window before and after every wait.
/// Always yields once, including for a zero delay, preserving close scheduling.
Future<void> waitForMonotonicRetryDeadline({
  required Duration deadline,
  required Duration Function() elapsed,
  required void Function() check,
  required Future<void> Function(Duration) wait,
}) async {
  do {
    check();
    final remaining = deadline - elapsed();
    // Round positive waits up rather than intentionally asking a Timer to round
    // down. Recheck even after that: scheduler wakeups alone grant no admission.
    final delay = remaining <= Duration.zero
        ? Duration.zero
        : Duration(milliseconds: (remaining.inMicroseconds + 999) ~/ 1000);
    await wait(delay);
    check();
  } while (elapsed() < deadline);
}
