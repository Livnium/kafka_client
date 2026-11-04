// VM-only, implementation-specific diagnostic for the two recorded Dart SDKs.
// Runs the real private ordinary retry method with a synthetic transient error,
// not a broker, package:test, payload-retention probe, or heap/RSS leak benchmark.
// No VM service, credentials, dependencies, or externally reachable listener.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:mirrors';

import 'package:kafka_client/kafka_client.dart';

const rssLimit = 768 * 1024 * 1024;
const iterationLimit = 10000;

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

Symbol privateSymbol(String name, LibraryMirror library) =>
    MirrorSystem.getSymbol(name, library);

// Reading private SDK fields is intentional here, never in production code.
// Unknown layouts fail instead of treating an unavailable count as zero.
int listeners(Future<void> future, {required bool completed}) {
  final library = currentMirrorSystem().libraries[Uri.parse('dart:async')]!;
  final mirror = reflect(future);
  check(
      mirror.type.owner == library &&
          MirrorSystem.getName(mirror.type.simpleName) == '_Future',
      'Unexpected SDK Future owner or type');
  final state = mirror.getField(privateSymbol('_state', library)).reflectee;
  check(state == (completed ? 8 : 0), 'Unexpected SDK Future state: $state');
  dynamic next =
      mirror.getField(privateSymbol('_resultOrListeners', library)).reflectee;
  var count = 0;
  while (next != null) {
    check(!completed, 'Completed void future retained a non-null result');
    check(
        reflect(next).type.owner == library &&
            MirrorSystem.getName(reflect(next).type.simpleName) ==
                '_FutureListener',
        'Unexpected SDK Future listener type');
    count++;
    check(count <= iterationLimit, 'Unexpected or cyclic listener chain');
    next = reflect(next)
        .getField(privateSymbol('_nextListener', library))
        .reflectee;
  }
  return count;
}

Future<void> main(List<String> args) async {
  if (args.length != 2 ||
      !['legacy', 'removable'].contains(args[0]) ||
      int.tryParse(args[1]) == null ||
      int.parse(args[1]) < 1 ||
      int.parse(args[1]) > iterationLimit) {
    stderr
        .writeln('Usage: measure_retry_waiters.dart legacy|removable 1..10000');
    exitCode = 64;
    return;
  }
  final mode = args[0], iterations = int.parse(args[1]);
  final client = KafkaClient(
      brokers: [Broker('127.0.0.1', 1)],
      maxReadRetries: 1,
      retryBaseDelay: Duration.zero);
  final reflected = reflect(client);
  final library = reflected.type.owner as LibraryMirror;
  final closingSymbol = privateSymbol('_closing', library);
  final waitsSymbol = privateSymbol('_retryWaits', library);
  final hasClosing = reflected.type.declarations.containsKey(closingSymbol);
  check(
      hasClosing == (mode == 'legacy'), 'Source does not match requested mode');
  final Completer<void>? closing = mode == 'legacy'
      ? reflected.getField(closingSymbol).reflectee as Completer<void>
      : null;
  final dynamic waits =
      mode == 'removable' ? reflected.getField(waitsSymbol).reflectee : null;
  final clock = Stopwatch()..start();
  final initialRss = ProcessInfo.currentRss;
  var peakRss = initialRss, operations = 0, retries = 0;
  String? guardFailure;
  final samples = <Map<String, Object>>[];
  void sample(int completed) {
    check(closing == null || !closing.isCompleted,
        'Close future completed early');
    final rss = ProcessInfo.currentRss;
    if (rss > peakRss) peakRss = rss;
    if (rss > rssLimit) guardFailure = 'RSS exceeded fixed 768 MiB limit';
    final count = closing == null
        ? waits.activeCount as int
        : listeners(closing.future, completed: false);
    check(count == (mode == 'legacy' ? completed : 0),
        'Waiter count $count does not match completed retries $completed');
    samples.add({
      'completed_retries': completed,
      'retained_close_listeners': closing == null ? 0 : count,
      'active_removable_waiters': closing == null ? count : 0,
      'rss_bytes': rss,
      'elapsed_ms': clock.elapsedMilliseconds
    });
  }

  final guard = Timer.periodic(const Duration(milliseconds: 100), (_) {
    final rss = ProcessInfo.currentRss;
    if (rss > peakRss) peakRss = rss;
    if (rss > rssLimit) guardFailure = 'RSS exceeded fixed 768 MiB limit';
    if (clock.elapsed > const Duration(seconds: 120)) {
      guardFailure = 'Fixed 120 second deadline exceeded';
    }
  });
  var passed = false;
  Object? failure;
  var closeMicros = 0;
  try {
    sample(0);
    for (var i = 1; i <= iterations; i++) {
      var attempts = 0;
      Future<dynamic> operation() async {
        attempts++;
        operations++;
        if (attempts == 1) {
          retries++;
          throw const SocketException('Synthetic diagnostic transient failure');
        }
        return 123;
      }

      final dynamic result = await (reflected.invoke(
              privateSymbol('_safeRetry', library), [operation]).reflectee
          as Future<dynamic>);
      check(result == 123 && attempts == 2, 'Retry semantics changed');
      check(guardFailure == null, guardFailure ?? 'Guard failed');
      if (mode == 'removable') {
        check(waits.activeCount == 0, 'Completed wait retained');
      }
      if ([10, 100, 1000, iterations].contains(i)) sample(i);
    }
    check(operations == 2 * iterations && retries == iterations,
        'Unexpected retry counts');
    check(
        client.connectionCount == 0 &&
            client.pendingRequestCount == 0 &&
            client.admittedRequestCount == 0 &&
            client.queuedRequestBytes == 0 &&
            client.bufferedResponseBytes == 0,
        'Diagnostic unexpectedly used transport');
    passed = true;
  } catch (error) {
    failure = error;
  } finally {
    final closeClock = Stopwatch()..start();
    await client.close();
    // Legacy close completes its async Future and drains listeners in microtasks.
    await Future<void>.delayed(Duration.zero);
    closeMicros = closeClock.elapsedMicroseconds;
    guard.cancel();
  }
  check(
      closing == null || closing.isCompleted, 'Close future did not complete');
  final finalRss = ProcessInfo.currentRss;
  if (finalRss > peakRss) peakRss = finalRss;
  if (finalRss > rssLimit) guardFailure = 'RSS exceeded fixed 768 MiB limit';
  if (clock.elapsed > const Duration(seconds: 120)) {
    guardFailure = 'Fixed 120 second deadline exceeded';
  }
  final afterClose = closing == null
      ? waits.activeCount as int
      : listeners(closing.future, completed: true);
  passed = passed && afterClose == 0 && guardFailure == null;
  stdout.writeln('RETRY_WAITER_PROBE ${jsonEncode({
        'passed': passed,
        'mode': mode,
        'dart_version': Platform.version,
        'iterations': iterations,
        'operations': operations,
        'retries': retries,
        'samples': samples,
        'after_close_waiters': afterClose,
        'initial_rss_bytes': initialRss,
        'sampled_peak_rss_bytes': peakRss,
        'final_rss_bytes': finalRss,
        'rss_limit_bytes': rssLimit,
        'rss_sample_interval_ms': 100,
        'wall_limit_seconds': 120,
        'elapsed_ms': clock.elapsedMilliseconds,
        'close_elapsed_us': closeMicros,
        'guard_failure': guardFailure,
        'failure': failure?.toString(),
        'source_method': '_safeRetry',
        'synthetic_operation': true,
        'network_used': false,
        'sdk_private_mirrors': true,
      })}');
  if (!passed) exitCode = 1;
}
