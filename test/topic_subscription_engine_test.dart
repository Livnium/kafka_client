import 'dart:async';
import 'dart:io';

import 'package:kafka_client/src/api/response/api1_fetch_response.dart';
import 'package:kafka_client/src/codec/record_batch.dart';
import 'package:kafka_client/src/reliability.dart';
import 'package:kafka_client/src/topic_subscription.dart';
import 'package:test/test.dart';

FetchResponse response(int partition, List<int> offsets,
        {int? end, int watermark = 999, int throttle = 0, int error = 0}) =>
    FetchResponse(1, throttle, [
      FetchTopicResponse('events', [
        FetchPartitionResponse(partition, error, watermark, -1,
            offsets.map((o) => KafkaRecord(offset: o)).toList(),
            lastBatchOffset: end)
      ])
    ]);

Future<void> tick() => Future<void>.delayed(Duration.zero);

Future<void> until(bool Function() condition) async {
  final watch = Stopwatch()..start();
  while (!condition()) {
    if (watch.elapsed > const Duration(seconds: 3)) {
      fail('Condition did not become true');
    }
    await tick();
  }
}

TopicSubscriptionController engine(
        Future<FetchResponse> Function(int, int) fetch,
        {Map<int, int> offsets = const {0: 0},
        bool Function(Object)? retry,
        bool Function()? closed,
        void Function()? start,
        void Function()? finish,
        Duration poll = const Duration(milliseconds: 1),
        Duration retryDelay = const Duration(milliseconds: 1)}) =>
    TopicSubscriptionController(
        topic: 'events',
        startOffsets: offsets,
        fetch: fetch,
        isRetryable: retry ?? (_) => false,
        isClientClosed: closed ?? () => false,
        onStart: start ?? () {},
        onFinish: finish ?? () {},
        pollInterval: poll,
        retryDelay: retryDelay);

void main() {
  test('lazy single listener, copied offsets, registration ordering', () async {
    final calls = <String>[];
    final offsets = {0: 7};
    final e = engine((p, o) async {
      calls.add('fetch:$o');
      return response(p, []);
    },
        offsets: offsets,
        start: () => calls.add('start'),
        finish: () => calls.add('finish'));
    offsets[0] = 90;
    await tick();
    expect(calls, isEmpty);
    final sub = e.stream.listen((_) {});
    expect(() => e.stream.listen((_) {}), throwsStateError);
    await until(() => calls.length >= 2);
    await sub.cancel();
    expect(calls.first, 'start');
    expect(calls[1], 'fetch:7');
    expect(calls.last, 'finish');
    expect(calls.where((c) => c == 'finish'), hasLength(1));
  });

  test('round robin, positive gaps and independent increasing cursors',
      () async {
    final calls = <String>[];
    final seen = <String>[];
    final e = engine((p, o) async {
      calls.add('$p:$o');
      return response(p, [o, o + 2], end: o + 4);
    }, offsets: {2: 10, 0: 30});
    late StreamSubscription<KafkaTopicRecord> sub;
    sub = e.stream.listen((r) {
      seen.add('${r.partition}:${r.offset}');
      if (seen.length == 5) unawaited(sub.cancel());
    });
    await until(() => seen.length == 5);
    await e.stop();
    expect(calls, ['2:10', '0:30', '2:15']);
    expect(seen, ['2:10', '2:12', '0:30', '0:32', '2:15']);
  });

  test('pause holds remaining decoded records and prevents further fetches',
      () async {
    var calls = 0;
    final seen = <int>[];
    final e = engine((p, o) async {
      calls++;
      return response(p, [o, o + 1, o + 2]);
    });
    late StreamSubscription<KafkaTopicRecord> sub;
    sub = e.stream.listen((r) {
      seen.add(r.offset);
      if (seen.length == 1) sub.pause();
      if (seen.length == 3) unawaited(sub.cancel());
    });
    await until(() => seen.isNotEmpty);
    await Future<void>.delayed(const Duration(milliseconds: 15));
    expect(seen, [0]);
    expect(calls, 1);
    sub.resume();
    await until(() => seen.length == 3);
    await e.stop();
    expect(calls, 1);
  });

  for (final paused in [false, true]) {
    test('cancellation wakes ${paused ? 'paused' : 'idle'} stream', () async {
      var calls = 0;
      final e = engine((p, o) async {
        calls++;
        return response(p, []);
      }, poll: const Duration(days: 1));
      final sub = e.stream.listen((_) {});
      await until(() => calls == 1);
      if (paused) sub.pause();
      await sub.cancel().timeout(const Duration(seconds: 1));
      expect(calls, 1);
    });
  }

  test('pause before first fetch prevents admission until resumed', () async {
    var calls = 0;
    final e = engine((p, o) async {
      calls++;
      return response(p, []);
    });
    final sub = e.stream.listen((_) {});
    sub.pause();
    await Future<void>.delayed(const Duration(milliseconds: 10));
    expect(calls, 0);
    sub.resume();
    await until(() => calls > 0);
    await sub.cancel();
  });

  test('Fetch admission observes actual pause state during resume and re-pause',
      () async {
    var calls = 0;
    final admittedWhilePaused = <bool>[];
    late StreamSubscription<KafkaTopicRecord> sub;
    final e = engine((p, o) async {
      calls++;
      admittedWhilePaused.add(sub.isPaused);
      return response(p, []);
    });
    sub = e.stream.listen((_) {});
    addTearDown(sub.cancel);
    sub.pause();
    // Let the pump reach its suspended readiness wait.
    await tick();
    await tick();
    sub.resume();
    // Work may legally resume before this microtask; inspect actual state.
    scheduleMicrotask(sub.pause);
    await tick();
    await tick();
    expect(admittedWhilePaused, everyElement(isFalse));
    final callsAfterRePause = calls;
    await tick();
    expect(calls, callsAfterRePause);
    await sub.cancel();
  });

  test('record admission checks authoritative pause state during re-pause',
      () async {
    var calls = 0;
    final seen = <int>[];
    final records = [
      KafkaRecord(offset: 0),
      KafkaRecord(offset: 1),
      KafkaRecord(offset: 2),
    ];
    final e = engine((p, o) async {
      calls++;
      return FetchResponse(1, 0, [
        FetchTopicResponse('events',
            [FetchPartitionResponse(p, 0, 3, -1, records, lastBatchOffset: 2)])
      ]);
    });
    late StreamSubscription<KafkaTopicRecord> sub;
    sub = e.stream.listen((r) {
      seen.add(r.offset);
      if (seen.length == 1) sub.pause();
    });
    addTearDown(sub.cancel);
    await until(() => seen.length == 1);
    await tick();
    await tick();
    sub.resume();
    scheduleMicrotask(sub.pause);
    await tick();
    await tick();
    expect(seen, [0]);
    expect(calls, 1);
    // A record may legally be admitted after resume and before the later pause.
    // The engine's admission assertion checks actual controller pause state.
    // Once re-paused, no records are delivered and no new fetch is admitted.
    await tick();
    expect(seen, [0]);
    expect(calls, 1);
    await sub.cancel();
  });

  test('pause during fetch holds returned data until resumed', () async {
    var calls = 0;
    final pending = Completer<FetchResponse>();
    final seen = <int>[];
    final e = engine((p, o) {
      calls++;
      return pending.future;
    });
    late StreamSubscription<KafkaTopicRecord> sub;
    sub = e.stream.listen((r) {
      seen.add(r.offset);
      unawaited(sub.cancel());
    });
    await until(() => calls == 1);
    sub.pause();
    pending.complete(response(0, [0, 1]));
    await Future<void>.delayed(const Duration(milliseconds: 10));
    expect(seen, isEmpty);
    expect(calls, 1);
    sub.resume();
    await until(() => seen.isNotEmpty);
    await e.stop();
    expect(seen, [0]);
  });

  test('listen after client close emits StateError without registration',
      () async {
    var closed = false;
    var starts = 0;
    var finishes = 0;
    var calls = 0;
    final e = engine((p, o) async {
      calls++;
      return response(p, []);
    }, closed: () => closed, start: () => starts++, finish: () => finishes++);
    closed = true;
    await expectLater(
        e.stream, emitsInOrder([emitsError(isA<StateError>()), emitsDone]));
    await e.stop();
    expect(calls, 0);
    expect(starts, 0);
    expect(finishes, 0);
  });

  test('client close between listen and deferred pump terminates without work',
      () async {
    var closed = false;
    final errors = <Object>[];
    final done = Completer<void>();
    final e = engine((p, o) async => throw StateError('Unexpected fetch'),
        closed: () => closed, start: () => fail('Unexpected registration'));
    e.stream.listen((_) => fail('Unexpected data'),
        onError: errors.add, onDone: done.complete);
    closed = true;
    await done.future.timeout(const Duration(seconds: 1));
    await e.stop();
    expect(errors, [isA<StateError>()]);
  });

  test('explicit stop before deferred pump suppresses closed-client error',
      () async {
    var closed = false;
    final errors = <Object>[];
    final done = Completer<void>();
    final e = engine((p, o) async => throw StateError('Unexpected fetch'),
        closed: () => closed, start: () => fail('Unexpected registration'));
    e.stream.listen((_) => fail('Unexpected data'),
        onError: errors.add, onDone: done.complete);
    closed = true;
    await e.stop().timeout(const Duration(seconds: 1));
    await done.future;
    expect(errors, isEmpty);
  });

  test('stopping before listen never registers or fetches', () async {
    final e = engine((p, o) async => throw StateError('Unexpected fetch'),
        start: () => fail('Unexpected registration'));
    await e.stop();
    expect(await e.stream.toList(), isEmpty);
  });

  test('cancel waits for in-flight work and suppresses late data', () async {
    final pending = Completer<FetchResponse>();
    var calls = 0;
    var finished = false;
    final seen = <KafkaTopicRecord>[];
    final e = engine((p, o) {
      calls++;
      return pending.future;
    });
    final sub = e.stream.listen(seen.add);
    await until(() => calls == 1);
    final cancel = sub.cancel().then((_) => finished = true);
    await tick();
    expect(finished, isFalse);
    pending.complete(response(0, [0]));
    await cancel;
    expect(seen, isEmpty);
    expect(calls, 1);
  });

  test('client close stops admission then transport failure settles fetch',
      () async {
    var closed = false;
    var started = false;
    final pending = Completer<FetchResponse>();
    final errors = <Object>[];
    final done = Completer<void>();
    final e = engine((p, o) {
      started = true;
      return pending.future;
    }, closed: () => closed);
    e.stream.listen((_) {}, onError: errors.add, onDone: done.complete);
    await until(() => started);
    closed = true;
    final stopped = e.stop();
    pending.completeError(KafkaConnectionException());
    await stopped;
    await done.future;
    expect(errors, isEmpty);
  });

  test('transient retry keeps cursor and cancellation wakes retry delay',
      () async {
    final calls = <int>[];
    final e = engine((p, o) async {
      calls.add(o);
      if (calls.length < 3) throw KafkaConnectionException();
      return response(p, [o]);
    }, offsets: {0: 17}, retry: (e) => e is KafkaConnectionException);
    expect((await e.stream.first).offset, 17);
    expect(calls, [17, 17, 17]);
    var called = false;
    final waiting = engine((p, o) async {
      called = true;
      throw KafkaConnectionException();
    }, retry: (_) => true, retryDelay: const Duration(days: 1));
    final sub = waiting.stream.listen((_) {});
    await until(() => called);
    await sub.cancel().timeout(const Duration(seconds: 1));
  });

  test('permanently retrying partition cannot starve healthy partitions',
      () async {
    final calls = <String>[];
    final e = engine((p, o) async {
      calls.add('$p:$o');
      if (p == 0) throw KafkaConnectionException();
      return response(p, [o]);
    },
        offsets: {0: 9, 1: 20},
        retry: (error) => error is KafkaConnectionException);
    addTearDown(e.stop);
    final records =
        await e.stream.take(2).toList().timeout(const Duration(seconds: 2));
    expect(records.map((r) => '${r.partition}:${r.offset}'), ['1:20', '1:21']);
    expect(calls, ['0:9', '1:20', '0:9', '1:21']);
  });

  for (final error in <Object>[
    const FormatException('protocol'),
    HandshakeException('TLS'),
    KafkaResourceLimitException('bytes'),
    KafkaBrokerException(29, 'Fetch'),
    KafkaBrokerException(1, 'Fetch'),
  ]) {
    test('terminal callback error delivered once: $error', () async {
      var calls = 0;
      final errors = <Object>[];
      final done = Completer<void>();
      final e = engine((p, o) async {
        calls++;
        throw error;
      }, retry: (e) => e is KafkaConnectionException);
      e.stream.listen((_) => fail('Unexpected data'),
          onError: errors.add, onDone: done.complete);
      await done.future;
      expect(errors, [same(error)]);
      expect(calls, 1);
    });
  }

  test('control/compacted progress advances but empty watermark does not',
      () async {
    final calls = <int>[];
    final e = engine((p, o) async {
      calls.add(o);
      if (calls.length == 1) return response(p, [], end: 8);
      if (calls.length == 2) return response(p, [], watermark: 10000);
      return response(p, [12], end: 15);
    }, offsets: {0: 3});
    expect((await e.stream.first).offset, 12);
    expect(calls, [3, 9, 9]);
  });

  test('broker throttle delays next fetch', () async {
    final times = <int>[];
    final watch = Stopwatch()..start();
    final e = engine((p, o) async {
      times.add(watch.elapsedMilliseconds);
      return response(p, [o], throttle: 35);
    });
    await e.stream.take(2).toList();
    expect(times, hasLength(2));
    expect(times[1] - times[0], greaterThanOrEqualTo(30));
  });

  final invalid = <String, FetchResponse>{
    'backwards': response(0, [4]),
    'duplicate': response(0, [5, 5]),
    'unordered': response(0, [6, 5]),
    'end before record': response(0, [8], end: 7),
    'end before cursor': response(0, [], end: 3),
    'overflow': response(0, [], end: 9223372036854775807),
    'negative throttle': response(0, [5], throttle: -1),
  };
  for (final entry in invalid.entries) {
    test('fails closed before delivery for ${entry.key}', () async {
      var calls = 0;
      final e = engine((p, o) async {
        calls++;
        return entry.value;
      }, offsets: {0: 5}, retry: (_) => true);
      await expectLater(e.stream,
          emitsInOrder([emitsError(isA<FormatException>()), emitsDone]));
      expect(calls, 1);
    });
  }

  test('broker offset-out-of-range response is terminal without resetting',
      () async {
    final calls = <int>[];
    final e = engine((p, o) async {
      calls.add(o);
      return response(p, [], error: 1);
    }, offsets: {0: 100}, retry: (_) => true);
    await expectLater(e.stream,
        emitsInOrder([emitsError(isA<KafkaBrokerException>()), emitsDone]));
    expect(calls, [100]);
  });
}
