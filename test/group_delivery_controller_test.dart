import 'dart:async';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/src/consumer_group_metadata.dart';
import 'package:kafka_client/src/classic_group_session.dart';
import 'package:kafka_client/src/group_delivery.dart';
import 'package:test/test.dart';

import 'classic_group_session_test.dart' as fixture;

Future<void> tick() => Future<void>.delayed(Duration.zero);
Future<void> until(bool Function() condition) async {
  final watch = Stopwatch()..start();
  while (!condition()) {
    if (watch.elapsed > const Duration(seconds: 2)) fail('Timed out');
    await tick();
  }
}

FetchResponse reply(String topic, int partition, List<int> offsets,
        {int? end,
        int error = 0,
        int throttle = 0,
        List<KafkaRecord>? records}) =>
    FetchResponse(1, throttle, [
      FetchTopicResponse(topic, [
        FetchPartitionResponse(partition, error, 999, -1,
            records ?? offsets.map((n) => KafkaRecord(offset: n)).toList(),
            lastBatchOffset: end)
      ])
    ]);

void main() {
  late fixture.Clock clock;
  late fixture.Driver driver;
  late ClassicGroupSessionController session;
  late KafkaGroupAssignment token;
  late List<String> calls;
  late List<KafkaGroupRecord> seen;
  late List<Object> errors;
  late GroupDeliveryController? delivery;
  late StreamSubscription<KafkaGroupRecord>? listener;

  Future<GroupDeliveryController> make({
    Map<String, List<int>> partitions = const {
      'a': [0]
    },
    Map<String, Map<int, int>> offsets = const {
      'a': {0: 0}
    },
    Future<FetchResponse> Function(String, int, int)? fetch,
    Future<void> Function()? close,
    Duration poll = const Duration(milliseconds: 1),
    int maxRecords = 100,
    int maxPayload = 1000,
  }) async {
    driver.onSync = () async => ConsumerGroupAssignment(partitions);
    token = await session.start();
    delivery = GroupDeliveryController(
        session: session,
        assignment: token,
        startOffsets: offsets,
        fetch: (t, p, o) {
          calls.add('$t:$p:$o');
          return fetch == null
              ? Future.value(reply(t, p, [o, o + 1, o + 2]))
              : fetch(t, p, o);
        },
        closeData: () async {
          calls.add('close');
          await close?.call();
        },
        pollInterval: poll,
        maxBufferedRecords: maxRecords,
        maxBufferedPayloadBytes: maxPayload);
    return delivery!;
  }

  StreamSubscription<KafkaGroupRecord> listen(GroupDeliveryController e,
      {void Function(KafkaGroupRecord)? onData, void Function()? onDone}) {
    listener = e.stream.listen((r) {
      seen.add(r);
      onData?.call(r);
    }, onError: errors.add, onDone: onDone);
    return listener!;
  }

  setUp(() {
    clock = fixture.Clock();
    driver = fixture.Driver();
    session = fixture.controller(driver, clock);
    calls = [];
    seen = [];
    errors = [];
    delivery = null;
    listener = null;
  });
  tearDown(() async {
    await delivery?.cancel();
    await listener?.cancel();
    await session.close();
  });

  test(
      'lazy deferred one-listener delivery uses exact token and copied offsets',
      () async {
    final offsets = {
      'a': {0: 7}
    };
    final e = await make(offsets: offsets);
    offsets['a']![0] = 50;
    await tick();
    expect(calls, isEmpty);
    var returned = false;
    listen(e, onData: (_) {
      expect(returned, isTrue);
      unawaited(e.cancel());
    });
    returned = true;
    expect(seen, isEmpty);
    expect(() => e.stream.listen((_) {}), throwsStateError);
    await e.stopped;
    expect(calls, ['a:0:7', 'close']);
    expect(seen.single.assignment, same(token));
    expect(seen.single.offset, 7);
    expect(seen.single.topic, 'a');
    expect(seen.single.partition, 0);
    expect(session.isCurrent(token), isFalse);
  });

  for (final action in ['session close', 'cancel', 'both']) {
    test('synchronous onData $action never queues second record or deadlocks',
        () async {
      final e = await make();
      listen(e, onData: (_) {
        if (action != 'cancel') unawaited(session.close());
        if (action != 'session close') unawaited(e.cancel());
      });
      await e.stopped.timeout(const Duration(seconds: 1));
      expect(seen.map((e) => e.offset), [0]);
      expect(e.bufferedRecordCount, 0);
      expect(errors, isEmpty);
      expect(calls.where((c) => c == 'close'), hasLength(1));
    });
  }

  test(
      'pause retains only remaining records, revoke clears before revoked future, resume drops',
      () async {
    final e = await make();
    final sub = listen(e, onData: (_) => listener!.pause());
    await until(() => seen.isNotEmpty);
    expect(e.bufferedRecordCount, 2);
    final close = session.close();
    expect(e.bufferedRecordCount, 0);
    await session.revoked;
    await e.stopped.timeout(const Duration(seconds: 1));
    expect(sub.isPaused, isTrue);
    sub.resume();
    await tick();
    await tick();
    expect(seen.map((e) => e.offset), [0]);
    expect(errors, isEmpty);
    await close;
  });

  test('resume then re-pause checks actual pause state at admission', () async {
    final admittedPaused = <bool>[];
    final e = await make();
    final sub = listen(e, onData: (_) {
      admittedPaused.add(listener!.isPaused);
      listener!.pause();
    });
    await until(() => seen.isNotEmpty);
    for (var i = 0; i < 100; i++) {
      sub.resume();
      scheduleMicrotask(sub.pause);
      await tick();
      expect(sub.isPaused, isTrue);
      // Work may legally resume before the queued pause. Check actual state,
      // then prove no additional work starts once the pause has happened.
      expect(admittedPaused, everyElement(isFalse));
      final count = seen.length, fetches = calls.length;
      await tick();
      await tick();
      expect(seen.length, count);
      expect(calls.length, fetches);
    }
  });

  test('pause before listening pump prevents first Fetch', () async {
    final e = await make();
    final sub = listen(e);
    sub.pause();
    await tick();
    await tick();
    expect(calls, isEmpty);
    await e.cancel();
    expect(calls, ['close']);
    expect(seen, isEmpty);
  });

  for (final fail in [false, true]) {
    test(
        'late Fetch ${fail ? 'error' : 'records'} discarded; barrier awaits real settlement',
        () async {
      final pending = Completer<FetchResponse>();
      final e = await make(fetch: (_, __, ___) => pending.future);
      listen(e);
      await until(() => calls.isNotEmpty);
      var stopped = false;
      unawaited(e.stopped.then((_) {
        stopped = true;
      }));
      unawaited(session.close());
      await tick();
      expect(calls.last, 'close');
      expect(stopped, isFalse);
      if (fail) {
        pending.completeError(StateError('late'));
      } else {
        pending.complete(reply('a', 0, [0, 1]));
      }
      await e.stopped;
      expect(seen, isEmpty);
      expect(errors, isEmpty);
      expect(e.bufferedRecordCount, 0);
    });
  }

  test('owned data close is initiated before draining admitted Fetch',
      () async {
    final pending = Completer<FetchResponse>();
    final e = await make(
        fetch: (_, __, ___) => pending.future,
        close: () async {
          pending.completeError(StateError('owned close'));
        });
    listen(e);
    await until(() => calls.isNotEmpty);
    await e.cancel().timeout(const Duration(seconds: 1));
    expect(seen, isEmpty);
    expect(errors, isEmpty);
  });

  test('stopped waits data close and preserves cleanup failure', () async {
    final closing = Completer<void>();
    final e = await make(close: () => closing.future);
    var stopped = false;
    unawaited(e.stopped.then((_) {
      stopped = true;
    }));
    unawaited(e.cancel());
    await tick();
    expect(stopped, isFalse);
    closing.completeError(StateError('cleanup failed'));
    final result = await e.stopped;
    expect(result.reason, KafkaGroupDeliveryStopReason.cancelled);
    expect(result.cleanupError, isA<StateError>());
    expect(await e.cancel(), same(result));
  });

  test('cancel before listen has independent barrier and never emits records',
      () async {
    final e = await make();
    await e.cancel();
    expect(calls, ['close']);
    final done = Completer<void>();
    listen(e, onDone: done.complete);
    await done.future;
    expect(seen, isEmpty);
  });

  test('take(1) cancellation has no reentrant done/data emission', () async {
    final e = await make();
    final records = await e.stream.take(1).toList();
    expect(records.map((e) => e.offset), [0]);
    expect((await e.stopped).reason, KafkaGroupDeliveryStopReason.cancelled);
  });

  test('blocked Fetch does not stop heartbeat scheduling', () async {
    final pending = Completer<FetchResponse>();
    final e = await make(
        fetch: (_, __, ___) => pending.future,
        close: () async {
          pending.complete(reply('a', 0, []));
        });
    listen(e);
    await until(() => calls.isNotEmpty);
    for (var i = 0; i < 20; i++) {
      clock.advance(fixture.tick);
      await tick();
      expect(session.isCurrent(token), isTrue);
    }
    expect(driver.calls.where((c) => c == 'heartbeat'), hasLength(21));
    expect(seen, isEmpty);
  });

  test(
      'paused delivery keeps heartbeats but stalled event-loop expiry blocks next record',
      () async {
    final e = await make();
    final sub = listen(e, onData: (_) => listener!.pause());
    await until(() => seen.isNotEmpty);
    for (var i = 0; i < 20; i++) {
      clock.advance(fixture.tick);
      await tick();
    }
    expect(session.isCurrent(token), isTrue);
    expect(driver.calls.where((c) => c == 'heartbeat'), hasLength(21));
    clock.advance(fixture.lease, fire: false);
    sub.resume();
    expect((await e.stopped).reason, KafkaGroupDeliveryStopReason.revoked);
    expect(seen, hasLength(1));
    expect((await session.revoked).reason, KafkaGroupStopReason.localExpiry);
  });

  test(
      'slow synchronous callback expiry prevents next admission without timer firing',
      () async {
    final e = await make();
    listen(e, onData: (_) {
      clock.advance(fixture.lease, fire: false);
    });
    await e.stopped;
    expect(seen, hasLength(1));
    expect((await session.revoked).reason, KafkaGroupStopReason.localExpiry);
  });

  test('empty assignment is idle and stops without Fetch', () async {
    final e = await make(partitions: {}, offsets: {});
    listen(e);
    await tick();
    await tick();
    expect(calls, isEmpty);
    unawaited(session.close());
    await e.stopped;
    expect(calls, ['close']);
    expect(seen, isEmpty);
  });

  test(
      'round robin multi-topic cursors use validated batch ends, not high watermarks',
      () async {
    final e = await make(partitions: {
      'a': [0, 2],
      'b': [1]
    }, offsets: {
      'a': {2: 10, 0: 30},
      'b': {1: 50}
    }, fetch: (t, p, o) async => reply(t, p, [o, o + 2], end: o + 4));
    listen(e, onData: (_) {
      if (seen.length == 7) unawaited(delivery!.cancel());
    });
    await e.stopped;
    expect(calls, ['a:2:10', 'a:0:30', 'b:1:50', 'a:2:15', 'close']);
    expect(seen.map((r) => r.offset), [10, 12, 30, 32, 50, 52, 15]);
  });

  test(
      'control/compacted-only batches advance while truly empty responses do not',
      () async {
    var n = 0;
    final e = await make(fetch: (t, p, o) async {
      n++;
      if (n == 1) return reply(t, p, [], end: 9);
      if (n == 2) return reply(t, p, []);
      return reply(t, p, [o]);
    });
    listen(e, onData: (_) => unawaited(delivery!.cancel()));
    await e.stopped;
    expect(calls, ['a:0:0', 'a:0:10', 'a:0:10', 'close']);
    expect(seen.single.offset, 10);
  });

  final malformed = <String, FetchResponse>{
    'topic': reply('wrong', 0, [0]),
    'partition': reply('a', 1, [0]),
    'empty topics': FetchResponse(1, 0, []),
    'duplicate topics': FetchResponse(
        1, 0, [FetchTopicResponse('a', []), FetchTopicResponse('a', [])]),
    'negative throttle': reply('a', 0, [0], throttle: -1),
    'decreasing': reply('a', 0, [2, 1]),
    'duplicate': reply('a', 0, [0, 0]),
    'negative offset': reply('a', 0, [-1]),
    'end before record': reply('a', 0, [1], end: 0),
    'overflow': reply('a', 0, [9223372036854775807]),
    'negative end': reply('a', 0, [], end: -1),
  };
  for (final c in malformed.entries) {
    test('validates entire ${c.key} response before any delivery', () async {
      final e = await make(fetch: (_, __, ___) async => c.value);
      listen(e);
      final result = await e.stopped;
      expect(result.reason, KafkaGroupDeliveryStopReason.failure);
      expect(result.error, isA<FormatException>());
      expect(errors, hasLength(1));
      expect(seen, isEmpty);
      expect(session.isCurrent(token), isFalse);
    });
  }

  test('broker error is terminal after underlying Fetch policy', () async {
    final e = await make(fetch: (t, p, o) async => reply(t, p, [], error: 27));
    listen(e);
    expect((await e.stopped).error, isA<KafkaBrokerException>());
    expect(calls, ['a:0:0', 'close']);
    expect(seen, isEmpty);
  });

  for (final bound in ['count', 'value', 'key', 'header key', 'header value']) {
    test('$bound overflow fails before delivering any record', () async {
      final records = bound == 'count'
          ? [KafkaRecord(), KafkaRecord(offset: 1)]
          : [
              KafkaRecord(
                  key: bound == 'key' ? [1, 2] : null,
                  value: bound == 'value' ? [1, 2] : null,
                  headers: bound.startsWith('header')
                      ? [
                          KafkaRecordHeader(bound == 'header key' ? 'é' : '',
                              bound == 'header value' ? [1, 2] : null)
                        ]
                      : [])
            ];
      final e = await make(
          maxRecords: 1,
          maxPayload: 1,
          fetch: (t, p, o) async => reply(t, p, [], records: records));
      listen(e);
      expect((await e.stopped).error, isA<KafkaResourceLimitException>());
      expect(seen, isEmpty);
      expect(errors, hasLength(1));
    });
  }

  test('cancelOnError does not deadlock or replace original failure', () async {
    final e = await make(
        fetch: (_, __, ___) async => throw StateError('fetch failure'));
    listener =
        e.stream.listen(seen.add, onError: errors.add, cancelOnError: true);
    final result = await e.stopped.timeout(const Duration(seconds: 1));
    expect(result.reason, KafkaGroupDeliveryStopReason.failure);
    expect(result.error, isA<StateError>());
    // cancelOnError may await cancellation before invoking the error handler.
    await until(() => errors.isNotEmpty);
    expect(errors, hasLength(1));
    expect(calls, ['a:0:0', 'close']);
    expect(seen, isEmpty);
  });

  test('paused terminal error does not delay stopped barrier', () async {
    final pending = Completer<FetchResponse>();
    final e = await make(fetch: (_, __, ___) => pending.future);
    final sub = listen(e);
    await until(() => calls.isNotEmpty);
    sub.pause();
    pending.completeError(StateError('fetch failure'));
    final result = await e.stopped.timeout(const Duration(seconds: 1));
    expect(result.reason, KafkaGroupDeliveryStopReason.failure);
    expect(errors, isEmpty);
    expect(seen, isEmpty);
    sub.resume();
    await tick();
    expect(errors, hasLength(1));
  });

  test('already-started asynchronous callback is deliberately not drained',
      () async {
    final processing = Completer<void>();
    var completed = false;
    final e = await make();
    listener = e.stream.listen((r) async {
      seen.add(r);
      unawaited(e.cancel());
      await processing.future;
      completed = true;
    });
    await e.stopped;
    expect(completed, isFalse);
    expect(seen, hasLength(1));
    processing.complete();
    await tick();
    expect(completed, isTrue);
  });

  test('expiry before deferred first pump admits no Fetch', () async {
    final e = await make();
    listen(e);
    clock.advance(fixture.lease, fire: false);
    await e.stopped;
    expect(calls, ['close']);
    expect(seen, isEmpty);
  });

  test('local expiry after Fetch reply before buffering drops data', () async {
    final e = await make(fetch: (t, p, o) async {
      clock.advance(fixture.lease, fire: false);
      return reply(t, p, [o]);
    });
    listen(e);
    await e.stopped;
    expect(seen, isEmpty);
    expect(e.bufferedRecordCount, 0);
  });

  test('synchronous closeData failure is captured without losing stop result',
      () async {
    final e = await make();
    // Separate fixture avoids the helper async wrapper and tests Future.sync.
    await e.cancel();
    final otherDriver = fixture.Driver(), otherClock = fixture.Clock();
    final other = fixture.controller(otherDriver, otherClock);
    final otherToken = await other.start();
    final raw = GroupDeliveryController(
        session: other,
        assignment: otherToken,
        startOffsets: {
          'a': {0: 0, 2: 0}
        },
        fetch: (t, p, o) async => reply(t, p, []),
        closeData: () => throw StateError('sync close'),
        pollInterval: fixture.tick,
        maxBufferedRecords: 1,
        maxBufferedPayloadBytes: 1);
    final result = await raw.cancel();
    expect(result.cleanupError, isA<StateError>());
    await other.closed;
  });

  test('payload/count exact limits are accepted', () async {
    final e = await make(
        maxRecords: 1,
        maxPayload: 4,
        fetch: (t, p, o) async => reply(t, p, [], records: [
              KafkaRecord(offset: o, key: [
                1
              ], value: [
                2
              ], headers: [
                KafkaRecordHeader('h', [3])
              ])
            ]));
    listen(e, onData: (_) => unawaited(delivery!.cancel()));
    expect((await e.stopped).reason, KafkaGroupDeliveryStopReason.cancelled);
    expect(seen, hasLength(1));
    expect(errors, isEmpty);
  });

  test('invalid exact assignment coverage fails without fetching or attaching',
      () async {
    token = await session.start();
    for (final positions in <Map<String, Map<int, int>>>[
      {},
      {
        'a': {0: 0}
      },
      {
        'a': {0: 0, 2: 0, 3: 0}
      },
      {
        'a': {0: 0, 2: -1}
      },
      {
        'b': {0: 0, 2: 0}
      },
      {
        'a': {0: 0, 2: 0},
        'extra': {}
      },
    ]) {
      expect(
          () => GroupDeliveryController.validate(
              token, positions, const Duration(milliseconds: 1), 1, 1),
          throwsArgumentError);
    }
    for (final poll in [Duration.zero, const Duration(minutes: 2)]) {
      expect(
          () => GroupDeliveryController.validate(
              token,
              {
                'a': {0: 0, 2: 0}
              },
              poll,
              1,
              1),
          throwsArgumentError);
    }
    for (final n in [0, -1, 2147483648]) {
      expect(
          () => GroupDeliveryController.validate(
              token,
              {
                'a': {0: 0, 2: 0}
              },
              fixture.tick,
              n,
              1),
          throwsArgumentError);
      expect(
          () => GroupDeliveryController.validate(
              token,
              {
                'a': {0: 0, 2: 0}
              },
              fixture.tick,
              1,
              n),
          throwsArgumentError);
    }
    expect(calls, isEmpty);
  });
}
