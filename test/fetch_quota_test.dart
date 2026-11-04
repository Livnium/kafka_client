import 'dart:async';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/src/consumer_group_metadata.dart';
import 'package:kafka_client/src/group_delivery.dart';
import 'package:test/test.dart';

import 'classic_group_session_test.dart' as session_fixture;
import 'group_delivery_controller_test.dart' as delivery_fixture;
import 'topic_subscription_engine_test.dart' as topic_fixture;

const _quota = Duration(milliseconds: 777);

class _HeldTimer implements Timer {
  final void Function() callback;
  bool _active = true;
  int _tick = 0;
  _HeldTimer(this.callback);
  @override
  bool get isActive => _active;
  @override
  int get tick => _tick;
  void fire() {
    if (!_active) return;
    _active = false;
    _tick++;
    callback();
  }

  @override
  void cancel() => _active = false;
}

class _QuotaWait {
  final Duration duration;
  final timers = <_HeldTimer>[];
  _QuotaWait([this.duration = _quota]);
  Future<void> run(Future<void> Function() action) =>
      runZoned(action, zoneSpecification: ZoneSpecification(
          createTimer: (self, parent, zone, duration, callback) {
        if (duration != this.duration) {
          return parent.createTimer(zone, duration, callback);
        }
        final timer = _HeldTimer(callback);
        timers.add(timer);
        return timer;
      }));
}

class _Group {
  final clock = session_fixture.Clock();
  final driver = session_fixture.Driver();
  late final session = session_fixture.controller(driver, clock);
  late KafkaGroupAssignment assignment;
  late GroupDeliveryController delivery;
  final seen = <KafkaGroupRecord>[];
  final errors = <Object>[];
  StreamSubscription<KafkaGroupRecord>? listener;
  int closes = 0;

  Future<void> start(Future<FetchResponse> Function(String, int, int) fetch,
      {Duration poll = const Duration(milliseconds: 1),
      Map<String, List<int>> partitions = const {
        'a': [0]
      }}) async {
    driver.onSync = () async => ConsumerGroupAssignment(partitions);
    assignment = await session.start();
    delivery = GroupDeliveryController(
      session: session,
      assignment: assignment,
      startOffsets: {
        for (final topic in partitions.entries)
          topic.key: {for (final p in topic.value) p: 7}
      },
      fetch: fetch,
      closeData: () async {
        closes++;
      },
      pollInterval: poll,
      maxBufferedRecords: 10,
      maxBufferedPayloadBytes: 1000,
    );
  }

  void listen({bool cancelAfterRecord = false}) {
    listener = delivery.stream.listen((record) {
      seen.add(record);
      if (cancelAfterRecord) unawaited(delivery.cancel());
    }, onError: errors.add);
  }

  Future<void> close() async {
    await delivery.cancel();
    await listener?.cancel();
    await session.close();
  }
}

void main() {
  test('subscription waits once then polls the exact unchanged quota cursor',
      () async {
    final wait = _QuotaWait();
    await wait.run(() async {
      final calls = <String>[];
      final seen = <KafkaTopicRecord>[];
      final errors = <Object>[];
      final engine = topic_fixture.engine((p, o) async {
        calls.add('$p:$o');
        return calls.length == 1
            ? FetchResponse(1, _quota.inMilliseconds, [])
            : topic_fixture.response(p, [o]);
      }, offsets: {0: 7, 1: 20});
      late StreamSubscription<KafkaTopicRecord> sub;
      sub = engine.stream.listen((r) {
        seen.add(r);
        unawaited(sub.cancel());
      }, onError: errors.add);
      try {
        await topic_fixture
            .until(() => wait.timers.isNotEmpty || errors.isNotEmpty);
        expect(errors, isEmpty);
        expect(calls, ['0:7']);
        expect(seen, isEmpty);
        await topic_fixture.tick();
        expect(calls, ['0:7'],
            reason: 'An unfired quota timer admits no Fetch');
        expect(wait.timers, hasLength(1));
        wait.timers.single.fire();
        await topic_fixture.until(() => seen.isNotEmpty);
        expect(calls, ['0:7', '0:7']);
        expect(seen.single.offset, 7);
      } finally {
        await sub.cancel();
        await engine.stop();
      }
    });
  });

  test('subscription quota retains the longer empty-poll cadence', () async {
    final wait = _QuotaWait(const Duration(milliseconds: 999));
    await wait.run(() async {
      var calls = 0;
      final errors = <Object>[], seen = <int>[];
      final engine = topic_fixture.engine((p, o) async {
        calls++;
        return calls == 1
            ? FetchResponse(1, _quota.inMilliseconds, [])
            : topic_fixture.response(p, [o]);
      }, poll: wait.duration);
      late StreamSubscription<KafkaTopicRecord> sub;
      sub = engine.stream.listen((record) {
        seen.add(record.offset);
        unawaited(sub.cancel());
      }, onError: errors.add);
      try {
        await topic_fixture
            .until(() => wait.timers.isNotEmpty || errors.isNotEmpty);
        expect(errors, isEmpty);
        expect(calls, 1);
        expect(seen, isEmpty);
        wait.timers.single.fire();
        await topic_fixture.until(() => seen.isNotEmpty);
        expect(calls, 2);
        expect(seen, [0]);
      } finally {
        await sub.cancel();
        await engine.stop();
      }
    });
  });

  for (final stop in ['cancel', 'close', 'pause']) {
    test('subscription $stop during quota wait cannot admit a late Fetch',
        () async {
      final wait = _QuotaWait();
      await wait.run(() async {
        var calls = 0, closed = false;
        final seen = <int>[], errors = <Object>[];
        final engine = topic_fixture.engine((p, o) async {
          calls++;
          return calls == 1
              ? FetchResponse(1, _quota.inMilliseconds, [])
              : topic_fixture.response(p, [o]);
        }, offsets: {0: 7}, closed: () => closed);
        late StreamSubscription<KafkaTopicRecord> sub;
        sub = engine.stream.listen((r) {
          seen.add(r.offset);
          unawaited(sub.cancel());
        }, onError: errors.add);
        try {
          await topic_fixture
              .until(() => wait.timers.isNotEmpty || errors.isNotEmpty);
          expect(errors, isEmpty);
          if (stop == 'pause') {
            sub.pause();
            wait.timers.single.fire();
            await topic_fixture.tick();
            expect(calls, 1);
            expect(seen, isEmpty);
            sub.resume();
            await topic_fixture.until(() => seen.isNotEmpty);
            expect(seen, [7]);
            expect(calls, 2);
          } else {
            if (stop == 'close') {
              closed = true;
              await engine.stop();
            } else {
              await sub.cancel();
            }
            expect(wait.timers.single.isActive, isFalse);
            wait.timers.single.fire();
            await topic_fixture.tick();
            expect(calls, 1);
            expect(seen, isEmpty);
          }
        } finally {
          await sub.cancel();
          await engine.stop();
        }
      });
    });
  }

  test('group waits at unchanged cursor while independent heartbeats continue',
      () async {
    final wait = _QuotaWait();
    final group = _Group();
    final calls = <String>[];
    await wait.run(() async {
      await group.start((t, p, o) async {
        calls.add('$t:$p:$o');
        return calls.length == 1
            ? FetchResponse(1, _quota.inMilliseconds, [])
            : delivery_fixture.reply(t, p, [o]);
      }, partitions: {
        'a': [0, 1]
      });
      group.listen(cancelAfterRecord: true);
      try {
        await delivery_fixture
            .until(() => wait.timers.isNotEmpty || group.errors.isNotEmpty);
        expect(group.errors, isEmpty);
        expect(group.seen, isEmpty);
        expect(group.delivery.bufferedRecordCount, 0);
        group.clock.advance(session_fixture.tick);
        await delivery_fixture.tick();
        group.clock.advance(session_fixture.tick);
        await delivery_fixture.tick();
        expect(group.driver.calls.where((call) => call == 'heartbeat'),
            hasLength(3));
        expect(calls, ['a:0:7']);
        expect(group.session.isCurrent(group.assignment), isTrue);
        wait.timers.single.fire();
        await group.delivery.stopped;
        expect(calls, ['a:0:7', 'a:0:7']);
        expect(group.seen.single.offset, 7);
        expect(group.closes, 1);
      } finally {
        await group.close();
      }
    });
  });

  for (final mode in ['pause', 'longer poll']) {
    test('group quota preserves $mode admission boundary', () async {
      final wait = _QuotaWait(
          mode == 'longer poll' ? const Duration(milliseconds: 999) : _quota);
      final group = _Group();
      final calls = <String>[];
      await wait.run(() async {
        await group.start((t, p, o) async {
          calls.add('$t:$p:$o');
          return calls.length == 1
              ? FetchResponse(1, _quota.inMilliseconds, [])
              : delivery_fixture.reply(t, p, [o]);
        },
            poll: mode == 'longer poll'
                ? wait.duration
                : const Duration(milliseconds: 1));
        group.listen(cancelAfterRecord: true);
        try {
          await delivery_fixture
              .until(() => wait.timers.isNotEmpty || group.errors.isNotEmpty);
          expect(group.errors, isEmpty);
          expect(group.seen, isEmpty);
          expect(calls, ['a:0:7']);
          if (mode == 'pause') group.listener!.pause();
          wait.timers.single.fire();
          if (mode == 'pause') {
            await delivery_fixture.tick();
            expect(calls, ['a:0:7']);
            expect(group.seen, isEmpty);
            group.listener!.resume();
          }
          await group.delivery.stopped;
          expect(calls, ['a:0:7', 'a:0:7']);
          expect(group.seen.single.offset, 7);
        } finally {
          await group.close();
        }
      });
    });
  }

  for (final stop in ['cancel', 'revoke', 'expiry']) {
    test('group $stop interrupts even the maximum quota wait', () async {
      final wait = _QuotaWait(const Duration(milliseconds: 2147483647));
      final group = _Group();
      var calls = 0;
      await wait.run(() async {
        await group.start((_, __, ___) async {
          calls++;
          return FetchResponse(1, wait.duration.inMilliseconds, []);
        });
        group.listen();
        try {
          await delivery_fixture
              .until(() => wait.timers.isNotEmpty || group.errors.isNotEmpty);
          expect(group.errors, isEmpty);
          if (stop == 'cancel') {
            await group.delivery.cancel();
          } else if (stop == 'revoke') {
            await group.session.close();
          } else {
            group.clock.advance(session_fixture.lease, fire: false);
            expect(group.session.isCurrent(group.assignment), isFalse);
          }
          final result = await group.delivery.stopped;
          expect(
              result.reason,
              stop == 'cancel'
                  ? KafkaGroupDeliveryStopReason.cancelled
                  : KafkaGroupDeliveryStopReason.revoked);
          expect(wait.timers.single.isActive, isFalse);
          wait.timers.single.fire();
          await delivery_fixture.tick();
          expect(calls, 1);
          expect(group.seen, isEmpty);
          expect(group.closes, 1);
        } finally {
          await group.close();
        }
      });
    });
  }

  for (final throttle in [0, -1]) {
    test('subscription rejects empty response with throttle $throttle',
        () async {
      var calls = 0;
      final errors = <Object>[];
      final done = Completer<void>();
      final engine = topic_fixture.engine((_, __) async {
        calls++;
        return FetchResponse(1, throttle, []);
      });
      final sub = engine.stream.listen((_) => fail('Unexpected record'),
          onError: errors.add, onDone: done.complete);
      await done.future;
      expect(errors.single, isA<FormatException>());
      expect(calls, 1);
      await sub.cancel();
      await engine.stop();
    });
    test('group rejects empty response with throttle $throttle', () async {
      final group = _Group();
      var calls = 0;
      await group.start((_, __, ___) async {
        calls++;
        return FetchResponse(1, throttle, []);
      });
      group.listen();
      try {
        final result = await group.delivery.stopped;
        expect(result.error, isA<FormatException>());
        expect(group.seen, isEmpty);
        expect(calls, 1);
      } finally {
        await group.close();
      }
    });
  }
}
