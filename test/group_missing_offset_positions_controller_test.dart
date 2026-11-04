import 'dart:async';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/src/group_consumer.dart';
import 'package:kafka_client/src/group_offsets_protocol.dart';
import 'package:test/test.dart';

import 'group_consumer_controller_test.dart' as fixture;
import 'group_consumer_coordinator_loss_test.dart' show loseCoordinator;
import 'group_processing_controller_test.dart' as processing_fixture;

KafkaCommittedOffset stored(int next) =>
    KafkaCommittedOffset(nextOffset: next, leaderEpoch: -1);

void main() {
  final controllers = <GroupConsumerController>[];
  final releases = <void Function()>[];
  GroupConsumerController make(List<fixture.ConsumerCycle> cycles,
      {KafkaOffsetPosition? position,
      Map<String, Map<int, int>> fallback = const {},
      bool floors = false,
      bool commit = false}) {
    var index = 1;
    final c = GroupConsumerController(
        initial: cycles.first,
        createCycle: () => cycles[index++],
        commitAfterProcessing: commit,
        missingOffsets: fallback,
        missingOffsetPosition: position,
        rejoinOnCoordinatorLoss: floors,
        maxRejoins: 1,
        rejoinDelay: const Duration(milliseconds: 1));
    controllers.add(c);
    return c;
  }

  Completer<T> gate<T>(T fallback) {
    final pending = Completer<T>();
    releases.add(() {
      if (!pending.isCompleted) pending.complete(fallback);
    });
    return pending;
  }

  tearDown(() async {
    for (final c in controllers) {
      c.stop();
    }
    for (final release in releases) {
      release();
    }
    for (final c in controllers) {
      await c.closed.timeout(const Duration(seconds: 3));
    }
    controllers.clear();
    releases.clear();
  });

  test('default missing position still fails without a boundary request',
      () async {
    final a = fixture.ConsumerCycle()..read = ((_, __) async => {0: null});
    final c = make([a]);
    final result = await c.run((_) {});
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(result.error, isA<StateError>());
    expect(a.boundaryRequests, isEmpty);
    expect(a.fetches, isEmpty);
  });

  for (final position in KafkaOffsetPosition.values) {
    test('$position resolves null only after stored and numeric precedence',
        () async {
      final a = fixture.ConsumerCycle(partitions: {
        'a': [0, 1, 2]
      })
        ..emit = false
        ..read = ((_, __) async => {0: stored(10), 1: null, 2: null})
        ..resolveBoundary = ((_, __, ___) async => 30);
      final c = make([a],
          position: position,
          fallback: {
            'a': {0: 80, 1: 20}
          });
      c.run((_) {});
      await fixture.until(() => a.loaded != null);
      expect(a.loaded, {
        'a': {0: 10, 1: 20, 2: 30}
      });
      expect(a.boundaryRequests, [('a', 2, position)]);
      expect(() => a.loaded!['a']![2] = 90, throwsUnsupportedError);
      expect(() => a.loaded!['b'] = {}, throwsUnsupportedError);
    });
  }

  for (final next in [0, 9223372036854775807]) {
    test('stored $next wins over both fallback types', () async {
      final a = fixture.ConsumerCycle()
        ..emit = false
        ..read = ((_, __) async => {0: stored(next)});
      final c = make([a],
          position: KafkaOffsetPosition.latest,
          fallback: {
            'a': {0: 7}
          });
      c.run((_) {});
      await fixture.until(() => a.loaded != null);
      expect(a.loaded!['a']![0], next);
      expect(a.boundaryRequests, isEmpty);
    });
  }

  test('Fetch out-of-range after boundary start remains terminal without reset',
      () async {
    final error = KafkaBrokerException(1, 'Fetch');
    final a = fixture.ConsumerCycle()
      ..read = ((_, __) async => {0: null})
      ..resolveBoundary = ((_, __, ___) async => 10)
      ..fetchError = error;
    final b = fixture.ConsumerCycle();
    final c =
        make([a, b], position: KafkaOffsetPosition.earliest, floors: true);
    final result = await c.run((_) {});
    expect(result.reason, KafkaGroupConsumerStopReason.failure);
    expect(result.error, same(error));
    expect(a.boundaryRequests, hasLength(1));
    expect(a.fetches, [10]);
    expect(b.starts, 0);
  });

  test('empty assignment never reads offsets or boundaries or fetches',
      () async {
    final a = fixture.ConsumerCycle(partitions: {});
    final c = make([a], position: KafkaOffsetPosition.earliest);
    c.run((_) => fail('empty assignment delivered a record'));
    await fixture.until(() => a.loaded != null);
    expect(a.loaded, isEmpty);
    expect(a.reads, isEmpty);
    expect(a.boundaryRequests, isEmpty);
    expect(a.fetches, isEmpty);
  });

  test('snapshots all topic responses before awaiting serial boundaries',
      () async {
    final response = <int, KafkaCommittedOffset?>{0: stored(10), 1: null};
    final pending = gate<int>(40);
    final a = fixture.ConsumerCycle(partitions: {
      'a': [0, 1],
      'b': [0]
    })
      ..emit = false;
    a.read = (topic, _) async {
      if (topic == 'a') return response;
      response[0] = stored(999);
      response[1] = stored(888);
      return {0: null};
    };
    a.resolveBoundary = (topic, _, __) {
      expect(a.reads, ['a', 'b']);
      return topic == 'a' ? pending.future : Future.value(50);
    };
    final c = make([a], position: KafkaOffsetPosition.latest);
    c.run((_) {});
    await fixture.until(() => a.boundaryRequests.isNotEmpty);
    expect(a.boundaryRequests, [('a', 1, KafkaOffsetPosition.latest)]);
    expect(a.loaded, isNull);
    pending.complete(40);
    await fixture.until(() => a.loaded != null);
    expect(a.loaded, {
      'a': {0: 10, 1: 40},
      'b': {0: 50}
    });
    expect(a.boundaryRequests, [
      ('a', 1, KafkaOffsetPosition.latest),
      ('b', 0, KafkaOffsetPosition.latest)
    ]);
  });

  for (final defect in ['missing', 'extra', 'negative', 'error']) {
    test('later topic $defect fails before any earlier null boundary lookup',
        () async {
      final error = StateError('offset read failed');
      final a = fixture.ConsumerCycle(partitions: {
        'a': [0],
        'b': [0]
      });
      a.read = (topic, _) async {
        if (topic == 'a') return {0: null};
        if (defect == 'missing') return {};
        if (defect == 'extra') return {0: null, 1: null};
        if (defect == 'negative') return {0: stored(-1)};
        throw error;
      };
      final c = make([a], position: KafkaOffsetPosition.earliest);
      final result = await c.run((_) {});
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      if (defect == 'error') expect(result.error, same(error));
      expect(a.boundaryRequests, isEmpty);
      expect(a.fetches, isEmpty);
      expect(a.loaded, isNull);
      expect(a.positionCloses, 1);
    });
  }

  for (final stop in [false, true]) {
    for (final phase in ['read', 'boundary', 'between']) {
      for (final lateError in [false, true]) {
        test(
            '${stop ? 'stop' : 'revoke'} during $phase with lateError=$lateError fences all work',
            () async {
          final read =
              gate<Map<int, KafkaCommittedOffset?>>({0: null, 1: null});
          final boundary = gate<int>(20);
          final a = fixture.ConsumerCycle(partitions: {
            'a': [0, 1]
          });
          final b = fixture.ConsumerCycle();
          final error = StateError('late $phase failure');
          a.read = (_, __) =>
              phase == 'read' ? read.future : Future.value({0: null, 1: null});
          a.resolveBoundary = (_, __, ___) => boundary.future;
          final c = make([a, b], position: KafkaOffsetPosition.latest);
          void invalidate() {
            if (stop) {
              c.stop();
            } else {
              a.reject();
            }
          }

          if (phase == 'between') {
            a.resolveBoundary = (_, __, ___) async {
              invalidate();
              if (lateError) throw error;
              return 20;
            };
          }
          final done = c.run((_) => fail('revoked generation delivered'));
          if (phase == 'read') {
            await fixture.until(() => a.reads.isNotEmpty);
            invalidate();
            if (lateError) {
              read.completeError(error);
            } else {
              read.complete({0: null, 1: null});
            }
          } else if (phase == 'boundary') {
            await fixture.until(() => a.boundaryRequests.isNotEmpty);
            invalidate();
            if (lateError) {
              boundary.completeError(error);
            } else {
              boundary.complete(20);
            }
          }
          final result = await done;
          expect(
              result.reason,
              stop
                  ? KafkaGroupConsumerStopReason.cancelled
                  : KafkaGroupConsumerStopReason.failure);
          if (lateError) {
            expect(stop ? result.interruptedOperationError : result.error,
                same(error));
          }
          expect(a.boundaryRequests.length, phase == 'read' ? 0 : 1);
          expect(a.fetches, isEmpty);
          expect(a.loaded, isNull);
          expect(b.starts, 0);
          expect(a.positionCloses, 1);
        });
      }
    }
  }

  for (final failClose in [false, true]) {
    test('boundary result waits for position close barrier fail=$failClose',
        () async {
      final close = gate<void>(null);
      final error = StateError('position cleanup failed');
      final a = fixture.ConsumerCycle()
        ..emit = false
        ..read = ((_, __) async => {0: null})
        ..resolveBoundary = ((_, __, ___) async => 42)
        ..closePosition = (() => close.future);
      final c = make([a], position: KafkaOffsetPosition.earliest);
      final done = c.run((_) {});
      await fixture.until(() => a.positionCloses == 1);
      expect(a.fetches, isEmpty);
      expect(a.loaded, isNull);
      if (failClose) {
        close.completeError(error);
        final result = await done;
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(result.cleanupErrors, contains(same(error)));
        expect(a.loaded, isNull);
      } else {
        close.complete();
        await fixture.until(() => a.loaded != null);
        expect(a.loaded, {
          'a': {0: 42}
        });
      }
      expect(a.positionCloses, 1);
    });
  }

  for (final outcome in ['error', 'negative', 'zero', 'max']) {
    test('boundary $outcome is validated before admission without retry',
        () async {
      final error = StateError('boundary lookup failed');
      final a = fixture.ConsumerCycle()
        ..emit = false
        ..read = ((_, __) async => {0: null});
      a.resolveBoundary = (_, __, ___) async {
        if (outcome == 'error') throw error;
        return outcome == 'negative'
            ? -1
            : outcome == 'zero'
                ? 0
                : 9223372036854775807;
      };
      final c = make([a], position: KafkaOffsetPosition.latest);
      final done = c.run((_) {});
      if (outcome == 'error' || outcome == 'negative') {
        final result = await done;
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(result.error,
            outcome == 'error' ? same(error) : isA<FormatException>());
        expect(a.fetches, isEmpty);
      } else {
        await fixture.until(() => a.loaded != null);
        expect(a.loaded!['a']![0], outcome == 'zero' ? 0 : 9223372036854775807);
      }
      expect(a.boundaryRequests, hasLength(1));
      expect(a.positionCloses, 1);
    });
  }

  for (final source in ['boundary', 'numeric']) {
    for (final progress in [false, true]) {
      test(
          '$source start and uncommitted progress=$progress establish no floor',
          () async {
        final a = fixture.ConsumerCycle()
          ..emit = progress
          ..read = ((_, __) async => {0: null})
          ..resolveBoundary = ((_, __, ___) async => 100);
        final b = fixture.ConsumerCycle()
          ..emit = false
          ..read = ((_, __) async => {0: null})
          ..resolveBoundary = ((_, __, ___) async => 5);
        final c = make([a, b],
            position: KafkaOffsetPosition.earliest,
            floors: true,
            fallback: source == 'numeric'
                ? {
                    'a': {0: 100}
                  }
                : {});
        var callbacks = 0;
        c.run((_) {
          callbacks++;
        });
        await fixture.until(() => progress ? callbacks == 2 : a.loaded != null);
        loseCoordinator(a, KafkaConnectionException());
        await fixture.until(() => b.loaded != null);
        expect(b.loaded!['a']![0], source == 'numeric' ? 100 : 5);
        expect(a.boundaryRequests.length, source == 'numeric' ? 0 : 1);
        expect(b.boundaryRequests.length, source == 'numeric' ? 0 : 1);
        expect(a.commits, isEmpty);
      });
    }
  }

  for (final next in [null, 9]) {
    test('known broker floor rejects fresh $next before earlier-topic boundary',
        () async {
      final a = fixture.ConsumerCycle(partitions: {
        'a': [0],
        'b': [0]
      })
        ..emit = false
        ..read = ((topic, _) async => {0: topic == 'a' ? null : stored(10)})
        ..resolveBoundary = ((_, __, ___) async => 100);
      final b = fixture.ConsumerCycle(partitions: {
        'a': [0],
        'b': [0]
      })
        ..read = ((topic, _) async =>
            {0: topic == 'a' || next == null ? null : stored(next)})
        ..resolveBoundary = ((_, __, ___) async => 5);
      final c =
          make([a, b], position: KafkaOffsetPosition.latest, floors: true);
      final done = c.run((_) {});
      await fixture.until(() => a.loaded != null);
      loseCoordinator(a, KafkaConnectionException());
      final result = await done;
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.error, isA<StateError>());
      expect(b.reads, ['a', 'b']);
      expect(b.boundaryRequests, isEmpty);
      expect(b.fetches, isEmpty);
    });
  }

  for (final fresh in [null, 9, 10, 15]) {
    test(
        'expanded assignment preserves known floor and resolves only new partition: $fresh',
        () async {
      final a = fixture.ConsumerCycle()
        ..emit = false
        ..read = ((_, __) async => {0: stored(10)});
      final b = fixture.ConsumerCycle(partitions: {
        'a': [0, 1]
      })
        ..emit = false
        ..read = ((_, __) async =>
            {0: fresh == null ? null : stored(fresh), 1: null})
        ..resolveBoundary = ((_, __, ___) async => 50);
      final c = make([a, b],
          position: KafkaOffsetPosition.latest,
          floors: true,
          fallback: {
            'a': {0: 100}
          });
      final done = c.run((_) {});
      await fixture.until(() => a.loaded != null);
      expect(a.loaded, {
        'a': {0: 10}
      });
      expect(a.boundaryRequests, isEmpty);
      loseCoordinator(a, KafkaConnectionException());
      if (fresh != null && fresh >= 10) {
        await fixture.until(() => b.loaded != null);
        expect(b.loaded, {
          'a': {0: fresh, 1: 50}
        });
        expect(b.boundaryRequests, [('a', 1, KafkaOffsetPosition.latest)]);
        c.stop();
        expect((await done).reason, KafkaGroupConsumerStopReason.cancelled);
      } else {
        final result = await done;
        expect(result.reason, KafkaGroupConsumerStopReason.failure);
        expect(result.error, isA<StateError>());
        expect(b.boundaryRequests, isEmpty);
        expect(b.loaded, isNull);
        expect(b.fetches, isEmpty);
      }
      expect(b.starts, 1);
      expect(b.reads, ['a']);
    });
  }

  for (final late in [false, true]) {
    for (final next in [null, 100, 101]) {
      test(
          'confirmed boundary-start commit late=$late establishes floor for fresh $next',
          () async {
        final pending = gate<GroupOffsetCommitResponse>(
            processing_fixture.committed('a', {0: 101}));
        final a = fixture.ConsumerCycle()
          ..read = ((_, __) async => {0: null})
          ..resolveBoundary = ((_, __, ___) async => 100)
          ..commit = ((_, __) => pending.future);
        final b = fixture.ConsumerCycle()
          ..emit = false
          ..read = ((_, __) async => {0: next == null ? null : stored(next)});
        final c = make([a, b],
            position: KafkaOffsetPosition.earliest, floors: true, commit: true);
        final secondCallback = gate<void>(null);
        final done = c.run((record) {
          if (record.record.offset == 101) return secondCallback.future;
        });
        await fixture.until(() => a.commits.length == 1);
        if (late) {
          loseCoordinator(a, KafkaConnectionException());
          await a.session.revoked;
          expect(b.starts, 0);
          pending.complete(processing_fixture.committed('a', {0: 101}));
        } else {
          // Hold the next callback so only the first record can commit.
          pending.complete(processing_fixture.committed('a', {0: 101}));
          await fixture
              .until(() => a.progress!.confirmedNextOffsets['a']?[0] == 101);
          loseCoordinator(a, KafkaConnectionException());
        }
        secondCallback.complete();
        if (next == 101) {
          await fixture.until(() => b.loaded != null);
          c.stop();
          expect((await done).reason, KafkaGroupConsumerStopReason.cancelled);
        } else {
          final result = await done;
          expect(result.reason, KafkaGroupConsumerStopReason.failure);
          expect(result.error, isA<StateError>());
          expect(b.loaded, isNull);
        }
        expect(b.boundaryRequests, isEmpty);
      });
    }
  }
}
