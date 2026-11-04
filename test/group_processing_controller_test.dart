import 'dart:async';

import 'package:kafka_client/kafka_group.dart';
import 'package:kafka_client/src/classic_group_session.dart';
import 'package:kafka_client/src/consumer_group_metadata.dart';
import 'package:kafka_client/src/group_delivery.dart';
import 'package:kafka_client/src/group_offsets_protocol.dart';
import 'package:kafka_client/src/group_processing.dart';
import 'package:test/test.dart';

import 'classic_group_session_test.dart' as fixture;
import 'group_delivery_controller_test.dart' as delivery_fixture;

class GuardExpiryClock extends fixture.Clock {
  Duration _now = Duration.zero;
  int? readsUntilExpiry;
  @override
  Duration get now {
    if (readsUntilExpiry != null) {
      readsUntilExpiry = readsUntilExpiry! - 1;
      if (readsUntilExpiry == 0) {
        _now += fixture.lease;
        readsUntilExpiry = null;
      }
    }
    return _now;
  }

  @override
  set now(Duration value) {
    _now = value;
  }
}

GroupOffsetCommitResponse committed(String topic, Map<int, int> offsets,
        {Map<int, int> codes = const {}, int throttle = 0}) =>
    GroupOffsetCommitResponse(1, throttle, [
      GroupOffsetCommitTopicResponse(topic, [
        for (final p in offsets.keys)
          GroupOffsetCommitPartitionResponse(p, codes[p] ?? 0)
      ])
    ]);

void main() {
  late fixture.Clock clock;
  late fixture.Driver driver;
  late ClassicGroupSessionController session;
  late KafkaGroupAssignment token;
  late GroupProcessingController progress;
  late GroupDeliveryController delivery;
  late List<Map<int, int>> commits;
  late List<String> commitTopics;
  late List<KafkaCancellationToken> cancellations;
  late List<KafkaGroupRecord> seen;
  late List<Object> errors;
  StreamSubscription<KafkaGroupRecord>? listener;
  var initialized = false;

  Future<void> make({
    Map<String, List<int>> partitions = const {
      'a': [0]
    },
    int cap = 10,
    Duration timeout = const Duration(milliseconds: 30),
    Future<GroupOffsetCommitResponse> Function(
            String, Map<int, int>, KafkaCancellationToken)?
        commit,
    Future<FetchResponse> Function(String, int, int)? fetch,
    Future<void> Function()? closeData,
  }) async {
    driver.onSync = () async => ConsumerGroupAssignment(partitions);
    token = await session.start();
    progress = GroupProcessingController(
        session: session,
        assignment: token,
        maxPendingRecords: cap,
        processingTimeout: timeout,
        commit: (t, offsets, cancellation) {
          commits.add(offsets);
          commitTopics.add(t);
          cancellations.add(cancellation);
          return commit == null
              ? Future.value(committed(t, offsets))
              : commit(t, offsets, cancellation);
        });
    delivery = GroupDeliveryController(
        session: session,
        assignment: token,
        startOffsets: {
          for (final t in partitions.entries)
            if (t.value.isNotEmpty) t.key: {for (final p in t.value) p: 0}
        },
        fetch: fetch ?? (t, p, o) async => delivery_fixture.reply(t, p, []),
        closeData: closeData ?? () async {},
        pollInterval: const Duration(milliseconds: 1),
        maxBufferedRecords: 100,
        maxBufferedPayloadBytes: 1000,
        waitForProcessingCapacity: progress.waitForCapacity,
        admitProcessing: progress.admit,
        stopProcessing: progress.stop);
    progress.bind(delivery);
    initialized = true;
  }

  KafkaGroupRecord admit(int offset, {String topic = 'a', int partition = 0}) {
    final record = KafkaGroupRecord(token, topic, partition,
        KafkaRecord(offset: offset, value: [offset % 256]));
    progress.admit(record);
    return record;
  }

  void listen([void Function(KafkaGroupRecord)? callback]) {
    listener = progress.processing.records.listen((r) {
      seen.add(r);
      callback?.call(r);
    }, onError: errors.add);
  }

  setUp(() {
    clock = fixture.Clock();
    driver = fixture.Driver();
    session = fixture.controller(driver, clock);
    commits = [];
    commitTopics = [];
    cancellations = [];
    seen = [];
    errors = [];
    listener = null;
    initialized = false;
  });
  tearDown(() async {
    if (initialized) await progress.cancel();
    await listener?.cancel();
    await session.close();
  });

  test('delivery and explicit positions never acknowledge or commit', () async {
    await make();
    admit(8);
    admit(11);
    expect(progress.acknowledgedNextOffsets, isEmpty);
    expect(progress.confirmedNextOffsets, isEmpty);
    expect(progress.pendingRecordCount, 2);
    final result = await progress.commitAcknowledged('a');
    expect(result.partitions, isEmpty);
    expect(result.locallyCurrent, isTrue);
    expect(commits, isEmpty);
  });

  for (final order in [
    [0, 1, 2],
    [0, 2, 1],
    [1, 0, 2],
    [1, 2, 0],
    [2, 0, 1],
    [2, 1, 0]
  ]) {
    test('sparse offsets follow admitted FIFO, acknowledgement order $order',
        () async {
      await make();
      final records = [admit(10), admit(12), admit(15)];
      final acked = <int>{};
      for (final i in order) {
        expect(progress.acknowledge(records[i]), isTrue);
        acked.add(i);
        var prefix = 0;
        while (acked.contains(prefix)) {
          prefix++;
        }
        expect(progress.pendingRecordCount, 3 - prefix);
        expect(
            progress.acknowledgedNextOffsets,
            prefix == 0
                ? {}
                : {
                    'a': {
                      0: [11, 13, 16][prefix - 1]
                    }
                  });
      }
      final result = await progress.commitAcknowledged('a');
      expect(commits, [
        {0: 16}
      ]);
      expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.confirmed);
      expect(progress.confirmedNextOffsets, {
        'a': {0: 16}
      });
    });
  }

  test('forged matching object and foreign token never acknowledge', () async {
    await make();
    final record = admit(10);
    final forged = KafkaGroupRecord(token, 'a', 0, record.record);
    expect(() => progress.acknowledge(forged), throwsArgumentError);
    final otherDriver = fixture.Driver();
    final other = fixture.controller(otherDriver, fixture.Clock());
    final otherToken = await other.start();
    expect(
        () => progress
            .acknowledge(KafkaGroupRecord(otherToken, 'a', 0, record.record)),
        throwsArgumentError);
    await other.close();
    expect(progress.pendingRecordCount, 1);
    expect(progress.acknowledgedNextOffsets, isEmpty);
  });

  test('duplicates are idempotent behind a gap and after retirement', () async {
    await make();
    final first = admit(0), second = admit(1);
    expect(progress.acknowledge(second), isTrue);
    expect(progress.acknowledge(second), isFalse);
    expect(progress.pendingRecordCount, 2);
    expect(progress.acknowledge(first), isTrue);
    expect(progress.acknowledge(first), isFalse);
    expect(progress.acknowledge(second), isFalse);
    expect(progress.pendingRecordCount, 0);
  });

  test('candidate and confirmation snapshots are deep immutable copies',
      () async {
    await make();
    progress.acknowledge(admit(2));
    final snapshot = progress.acknowledgedNextOffsets;
    expect(() => snapshot['b'] = {}, throwsUnsupportedError);
    expect(() => snapshot['a']![0] = 9, throwsUnsupportedError);
    final result = await progress.commitAcknowledged('a');
    expect(() => result.partitions.clear(), throwsUnsupportedError);
    progress.acknowledge(admit(5));
    expect(snapshot, {
      'a': {0: 3}
    });
    expect(progress.acknowledgedNextOffsets, {
      'a': {0: 6}
    });
    expect(progress.confirmedNextOffsets, {
      'a': {0: 3}
    });
  });

  test('all blocked ledger entries count against capacity and wake on prefix',
      () async {
    await make(cap: 3);
    final first = admit(0), second = admit(1), third = admit(2);
    progress.acknowledge(second);
    progress.acknowledge(third);
    expect(progress.pendingRecordCount, 3);
    var released = false;
    final wait = progress.waitForCapacity().then((_) => released = true);
    await fixture.flush();
    expect(released, isFalse);
    progress.acknowledge(first);
    await wait;
    expect(progress.pendingRecordCount, 0);
    expect(released, isTrue);
  });

  test(
      'real synchronous stream registers receipt before callback and caps pump',
      () async {
    var fetched = 0;
    await make(
        cap: 2,
        fetch: (t, p, o) async {
          fetched++;
          return delivery_fixture.reply(t, p, [o, o + 1, o + 2, o + 3]);
        });
    listen((r) {
      if (r.offset == 1) progress.acknowledge(r);
    });
    await delivery_fixture.until(() => seen.length == 2);
    await fixture.flush();
    await fixture.flush();
    expect(seen.map((r) => r.offset), [0, 1]);
    expect(progress.pendingRecordCount, 2);
    expect(fetched, 1);
    progress.acknowledge(seen.first);
    await delivery_fixture.until(() => seen.length == 4);
    expect(progress.pendingRecordCount, 2);
    await progress.cancel();
    expect(errors, isEmpty);
  });

  test('acknowledgement inside original callback can release next admission',
      () async {
    await make(
        cap: 1,
        fetch: (t, p, o) async => delivery_fixture.reply(t, p, [o, o + 1]));
    listen((r) {
      expect(progress.pendingRecordCount, 1);
      expect(progress.acknowledge(r), isTrue);
      if (r.offset == 1) unawaited(progress.cancel());
    });
    await progress.stopped;
    expect(seen.map((r) => r.offset), [0, 1]);
    expect(progress.acknowledgedNextOffsets, {
      'a': {0: 2}
    });
    expect(commits, isEmpty);
    expect(errors, isEmpty);
  });

  test(
      'revoke clears full capacity before revoked, wakes waits, rejects stale ack',
      () async {
    await make(cap: 1);
    final record = admit(0);
    var woke = false;
    final wait = progress.waitForCapacity().then((_) => woke = true);
    final closing = session.close();
    expect(progress.pendingRecordCount, 0);
    expect(() => progress.acknowledge(record), throwsStateError);
    await session.revoked;
    await wait;
    expect(woke, isTrue);
    await expectLater(progress.commitAcknowledged('a'), throwsStateError);
    await progress.stopped;
    await closing;
    expect(commits, isEmpty);
  });

  for (final action in ['ack', 'commit', 'admit', 'capacity']) {
    test(
        'monotonic processing expiry is checked without watchdog before $action',
        () async {
      await make(timeout: const Duration(milliseconds: 20));
      final record = admit(0);
      clock.advance(const Duration(milliseconds: 20), fire: false);
      if (action == 'ack') {
        expect(() => progress.acknowledge(record), throwsStateError);
      }
      if (action == 'admit') expect(() => admit(1), throwsStateError);
      if (action == 'commit') {
        await expectLater(progress.commitAcknowledged('a'), throwsStateError);
      }
      if (action == 'capacity') {
        await expectLater(progress.waitForCapacity(), throwsStateError);
      }
      final stop = await progress.stopped;
      expect(stop.reason, KafkaGroupDeliveryStopReason.failure);
      expect(stop.error, isA<TimeoutException>());
      expect(driver.calls.where((c) => c == 'leave'), isEmpty);
      expect(commits, isEmpty);
    });
  }

  test('watchdog ends paused stream even if membership heartbeat succeeds',
      () async {
    await make(
        timeout: const Duration(milliseconds: 25),
        fetch: (t, p, o) async => delivery_fixture.reply(t, p, [o, o + 1]));
    listen((_) => listener!.pause());
    await delivery_fixture.until(() => seen.length == 1);
    for (var i = 0; i < 2; i++) {
      clock.advance(const Duration(milliseconds: 10));
      await fixture.flush();
    }
    expect(session.isCurrent(token), isTrue);
    clock.advance(const Duration(milliseconds: 5));
    final stop = await progress.stopped;
    expect(stop.error, isA<TimeoutException>());
    expect(driver.calls.where((c) => c == 'heartbeat').length,
        greaterThanOrEqualTo(3));
    expect(listener!.isPaused, isTrue);
    listener!.resume();
    await fixture.flush();
    expect(seen, hasLength(1));
  });

  test(
      'later acknowledgements and other partition progress do not reset old head',
      () async {
    await make(partitions: {
      'a': [0, 1]
    }, timeout: const Duration(milliseconds: 20));
    admit(0);
    clock.advance(const Duration(milliseconds: 5), fire: false);
    final later = admit(1);
    progress.acknowledge(later);
    progress.acknowledge(admit(7, partition: 1));
    clock.advance(const Duration(milliseconds: 15));
    expect((await progress.stopped).error, isA<TimeoutException>());
  });

  test(
      'new head retains original admission deadline after earlier prefix retires',
      () async {
    await make(timeout: const Duration(milliseconds: 20));
    final first = admit(0);
    clock.advance(const Duration(milliseconds: 5), fire: false);
    admit(1);
    clock.advance(const Duration(milliseconds: 10), fire: false);
    progress.acknowledge(first);
    clock.advance(const Duration(milliseconds: 9), fire: false);
    expect(session.isCurrent(token), isTrue);
    clock.advance(const Duration(milliseconds: 1));
    expect((await progress.stopped).error, isA<TimeoutException>());
  });

  test('no outstanding work has no processing deadline', () async {
    await make(timeout: const Duration(milliseconds: 1));
    progress.acknowledge(admit(0));
    for (var i = 0; i < 5; i++) {
      clock.advance(const Duration(milliseconds: 10));
      await fixture.flush();
    }
    expect(session.isCurrent(token), isTrue);
    expect(progress.pendingRecordCount, 0);
  });

  test('early watchdog firing rearms instead of dropping the deadline',
      () async {
    await make(timeout: const Duration(milliseconds: 20));
    admit(0);
    final timers = clock.timers
        .where((t) => !t.cancelled && t.at == const Duration(milliseconds: 20));
    expect(timers, hasLength(1));
    timers.single.callback();
    expect(session.isCurrent(token), isTrue);
    clock.advance(const Duration(milliseconds: 20));
    expect((await progress.stopped).error, isA<TimeoutException>());
  });

  test('empty assignment issues neither Fetch nor commit', () async {
    var fetches = 0;
    await make(
        partitions: {},
        fetch: (t, p, o) async {
          fetches++;
          return delivery_fixture.reply(t, p, []);
        });
    listen();
    await fixture.flush();
    await fixture.flush();
    expect(fetches, 0);
    await expectLater(progress.commitAcknowledged('a'), throwsArgumentError);
    expect(commits, isEmpty);
  });

  test('one topic snapshot includes only advanced acknowledged partitions',
      () async {
    await make(partitions: {
      'a': [0, 1, 2],
      'b': [0]
    });
    progress.acknowledge(admit(2));
    admit(8, partition: 1);
    progress.acknowledge(admit(10, partition: 2));
    progress.acknowledge(admit(99, topic: 'b'));
    final result = await progress.commitAcknowledged('a');
    expect(commitTopics, ['a']);
    expect(commits, [
      {0: 3, 2: 11}
    ]);
    expect(result.partitions.keys, unorderedEquals([0, 2]));
    expect(progress.confirmedNextOffsets, {
      'a': {0: 3, 2: 11}
    });
    final empty = await progress.commitAcknowledged('a');
    expect(empty.partitions, isEmpty);
    expect(commits, hasLength(1));
    await progress.commitAcknowledged('b');
    expect(commitTopics, ['a', 'b']);
  });

  test('commit captures immutable snapshot; new acknowledgements remain dirty',
      () async {
    final response = Completer<GroupOffsetCommitResponse>();
    await make(commit: (t, o, c) => response.future);
    progress.acknowledge(admit(2));
    final pending = progress.commitAcknowledged('a');
    expect(commits, [
      {0: 3}
    ]);
    expect(() => commits.single[0] = 100, throwsUnsupportedError);
    progress.acknowledge(admit(5));
    response.complete(committed('a', {0: 3}));
    await pending;
    expect(progress.acknowledgedNextOffsets, {
      'a': {0: 6}
    });
    expect(progress.confirmedNextOffsets, {
      'a': {0: 3}
    });
  });

  test(
      'overlap is rejected across topics and timeout wrappers cannot release gate',
      () async {
    final response = Completer<GroupOffsetCommitResponse>();
    await make(partitions: {
      'a': [0],
      'b': [0]
    }, commit: (t, o, c) => response.future);
    progress.acknowledge(admit(0));
    progress.acknowledge(admit(0, topic: 'b'));
    final pending = progress.commitAcknowledged('a');
    await expectLater(
        pending.timeout(Duration.zero), throwsA(isA<TimeoutException>()));
    await expectLater(progress.commitAcknowledged('b'), throwsStateError);
    expect(commits, hasLength(1));
    response.complete(committed('a', {0: 1}));
    await pending;
  });

  test(
      'mixed broker rejection preserves successes and terminally stops all work',
      () async {
    await make(partitions: {
      'a': [0, 1]
    }, commit: (t, o, c) async => committed(t, o, codes: {1: 22}));
    progress.acknowledge(admit(5));
    progress.acknowledge(admit(7, partition: 1));
    final result = await progress.commitAcknowledged('a');
    expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.confirmed);
    expect(result.partitions[1]!.outcome, KafkaGroupCommitOutcome.rejected);
    expect(result.partitions[1]!.errorCode, 22);
    expect(progress.confirmedNextOffsets, {
      'a': {0: 6}
    });
    expect(result.error, isA<KafkaBrokerException>());
    expect(result.locallyCurrent, isFalse);
    expect(
        (await progress.stopped).reason, KafkaGroupDeliveryStopReason.failure);
    expect(driver.calls.where((c) => c == 'leave'), isEmpty);
    await expectLater(progress.commitAcknowledged('a'), throwsStateError);
    expect(commits, hasLength(1));
  });

  for (final code in [-1, 14, 15, 16, 22, 25, 27, 29, 30]) {
    test('partition rejection $code never retries or falls back', () async {
      await make(commit: (t, o, c) async => committed(t, o, codes: {0: code}));
      progress.acknowledge(admit(1));
      final result = await progress.commitAcknowledged('a');
      expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.rejected);
      expect(result.partitions[0]!.errorCode, code);
      expect(progress.confirmedNextOffsets, isEmpty);
      await progress.stopped;
      expect(commits, hasLength(1));
    });
  }

  for (final error in [
    TimeoutException('lost'),
    KafkaConnectionException(),
    const FormatException('bad frame'),
    StateError('closed')
  ]) {
    test('admitted $error is unknown and never replayed', () async {
      await make(commit: (t, o, c) async => throw error);
      progress.acknowledge(admit(1));
      final result = await progress.commitAcknowledged('a');
      expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.unknown);
      expect(result.partitions[0]!.errorCode, isNull);
      expect(result.error, same(error));
      expect(result.locallyCurrent, isFalse);
      await progress.stopped;
      expect(commits, hasLength(1));
      expect(driver.calls.where((c) => c == 'leave'), isEmpty);
    });
  }

  final malformed = <String, GroupOffsetCommitResponse>{
    'missing topic': GroupOffsetCommitResponse(0, 0, []),
    'wrong topic': committed('b', {0: 1}),
    'missing partition': committed('a', {}),
    'extra partition': committed('a', {0: 1, 1: 1}),
    'wrong partition': committed('a', {1: 1}),
    'duplicate partition': GroupOffsetCommitResponse(0, 0, [
      GroupOffsetCommitTopicResponse('a', [
        GroupOffsetCommitPartitionResponse(0, 0),
        GroupOffsetCommitPartitionResponse(0, 0)
      ])
    ]),
    'negative throttle': committed('a', {0: 1}, throttle: -1),
  };
  for (final case_ in malformed.entries) {
    test('${case_.key} cannot become partial/confirmed evidence', () async {
      await make(commit: (t, o, c) async => case_.value);
      progress.acknowledge(admit(0));
      final result = await progress.commitAcknowledged('a');
      expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.unknown);
      expect(progress.confirmedNextOffsets, isEmpty);
      expect(result.error, isA<FormatException>());
      await progress.stopped;
    });
  }

  test(
      'positive throttle preserves confirmations but closes unsupported continuation',
      () async {
    await make(commit: (t, o, c) async => committed(t, o, throttle: 10));
    progress.acknowledge(admit(0));
    final result = await progress.commitAcknowledged('a');
    expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.confirmed);
    expect(result.throttleTimeMs, 10);
    expect(result.error, isA<UnsupportedError>());
    expect(result.locallyCurrent, isFalse);
    await progress.stopped;
  });

  for (final success in [true, false]) {
    test(
        'revocation cancels synchronously; stopped drains late ${success ? 'success' : 'failure'}',
        () async {
      final response = Completer<GroupOffsetCommitResponse>();
      await make(commit: (t, o, c) => response.future);
      final record = admit(0);
      progress.acknowledge(record);
      final pending = progress.commitAcknowledged('a');
      var finished = false;
      progress.stopped.then((_) => finished = true);
      final closing = session.close();
      expect(cancellations.single.isCancelled, isTrue);
      expect(() => progress.acknowledge(record), throwsStateError);
      await fixture.flush();
      expect(finished, isFalse);
      expect(driver.calls.where((c) => c == 'leave'), isEmpty);
      if (success) {
        response.complete(committed('a', {0: 1}));
      } else {
        response.completeError(KafkaCancelledException());
      }
      final result = await pending;
      expect(result.locallyCurrent, isFalse);
      expect(
          result.partitions[0]!.outcome,
          success
              ? KafkaGroupCommitOutcome.confirmed
              : KafkaGroupCommitOutcome.unknown);
      expect(
          progress.confirmedNextOffsets,
          success
              ? {
                  'a': {0: 1}
                }
              : {});
      await progress.stopped;
      await closing;
      expect(finished, isTrue);
      expect(commits, hasLength(1));
    });
  }

  test('cancel during pending commit also drains owned data closure', () async {
    final response = Completer<GroupOffsetCommitResponse>(),
        data = Completer<void>();
    await make(
        commit: (t, o, c) => response.future, closeData: () => data.future);
    progress.acknowledge(admit(0));
    final pending = progress.commitAcknowledged('a');
    var finished = false;
    final stopping = progress.cancel().then((_) => finished = true);
    response.completeError(KafkaCancelledException());
    await pending;
    await fixture.flush();
    expect(finished, isFalse);
    data.complete();
    await stopping;
    expect(finished, isTrue);
  });

  test('data cleanup error remains visible after processing shutdown',
      () async {
    final error = StateError('close failed');
    await make(closeData: () async => throw error);
    expect((await progress.cancel()).cleanupError, same(error));
  });

  for (final dataFails in [false, true]) {
    test(
        'throwing cancellation hook still revokes and closes data: dataFails=$dataFails',
        () async {
      final response = Completer<GroupOffsetCommitResponse>();
      final cancelError = StateError('cancel callback failed');
      final dataError = StateError('data close failed');
      var dataClosed = false;
      await make(commit: (t, o, cancellation) {
        cancellation.onCancel(() => throw cancelError);
        return response.future;
      }, closeData: () async {
        dataClosed = true;
        if (dataFails) throw dataError;
      });
      progress.acknowledge(admit(0));
      final pending = progress.commitAcknowledged('a');
      final stopping = progress.cancel();
      expect(dataClosed, isTrue);
      expect(session.isCurrent(token), isFalse);
      await session.revoked;
      response.completeError(KafkaCancelledException());
      final result = await pending;
      expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.unknown);
      final termination = await stopping;
      final error =
          termination.cleanupError as KafkaGroupProcessingCleanupException;
      expect(error.commitCancellationError, same(cancelError));
      expect(error.dataCleanupError, dataFails ? same(dataError) : isNull);
      expect((await session.closed).cleanupError, isNull);
    });
  }

  test(
      'successful commit cannot hide processing expiry when watchdog is delayed',
      () async {
    final response = Completer<GroupOffsetCommitResponse>();
    await make(
        timeout: const Duration(milliseconds: 20),
        commit: (t, o, c) => response.future);
    progress.acknowledge(admit(0));
    admit(1);
    final pending = progress.commitAcknowledged('a');
    clock.advance(const Duration(milliseconds: 20), fire: false);
    response.complete(committed('a', {0: 1}));
    final result = await pending;
    expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.confirmed);
    expect(result.locallyCurrent, isFalse);
    expect(progress.confirmedNextOffsets, {
      'a': {0: 1}
    });
    expect((await progress.stopped).error, isA<TimeoutException>());
    expect(driver.calls.where((c) => c == 'leave'), isEmpty);
  });

  test('compacted/control-only progress never creates a processing candidate',
      () async {
    var polls = 0;
    await make(fetch: (t, p, o) async {
      polls++;
      return delivery_fixture.reply(t, p, [], end: polls == 1 ? 15 : null);
    });
    listen();
    await delivery_fixture.until(() => polls >= 2);
    expect(seen, isEmpty);
    expect(progress.acknowledgedNextOffsets, isEmpty);
    expect((await progress.commitAcknowledged('a')).partitions, isEmpty);
    expect(commits, isEmpty);
  });

  test(
      'tail batch offset cannot advance acknowledgement past last delivered record',
      () async {
    var polls = 0;
    await make(fetch: (t, p, o) async {
      polls++;
      return delivery_fixture.reply(t, p, polls == 1 ? [2] : [],
          end: polls == 1 ? 15 : null);
    });
    listen((r) => progress.acknowledge(r));
    await delivery_fixture.until(() => polls >= 2);
    expect(progress.acknowledgedNextOffsets, {
      'a': {0: 3}
    });
    expect(
        (await progress.commitAcknowledged('a')).partitions[0]!.nextOffset, 3);
  });

  test(
      'expiry at final membership guard reports notAttempted and no driver call',
      () async {
    await session.close();
    final edgeClock = GuardExpiryClock();
    clock = edgeClock;
    driver = fixture.Driver();
    session = fixture.controller(driver, clock);
    await make();
    progress.acknowledge(admit(0));
    // First read is the public processing check; second is final session gate.
    edgeClock.readsUntilExpiry = 2;
    final result = await progress.commitAcknowledged('a');
    expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.notAttempted);
    expect(result.locallyCurrent, isFalse);
    expect(commits, isEmpty);
    await progress.stopped;
  });

  for (final cap in [0, -1, 2147483648]) {
    test('invalid pending cap $cap rejected before attachment', () async {
      await expectLater(make(cap: cap), throwsArgumentError);
    });
  }
  for (final timeout in [
    Duration.zero,
    const Duration(microseconds: -1),
    const Duration(hours: 2)
  ]) {
    test('invalid processing timeout $timeout rejected before attachment',
        () async {
      await expectLater(make(timeout: timeout), throwsArgumentError);
    });
  }
}
