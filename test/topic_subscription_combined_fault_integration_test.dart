import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';

// Reuse only the existing private fixture's bounded read/control helpers. Its
// main is not invoked. Run this file through the unchanged combined runner with
// an explicit, already-resolved test command. It owns three SAME-HOST brokers,
// six opaque proxies (three per security mode), guards and private cleanup.
import 'combined_secure_fault_integration_test.dart' as fixture
    show control, metadataReady, proxy, proxies, readUntil, resources;

const _deadline = Duration(seconds: 30);
const _cleanupDeadline = Duration(seconds: 15);

Future<void> _until(bool Function() ready,
    {required String phase,
    required void Function() check,
    Duration timeout = _deadline}) async {
  final clock = Stopwatch()..start();
  while (true) {
    check();
    if (ready()) return;
    if (clock.elapsed >= timeout) throw TimeoutException(phase, timeout);
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

void _bounded(KafkaClient client) {
  expect(client.connectionCount, lessThanOrEqualTo(3));
  expect(client.admittedRequestCount, lessThanOrEqualTo(8));
  expect(client.queuedRequestBytes, lessThanOrEqualTo(1048576));
  expect(client.bufferedResponseBytes, lessThanOrEqualTo(2097152));
}

Future<void> _idle(KafkaClient client) => _until(
    () =>
        client.pendingRequestCount == 0 &&
        client.admittedRequestCount == 0 &&
        client.queuedRequestBytes == 0 &&
        client.bufferedResponseBytes == 0,
    phase: 'Subscription requests did not settle',
    timeout: _cleanupDeadline,
    check: () => _bounded(client));

// These are APPLICATION-owned, in-memory checkpoints, advanced only after the
// synchronous verifier succeeds. They are not broker commits or durable state.
// Each independent consumer verifies its own once-per-partition sequence. This
// finite fixture does not assert exactly-once processing across real failures.
class _Ledger {
  final String topic;
  final Map<int, Map<int, KafkaRecord>> acknowledged;
  final Map<int, int> next;
  final seen = <int, List<int>>{0: [], 1: [], 2: []};
  int count = 0;

  _Ledger(this.topic, this.acknowledged, Map<int, int> start)
      : next = Map<int, int>.of(start);

  void accept(KafkaTopicRecord event) {
    expect(event.topic, topic);
    expect(next.containsKey(event.partition), isTrue);
    expect(event.offset, next[event.partition],
        reason: 'No duplicate, skipped or reordered application offset');
    final expected = acknowledged[event.partition]![event.offset];
    expect(expected, isNotNull, reason: 'Only acknowledged markers may arrive');
    expect(event.record.key, expected!.key);
    expect(event.record.value, expected.value);
    expect(seen[event.partition], isNot(contains(event.offset)));
    seen[event.partition]!.add(event.offset);
    next[event.partition] = event.offset + 1;
    count++;
  }
}

// Both listener assertions and stream failures are captured immediately and
// checked by awaited test work. A terminal error must also close its stream.
class _Probe {
  final _Ledger ledger;
  final done = Completer<void>();
  late final StreamSubscription<KafkaTopicRecord> subscription;
  Object? error;
  StackTrace? _errorStack;
  Object? _callbackError;
  StackTrace? _callbackStack;
  Future<void>? _cancellation;
  bool _stopping = false;
  int errors = 0;
  int pauseAt;

  _Probe(Stream<KafkaTopicRecord> stream, this.ledger, this.pauseAt) {
    subscription = stream.listen((event) {
      if (_callbackError != null) return;
      try {
        ledger.accept(event);
        if (ledger.count == pauseAt) subscription.pause();
      } catch (error, stack) {
        _callbackError = error;
        _callbackStack = stack;
        subscription.pause();
      }
    }, onError: (Object failure, StackTrace stack) {
      errors++;
      error ??= failure;
      _errorStack ??= stack;
    }, onDone: () {
      if (!done.isCompleted) done.complete();
    });
  }

  void checkCallbacks() {
    if (_callbackError != null) {
      Error.throwWithStackTrace(_callbackError!, _callbackStack!);
    }
  }

  void checkHealthy() {
    checkCallbacks();
    if (error != null) Error.throwWithStackTrace(error!, _errorStack!);
    if (done.isCompleted && !_stopping) {
      throw StateError('Subscription ended before cancellation');
    }
  }

  Future<void> received(int count) => _until(() => ledger.count >= count,
      phase: 'Subscription did not deliver $count acknowledged records',
      check: checkHealthy);

  Future<void> cancel() {
    _stopping = true;
    return _cancellation ??= subscription.cancel().timeout(_cleanupDeadline);
  }
}

int _sum(Map<String, dynamic> state, String counter) => fixture
    .proxies(state)
    .fold<int>(0, (sum, proxy) => sum + (proxy[counter] as int));

void main() {
  final env = Platform.environment;
  final mode = env['KAFKA_COMBINED_MODE'];
  final rawPorts = env['KAFKA_COMBINED_PORTS'];
  final rawTopic = env['KAFKA_COMBINED_TOPIC'];
  final rawDirectory = env['KAFKA_COMBINED_CONTROL'];
  final ca = env['KAFKA_COMBINED_CA'];
  final configured = [mode, rawPorts, rawTopic, rawDirectory, ca]
          .every((value) => value != null && value.isNotEmpty) &&
      (mode != 'sasl' ||
          [env['KAFKA_COMBINED_USERNAME'], env['KAFKA_COMBINED_PASSWORD']]
              .every((value) => value != null && value.isNotEmpty));

  test('$mode: topic subscriptions across overlapping secure faults', () async {
    expect(['tls', 'sasl'], contains(mode));
    final ports = rawPorts!.split(',').map(int.parse).toList();
    expect(ports, hasLength(3));
    expect(ports.toSet(), hasLength(3));
    final topic = rawTopic!;
    final directory = rawDirectory!;
    final clients = <KafkaClient>[];
    final probes = <_Probe>[];
    final pendingEvidence = <Future<Object?>>[];
    int? needsRestore;
    final recovery = KafkaTlsFetchRecovery(
        maxAttempts: 20,
        window: const Duration(seconds: 10),
        retryDelay: const Duration(milliseconds: 500));

    KafkaClient make({bool optIn = false, int connections = 3}) {
      final client = KafkaClient(
          brokers: ports.map((port) => Broker('localhost', port)).toList(),
          tls: KafkaTlsConfig(
              trustedCertificates: File(ca!).readAsBytesSync(),
              withTrustedRoots: false),
          sasl: mode == 'sasl'
              ? KafkaSaslPlain(
                  username: env['KAFKA_COMBINED_USERNAME']!,
                  password: env['KAFKA_COMBINED_PASSWORD']!)
              : null,
          requestTimeout: const Duration(milliseconds: 2500),
          connectionTimeout: const Duration(milliseconds: 700),
          maxReadRetries: 0,
          allowTlsBootstrapFallback: optIn,
          tlsFetchRecovery: optIn ? recovery : null,
          maxConnections: connections,
          maxInFlightRequests: 8,
          maxFrameBytes: 1048576,
          maxPendingRequestBytes: 1048576,
          maxBufferedResponseBytes: 2097152);
      clients.add(client);
      return client;
    }

    _Probe listen(KafkaClient client, _Ledger ledger, int pauseAt) {
      final probe = _Probe(
          client.subscribe(topic,
              startOffsets: Map<int, int>.of(ledger.next),
              pollInterval: const Duration(milliseconds: 20),
              retryDelay: const Duration(milliseconds: 250),
              maxWaitMs: 1200,
              maxBytes: 65536),
          ledger,
          pauseAt);
      probes.add(probe);
      return probe;
    }

    try {
      await fixture.control(directory, 'pass');
      // The proof/producer client has two slots, matching shared read helpers.
      // Each consumer has three so every healthy peer stays authenticated before
      // the blackhole. The independent raw long-poll is warmed separately.
      final producer = make(connections: 2);
      final initial = await fixture.readUntil(
          producer,
          () => producer.metadata({topic}),
          (m) => fixture.metadataReady(
              m, (parts) => parts.every((p) => p.isrNodes.toSet().length == 3)),
          'initial subscription ISR');
      expect(initial.brokers.map((b) => b.nodeId).toSet(), {1, 2, 3});
      for (final broker in initial.brokers) {
        expect(broker.host, 'localhost');
        expect(broker.port, ports[broker.nodeId - 1]);
      }
      final partitions = initial.topics.single.partitions;
      expect(partitions.map((p) => p.partitionIndex).toSet(), {0, 1, 2});
      expect(partitions.map((p) => p.leaderId).toSet(), {1, 2, 3});
      for (final partition in partitions) {
        expect(partition.errorCode, 0);
        expect(partition.replicaNodes.toSet(), {1, 2, 3});
        expect(partition.isrNodes.toSet(), {1, 2, 3});
      }
      final victim =
          partitions.singleWhere((p) => p.partitionIndex == 0).leaderId;
      // The fixture explicitly assigns partition zero to bootstrap node one.
      // The strict case observes a terminal HandshakeException. It does not
      // pinpoint discovery versus leader connection, or certificate rejection.
      expect(victim, 1);
      final survivingPartition =
          partitions.firstWhere((p) => p.leaderId != victim);
      final survivor = survivingPartition.leaderId;
      final survivorIndex = survivingPartition.partitionIndex;
      final survivorBroker = Broker('localhost', ports[survivor - 1]);
      final acknowledged = <int, Map<int, KafkaRecord>>{0: {}, 1: {}, 2: {}};
      final run = '$mode-$pid-${DateTime.now().microsecondsSinceEpoch}';
      var produceCalls = 0;
      var outage = false;

      Future<void> producePhase(String phase, int expectedOffset) async {
        expect(outage, isFalse,
            reason: 'Never Produce during the fault window');
        for (var partition = 0; partition < 3; partition++) {
          final record = KafkaRecord(
              key: utf8.encode('$run/$partition/$phase'),
              value: utf8.encode('acknowledged-$phase-$partition'));
          produceCalls++;
          // Exactly one call per marker, all-ISR acknowledgment. No write retry
          // or replay after an ambiguous outcome, including in cleanup.
          final response = (await producer.produce(topic, [record],
              partition: partition, acks: -1))!;
          expect(response.responses.single.name, topic);
          final result = response.responses.single.partitionResponses.single;
          expect(result.index, partition);
          expect(result.errorCode, 0);
          expect(result.baseOffset, expectedOffset);
          acknowledged[partition]![result.baseOffset] = record;
          _bounded(producer);
        }
      }

      await producePhase('before', 0);
      final strict = make();
      final opted = make(optIn: true);
      expect(strict.allowTlsBootstrapFallback, isFalse);
      expect(strict.tlsFetchRecovery, isNull);
      final strictLedger = _Ledger(topic, acknowledged, {0: 0, 1: 0, 2: 0});
      final optedLedger = _Ledger(topic, acknowledged, {0: 0, 1: 0, 2: 0});
      final strictProbe = listen(strict, strictLedger, 3);
      final optedProbe = listen(opted, optedLedger, 3);
      await Future.wait([strictProbe.received(3), optedProbe.received(3)]);
      await Future.wait([_idle(strict), _idle(opted)]);
      expect(strict.connectionCount, 3);
      expect(opted.connectionCount, 3);
      expect(strictLedger.next, {0: 1, 1: 1, 2: 1});
      expect(optedLedger.next, strictLedger.next);
      // Ack a backlog BEFORE the fault, while both listeners are paused after
      // processing one marker per partition. Nothing is produced in the outage.
      await producePhase('backlog', 1);
      await producer.fetch(topic, partition: survivorIndex, offset: 1);
      await _idle(producer);
      final ready = await fixture.control(directory, 'status');
      var longPollPending = true;
      final longPoll = producer
          .send(
              FetchRequest(
                  -1,
                  1200,
                  65536,
                  [
                    FetchTopic(topic, [FetchPartition(survivorIndex, 2, 65536)])
                  ],
                  maxBytes: 65536),
              broker: survivorBroker)
          .then<Object?>((_) {
        longPollPending = false;
        return null;
      }, onError: (Object error, StackTrace stack) {
        longPollPending = false;
        return error;
      });
      pendingEvidence.add(longPoll);
      final dispatchClock = Stopwatch()..start();
      while (fixture.proxy(await fixture.control(directory, 'status'),
              survivor)['upstream_bytes'] ==
          fixture.proxy(ready, survivor)['upstream_bytes']) {
        expect(longPollPending, isTrue);
        if (dispatchClock.elapsed > const Duration(milliseconds: 500)) {
          fail('Surviving authenticated long-poll was not forwarded');
        }
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
      expect(longPollPending, isTrue);
      expect(producer.pendingRequestCount, 1);
      outage = true;
      final before = await fixture.control(directory, 'blackhole');
      final outageClock = Stopwatch()..start();
      // The strict listener must terminate itself, not pause after its backlog.
      strictProbe.pauseAt = 9;
      optedProbe.pauseAt = 6;
      strictProbe.subscription.resume();
      optedProbe.subscription.resume();
      await _until(
          () =>
              strict.pendingRequestCount == 1 && opted.pendingRequestCount == 1,
          phase: 'Both subscriptions must admit reads under the blackhole',
          timeout: const Duration(milliseconds: 500), check: () {
        strictProbe.checkHealthy();
        optedProbe.checkHealthy();
        expect(longPollPending, isTrue);
      });
      needsRestore = victim; // Also covers uncertain control completion.
      final overlap = await fixture.control(directory, 'crash:$victim');
      expect(overlap['nodes']['$victim']['running'], isFalse);
      expect(overlap['nodes']['$survivor']['running'], isTrue);
      expect(fixture.proxies(overlap), hasLength(3));
      expect(fixture.proxies(overlap).every((p) => p['mode'] == 'blackhole'),
          isTrue);
      expect(longPollPending, isTrue);
      expect(producer.pendingRequestCount, 1);
      expect(strict.activeSubscriptionCount, 1);
      expect(opted.activeSubscriptionCount, 1);
      expect(strictLedger.count, 3);
      expect(optedLedger.count, 3);
      print('SUBSCRIPTION_COMBINED_OVERLAP ${jsonEncode({
            'mode': mode,
            'same_host_nodes': 3,
            'victim': victim,
            'survivor': survivor,
            'victim_running': false,
            'blackhole_proxies': 3,
            'survivor_long_poll_pending': longPollPending,
            'strict_subscriptions': strict.activeSubscriptionCount,
            'opt_in_subscriptions': opted.activeSubscriptionCount,
            'elapsed_ms': outageClock.elapsedMilliseconds,
          })}');
      // A second read on that SAME warmed surviving connection supplies the
      // upstream drop opportunity; the pending long-poll provides downstream
      // traffic. Aggregate counters prove dropped ciphertext, not the content
      // or exact TLS messages dropped. The proxy never decrypts Kafka frames.
      final droppedRead = producer
          .send(
              FetchRequest(
                  -1,
                  50,
                  1,
                  [
                    FetchTopic(topic, [FetchPartition(survivorIndex, 1, 65536)])
                  ],
                  maxBytes: 65536),
              broker: survivorBroker)
          .then<Object?>((_) => null,
              onError: (Object error, StackTrace stack) => error);
      pendingEvidence.add(droppedRead);
      final failures = await Future.wait([longPoll, droppedRead]);
      expect(failures.first, isA<TimeoutException>());
      expect(failures.last,
          anyOf(isA<TimeoutException>(), isA<KafkaConnectionException>()));
      final fault = await fixture.control(directory, 'status');
      final up = (fixture.proxy(fault, survivor)['dropped_upstream'] as int) -
          (fixture.proxy(before, survivor)['dropped_upstream'] as int);
      final down =
          (fixture.proxy(fault, survivor)['dropped_downstream'] as int) -
              (fixture.proxy(before, survivor)['dropped_downstream'] as int);
      expect(up, greaterThan(0));
      expect(down, greaterThan(0));
      expect(fault['nodes']['$victim']['running'], isFalse);
      expect(fixture.proxies(fault).every((p) => p['mode'] == 'blackhole'),
          isTrue);
      expect(outageClock.elapsed, lessThan(const Duration(seconds: 5)));
      expect(produceCalls, 6);
      final disconnected = await fixture.control(directory, 'disconnect');
      final disconnects =
          _sum(disconnected, 'disconnects') - _sum(fault, 'disconnects');
      expect(disconnects, greaterThan(0));
      final fragment = await fixture.control(directory, 'fragment');
      final recoveryClock = Stopwatch()..start();
      // No application metadata convergence wait or replacement stream can turn
      // the opt-in failure into a pass. This SAME stream must drain its backlog.
      // The bounded Fetch policy is per operation, not an overall stream SLA.
      await optedProbe.received(6);
      await _until(() => strictProbe.done.isCompleted,
          phase: 'Strict-default TLS failure must terminate its stream',
          check: () {
        strictProbe.checkCallbacks();
        optedProbe.checkHealthy();
      });
      expect(strictProbe.errors, 1);
      expect(strictProbe.error, isA<HandshakeException>());
      expect(strict.activeSubscriptionCount, 0);
      expect(strict.bootstrapTlsFallbackCount, 0);
      expect(optedProbe.errors, 0);
      expect(opted.activeSubscriptionCount, 1);
      expect(opted.bootstrapTlsFallbackCount, greaterThan(0));
      expect(optedLedger.next, {0: 2, 1: 2, 2: 2});
      final recovered = await fixture.control(directory, 'status');
      expect(recovered['nodes']['$victim']['running'], isFalse);
      expect(fixture.proxies(recovered).every((p) => p['mode'] == 'fragment'),
          isTrue);
      final fragments =
          _sum(recovered, 'fragments') - _sum(fragment, 'fragments');
      expect(fragments, greaterThan(0));
      expect(produceCalls, 6);
      print('SUBSCRIPTION_COMBINED_RECOVERY ${jsonEncode({
            'mode': mode,
            'strict_terminal_handshake_error':
                strictProbe.error is HandshakeException,
            'strict_error_count': strictProbe.errors,
            'strict_checkpoint': {
              for (final e in strictLedger.next.entries) '${e.key}': e.value
            },
            'opt_in_same_stream_records': optedLedger.count,
            'opt_in_bootstrap_tls_fallback_count':
                opted.bootstrapTlsFallbackCount,
            'fetch_max_attempts': recovery.maxAttempts,
            'fetch_window_ms': recovery.window.inMilliseconds,
            'dropped_upstream': up,
            'dropped_downstream': down,
            'disconnects': disconnects,
            'opaque_ciphertext_fragments': fragments,
            'recovery_ms': recoveryClock.elapsedMilliseconds,
          })}');
      await optedProbe.cancel();
      await strict.close();
      await opted.close();
      await Future.wait([_idle(strict), _idle(opted)]);
      expect(opted.activeSubscriptionCount, 0);

      // Restore the owned node for the next security mode, then prove all ISR
      // members returned before any further Produce. Cleanup also restores on
      // errors, and the outer runner independently removes its private fixture.
      final restored = await fixture.control(directory, 'start:$victim');
      expect(restored['nodes']['$victim']['running'], isTrue);
      expect(restored['nodes']['$victim']['generation'],
          greaterThan(overlap['nodes']['$victim']['generation'] as int));
      needsRestore = null;
      await fixture.control(directory, 'pass');
      await fixture.readUntil(
          producer,
          () => producer.metadata({topic}),
          (m) => fixture.metadataReady(
              m,
              (parts) => parts.every((p) =>
                  p.errorCode == 0 &&
                  p.isrNodes.toSet().containsAll({1, 2, 3}))),
          'restored subscription ISR');
      outage = false;
      await producePhase('restart', 2);
      expect(produceCalls, 9);
      final strictRestart = make();
      final optedRestart = make(optIn: true);
      // Fresh clients/streams receive ONLY the saved application next-offset
      // maps. Their prior seen sets are retained solely as duplicate detectors.
      final strictResumed = listen(strictRestart, strictLedger, 9);
      final optedResumed = listen(optedRestart, optedLedger, 9);
      await Future.wait([strictResumed.received(9), optedResumed.received(9)]);
      for (final ledger in [strictLedger, optedLedger]) {
        expect(ledger.count, 9);
        expect(ledger.next, {0: 3, 1: 3, 2: 3});
        for (var partition = 0; partition < 3; partition++) {
          expect(ledger.seen[partition], [0, 1, 2]);
          expect(acknowledged[partition]!.keys, [0, 1, 2]);
        }
      }
      await Future.wait([strictResumed.cancel(), optedResumed.cancel()]);
      for (final client in clients) {
        await client.close().timeout(_cleanupDeadline);
        await _idle(client);
        expect(client.activeSubscriptionCount, 0);
        expect(client.connectionCount, 0);
      }
      var closed = await fixture.control(directory, 'status');
      final closeClock = Stopwatch()..start();
      while (fixture
          .proxies(closed)
          .any((p) => p['active'] != 0 || p['workers'] != 0)) {
        if (closeClock.elapsed >= _cleanupDeadline) {
          fail('Combined subscription proxy connections did not close');
        }
        await Future<void>.delayed(const Duration(milliseconds: 20));
        closed = await fixture.control(directory, 'status');
      }
      for (final proxy in fixture.proxies(closed)) {
        expect(proxy['rejected'], 0);
        expect(proxy['peak_connections'], lessThanOrEqualTo(16));
        expect(proxy['peak_workers'], lessThanOrEqualTo(32));
      }
      print('SUBSCRIPTION_COMBINED_RESULT ${jsonEncode({
            'mode': mode,
            'overlap_proven': true,
            'acknowledged_markers': 9,
            'verified_per_independent_consumer': 9,
            'produce_calls': produceCalls,
            'application_checkpoint_restart': true,
            'strict_terminal_expected': true,
            'opt_in_continued_same_stream': true,
            'restored_node': victim,
            'next_offsets': {'0': 3, '1': 3, '2': 3},
            'clients': clients.map(fixture.resources).toList(),
            'proxy_active':
                fixture.proxies(closed).map((p) => p['active']).toList(),
            'proxy_workers':
                fixture.proxies(closed).map((p) => p['workers']).toList(),
          })}');
    } finally {
      // Attempt every cleanup even if a previous close/assertion failed. Closing
      // clients first interrupts admitted I/O; cancelling also drains paused
      // stream controllers. Captured raw requests are never left unobserved.
      Object? cleanupError;
      StackTrace? cleanupStack;
      Future<void> attempt(Future<void> Function() action) async {
        try {
          await action();
        } catch (error, stack) {
          cleanupError ??= error;
          cleanupStack ??= stack;
        }
      }

      for (final client in clients) {
        await attempt(() => client.close().timeout(_cleanupDeadline));
      }
      for (final probe in probes) {
        await attempt(probe.cancel);
      }
      for (final evidence in pendingEvidence) {
        await attempt(() async {
          await evidence.timeout(_cleanupDeadline);
        });
      }
      await attempt(() async {
        await fixture.control(directory, 'pass');
      });
      if (needsRestore != null) {
        await attempt(() async {
          final state = await fixture.control(directory, 'status');
          if (state['nodes']['$needsRestore']['running'] != true) {
            await fixture.control(directory, 'start:$needsRestore');
          }
        });
      }
      for (final client in clients) {
        await attempt(() async {
          await _idle(client);
          expect(client.activeSubscriptionCount, 0);
          expect(client.connectionCount, 0);
        });
      }
      if (cleanupError != null) {
        Error.throwWithStackTrace(cleanupError!, cleanupStack!);
      }
    }
  },
      skip: configured
          ? false
          : 'Run the combined secure fixture with an explicit cached test command',
      timeout: const Timeout(Duration(minutes: 3)));
}
