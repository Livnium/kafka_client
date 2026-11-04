import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';

// Private, atomic-file control never transports credentials or Kafka payloads.
Future<Map<String, dynamic>> control(String directory, String action) async {
  final id = '$pid-${DateTime.now().microsecondsSinceEpoch}';
  final temporary = File('$directory/$id.temporary');
  final response = File('$directory/$id.response');
  await temporary.writeAsString(jsonEncode({'action': action}), flush: true);
  await temporary.rename('$directory/$id.request');
  final clock = Stopwatch()..start();
  final limit = Duration(seconds: action.startsWith('start:') ? 85 : 5);
  while (!await response.exists()) {
    if (clock.elapsed > limit) {
      throw TimeoutException('Combined fixture control timed out: $action');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  final result =
      jsonDecode(await response.readAsString()) as Map<String, dynamic>;
  await response.delete();
  // Do not print raw fixture/broker errors, which may contain private paths.
  expect(result['ok'], isTrue, reason: 'Combined fixture command $action');
  expect(result['action'], action);
  return result;
}

List<Map<String, dynamic>> proxies(Map<String, dynamic> state) =>
    (state['proxies'] as List).cast<Map<String, dynamic>>();

Map<String, dynamic> proxy(Map<String, dynamic> state, int node) =>
    proxies(state).singleWhere((p) => p['node'] == node);

Map<String, dynamic> resources(KafkaClient client) => {
      'connections': client.connectionCount,
      'pending': client.pendingRequestCount,
      'admitted': client.admittedRequestCount,
      'queued_bytes': client.queuedRequestBytes,
      'buffered_bytes': client.bufferedResponseBytes,
    };

void bounded(KafkaClient client) =>
    expect(client.connectionCount, lessThanOrEqualTo(2));

Future<void> settled(KafkaClient client) async {
  final clock = Stopwatch()..start();
  while (client.admittedRequestCount != 0 &&
      clock.elapsed < const Duration(seconds: 3)) {
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  bounded(client);
  expect(client.pendingRequestCount, 0);
  expect(client.admittedRequestCount, 0);
  expect(client.queuedRequestBytes, 0);
  expect(client.bufferedResponseBytes, 0);
}

bool readTransient(Object error) =>
    error is TimeoutException ||
    error is KafkaConnectionException ||
    error is SocketException ||
    (error is KafkaBrokerException && [5, 6, 9].contains(error.errorCode));

// Only read-safe transport/election failures may be retried here. Authentication,
// TLS handshake, malformed response and resource-limit errors fail immediately.
Future<T> readUntil<T>(KafkaClient client, Future<T> Function() read,
    bool Function(T) ready, String phase,
    {Duration limit = const Duration(seconds: 30)}) async {
  final clock = Stopwatch()..start();
  while (clock.elapsed < limit) {
    try {
      final result = await read();
      bounded(client);
      if (ready(result)) return result;
    } catch (error) {
      if (!readTransient(error)) rethrow;
      bounded(client);
    }
    await Future<void>.delayed(const Duration(milliseconds: 150));
  }
  throw TimeoutException('Combined secured read convergence timed out: $phase');
}

bool metadataReady(
    MetadataResponse metadata, bool Function(List<PartitionMetadata>) ready) {
  final topic = metadata.topics.single;
  if (topic.errorCode != 0) {
    if ([5, 6, 9].contains(topic.errorCode)) return false;
    throw KafkaBrokerException(topic.errorCode, 'metadata');
  }
  for (final partition in topic.partitions) {
    if (partition.errorCode != 0 && ![5, 6, 9].contains(partition.errorCode)) {
      throw KafkaBrokerException(partition.errorCode, 'metadata');
    }
    if (partition.leaderId < 0 || ![0, 9].contains(partition.errorCode)) {
      return false;
    }
  }
  return topic.partitions.length == 3 && ready(topic.partitions);
}

void main() => runCombinedSecureFaultTest();

void runCombinedSecureFaultTest({
  bool allowTlsBootstrapFallback = false,
  bool awaitMetadataElection = false,
  KafkaTlsFetchRecovery? tlsFetchRecovery,
}) {
  final env = Platform.environment;
  final mode = env['KAFKA_COMBINED_MODE'];
  final rawPorts = env['KAFKA_COMBINED_PORTS'];
  final topic = env['KAFKA_COMBINED_TOPIC'];
  final directory = env['KAFKA_COMBINED_CONTROL'];
  test(
      '$mode: secured leader crash overlaps bidirectional opaque blackhole '
      '(TLS bootstrap fallback=$allowTlsBootstrapFallback, '
      'await metadata election=$awaitMetadataElection, '
      'TLS Fetch recovery=${tlsFetchRecovery != null})', () async {
    expect(['tls', 'sasl'], contains(mode));
    final ports = rawPorts!.split(',').map(int.parse).toList();
    expect(ports, hasLength(3));
    expect(ports.toSet(), hasLength(3));
    final client = KafkaClient(
      // Bootstrap AND advertised leader connections traverse opaque proxies.
      brokers: ports.map((port) => Broker('localhost', port)).toList(),
      tls: KafkaTlsConfig(
        trustedCertificates: File(env['KAFKA_COMBINED_CA']!).readAsBytesSync(),
        withTrustedRoots: false,
      ),
      sasl: mode == 'sasl'
          ? KafkaSaslPlain(
              username: env['KAFKA_COMBINED_USERNAME']!,
              password: env['KAFKA_COMBINED_PASSWORD']!,
            )
          : null,
      requestTimeout: const Duration(milliseconds: 2500),
      connectionTimeout: const Duration(milliseconds: 700),
      maxReadRetries: 0,
      allowTlsBootstrapFallback: allowTlsBootstrapFallback,
      tlsFetchRecovery: tlsFetchRecovery,
      maxConnections: 2,
      maxInFlightRequests: 8,
      maxFrameBytes: 1048576,
      maxBufferedResponseBytes: 2097152,
    );
    addTearDown(client.close);
    await control(directory!, 'pass');
    final initial = await readUntil(
        client,
        () => client.metadata({topic!}),
        (m) => metadataReady(
            m, (parts) => parts.every((p) => p.isrNodes.toSet().length == 3)),
        'initial ISR');
    expect(initial.brokers, hasLength(3));
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
    final offsets = <int, int>{};
    final run = '$mode-$pid-${DateTime.now().microsecondsSinceEpoch}';
    String marker(String phase, int partition) => '$run-$phase-$partition';
    for (var partition = 0; partition < 3; partition++) {
      // One call per marker, all-ISR ack; ambiguous writes fail, never retry.
      final result = (await client.produce(topic!,
          [KafkaRecord(value: utf8.encode(marker('before', partition)))],
          partition: partition, acks: -1))!;
      offsets[partition] =
          result.responses.single.partitionResponses.single.baseOffset;
      expect(offsets[partition], 0,
          reason: 'Fresh mode-specific fixture topic');
      bounded(client);
    }
    final victim =
        partitions.singleWhere((p) => p.partitionIndex == 0).leaderId;
    final survivorPartition =
        partitions.firstWhere((p) => p.leaderId != victim);
    final survivor = survivorPartition.leaderId;
    expect(survivor, isNot(victim));
    final survivorIndex = survivorPartition.partitionIndex;
    final survivorBroker = Broker('localhost', ports[survivor - 1]);
    // Warm THIS survivor's authenticated/version-checked connection, then leave
    // it as the most recently used connection before issuing the raw long-poll.
    final warm = await client.fetch(topic!,
        partition: survivorIndex, offset: offsets[survivorIndex]!);
    expect(warm.topics.single.partitions.single.records.single.value,
        utf8.encode(marker('before', survivorIndex)));
    await settled(client);
    final ready = await control(directory, 'status');
    var longPollPending = true;
    final longPoll = client
        .send(
            FetchRequest(
                -1,
                1200,
                65536,
                [
                  FetchTopic(topic, [
                    FetchPartition(
                        survivorIndex, offsets[survivorIndex]! + 1, 65536)
                  ])
                ],
                maxBytes: 65536),
            broker: survivorBroker)
        .then<Object?>((_) {
      longPollPending = false;
      return null;
    }, onError: (Object error, StackTrace stack) {
      longPollPending = false;
      return error;
    }); // Attach error handling immediately, before any control awaits.
    final dispatchClock = Stopwatch()..start();
    while (
        proxy(await control(directory, 'status'), survivor)['upstream_bytes'] ==
            proxy(ready, survivor)['upstream_bytes']) {
      expect(longPollPending, isTrue);
      if (dispatchClock.elapsed > const Duration(milliseconds: 500)) {
        fail('Surviving leader long-poll was not forwarded before blackhole');
      }
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    expect(longPollPending, isTrue);
    expect(client.pendingRequestCount, 1);
    final before = await control(directory, 'blackhole');
    final outageClock = Stopwatch()..start();
    expect(longPollPending, isTrue);
    final overlap = await control(directory, 'crash:$victim');
    expect(overlap['nodes']['$victim']['running'], isFalse);
    expect(overlap['nodes']['$survivor']['running'], isTrue);
    expect(proxies(overlap), hasLength(3));
    expect(proxies(overlap).every((p) => p['mode'] == 'blackhole'), isTrue);
    expect(longPollPending, isTrue,
        reason: 'Victim crash must overlap the surviving leader pending read');
    expect(client.pendingRequestCount, 1);
    print('COMBINED_OVERLAP ${jsonEncode({
          'mode': mode,
          'victim': victim,
          'survivor': survivor,
          'allow_tls_bootstrap_fallback': allowTlsBootstrapFallback,
          'await_metadata_election': awaitMetadataElection,
          'tls_fetch_recovery': tlsFetchRecovery != null,
          'pending': longPollPending,
          'victim_running': false,
          'blackhole_proxies': 3,
          'elapsed_ms': outageClock.elapsedMilliseconds,
        })}');
    // Independent upstream evidence on that same surviving secured connection.
    // There are deliberately no Produce calls during the outage.
    final droppedRead = client
        .send(
            FetchRequest(
                -1,
                50,
                1,
                [
                  FetchTopic(topic, [
                    FetchPartition(
                        survivorIndex, offsets[survivorIndex]!, 65536)
                  ])
                ],
                maxBytes: 65536),
            broker: survivorBroker)
        .then<Object?>((_) => null,
            onError: (Object error, StackTrace stack) => error);
    final failures = await Future.wait([longPoll, droppedRead]);
    expect(failures.first, isA<TimeoutException>(),
        reason: 'A different crashed node cannot substitute EOF for blackhole');
    expect(failures.last,
        anyOf(isA<TimeoutException>(), isA<KafkaConnectionException>()));
    expect(outageClock.elapsed, lessThan(const Duration(seconds: 4)));
    await settled(client);
    final fault = await control(directory, 'status');
    final up = (proxy(fault, survivor)['dropped_upstream'] as int) -
        (proxy(before, survivor)['dropped_upstream'] as int);
    final down = (proxy(fault, survivor)['dropped_downstream'] as int) -
        (proxy(before, survivor)['dropped_downstream'] as int);
    expect(up, greaterThan(0));
    expect(down, greaterThan(0));
    expect(fault['nodes']['$victim']['running'], isFalse);
    print('COMBINED_BLACKHOLE ${jsonEncode({
          'mode': mode,
          'victim': victim,
          'survivor': survivor,
          'dropped_upstream': up,
          'dropped_downstream': down,
          'elapsed_ms': outageClock.elapsedMilliseconds,
          'resources': resources(client),
        })}');

    final fragment = await control(directory, 'fragment');
    final recoveryClock = Stopwatch()..start();
    final fallbackBeforeRecovery = client.bootstrapTlsFallbackCount;
    var metadataConvergenceMs = 0;
    final survivingNodes = {1, 2, 3}..remove(victim);
    // This optional scenario is explicitly application-coordinated recovery.
    // Keep the original immediate-Fetch scenario separate: stale leader TLS
    // failure remains terminal and must not be disguised as a transparent fix.
    if (awaitMetadataElection) {
      final observed = await readUntil(
          client,
          () => client.metadata({topic}),
          (m) => metadataReady(
              m,
              (parts) =>
                  parts
                      .map((p) => p.partitionIndex)
                      .toSet()
                      .containsAll([0, 1, 2]) &&
                  parts.every((p) =>
                      survivingNodes.contains(p.leaderId) &&
                      p.isrNodes
                              .where(survivingNodes.contains)
                              .toSet()
                              .length >=
                          2)),
          'application-observed metadata election');
      metadataConvergenceMs = recoveryClock.elapsedMilliseconds;
      expect(client.bootstrapTlsFallbackCount - fallbackBeforeRecovery,
          greaterThan(0));
      print('COMBINED_METADATA_CONVERGED ${jsonEncode({
            'mode': mode,
            'await_metadata_election': true,
            'elapsed_ms': recoveryClock.elapsedMilliseconds,
            'bootstrap_tls_fallback_count': client.bootstrapTlsFallbackCount,
            'partitions': [
              for (final partition in observed.topics.single.partitions)
                {
                  'partition': partition.partitionIndex,
                  'leader': partition.leaderId,
                  'isr': partition.isrNodes,
                }
            ],
          })}');
    }
    var recoveryFetchCalls = 0;
    Future<FetchResponse> recoveryFetch() async {
      recoveryFetchCalls++;
      try {
        return await client.fetch(topic, partition: 0, offset: offsets[0]!);
      } catch (error, stack) {
        if (tlsFetchRecovery != null) {
          // Diagnostic only, after the single failed Fetch has completed.
          // These separate metadata reads never turn its failure into a pass.
          print('COMBINED_FETCH_FAILURE ${jsonEncode({
                'mode': mode,
                'elapsed_ms': recoveryClock.elapsedMilliseconds,
                'initial_recovery_fetch_calls': recoveryFetchCalls,
                'max_attempts': tlsFetchRecovery.maxAttempts,
                'window_ms': tlsFetchRecovery.window.inMilliseconds,
                'handshake_failure': error is HandshakeException,
              })}');
          for (var observation = 0; observation < 5; observation++) {
            try {
              final diagnostic = await client.metadata({topic});
              print('COMBINED_POST_FAILURE_METADATA ${jsonEncode({
                    'mode': mode,
                    'observation': observation,
                    'elapsed_ms': recoveryClock.elapsedMilliseconds,
                    'partitions': [
                      for (final partition
                          in diagnostic.topics.single.partitions)
                        {
                          'partition': partition.partitionIndex,
                          'leader': partition.leaderId,
                          'error_code': partition.errorCode,
                          'isr': partition.isrNodes,
                        }
                    ],
                  })}');
              if (diagnostic.topics.single.partitions
                  .every((p) => survivingNodes.contains(p.leaderId))) {
                break;
              }
            } catch (_) {
              print('COMBINED_POST_FAILURE_METADATA unavailable');
              break;
            }
            await Future<void>.delayed(const Duration(milliseconds: 250));
          }
        }
        Error.throwWithStackTrace(error, stack);
      }
    }

    // The new policy gate proves one immediate Fetch call. No application
    // metadata wait or readUntil retry is allowed to substitute for recovery.
    final initialRecovered = tlsFetchRecovery != null
        ? await recoveryFetch()
        : await readUntil(
            client,
            recoveryFetch,
            (response) =>
                response.topics.single.partitions.single.records.isNotEmpty,
            'acknowledged partition zero marker');
    if (tlsFetchRecovery != null) {
      expect(awaitMetadataElection, isFalse);
      expect(recoveryFetchCalls, 1);
    }
    final initialFetchMs =
        recoveryClock.elapsedMilliseconds - metadataConvergenceMs;
    final initialPartition = initialRecovered.topics.single.partitions.single;
    final initialRecordCount = initialPartition.records.length;
    expect(initialPartition.errorCode, 0);
    print('COMBINED_INITIAL_FETCH_RESULT ${jsonEncode({
          'mode': mode,
          'tls_fetch_recovery': tlsFetchRecovery != null,
          'initial_recovery_fetch_calls': recoveryFetchCalls,
          'initial_fetch_ms': initialFetchMs,
          'initial_records': initialRecordCount,
          'initial_high_watermark': initialPartition.highWatermark,
        })}');
    var recovered = initialRecovered;
    var visibilityFetchCalls = 0;
    var visibilityMs = 0;
    if (tlsFetchRecovery != null && initialRecordCount == 0) {
      // Successful Fetch may be empty during high-watermark convergence. Do
      // not make the client retry empty reads. Verify later marker visibility
      // separately, without relabeling these extra calls as single-call data.
      final visibilityClock = Stopwatch()..start();
      recovered = await readUntil(client, () {
        visibilityFetchCalls++;
        return client.fetch(topic, partition: 0, offset: offsets[0]!);
      },
          (response) =>
              response.topics.single.partitions.single.records.isNotEmpty,
          'marker visibility after successful empty Fetch');
      visibilityMs = visibilityClock.elapsedMilliseconds;
    }
    expect(recovered.topics.single.partitions.single.records.single.value,
        utf8.encode(marker('before', 0)));
    expect(recovered.topics.single.partitions.single.records.single.offset,
        offsets[0]);
    final recoveryMs = recoveryClock.elapsedMilliseconds;
    if (allowTlsBootstrapFallback) {
      expect(client.bootstrapTlsFallbackCount, greaterThan(0));
    } else {
      expect(client.bootstrapTlsFallbackCount, 0);
    }
    final elected = await readUntil(
        client,
        () => client.metadata({topic}),
        (m) => metadataReady(
            m,
            (parts) => parts.every((p) =>
                p.leaderId > 0 &&
                p.leaderId != victim &&
                p.isrNodes.where((node) => node != victim).toSet().length >=
                    2)),
        'surviving leaders');
    expect(
        elected.topics.single.partitions
            .singleWhere((p) => p.partitionIndex == 0)
            .leaderId,
        isNot(victim));
    for (var partition = 0; partition < 3; partition++) {
      final response = await readUntil(
          client,
          () => client.fetch(topic,
              partition: partition, offset: offsets[partition]!),
          (r) => r.topics.single.partitions.single.records.isNotEmpty,
          'original markers');
      final records = response.topics.single.partitions.single.records;
      expect(records.map((r) => r.offset), [offsets[partition]]);
      expect(records.map((r) => utf8.decode(r.value!)),
          [marker('before', partition)]);
    }
    final converged = await control(directory, 'status');
    expect(converged['nodes']['$victim']['running'], isFalse);
    expect(proxies(converged).every((p) => p['mode'] == 'fragment'), isTrue);
    final fragments = proxies(converged)
            .fold<int>(0, (sum, p) => sum + (p['fragments'] as int)) -
        proxies(fragment)
            .fold<int>(0, (sum, p) => sum + (p['fragments'] as int));
    expect(fragments, greaterThan(0));
    // This is opaque ciphertext fragmentation, not TLS plaintext/parser splits.
    for (var partition = 0; partition < 3; partition++) {
      final result = (await client.produce(
          topic, [KafkaRecord(value: utf8.encode(marker('after', partition)))],
          partition: partition, acks: -1))!;
      expect(result.responses.single.partitionResponses.single.baseOffset,
          offsets[partition]! + 1);
      bounded(client);
    }
    final restarted = await control(directory, 'start:$victim');
    expect(restarted['nodes']['$victim']['running'], isTrue);
    expect(restarted['nodes']['$victim']['generation'],
        greaterThan(overlap['nodes']['$victim']['generation'] as int));
    await control(directory, 'pass');
    await readUntil(
        client,
        () => client.metadata({topic}),
        (m) => metadataReady(
            m,
            (parts) => parts.every((p) =>
                p.errorCode == 0 && p.isrNodes.toSet().containsAll({1, 2, 3}))),
        'restored ISR');
    for (var partition = 0; partition < 3; partition++) {
      final records = (await client.fetch(topic,
              partition: partition, offset: offsets[partition]!))
          .topics
          .single
          .partitions
          .single
          .records;
      expect(records.map((r) => r.offset),
          [offsets[partition], offsets[partition]! + 1]);
      expect(records.map((r) => utf8.decode(r.value!)),
          [marker('before', partition), marker('after', partition)]);
      bounded(client);
    }
    await settled(client);
    await client.close();
    await settled(client);
    expect(client.connectionCount, 0);
    var closed = await control(directory, 'status');
    final closeClock = Stopwatch()..start();
    while (proxies(closed).any((p) => p['active'] != 0 || p['workers'] != 0) &&
        closeClock.elapsed < const Duration(seconds: 3)) {
      await Future<void>.delayed(const Duration(milliseconds: 20));
      closed = await control(directory, 'status');
    }
    for (final p in proxies(closed)) {
      expect(p['active'], 0);
      expect(p['workers'], 0);
      expect(p['rejected'], 0);
      expect(p['peak_connections'], lessThanOrEqualTo(16));
      expect(p['peak_workers'], lessThanOrEqualTo(32));
    }
    print('COMBINED_SECURE_FAULT_RESULT ${jsonEncode({
          'mode': mode,
          'victim': victim,
          'survivor': survivor,
          'allow_tls_bootstrap_fallback': allowTlsBootstrapFallback,
          'await_metadata_election': awaitMetadataElection,
          'tls_fetch_recovery': tlsFetchRecovery != null,
          'bootstrap_tls_fallback_count': client.bootstrapTlsFallbackCount,
          'overlap_proven': true,
          'dropped_upstream': up,
          'dropped_downstream': down,
          'opaque_ciphertext_fragments': fragments,
          'recovery_ms': recoveryMs,
          'initial_recovery_fetch_calls': recoveryFetchCalls,
          'initial_fetch_ms': initialFetchMs,
          'initial_records': initialRecordCount,
          'initial_high_watermark': initialPartition.highWatermark,
          'visibility_fetch_calls': visibilityFetchCalls,
          'visibility_wait_ms': visibilityMs,
          'tls_fetch_max_attempts': tlsFetchRecovery?.maxAttempts,
          'tls_fetch_window_ms': tlsFetchRecovery?.window.inMilliseconds,
          'metadata_convergence_ms': metadataConvergenceMs,
          'post_convergence_fetch_ms': initialFetchMs,
          'recovery_tls_fallback_count':
              client.bootstrapTlsFallbackCount - fallbackBeforeRecovery,
          'offsets': {
            for (final entry in offsets.entries)
              '${entry.key}': [entry.value, entry.value + 1]
          },
          'verified_markers': 6,
          'resources': resources(client),
          'proxy_active': proxies(closed).map((p) => p['active']).toList(),
          'proxy_workers': proxies(closed).map((p) => p['workers']).toList(),
        })}');
  },
      skip:
          mode == null || rawPorts == null || topic == null || directory == null
              ? 'Run python3 tool/run_combined_secure_fault_integration.py'
              : false,
      timeout: const Timeout(Duration(minutes: 3)));
}
