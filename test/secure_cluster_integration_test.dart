import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';

Future<void> control(String directory, String action, int node) async {
  final id = '$pid-${DateTime.now().microsecondsSinceEpoch}';
  final temporary = File('$directory/$id.temporary');
  await temporary.writeAsString(jsonEncode({'action': action, 'node': node}),
      flush: true);
  await temporary.rename('$directory/$id.request');
  final response = File('$directory/$id.response');
  final deadline = DateTime.now().add(const Duration(seconds: 85));
  while (!await response.exists()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('Owned cluster command $action timed out');
    }
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
  final result = jsonDecode(await response.readAsString()) as Map;
  await response.delete();
  expect(result['ok'], isTrue, reason: '${result['error']}');
  expect(result['node'], node);
}

void boundedConnections(KafkaClient client) {
  expect(client.connectionCount, lessThanOrEqualTo(2));
}

Future<MetadataResponse> waitMetadata(KafkaClient client, String topic,
    bool Function(List<PartitionMetadata>) ready,
    {bool allowReplicaWarning = false}) async {
  final deadline = DateTime.now().add(const Duration(seconds: 45));
  MetadataResponse? latest;
  while (DateTime.now().isBefore(deadline)) {
    latest = await client.metadata({topic});
    boundedConnections(client);
    final partitions = latest.topics.single.partitions;
    if (partitions.length == 3 &&
        latest.topics.single.errorCode == 0 &&
        partitions.every((p) =>
            p.errorCode == 0 ||
            (allowReplicaWarning && p.errorCode == 9 && p.leaderId >= 0)) &&
        ready(partitions)) {
      return latest;
    }
    await Future<void>.delayed(const Duration(milliseconds: 200));
  }
  throw TimeoutException(
      'Cluster metadata did not converge: ${latest?.toJson()}');
}

void main() {
  final env = Platform.environment;
  final selectedMode = env['KAFKA_SECURE_CLUSTER_MODE'];
  for (final mode in selectedMode == null ? ['tls', 'sasl'] : [selectedMode]) {
    final rawPorts = env['KAFKA_SECURE_CLUSTER_${mode.toUpperCase()}_PORTS'];
    final baseTopic = env['KAFKA_SECURE_CLUSTER_TOPIC'];
    final topic = baseTopic == null ? null : '$baseTopic-$mode';
    final directory = Platform.environment['KAFKA_SECURE_CLUSTER_CONTROL'];
    test(
        '$mode: RF3 multiple leaders, bootstrap fallback, failover and same-client recovery',
        () async {
      final ports = rawPorts!.split(',').map(int.parse).toList();
      final deadPort =
          int.parse(Platform.environment['KAFKA_SECURE_CLUSTER_DEAD_PORT']!);
      final client = KafkaClient(
        brokers: [
          Broker('localhost', deadPort),
          ...ports.map((port) => Broker('localhost', port)),
        ],
        tls: KafkaTlsConfig(
          trustedCertificates:
              File(env['KAFKA_SECURE_CLUSTER_CA']!).readAsBytesSync(),
          withTrustedRoots: false,
        ),
        sasl: mode == 'sasl'
            ? KafkaSaslPlain(
                username: env['KAFKA_SECURE_CLUSTER_USERNAME']!,
                password: env['KAFKA_SECURE_CLUSTER_PASSWORD']!,
              )
            : null,
        connectionTimeout: const Duration(seconds: 3),
        requestTimeout: const Duration(seconds: 4),
        retryBaseDelay: const Duration(seconds: 1),
        maxReadRetries: 10,
        // Three actual leaders with only two slots forces idle LRU eviction.
        maxConnections: 2,
      );
      addTearDown(client.close);
      final initial = await waitMetadata(client, topic!,
          (parts) => parts.every((p) => p.isrNodes.length == 3));
      print('Initial RF3 metadata: ${initial.toJson()}');
      expect(initial.brokers, hasLength(3));
      expect(initial.topics.single.partitions.map((p) => p.leaderId).toSet(),
          {1, 2, 3});
      for (final partition in initial.topics.single.partitions) {
        expect(partition.replicaNodes.toSet(), {1, 2, 3});
      }
      final offsets = <int, int>{};
      for (var partition = 0; partition < 3; partition++) {
        // Exactly one Produce call per marker. Ambiguous failures fail the test;
        // no retry helper wraps a write and no success is inferred from a timeout.
        final response = await client.produce(
            topic, [KafkaRecord(value: utf8.encode('before-$partition'))],
            partition: partition, acks: -1);
        boundedConnections(client);
        offsets[partition] =
            response!.responses.single.partitionResponses.single.baseOffset;
      }
      final victim = initial.topics.single.partitions
          .singleWhere((p) => p.partitionIndex == 0)
          .leaderId;
      final failover = Stopwatch()..start();
      print('Crashing owned leader node $victim for partition 0');
      await control(directory!, 'crash', victim);
      // Repeated bounded read-only calls cover the cluster election window.
      // Each call exercises internal metadata refresh/retry on the SAME client.
      // No external metadata wait and no Produce call/replay until recovery.
      late FetchPartitionResponse recovered;
      var fetchCalls = 0;
      final recoveryDeadline = DateTime.now().add(const Duration(seconds: 45));
      while (true) {
        fetchCalls++;
        try {
          recovered =
              (await client.fetch(topic, partition: 0, offset: offsets[0]!))
                  .topics
                  .single
                  .partitions
                  .single;
          boundedConnections(client);
          if (recovered.records.isNotEmpty) break;
          print('Read-only Fetch call $fetchCalls returned empty records; '
              'highWatermark=${recovered.highWatermark}');
          if (DateTime.now().isAfter(recoveryDeadline)) {
            fail('Acknowledged marker not visible before recovery deadline');
          }
          await Future<void>.delayed(const Duration(milliseconds: 250));
        } catch (error) {
          if (error is! KafkaConnectionException &&
              error is! SocketException &&
              error is! TimeoutException &&
              !(error is KafkaBrokerException &&
                  [5, 6, 9].contains(error.errorCode))) {
            rethrow;
          }
          print('Read-only Fetch call $fetchCalls exhausted retries: $error');
          print('Live metadata: ${(await client.metadata({topic})).toJson()}');
          boundedConnections(client);
          if (DateTime.now().isAfter(recoveryDeadline)) rethrow;
          await Future<void>.delayed(const Duration(milliseconds: 250));
        }
      }
      print('Recovery used $fetchCalls bounded read-only Fetch call(s)');
      print(
          'Same-client Fetch recovered in ${failover.elapsedMilliseconds} ms');
      expect(recovered.records.map((r) => utf8.decode(r.value!)), ['before-0']);
      expect(recovered.records.single.offset, offsets[0]);
      expect(recovered.highWatermark, greaterThan(offsets[0]!));
      final after = await waitMetadata(client, topic,
          (parts) => parts.every((p) => p.leaderId != victim && p.leaderId > 0),
          allowReplicaWarning: true);
      print('Post-failover metadata: ${after.toJson()}');
      expect(
          after.topics.single.partitions
              .singleWhere((p) => p.partitionIndex == 0)
              .leaderId,
          isNot(victim));
      for (var partition = 0; partition < 3; partition++) {
        final fetched = (await client.fetch(topic,
                partition: partition, offset: offsets[partition]!))
            .topics
            .single
            .partitions
            .single;
        boundedConnections(client);
        expect(fetched.records.map((r) => utf8.decode(r.value!)),
            ['before-$partition']);
        expect(fetched.records.single.offset, offsets[partition]);
        final response = await client.produce(
            topic, [KafkaRecord(value: utf8.encode('after-$partition'))],
            partition: partition, acks: -1);
        boundedConnections(client);
        expect(response!.responses.single.partitionResponses.single.baseOffset,
            offsets[partition]! + 1);
      }
      await control(directory, 'start', victim);
      await waitMetadata(
          client, topic, (parts) => parts.every((p) => p.isrNodes.length == 3));
      for (var partition = 0; partition < 3; partition++) {
        final fetched = (await client.fetch(topic,
                partition: partition, offset: offsets[partition]!))
            .topics
            .single
            .partitions
            .single;
        boundedConnections(client);
        expect(fetched.records.map((r) => utf8.decode(r.value!)),
            ['before-$partition', 'after-$partition']);
        expect(fetched.records.map((r) => r.offset),
            [offsets[partition], offsets[partition]! + 1]);
      }
      print(
          'Restarted node $victim; all partitions restored to ISR=3; six markers verified');
      boundedConnections(client);
      expect(client.pendingRequestCount, 0);
      expect(client.admittedRequestCount, 0);
      expect(client.queuedRequestBytes, 0);
      expect(client.bufferedResponseBytes, 0);
    },
        skip: rawPorts == null || topic == null || directory == null
            ? 'Run tool/run_secure_cluster_integration.py for a disposable secured loopback cluster'
            : false,
        timeout: const Timeout(Duration(minutes: 3)));
  }
}
