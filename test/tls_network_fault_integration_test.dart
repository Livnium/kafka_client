import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';

// Commands are atomic files in this invocation's private fixture directory.
Future<Map<String, dynamic>> control(String path, String action) async {
  final id = '$pid-${DateTime.now().microsecondsSinceEpoch}';
  final temporary = File('$path/$id.temporary');
  final response = File('$path/$id.response');
  await temporary.writeAsString(jsonEncode({'action': action}), flush: true);
  await temporary.rename('$path/$id.request');
  final clock = Stopwatch()..start();
  while (!await response.exists()) {
    if (clock.elapsed > const Duration(seconds: 5)) {
      throw TimeoutException('Proxy control $action timed out');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  final result =
      jsonDecode(await response.readAsString()) as Map<String, dynamic>;
  await response.delete();
  expect(result['ok'], isTrue, reason: '$action: ${result['error']}');
  return result;
}

Future<void> settled(KafkaClient client) async {
  final clock = Stopwatch()..start();
  while (client.admittedRequestCount != 0 &&
      clock.elapsed < const Duration(seconds: 3)) {
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  expect(client.pendingRequestCount, 0);
  expect(client.admittedRequestCount, 0);
  expect(client.queuedRequestBytes, 0);
  expect(client.bufferedResponseBytes, 0);
}

void main() {
  final directPort =
      int.tryParse(Platform.environment['KAFKA_TLS_FAULT_DIRECT_PORT'] ?? '');
  final proxyPort =
      int.tryParse(Platform.environment['KAFKA_TLS_FAULT_PROXY_PORT'] ?? '');
  final topic = Platform.environment['KAFKA_TLS_FAULT_TOPIC'];
  final path = Platform.environment['KAFKA_TLS_FAULT_CONTROL'];
  final env = Platform.environment;
  final selectedMode = env['KAFKA_TLS_FAULT_MODE'];
  for (final mode in selectedMode == null ? ['tls', 'sasl'] : [selectedMode]) {
    KafkaClient make({bool direct = false, int retries = 1}) => KafkaClient(
          // Bootstrap is deliberately direct. Discovered leader traffic must use
          // the proxy advertised by Kafka, proving this is not bootstrap-only.
          brokers: [Broker('localhost', directPort!)],
          tls: KafkaTlsConfig(
            trustedCertificates:
                File(env['KAFKA_TLS_FAULT_CA']!).readAsBytesSync(),
            withTrustedRoots: false,
          ),
          sasl: mode == 'sasl'
              ? KafkaSaslPlain(
                  username: env['KAFKA_TLS_FAULT_USERNAME']!,
                  password: env['KAFKA_TLS_FAULT_PASSWORD']!,
                )
              : null,
          requestTimeout: Duration(milliseconds: direct ? 3000 : 900),
          connectionTimeout: const Duration(milliseconds: 400),
          maxReadRetries: retries,
          retryBaseDelay: const Duration(milliseconds: 10),
          maxInFlightRequests: 16,
          maxFrameBytes: 1048576,
          maxBufferedResponseBytes: 2097152,
        );

    Future<int> warm(KafkaClient client, int value) async {
      final metadata = await client.metadata({topic!});
      expect(metadata.brokers.single.host, 'localhost');
      expect(metadata.brokers.single.port, proxyPort);
      final result = (await client.produce(
          topic,
          [
            KafkaRecord(value: [value])
          ],
          acks: -1))!;
      expect(client.connectionCount, 2,
          reason: 'Direct bootstrap and independently discovered proxy leader');
      return result.responses.single.partitionResponses.single.baseOffset;
    }

    Future<List<KafkaRecord>> oracle(KafkaClient client, int offset) async {
      // Independent connection, explicit versioned Fetch, no metadata/leader
      // discovery and no retry. The proxy cannot influence this storage oracle.
      final response = await client.send(FetchRequest(
          -1,
          50,
          1,
          [
            FetchTopic(topic!, [FetchPartition(0, offset, 65536)])
          ],
          maxBytes: 65536));
      final partition = response.topics.single.partitions.single;
      expect(partition.errorCode, 0);
      return (partition.records as List<KafkaRecord>)
          .where((record) => record.offset >= offset)
          .toList();
    }

    group('$mode owned Kafka TLS transport faults', () {
      setUp(() async {
        await control(path!, 'pass');
      });
      tearDown(() async {
        await control(path!, 'pass');
      });

      test(
          'lost Produce acknowledgement stores one marker and is never replayed',
          () async {
        final client = make();
        final independent = make(direct: true, retries: 0);
        addTearDown(client.close);
        addTearDown(independent.close);
        final offset = await warm(client, 70);
        final marker = utf8.encode(
            'ambiguous-produce-$pid-${DateTime.now().microsecondsSinceEpoch}');
        final before = await control(path!, 'drop_down');
        // Warmup authenticated/version-checked the proxied leader. Metadata uses
        // direct secured bootstrap; only the encrypted Produce ack is dropped.
        // The proxy does not parse ciphertext or claim to count Produce frames.
        await expectLater(
            client.produce(topic!, [KafkaRecord(value: marker)], acks: -1),
            throwsA(isA<TimeoutException>()));
        await settled(client);
        final fault = await control(path, 'status');
        expect(fault['dropped_downstream'],
            greaterThan(before['dropped_downstream']));
        final stored = await oracle(independent, offset + 1);
        expect(stored, hasLength(1));
        expect(stored.single.offset, offset + 1);
        expect(stored.single.value, marker);
        await control(path, 'pass');
        final recovered = await client.fetch(topic, offset: offset + 1);
        expect(recovered.topics.single.partitions.single.records.single.value,
            marker);
        // A later acknowledged Produce gives an exact contiguous-offset oracle:
        // any hidden replay would appear before this sentinel or in the scan.
        final sentinel = (await client.produce(
            topic,
            [
              KafkaRecord(value: [71])
            ],
            acks: -1))!;
        expect(sentinel.responses.single.partitionResponses.single.baseOffset,
            offset + 2);
        final all = await oracle(independent, offset + 1);
        expect(all, hasLength(2));
        expect(all.first.value, marker);
        expect(all.last.value, [71]);
        await settled(client);
        await settled(independent);
        print('AMBIGUOUS_PRODUCE ${jsonEncode({
              'marker_offset': offset + 1,
              'stored_marker_count': 1,
              'sentinel_offset': offset + 2,
              'mode': mode,
              'oracle':
                  'independent direct secured Fetch and contiguous sentinel'
            })}');
      });

      test(
          'bidirectional blackhole bounds concurrent reads and same-client recovery',
          () async {
        final client = make();
        addTearDown(client.close);
        final offset = await warm(client, 72);
        for (var cycle = 0; cycle < 3; cycle++) {
          final ready = await control(path!, 'status');
          // Dispatch a broker long-poll before cutting both directions, so the
          // broker's later reply independently proves downstream bytes are lost.
          final longPoll = expectLater(
              client.send(
                  FetchRequest(-1, 400, 65536, [
                    FetchTopic(topic!, [FetchPartition(0, offset + 1, 65536)])
                  ]),
                  broker: Broker('localhost', proxyPort!)),
              throwsA(isA<TimeoutException>()));
          final dispatched = Stopwatch()..start();
          while ((await control(path, 'status'))['upstream_bytes'] ==
              ready['upstream_bytes']) {
            if (dispatched.elapsed > const Duration(milliseconds: 200)) {
              fail('Long-poll was not forwarded before the fault');
            }
            await Future<void>.delayed(const Duration(milliseconds: 5));
          }
          final before = await control(path, 'blackhole');
          final clock = Stopwatch()..start();
          await Future.wait(List.generate(
              4,
              (_) => expectLater(
                  client.fetch(topic, offset: offset),
                  throwsA(anyOf(isA<TimeoutException>(),
                      isA<KafkaConnectionException>())))));
          await longPoll;
          expect(clock.elapsed, lessThan(const Duration(seconds: 4)));
          await settled(client);
          final fault = await control(path, 'status');
          expect(fault['dropped_upstream'],
              greaterThan(before['dropped_upstream']));
          expect(fault['dropped_downstream'],
              greaterThan(before['dropped_downstream']));
          await control(path, 'pass');
          final fetched = await client.fetch(topic, offset: offset);
          expect(fetched.topics.single.partitions.single.records.first.value,
              [72]);
          await settled(client);
        }
      });

      test(
          'forced disconnect fails in-flight reads and reconnects on same client',
          () async {
        final client = make();
        addTearDown(client.close);
        final offset = await warm(client, 73);
        final pending = expectLater(
            client.send(
                FetchRequest(-1, 60000, 65536, [
                  FetchTopic(topic!, [FetchPartition(0, offset + 1, 65536)])
                ]),
                broker: Broker('localhost', proxyPort!)),
            throwsA(isA<KafkaConnectionException>()));
        await Future<void>.delayed(const Duration(milliseconds: 50));
        expect(client.pendingRequestCount, 1);
        final before = await control(path!, 'status');
        final disconnected = await control(path, 'disconnect');
        expect(disconnected['disconnects'], greaterThan(before['disconnects']));
        await pending;
        await settled(client);
        expect(
            (await client.fetch(topic, offset: offset))
                .topics
                .single
                .partitions
                .single
                .records
                .first
                .value,
            [73]);
        final after = await control(path, 'status');
        expect(after['accepted'], greaterThan(before['accepted']));
        await settled(client);
      });

      test(
          'delayed fragmented responses preserve concurrent framing and counters',
          () async {
        final client = make();
        addTearDown(client.close);
        final offset = await warm(client, 74);
        final before = await control(path!, 'fragment');
        for (var cycle = 0; cycle < 3; cycle++) {
          await Future.wait(List.generate(4, (_) async {
            final response = await client.fetch(topic!, offset: offset);
            expect(response.topics.single.partitions.single.records.first.value,
                [74]);
          }));
          await settled(client);
        }
        final after = await control(path, 'status');
        expect(after['fragments'] - before['fragments'], greaterThan(24));
        expect(after['rejected'], 0);
        expect(after['peak_connections'], lessThanOrEqualTo(16));
        expect(after['peak_workers'], lessThanOrEqualTo(32));
        await client.close();
        await settled(client);
        expect(client.connectionCount, 0);
      });
    },
        skip: directPort == null ||
                proxyPort == null ||
                topic == null ||
                path == null
            ? 'Run python3 tool/run_tls_network_fault_integration.py'
            : false,
        timeout: const Timeout(Duration(seconds: 60)));
  }
}
