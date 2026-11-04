import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

import 'operational_integration_test.dart' as fixture;

// Each case owns its group; these APIs do not fence concurrent manual writers.
String _group(String label) =>
    'manual-$label-$pid-${DateTime.now().microsecondsSinceEpoch}';

void main() {
  final env = Platform.environment;
  final port = int.tryParse(env['KAFKA_OPERATIONAL_PORT'] ?? '');
  final topic = env['KAFKA_OPERATIONAL_TOPIC'];
  final control = env['KAFKA_OPERATIONAL_CONTROL'];
  final classpath = env['KAFKA_GROUP_ORACLE_CLASSPATH'];

  KafkaClient make() {
    final client = KafkaClient(
      brokers: [Broker('127.0.0.1', port!)],
      requestTimeout: const Duration(seconds: 10),
      connectionTimeout: const Duration(seconds: 2),
      maxReadRetries: 10,
      retryBaseDelay: const Duration(milliseconds: 100),
    );
    addTearDown(() async {
      await client.close();
      await fixture.idle(client);
      expect(client.activeSubscriptionCount, 0);
      expect(client.connectionCount, 0);
    });
    return client;
  }

  Future<List<String>> oracle(List<String> args) async {
    final process = await Process.start(env['JAVA'] ?? 'java',
        ['-cp', classpath!, 'GroupOffsetOracle', ...args]);
    final stdoutResult = process.stdout.transform(utf8.decoder).join();
    final stderrResult = process.stderr.transform(utf8.decoder).join();
    int code;
    try {
      code = await process.exitCode.timeout(const Duration(seconds: 40));
    } on TimeoutException {
      process.kill(ProcessSignal.sigkill);
      await process.exitCode;
      rethrow;
    }
    final output = await stdoutResult;
    final errors = await stderrResult;
    expect(code, 0, reason: 'Official Java oracle failed: $errors\n$output');
    return const LineSplitter()
        .convert(output)
        .where((line) => line.startsWith('ORACLE_'))
        .toList();
  }

  group('real Kafka manual committed offsets', () {
    test('unknown group has absent offsets and immutable result', () async {
      final client = make();
      final group = _group('absent');
      final coordinator = await client.findGroupCoordinator(group);
      expect(coordinator.port, port);
      final offsets =
          await client.committedOffsets(group, topic!, partitions: {0});
      expect(offsets.keys, [0]);
      expect(offsets[0], isNull);
      expect(() => offsets.clear(), throwsUnsupportedError);
      await fixture.idle(client);
    });

    test('acknowledged metadata and offsets survive a fresh client', () async {
      final group = _group('fresh');
      final writer = make();
      await writer.findGroupCoordinator(group);
      await writer.commitOffsets(group, topic!,
          nextOffsets: {0: 123}, metadata: 'manual-नमस्ते-☃');
      await writer.close();
      final reader = make();
      final value =
          (await reader.committedOffsets(group, topic, partitions: {0}))[0]!;
      expect(value.nextOffset, 123);
      expect(value.leaderEpoch, -1);
      expect(value.metadata, 'manual-नमस्ते-☃');
      await fixture.idle(reader);
    });

    test('explicit processing then commit resumes at the next record',
        () async {
      final client = make();
      final group = _group('resume');
      final produced = await client.produce(
          topic!,
          [
            KafkaRecord(value: utf8.encode('processed')),
            KafkaRecord(value: utf8.encode('remaining')),
          ],
          acks: -1);
      final start =
          produced!.responses.single.partitionResponses.single.baseOffset;
      final first = await client
          .subscribe(topic, startOffsets: {0: start})
          .take(1)
          .single
          .timeout(const Duration(seconds: 10));
      expect(utf8.decode(first.record.value!), 'processed');
      // Processing above is explicit. Delivery itself never stores progress.
      expect((await client.committedOffsets(group, topic, partitions: {0}))[0],
          isNull);
      await client.commitOffsets(group, topic,
          nextOffsets: {0: first.offset + 1}, metadata: 'processed-one');
      await client.close();
      final resumed = make();
      final saved =
          (await resumed.committedOffsets(group, topic, partitions: {0}))[0]!;
      final next = await resumed
          .subscribe(topic, startOffsets: {0: saved.nextOffset})
          .take(1)
          .single
          .timeout(const Duration(seconds: 10));
      expect(next.offset, start + 1);
      expect(utf8.decode(next.record.value!), 'remaining');
      expect(
          (await resumed.committedOffsets(group, topic, partitions: {0}))[0]!
              .nextOffset,
          start + 1,
          reason: 'Subscription must not auto commit');
    });

    test('fresh groups have independent committed progress', () async {
      final client = make();
      final a = _group('a');
      final b = _group('b');
      await client.findGroupCoordinator(a);
      await client.commitOffsets(a, topic!,
          nextOffsets: {0: 17}, metadata: 'a');
      expect((await client.committedOffsets(b, topic, partitions: {0}))[0],
          isNull);
      await client.commitOffsets(b, topic, nextOffsets: {0: 29}, metadata: 'b');
      expect(
          (await client.committedOffsets(a, topic, partitions: {0}))[0]!
              .nextOffset,
          17);
      expect(
          (await client.committedOffsets(b, topic, partitions: {0}))[0]!
              .nextOffset,
          29);
    });

    test('official Java reads Dart commit and Dart reads Java commit',
        () async {
      final client = make();
      final group = _group('oracle');
      final bootstrap = '127.0.0.1:$port';
      await client.findGroupCoordinator(group);
      await client.commitOffsets(group, topic!,
          nextOffsets: {0: 321}, metadata: 'dart-नमस्ते');
      expect(await oracle(['read', bootstrap, group, topic, '0']), [
        'ORACLE_OFFSET 321 -1 ${base64.encode(utf8.encode('dart-नमस्ते'))}'
      ]);
      expect(
          await oracle([
            'commit',
            bootstrap,
            group,
            topic,
            '0',
            '654',
            base64.encode(utf8.encode('java-☃'))
          ]),
          ['ORACLE_COMMITTED']);
      final fresh = make();
      final value =
          (await fresh.committedOffsets(group, topic, partitions: {0}))[0]!;
      expect(value.nextOffset, 654);
      expect(value.leaderEpoch, -1);
      expect(value.metadata, 'java-☃');
    },
        skip: classpath == null
            ? 'Compile tool/GroupOffsetOracle.java and set '
                'KAFKA_GROUP_ORACLE_CLASSPATH to classes plus official Kafka libs/*'
            : false);

    test('mixed valid and unknown partition commit reports partial outcome',
        () async {
      final client = make();
      final group = _group('partial');
      await client.findGroupCoordinator(group);
      Object? failure;
      try {
        await client.commitOffsets(group, topic!,
            nextOffsets: {0: 77, 999: 88}, metadata: 'partial');
      } catch (error) {
        failure = error;
      }
      expect(failure, isA<KafkaOffsetCommitException>());
      final error = failure! as KafkaOffsetCommitException;
      expect(error.partitionErrors, {0: 0, 999: 3});
      expect(() => error.partitionErrors.clear(), throwsUnsupportedError);
      expect(
          (await client.committedOffsets(group, topic!, partitions: {0}))[0]!
              .nextOffset,
          77);
    });

    test('acknowledged offset survives fixture crash and persisted restart',
        () async {
      final group = _group('restart');
      final client = make();
      await client.findGroupCoordinator(group);
      await client.commitOffsets(group, topic!,
          nextOffsets: {0: 987}, metadata: 'before-crash');
      await fixture.idle(client);
      try {
        await fixture.control(control!, 'crash');
      } finally {
        await fixture.control(control!, 'start');
      }
      final fresh = make();
      final value =
          (await fresh.committedOffsets(group, topic, partitions: {0}))[0]!;
      expect(value.nextOffset, 987);
      expect(value.metadata, 'before-crash');
    }, timeout: const Timeout(Duration(minutes: 3)));
  },
      skip: port == null || topic == null || control == null
          ? 'Run tool/run_operational_integration.py -- dart test '
              'test/group_offsets_integration_test.dart --concurrency=1'
          : false,
      timeout: const Timeout(Duration(seconds: 90)));
}
