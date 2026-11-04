import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

void main() {
  final port = int.tryParse(Platform.environment['KAFKA_TEST_PORT'] ?? '');
  final topic = Platform.environment['KAFKA_TEST_TOPIC'];
  final oracleTopic = Platform.environment['KAFKA_TEST_ORACLE_TOPIC'];
  group('real Kafka 4.0.2', () {
    late KafkaClient client;
    setUp(() {
      client = KafkaClient(
        brokers: [Broker('127.0.0.1', port!)],
        requestTimeout: const Duration(seconds: 10),
      );
    });
    tearDown(() => client.close());

    test(
        'produce/fetch preserves binary, UTF8, tombstones, headers and offsets',
        () async {
      final timestamp = DateTime.now().millisecondsSinceEpoch;
      final records = [
        KafkaRecord(
            key: [0, 255, 128],
            value: utf8.encode('Kafka Dart — नमस्ते 🌍'),
            timestamp: timestamp,
            headers: [
              KafkaRecordHeader('प्रकार', [255, 0]),
              KafkaRecordHeader('nullable', null)
            ]),
        KafkaRecord(
            key: utf8.encode('deleted'), value: null, timestamp: timestamp + 1),
        KafkaRecord(key: [], value: [], timestamp: timestamp + 2),
      ];
      final response = await client.produce(topic!, records, acks: -1);
      expect(response, isNotNull);
      final produced = response!.responses.single.partitionResponses.single;
      expect(produced.errorCode, 0);
      final fetched = (await client.fetch(topic, offset: produced.baseOffset))
          .topics
          .single
          .partitions
          .single
          .records;
      expect(fetched, hasLength(3));
      for (var i = 0; i < records.length; i++) {
        expect(fetched[i].key, records[i].key);
        expect(fetched[i].value, records[i].value);
        expect(fetched[i].timestamp, records[i].timestamp);
        expect(fetched[i].offset, produced.baseOffset + i);
      }
      expect(fetched.first.headers.map((h) => h.key), ['प्रकार', 'nullable']);
      expect(fetched.first.headers.first.value, [255, 0]);
      expect(fetched.first.headers.last.value, isNull);
      final middle =
          (await client.fetch(topic, offset: produced.baseOffset + 1))
              .topics
              .single
              .partitions
              .single
              .records;
      expect(middle.map((r) => r.offset),
          [produced.baseOffset + 1, produced.baseOffset + 2]);
      final exhausted =
          (await client.fetch(topic, offset: produced.baseOffset + 3))
              .topics
              .single
              .partitions
              .single
              .records;
      expect(exhausted, isEmpty);
    });

    test(
        'acks=0 finishes without response and record eventually becomes readable',
        () async {
      final marker =
          utf8.encode('acks-zero-${DateTime.now().microsecondsSinceEpoch}');
      expect(
          await client
              .produce(
                  topic!,
                  [
                    KafkaRecord(
                        value: marker,
                        timestamp: DateTime.now().millisecondsSinceEpoch)
                  ],
                  acks: 0)
              .timeout(const Duration(seconds: 5)),
          isNull);
      final deadline = DateTime.now().add(const Duration(seconds: 5));
      var found = false;
      while (DateTime.now().isBefore(deadline)) {
        final records =
            (await client.fetch(topic)).topics.single.partitions.single.records;
        found = records
            .any((r) => const ListEqualityBytes().equals(r.value, marker));
        if (found) break;
        await Future<void>.delayed(const Duration(milliseconds: 25));
      }
      expect(found, isTrue,
          reason: 'acks=0 only promises local flush, not broker delivery');
    });

    test('invalid partition and out-of-range offsets fail rather than hang',
        () async {
      await expectLater(
          client.produce(
              topic!,
              [
                KafkaRecord(value: [1])
              ],
              partition: 99),
          throwsStateError);
      await expectLater(
          client.fetch(topic, offset: 999999999),
          throwsA(isA<KafkaBrokerException>()
              .having((e) => e.errorCode, 'errorCode', 1)));
    });

    test('interoperates with official Java console producer and consumer',
        () async {
      final fetched = (await client.fetch(oracleTopic!))
          .topics
          .single
          .partitions
          .single
          .records;
      expect(utf8.decode(fetched.single.value!), 'java-to-dart-नमस्ते');
      await client.produce(oracleTopic, [
        KafkaRecord(
            value: utf8.encode('dart-to-java-नमस्ते'),
            timestamp: DateTime.now().millisecondsSinceEpoch)
      ]);
    }, skip: oracleTopic == null ? 'Oracle topic not configured' : false);
  },
      skip: port == null || topic == null
          ? 'Run tool/run_broker_integration.sh'
          : false,
      timeout: const Timeout(Duration(seconds: 30)));
}

class ListEqualityBytes {
  const ListEqualityBytes();
  bool equals(List<int>? a, List<int> b) =>
      a != null &&
      a.length == b.length &&
      Iterable<int>.generate(b.length).every((i) => a[i] == b[i]);
}
