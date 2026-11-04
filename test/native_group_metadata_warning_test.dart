import 'dart:convert';

import 'package:kafka_client/src/api/response/api3_metadata_response.dart';
import 'package:kafka_client/src/consumer_group_metadata.dart';
import 'package:kafka_client/src/group_membership_protocol.dart';
import 'package:kafka_client/src/reliability.dart';
import 'package:test/test.dart';

import 'native_classic_group_driver_test.dart' as fixture;

BrokerMetadata endpoint(
        {int id = 7, String host = 'leader.invalid', int port = 9092}) =>
    BrokerMetadata(nodeId: id, host: host, port: port);

PartitionMetadata partition(int id, {int code = 9, int leader = 7}) =>
    PartitionMetadata(
        errorCode: code,
        partitionIndex: id,
        leaderId: leader,
        replicaNodes: [7],
        isrNodes: [7]);

TopicMetadata topic(List<PartitionMetadata> partitions,
        {String name = 'x', int code = 0}) =>
    TopicMetadata(errorCode: code, name: name, partitions: partitions);

Matcher brokerError(int code, {bool topic = false}) => throwsA(
    isA<KafkaBrokerException>().having((e) => e.errorCode, 'code', code).having(
        (e) => e.operation,
        'phase',
        topic ? 'group topic metadata' : 'group partition metadata'));

void main() {
  for (final isLeader in [true, false]) {
    final role = isLeader ? 'leader' : 'follower';
    Future<void> reject(MetadataResponse metadata, Matcher matcher) async {
      final before = jsonEncode(metadata.toJson());
      final client = fixture.FakeClient()..metadataResponse = metadata;
      final d = fixture.driver(client);
      await d.prepare();
      await expectLater(
          d.sync(isLeader
              ? fixture.leader([
                  fixture.member('a', ['x'])
                ])
              : fixture.follower()),
          matcher);
      expect(client.metadataQueries, [
        {'x'}
      ]);
      expect(client.requests.whereType<SyncGroupRequest>().length,
          isLeader ? 0 : 1);
      expect(client.requests.length, isLeader ? 1 : 2);
      expect(jsonEncode(metadata.toJson()), before);
    }

    for (final codes in [
      [9, 9, 9],
      [0, 9, 0]
    ]) {
      test('$role preserves full partition set for $codes without replay',
          () async {
        final metadata = MetadataResponse(0, [
          endpoint()
        ], [
          topic([
            for (var i = 0; i < codes.length; i++) partition(i, code: codes[i])
          ])
        ]);
        final before = jsonEncode(metadata.toJson());
        final client = fixture.FakeClient()
          ..metadataResponse = metadata
          ..assignment = ConsumerGroupAssignment({
            'x': [0, 1, 2]
          }).toBytes();
        final d = fixture.driver(client);
        await d.prepare();
        final result = await d.sync(isLeader
            ? fixture.leader([
                fixture.member('a', ['x'])
              ])
            : fixture.follower());
        expect(result.partitions, {
          'x': [0, 1, 2]
        });
        expect(client.metadataQueries, [
          {'x'}
        ]);
        final sync = client.requests.whereType<SyncGroupRequest>().single;
        if (isLeader) {
          expect(
              ConsumerGroupAssignment.fromBytes(
                      sync.assignments.single.assignment)
                  .partitions,
              {
                'x': [0, 1, 2]
              });
        } else {
          expect(sync.assignments, isEmpty);
        }
        expect(client.requests.length, 2);
        expect(jsonEncode(metadata.toJson()), before);
        expect(
            (await client.metadata({'x'}))
                .topics
                .single
                .partitions
                .map((p) => p.errorCode),
            codes);
      });
    }

    final invalidEndpoints = <String, List<BrokerMetadata>>{
      'missing': [],
      'different ID': [endpoint(id: 8)],
      'duplicate same endpoint': [endpoint(), endpoint()],
      'third duplicate endpoint': [endpoint(), endpoint(), endpoint()],
      'third duplicate after invalid endpoint': [
        endpoint(host: ''),
        endpoint(),
        endpoint()
      ],
      'duplicate different endpoint': [
        endpoint(),
        endpoint(host: 'other.invalid')
      ],
      'duplicate with invalid endpoint': [endpoint(), endpoint(host: '')],
      'empty host': [endpoint(host: '')],
      'zero port': [endpoint(port: 0)],
      'negative port': [endpoint(port: -1)],
      'oversize port': [endpoint(port: 65536)],
    };
    for (final entry in invalidEndpoints.entries) {
      test('$role rejects warning with ${entry.key}', () async {
        await reject(
            MetadataResponse(0, entry.value, [
              topic([partition(0)])
            ]),
            brokerError(9));
      });
    }
    test('$role rejects negative leader even with matching broker', () async {
      await reject(
          MetadataResponse(0, [
            endpoint(id: -1)
          ], [
            topic([partition(0, leader: -1)])
          ]),
          brokerError(9));
    });
    for (final port in [1, 65535]) {
      test('$role accepts endpoint port boundary $port', () async {
        final client = fixture.FakeClient()
          ..metadataResponse = MetadataResponse(0, [
            endpoint(port: port)
          ], [
            topic([partition(0)])
          ]);
        final d = fixture.driver(client);
        await d.prepare();
        expect(
            (await d.sync(isLeader
                    ? fixture.leader([
                        fixture.member('a', ['x'])
                      ])
                    : fixture.follower()))
                .partitions,
            {
              'x': [0]
            });
      });
    }
    for (final code in [5, 29, 12345]) {
      test('$role retains partition error $code despite healthy leader',
          () async {
        await reject(
            MetadataResponse(0, [
              endpoint()
            ], [
              topic([partition(0, code: code)])
            ]),
            brokerError(code));
      });
    }
    for (final code in [9, 29]) {
      test('$role retains topic error $code despite healthy leader', () async {
        await reject(
            MetadataResponse(0, [
              endpoint()
            ], [
              topic([partition(0)], code: code)
            ]),
            brokerError(code, topic: true));
      });
    }
    final malformedPartitions = <String, List<PartitionMetadata>>{
      'negative ID': [partition(-1)],
      'oversize ID': [partition(2147483648)],
      'duplicate ID': [partition(0), partition(0)],
      'mixed duplicate ID': [partition(0, code: 0), partition(0)],
    };
    for (final entry in malformedPartitions.entries) {
      test('$role rejects warning with ${entry.key}', () async {
        await reject(MetadataResponse(0, [endpoint()], [topic(entry.value)]),
            throwsFormatException);
      });
    }
    final malformedTopics = <String, List<TopicMetadata>>{
      'missing': [],
      'duplicate': [
        topic([partition(0)]),
        topic([partition(1)])
      ],
      'unexpected': [
        topic([partition(0)], name: 'other')
      ],
    };
    for (final entry in malformedTopics.entries) {
      test('$role rejects ${entry.key} topics with warnings', () async {
        await reject(MetadataResponse(0, [endpoint()], entry.value),
            throwsFormatException);
      });
    }
  }
  test('warning does not permit nonexistent follower assignment', () async {
    final client = fixture.FakeClient()
      ..metadataResponse = MetadataResponse(0, [
        endpoint()
      ], [
        topic([partition(1)])
      ]);
    final d = fixture.driver(client);
    await d.prepare();
    await expectLater(d.sync(fixture.follower()), throwsFormatException);
    expect(client.metadataQueries.length, 1);
    expect(client.requests.whereType<SyncGroupRequest>().length, 1);
  });
}
