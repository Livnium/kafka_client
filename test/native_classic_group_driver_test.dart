import 'dart:async';
import 'package:kafka_client/src/api/abstract/kafka_request.dart';
import 'package:kafka_client/src/api/request/api18_api_vesions_request.dart';
import 'package:kafka_client/src/api/response/api18_api_versions_response.dart';
import 'package:kafka_client/src/api/response/api3_metadata_response.dart';
import 'package:kafka_client/src/broker/broker.dart';
import 'package:kafka_client/src/consumer_group_metadata.dart';
import 'package:kafka_client/src/group_membership_protocol.dart';
import 'package:kafka_client/src/kafka_client_base.dart';
import 'package:kafka_client/src/native_classic_group_driver.dart';
import 'package:kafka_client/src/reliability.dart';
import 'package:test/test.dart';

class FakeClient extends KafkaClient {
  final requests = <KafkaRequest>[];
  final metadataQueries = <Set<String>>[];
  List<ApiVersion> versions = [
    ApiVersion(11, 4, 4),
    ApiVersion(14, 2, 2),
    ApiVersion(12, 2, 2),
    ApiVersion(13, 2, 2)
  ];
  Map<String, List<int>> partitions = {
    'x': [0, 1, 2]
  };
  List<int> assignment = ConsumerGroupAssignment({
    'x': [0]
  }).toBytes();
  Object? failure;
  int error = 0;
  int throttle = 0;
  bool echoLeader = true;
  bool closed = false;
  MetadataResponse? metadataResponse;
  FakeClient() : super(brokers: [Broker('unused.invalid', 9092)]);
  @override
  Future<Broker> findGroupCoordinator(String groupId) async => brokers.first;
  @override
  Future<dynamic> send(KafkaRequest request,
      {Broker? broker, KafkaCancellationToken? cancellationToken}) async {
    requests.add(request);
    if (request is ApiVersionsRequest) {
      return ApiVersionsResponse(0, versions, 0);
    }
    if (failure != null) throw failure!;
    if (request is JoinGroupRequest) {
      return JoinGroupResponse(0, throttle, error, 1, 'range', 'a', 'b', []);
    }
    if (request is SyncGroupRequest) {
      return SyncGroupResponse(
          0,
          throttle,
          error,
          echoLeader && request.assignments.isNotEmpty
              ? request.assignments
                  .singleWhere((a) => a.memberId == request.memberId)
                  .assignment
              : assignment);
    }
    if (request is HeartbeatRequest) {
      return HeartbeatResponse(0, throttle, error);
    }
    if (request is LeaveGroupRequest) {
      return LeaveGroupResponse(0, throttle, error);
    }
    throw StateError('Unexpected request');
  }

  @override
  Future<MetadataResponse> metadata([Set<String>? topics]) async {
    metadataQueries.add(Set.of(topics!));
    return metadataResponse ??
        MetadataResponse(0, [], [
          for (final topic in topics)
            TopicMetadata(errorCode: 0, name: topic, partitions: [
              for (final id in partitions[topic] ?? [])
                PartitionMetadata(
                    errorCode: 0,
                    partitionIndex: id,
                    leaderId: 0,
                    replicaNodes: [0],
                    isrNodes: [0])
            ])
        ]);
  }

  @override
  Future<void> close() async {
    closed = true;
  }
}

NativeClassicGroupDriver driver(FakeClient client, [Set<String>? topics]) =>
    NativeClassicGroupDriver(
        client: client,
        groupId: 'g',
        topics: topics ?? {'x'},
        sessionTimeoutMs: 10000,
        rebalanceTimeoutMs: 10000);
JoinGroupResponse follower() =>
    JoinGroupResponse(0, 0, 0, 1, 'range', 'a', 'b', []);
JoinGroupResponse leader(List<JoinedGroupMember> members) =>
    JoinGroupResponse(0, 0, 0, 1, 'range', 'a', 'a', members);
JoinedGroupMember member(String id, List<String> topics) =>
    JoinedGroupMember(id, ConsumerGroupSubscription(topics).toBytes());

void main() {
  test('requires every fixed API version before any mutation', () async {
    for (final key in [11, 14, 12, 13]) {
      final client = FakeClient()..versions.removeWhere((v) => v.apiKey == key);
      await expectLater(driver(client).prepare(), throwsUnsupportedError);
      expect(client.requests.every((r) => r is ApiVersionsRequest), isTrue);
    }
  });
  test('join offers only eager range metadata0 and snapshots topics', () async {
    final client = FakeClient();
    final topics = {'x'};
    final d = driver(client, topics);
    topics.add('later');
    await d.prepare();
    client.error = 79;
    expect((await d.join('')).errorCode, 79);
    final request = client.requests.whereType<JoinGroupRequest>().single;
    expect(request.protocols.single.name, 'range');
    final metadata =
        ConsumerGroupSubscription.fromBytes(request.protocols.single.metadata);
    expect(metadata.version, 0);
    expect(metadata.topics, ['x']);
    await d.close();
    expect(client.closed, isTrue);
  });
  test('leader queries full union and sends exact per-member ranges', () async {
    final client = FakeClient()
      ..partitions = {
        'x': [2, 0, 1],
        'y': [0, 1, 2, 3, 4]
      };
    final d = driver(client);
    await d.prepare();
    await d.sync(leader([
      member('c', ['y']),
      member('b', ['x', 'y']),
      member('a', ['x'])
    ]));
    expect(client.metadataQueries.single, {'x', 'y'});
    final sent =
        client.requests.whereType<SyncGroupRequest>().single.assignments;
    expect(sent.map((a) => a.memberId), ['a', 'b', 'c']);
    expect(ConsumerGroupAssignment.fromBytes(sent[0].assignment).partitions, {
      'x': [0, 1]
    });
    expect(ConsumerGroupAssignment.fromBytes(sent[1].assignment).partitions, {
      'x': [2],
      'y': [0, 1, 2]
    });
    expect(ConsumerGroupAssignment.fromBytes(sent[2].assignment).partitions, {
      'y': [3, 4]
    });
  });
  test('follower sends no assignments and validates fresh local metadata',
      () async {
    final client = FakeClient();
    final d = driver(client);
    await d.prepare();
    expect((await d.sync(follower())).partitions, {
      'x': [0]
    });
    expect(client.requests.whereType<SyncGroupRequest>().single.assignments,
        isEmpty);
    expect(client.metadataQueries.single, {'x'});
  });
  test('empty assignment is valid', () async {
    final client = FakeClient()
      ..assignment = ConsumerGroupAssignment({}).toBytes();
    final d = driver(client);
    await d.prepare();
    expect((await d.sync(follower())).partitions, isEmpty);
    expect(client.metadataQueries.single, {'x'});
  });
  test('unsubscribed and nonexistent assignments are rejected', () async {
    for (final assignment in [
      {
        'other': [0]
      },
      {
        'x': [99]
      }
    ]) {
      final client = FakeClient()
        ..assignment = ConsumerGroupAssignment(assignment).toBytes();
      final d = driver(client);
      await d.prepare();
      await expectLater(d.sync(follower()), throwsFormatException);
    }
  });
  test('malformed peer metadata and rack claims stop before SyncGroup',
      () async {
    for (final bytes in <List<int>>[
      [0],
      ConsumerGroupSubscription(['x'], version: 3, rackId: 'r').toBytes()
    ]) {
      final client = FakeClient();
      final d = driver(client);
      await d.prepare();
      await expectLater(
          d.sync(leader([
            member('a', ['x']),
            JoinedGroupMember('b', bytes)
          ])),
          throwsA(anyOf(isA<FormatException>(), isA<UnsupportedError>())));
      expect(client.requests.whereType<SyncGroupRequest>(), isEmpty);
    }
  });
  test('malformed leader metadata stops before SyncGroup', () async {
    for (final parts in <List<int>>[
      [-1],
      [0, 0]
    ]) {
      final client = FakeClient()..partitions = {'x': parts};
      final d = driver(client);
      await d.prepare();
      await expectLater(
          d.sync(leader([
            member('a', ['x'])
          ])),
          throwsFormatException);
      expect(client.requests.whereType<SyncGroupRequest>(), isEmpty);
    }
  });

  test('missing, duplicate and unexpected topics fail before leader sync',
      () async {
    final x = TopicMetadata(errorCode: 0, name: 'x', partitions: []);
    final other = TopicMetadata(errorCode: 0, name: 'other', partitions: []);
    for (final metadata in [
      <TopicMetadata>[],
      [x, x],
      [other]
    ]) {
      final client = FakeClient()
        ..metadataResponse = MetadataResponse(0, [], metadata);
      final d = driver(client);
      await d.prepare();
      await expectLater(
          d.sync(leader([
            member('a', ['x'])
          ])),
          throwsFormatException);
      expect(client.requests.whereType<SyncGroupRequest>(), isEmpty);
    }
  });
  test('topic and partition errors retain broker code and phase', () async {
    for (final topicError in [true, false]) {
      final client = FakeClient()
        ..metadataResponse = MetadataResponse(0, [], [
          TopicMetadata(errorCode: topicError ? 29 : 0, name: 'x', partitions: [
            PartitionMetadata(
                errorCode: topicError ? 0 : 5,
                partitionIndex: 0,
                leaderId: 0,
                replicaNodes: [0],
                isrNodes: [0])
          ])
        ]);
      final d = driver(client);
      await d.prepare();
      await expectLater(
          d.sync(leader([
            member('a', ['x'])
          ])),
          throwsA(isA<KafkaBrokerException>()
              .having((e) => e.errorCode, 'code', topicError ? 29 : 5)
              .having(
                  (e) => e.operation,
                  'phase',
                  topicError
                      ? 'group topic metadata'
                      : 'group partition metadata')));
      expect(client.requests.whereType<SyncGroupRequest>(), isEmpty);
    }
  });
  test('malformed local assignment is never published', () async {
    final valid = ConsumerGroupAssignment({
      'x': [0, 1]
    }).toBytes().toList();
    // Duplicate the second INT32 partition ID in a valid version0 payload.
    valid[20] = 0;
    for (final bytes in <List<int>>[
      [0],
      [0, 99],
      valid
    ]) {
      final client = FakeClient()..assignment = bytes;
      final d = driver(client);
      await d.prepare();
      await expectLater(d.sync(follower()), throwsFormatException);
      expect(client.requests.whereType<SyncGroupRequest>().length, 1);
    }
  });

  test('leader rejects even valid IDs when opaque self-assignment changes',
      () async {
    for (final returned in [
      ConsumerGroupAssignment({
        'x': [0]
      }),
      ConsumerGroupAssignment({
        'x': [0, 1, 2]
      }, userData: [
        1
      ])
    ]) {
      final client = FakeClient()
        ..echoLeader = false
        ..assignment = returned.toBytes();
      final d = driver(client);
      await d.prepare();
      await expectLater(
          d.sync(leader([
            member('a', ['x'])
          ])),
          throwsFormatException);
      expect(client.requests.whereType<SyncGroupRequest>().length, 1);
    }
  });
  test('negative throttles fail closed on success and error without replay',
      () async {
    for (final code in [0, 16, 79]) {
      for (final operation in ['join', 'sync', 'heartbeat', 'leave']) {
        final client = FakeClient()
          ..throttle = -1
          ..error = code;
        final d = driver(client);
        await d.prepare();
        final result = switch (operation) {
          'join' => d.join(''),
          'sync' => d.sync(follower()),
          'heartbeat' => d.heartbeat(follower()),
          _ => d.leave('b'),
        };
        await expectLater(result, throwsFormatException);
        expect(
            client.requests.where((r) => r is! ApiVersionsRequest).length, 1);
      }
    }
  });

  test('positive throttles fail closed on success and error without replay',
      () async {
    for (final code in [0, 16, 79]) {
      for (final operation in ['join', 'sync', 'heartbeat', 'leave']) {
        final client = FakeClient()
          ..throttle = 1
          ..error = code;
        final d = driver(client);
        await d.prepare();
        final result = switch (operation) {
          'join' => d.join(''),
          'sync' => d.sync(follower()),
          'heartbeat' => d.heartbeat(follower()),
          _ => d.leave('b'),
        };
        await expectLater(result, throwsUnsupportedError);
        expect(
            client.requests.where((r) => r is! ApiVersionsRequest).length, 1);
      }
    }
  });
  test('all mutations dispatch once on timeout and broker error', () async {
    for (final failure in [TimeoutException('ambiguous'), null]) {
      for (final operation in ['join', 'sync', 'heartbeat', 'leave']) {
        final client = FakeClient()
          ..failure = failure
          ..error = 16;
        final d = driver(client);
        await d.prepare();
        final result = switch (operation) {
          'join' => d.join(''),
          'sync' => d.sync(follower()),
          'heartbeat' => d.heartbeat(follower()),
          _ => d.leave('b'),
        };
        await expectLater(
            result,
            throwsA(failure == null
                ? isA<KafkaBrokerException>()
                    .having((e) => e.errorCode, 'code', 16)
                : isA<TimeoutException>()));
        expect(
            client.requests.where((r) => r is! ApiVersionsRequest).length, 1);
      }
    }
  });
}
