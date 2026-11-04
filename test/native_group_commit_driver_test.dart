import 'dart:async';

import 'package:kafka_client/src/api/abstract/kafka_request.dart';
import 'package:kafka_client/src/api/response/api18_api_versions_response.dart';
import 'package:kafka_client/src/broker/broker.dart';
import 'package:kafka_client/src/group_offsets_protocol.dart';
import 'package:kafka_client/src/reliability.dart';
import 'package:test/test.dart';

import 'native_classic_group_driver_test.dart' as fixture;

class CommitClient extends fixture.FakeClient {
  final commits = <MemberOffsetCommitRequest>[];
  final targets = <Broker?>[];
  final cancellations = <KafkaCancellationToken?>[];
  Object? commitError;
  GroupOffsetCommitResponse? commitResponse;

  @override
  Future<dynamic> send(KafkaRequest request,
      {Broker? broker, KafkaCancellationToken? cancellationToken}) {
    if (request is! MemberOffsetCommitRequest) {
      return super
          .send(request, broker: broker, cancellationToken: cancellationToken);
    }
    commits.add(request);
    targets.add(broker);
    cancellations.add(cancellationToken);
    if (commitError != null) return Future.error(commitError!);
    return Future.value(commitResponse ??
        GroupOffsetCommitResponse(request.correlationId, 0, [
          GroupOffsetCommitTopicResponse(request.topic, [
            for (final p in request.nextOffsets.keys)
              GroupOffsetCommitPartitionResponse(p, 0)
          ])
        ]));
  }
}

void main() {
  for (final range in [
    <int>[],
    [0, 5],
    [7, 9]
  ]) {
    test(
        'membership may prepare but processing rejects unsupported commit range $range',
        () async {
      final client = CommitClient();
      if (range.isNotEmpty) {
        client.versions.add(ApiVersion(8, range.first, range.last));
      }
      final driver = fixture.driver(client);
      await driver.prepare();
      expect(driver.requireMemberCommitSupport, throwsUnsupportedError);
      expect(
          () => driver.commit('x', {0: 3},
              generationId: 7,
              memberId: 'member',
              cancellationToken: KafkaCancellationToken()),
          throwsUnsupportedError);
      expect(client.commits, isEmpty);
      await driver.close();
    });
  }
  test(
      'native commit carries exact generation member topic offsets and cancellation',
      () async {
    final client = CommitClient()..versions.add(ApiVersion(8, 0, 9));
    final driver = fixture.driver(client);
    await driver.prepare();
    driver.requireMemberCommitSupport();
    final cancellation = KafkaCancellationToken();
    final offsets = {0: 123, 2: 456};
    final future = driver.commit('x', offsets,
        generationId: 7, memberId: 'member', cancellationToken: cancellation);
    offsets[0] = 999;
    final result = await future, request = client.commits.single;
    expect(request.generationId, 7);
    expect(request.memberId, 'member');
    expect(request.groupId, 'g');
    expect(request.topic, 'x');
    expect(request.nextOffsets, {0: 123, 2: 456});
    expect(request.metadata, isNull);
    expect(client.cancellations.single, same(cancellation));
    expect(client.targets.single, same(client.brokers.first));
    expect(result.topics.single.partitions, hasLength(2));
    expect(client.metadataQueries, isEmpty);
    await driver.close();
  });
  for (final error in [
    TimeoutException('lost'),
    KafkaConnectionException(),
    KafkaBrokerException(16, 'commit')
  ]) {
    test('native commit $error is single attempt without rediscovery',
        () async {
      final client = CommitClient()
        ..versions.add(ApiVersion(8, 6, 6))
        ..commitError = error;
      final driver = fixture.driver(client);
      await driver.prepare();
      final before = client.requests.length;
      await expectLater(
          driver.commit('x', {0: 3},
              generationId: 7,
              memberId: 'm',
              cancellationToken: KafkaCancellationToken()),
          throwsA(same(error)));
      expect(client.commits, hasLength(1));
      expect(client.requests.length, before);
      await driver.close();
    });
  }
  test(
      'negative/positive response evidence remains for managed layer interpretation',
      () async {
    final client = CommitClient()..versions.add(ApiVersion(8, 6, 6));
    final driver = fixture.driver(client);
    await driver.prepare();
    for (final throttle in [-1, 5]) {
      final response = GroupOffsetCommitResponse(0, throttle, [
        GroupOffsetCommitTopicResponse(
            'x', [GroupOffsetCommitPartitionResponse(0, 22)])
      ]);
      client.commitResponse = response;
      expect(
          await driver.commit('x', {0: 3},
              generationId: 7,
              memberId: 'm',
              cancellationToken: KafkaCancellationToken()),
          same(response));
    }
    expect(client.commits, hasLength(2));
    await driver.close();
  });
}
