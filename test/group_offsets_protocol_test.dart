import 'dart:convert';
import 'dart:typed_data';

import 'package:kafka_client/src/api/abstract/kafka_request.dart';
import 'package:kafka_client/src/group_offsets_protocol.dart';
import 'package:test/test.dart';

// Independent wire fixtures: deliberately do not use KafkaBytesBuilder/Reader.
List<int> i16(int n) => (ByteData(2)..setInt16(0, n)).buffer.asUint8List();
List<int> i32(int n) => (ByteData(4)..setInt32(0, n)).buffer.asUint8List();
List<int> i64(int n) => (ByteData(8)..setInt64(0, n)).buffer.asUint8List();
List<int> str(String? s) =>
    s == null ? i16(-1) : [...i16(utf8.encode(s).length), ...utf8.encode(s)];
List<int> framed(int key, int version, int id, List<int> body) {
  final payload = [
    ...i16(key),
    ...i16(version),
    ...i32(id),
    ...str('dart_kafka'),
    ...body
  ];
  return [...i32(payload.length), ...payload];
}

List<int> coordinator(int id,
        {int error = 0,
        String? message,
        String? host = 'broker',
        int node = 3,
        int port = 9092}) =>
    [
      ...i32(id),
      ...i32(12),
      ...i16(error),
      ...str(message),
      ...i32(node),
      ...str(host),
      ...i32(port),
    ];
List<int> fetch(int id,
        {int error = 0,
        int partitionError = 0,
        int offset = 42,
        int epoch = 7,
        int index = 2,
        String? metadata = 'saved',
        String? topic = 't',
        int count = 1}) =>
    [
      ...i32(id),
      ...i32(13),
      ...i32(1),
      ...str(topic),
      ...i32(count),
      ...i32(index),
      ...i64(offset),
      ...i32(epoch),
      ...str(metadata),
      ...i16(partitionError),
      ...i16(error),
    ];
List<int> commit(int id,
        {int error = 0, int index = 2, String? topic = 't', int count = 1}) =>
    [
      ...i32(id),
      ...i32(14),
      ...i32(1),
      ...str(topic),
      ...i32(count),
      ...i32(index),
      ...i16(error),
    ];

void main() {
  test('FindCoordinator v2 exact header and group coordinator key type', () {
    final r = FindCoordinatorRequest('grπ');
    expect(r.apiKey, 10);
    expect(r.apiVersion, 2);
    expect(r.toBytes(), framed(10, 2, r.correlationId, [...str('grπ'), 0]));
  });

  test('OffsetFetch v5 exact explicit single-topic request', () {
    final r = GroupOffsetFetchRequest('g', 't', {2, 7});
    expect(r.apiKey, 9);
    expect(r.apiVersion, 5);
    expect(
        r.toBytes(),
        framed(9, 5, r.correlationId, [
          ...str('g'),
          ...i32(1),
          ...str('t'),
          ...i32(2),
          ...i32(2),
          ...i32(7),
        ]));
  });

  test('OffsetCommit v6 exact manual generation/member and leader epoch', () {
    for (final metadata in [null, '', 'π']) {
      final r = GroupOffsetCommitRequest(
          'g', 't', {2: 0, 7: 9223372036854775807},
          metadata: metadata);
      expect(r.apiKey, 8);
      expect(r.apiVersion, 6);
      expect(
          r.toBytes(),
          framed(8, 6, r.correlationId, [
            ...str('g'),
            ...i32(-1),
            ...str(''),
            ...i32(1),
            ...str('t'),
            ...i32(2),
            ...i32(2),
            ...i64(0),
            ...i32(-1),
            ...str(metadata),
            ...i32(7),
            ...i64(9223372036854775807),
            ...i32(-1),
            ...str(metadata),
          ]));
    }
  });

  test('request inputs are immutable snapshots', () {
    final partitions = {2};
    final offsets = {2: 42};
    final f = GroupOffsetFetchRequest('g', 't', partitions);
    final c = GroupOffsetCommitRequest('g', 't', offsets);
    final beforeF = f.toBytes();
    final beforeC = c.toBytes();
    partitions.add(3);
    offsets[2] = 99;
    expect(f.toBytes(), beforeF);
    expect(c.toBytes(), beforeC);
    expect(() => f.partitions.add(3), throwsUnsupportedError);
    expect(() => c.nextOffsets[2] = 99, throwsUnsupportedError);
  });

  test('validates nonempty and UTF8 byte-limited names and metadata', () {
    final max = '${List.filled(16383, 'π').join()}x';
    final tooLong = List.filled(16384, 'π').join();
    expect(() => FindCoordinatorRequest(max).toBytes(), returnsNormally);
    expect(() => GroupOffsetFetchRequest(max, max, {2147483647}).toBytes(),
        returnsNormally);
    expect(
        () => GroupOffsetCommitRequest(
                max, max, {2147483647: 9223372036854775807},
                metadata: max)
            .toBytes(),
        returnsNormally);
    for (final bad in ['', tooLong, String.fromCharCode(0xd800)]) {
      expect(() => FindCoordinatorRequest(bad), throwsArgumentError);
      expect(() => GroupOffsetFetchRequest(bad, 't', {0}), throwsArgumentError);
      expect(() => GroupOffsetFetchRequest('g', bad, {0}), throwsArgumentError);
      expect(() => GroupOffsetCommitRequest(bad, 't', {0: 0}),
          throwsArgumentError);
      expect(() => GroupOffsetCommitRequest('g', bad, {0: 0}),
          throwsArgumentError);
    }
    expect(() => GroupOffsetCommitRequest('g', 't', {0: 0}, metadata: tooLong),
        throwsArgumentError);
    expect(
        () => GroupOffsetCommitRequest('g', 't', {0: 0},
            metadata: String.fromCharCode(0xd800)),
        throwsArgumentError);
  });

  test('validates partitions and nonnegative next offsets before serialization',
      () {
    expect(() => GroupOffsetFetchRequest('g', 't', {}), throwsArgumentError);
    expect(() => GroupOffsetCommitRequest('g', 't', {}), throwsArgumentError);
    for (final index in [-1, 2147483648]) {
      expect(() => GroupOffsetFetchRequest('g', 't', {index}),
          throwsArgumentError);
      expect(() => GroupOffsetCommitRequest('g', 't', {index: 0}),
          throwsArgumentError);
    }
    for (final offset in [-1, -9223372036854775808]) {
      expect(() => GroupOffsetCommitRequest('g', 't', {0: offset}),
          throwsArgumentError);
    }
  });

  test('coordinator preserves success and signed error with sentinel endpoint',
      () {
    final r = FindCoordinatorRequest('g');
    final success = r.parseResponse(coordinator(r.correlationId));
    expect(success.toJson(), {
      'correlationId': r.correlationId,
      'throttleTimeMs': 12,
      'errorCode': 0,
      'errorMessage': null,
      'nodeId': 3,
      'host': 'broker',
      'port': 9092
    });
    final failed = r.parseResponse(coordinator(r.correlationId,
        error: -1, message: 'unavailable', host: '', node: -1, port: -1));
    expect(failed.errorCode, -1);
    expect(failed.errorMessage, 'unavailable');
    expect(failed.nodeId, -1);
  });

  test(
      'fetch preserves present/absent offsets, epochs, metadata and both errors',
      () {
    final r = GroupOffsetFetchRequest('g', 't', {2});
    final success = r.parseResponse(fetch(r.correlationId));
    expect(success.throttleTimeMs, 13);
    expect(success.errorCode, 0);
    expect(success.topics.single.name, 't');
    expect(success.topics.single.partitions.single.toJson(), {
      'partitionIndex': 2,
      'committedOffset': 42,
      'committedLeaderEpoch': 7,
      'metadata': 'saved',
      'errorCode': 0
    });
    final absent = r.parseResponse(
        fetch(r.correlationId, offset: -1, epoch: -1, metadata: null));
    expect(absent.topics.single.partitions.single.committedOffset, -1);
    expect(absent.topics.single.partitions.single.metadata, isNull);
    final failed = r.parseResponse(
        fetch(r.correlationId, error: -1, partitionError: -32768));
    expect(failed.errorCode, -1);
    expect(failed.topics.single.partitions.single.errorCode, -32768);
    expect(success.toJson()['topics'], isA<List>());
    expect(() => success.topics.clear(), throwsUnsupportedError);
    expect(
        () => success.topics.single.partitions.clear(), throwsUnsupportedError);
  });

  test('commit preserves success and signed partition errors', () {
    final r = GroupOffsetCommitRequest('g', 't', {2: 42});
    final success = r.parseResponse(commit(r.correlationId));
    expect(success.toJson(), {
      'correlationId': r.correlationId,
      'throttleTimeMs': 14,
      'topics': [
        {
          'name': 't',
          'partitions': [
            {'partitionIndex': 2, 'errorCode': 0}
          ]
        }
      ]
    });
    expect(
        r
            .parseResponse(commit(r.correlationId, error: -1))
            .topics
            .single
            .partitions
            .single
            .errorCode,
        -1);
    expect(() => success.topics.clear(), throwsUnsupportedError);
    expect(
        () => success.topics.single.partitions.clear(), throwsUnsupportedError);
  });

  test('response constructors snapshot lists at both nesting levels', () {
    final fp = [GroupOffsetFetchPartitionResponse(0, 0, -1, null, 0)];
    final ft = [GroupOffsetFetchTopicResponse('t', fp)];
    final f = GroupOffsetFetchResponse(0, 0, ft, 0);
    final cp = [GroupOffsetCommitPartitionResponse(0, 0)];
    final ct = [GroupOffsetCommitTopicResponse('t', cp)];
    final c = GroupOffsetCommitResponse(0, 0, ct);
    fp.clear();
    ft.clear();
    cp.clear();
    ct.clear();
    expect(f.topics.single.partitions.length, 1);
    expect(c.topics.single.partitions.length, 1);
  });

  test('empty topic arrays represent group errors or empty commit results', () {
    expect(
        GroupOffsetFetchResponse.fromBytes(
            [...i32(0), ...i32(0), ...i32(0), ...i16(15)]).errorCode,
        15);
    expect(
        GroupOffsetCommitResponse.fromBytes([...i32(0), ...i32(0), ...i32(0)])
            .topics,
        isEmpty);
  });

  test('every truncated prefix, trailing data and wrong correlation reject',
      () {
    final requests = <KafkaRequest>[
      FindCoordinatorRequest('g'),
      GroupOffsetFetchRequest('g', 't', {2}),
      GroupOffsetCommitRequest('g', 't', {2: 42})
    ];
    final fixtures = [
      coordinator(requests[0].correlationId),
      fetch(requests[1].correlationId),
      commit(requests[2].correlationId)
    ];
    for (var i = 0; i < requests.length; i++) {
      final r = requests[i];
      final bytes = fixtures[i];
      for (var n = 0; n < bytes.length; n++) {
        expect(
            () => r.parseResponse(bytes.sublist(0, n)), throwsFormatException,
            reason: 'request $i prefix $n');
      }
      expect(() => r.parseResponse([...bytes, 0]), throwsFormatException);
      expect(() => r.parseResponse([...i32(-1), ...bytes.skip(4)]),
          throwsFormatException);
      expect(
          () => r.parseResponse([-1, ...bytes.skip(1)]), throwsFormatException);
      expect(() => r.parseResponse([256, ...bytes.skip(1)]),
          throwsFormatException);
    }
  });

  test('strict required strings and malformed UTF8/string lengths reject', () {
    expect(() => FindCoordinatorResponse.fromBytes(coordinator(0, host: null)),
        throwsFormatException);
    expect(() => GroupOffsetFetchResponse.fromBytes(fetch(0, topic: null)),
        throwsFormatException);
    expect(() => GroupOffsetCommitResponse.fromBytes(commit(0, topic: null)),
        throwsFormatException);
    for (final malformed in [
      i16(-2),
      [...i16(1), 255],
      i16(32767)
    ]) {
      expect(
          () => FindCoordinatorResponse.fromBytes(
              [...i32(0), ...i32(0), ...i16(0), ...malformed]),
          throwsFormatException);
      expect(
          () => GroupOffsetFetchResponse.fromBytes(
              [...i32(0), ...i32(0), ...i32(1), ...malformed]),
          throwsFormatException);
      expect(
          () => GroupOffsetCommitResponse.fromBytes(
              [...i32(0), ...i32(0), ...i32(1), ...malformed]),
          throwsFormatException);
    }
  });

  test('negative and impossible topic/partition counts reject before loops',
      () {
    for (final count in [-1, -2, 2147483647]) {
      expect(
          () => GroupOffsetFetchResponse.fromBytes(
              [...i32(0), ...i32(0), ...i32(count), ...i16(0)]),
          throwsFormatException);
      expect(
          () => GroupOffsetCommitResponse.fromBytes(
              [...i32(0), ...i32(0), ...i32(count)]),
          throwsFormatException);
      expect(() => GroupOffsetFetchResponse.fromBytes(fetch(0, count: count)),
          throwsFormatException);
      expect(() => GroupOffsetCommitResponse.fromBytes(commit(0, count: count)),
          throwsFormatException);
    }
  });

  test('negative indices and offsets/epochs below the absent sentinel reject',
      () {
    expect(() => GroupOffsetFetchResponse.fromBytes(fetch(0, index: -1)),
        throwsFormatException);
    expect(() => GroupOffsetCommitResponse.fromBytes(commit(0, index: -1)),
        throwsFormatException);
    expect(() => GroupOffsetFetchResponse.fromBytes(fetch(0, offset: -2)),
        throwsFormatException);
    expect(() => GroupOffsetFetchResponse.fromBytes(fetch(0, epoch: -2)),
        throwsFormatException);
    expect(
        GroupOffsetFetchResponse.fromBytes(fetch(0,
                index: 2147483647,
                offset: 9223372036854775807,
                epoch: 2147483647))
            .topics
            .single
            .partitions
            .single
            .committedOffset,
        9223372036854775807);
  });
}
