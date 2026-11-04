import 'dart:convert';
import 'dart:typed_data';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';

List<int> int16(int value) =>
    (ByteData(2)..setInt16(0, value)).buffer.asUint8List();
List<int> int32(int value) =>
    (ByteData(4)..setInt32(0, value)).buffer.asUint8List();
List<int> int64(int value) =>
    (ByteData(8)..setInt64(0, value)).buffer.asUint8List();
List<int> string(String? value) => value == null
    ? int16(-1)
    : [...int16(utf8.encode(value).length), ...utf8.encode(value)];

List<int> expected(GroupOffsetCommitRequest request, int generation,
    String member, Map<int, int> offsets, String? metadata) {
  final body = <int>[
    ...int16(8),
    ...int16(6),
    ...int32(request.correlationId),
    ...string('dart_kafka'),
    ...string('g'),
    ...int32(generation),
    ...string(member),
    ...int32(1),
    ...string('t'),
    ...int32(offsets.length),
    for (final entry in offsets.entries) ...[
      ...int32(entry.key),
      ...int64(entry.value),
      ...int32(-1),
      ...string(metadata),
    ],
  ];
  return [...int32(body.length), ...body];
}

void main() {
  test('manual OffsetCommit v6 bytes and sentinel identity remain unchanged',
      () {
    final request = GroupOffsetCommitRequest('g', 't', {0: 23});
    expect(request.generationId, -1);
    expect(request.memberId, '');
    expect(request.toBytes(), expected(request, -1, '', {0: 23}, null));
  });

  test('member OffsetCommit v6 carries caller generation and member exactly',
      () {
    final request = MemberOffsetCommitRequest('g', 't', {0: 23, 7: 0},
        generationId: 42, memberId: 'member-π', metadata: 'processed-☃');
    expect(request.apiKey, 8);
    expect(request.apiVersion, 6);
    expect(request.generationId, 42);
    expect(request.memberId, 'member-π');
    expect(request.toBytes(),
        expected(request, 42, 'member-π', {0: 23, 7: 0}, 'processed-☃'));
  });

  test('member commit snapshots offsets and exposes an immutable map', () {
    final offsets = {0: 123};
    final request = MemberOffsetCommitRequest('g', 't', offsets,
        generationId: 0, memberId: 'member');
    offsets[0] = 999;
    offsets[1] = 777;
    expect(request.nextOffsets, {0: 123});
    expect(() => request.nextOffsets[0] = 999, throwsUnsupportedError);
    expect(request.toBytes(), expected(request, 0, 'member', {0: 123}, null));
  });

  for (final generation in [-1, -2147483648, 2147483648]) {
    test('member commit rejects generation $generation before sending', () {
      expect(
          () => MemberOffsetCommitRequest('g', 't', {0: 0},
              generationId: generation, memberId: 'member'),
          throwsArgumentError);
    });
  }
  test('member generation supports exact nonnegative INT32 boundaries', () {
    for (final generation in [0, 2147483647]) {
      final request = MemberOffsetCommitRequest('g', 't', {0: 0},
          generationId: generation, memberId: 'member');
      expect(request.toBytes(),
          expected(request, generation, 'member', {0: 0}, null));
    }
  });

  final invalidMembers = ['', 'a' * 32768, '\ud800', '\udfff'];
  for (var i = 0; i < invalidMembers.length; i++) {
    test('member commit rejects invalid member string $i', () {
      expect(
          () => MemberOffsetCommitRequest('g', 't', {0: 0},
              generationId: 1, memberId: invalidMembers[i]),
          throwsArgumentError);
    });
  }

  test(
      'member commit retains shared group topic offset and metadata validation',
      () {
    final operations = <void Function()>[
      () => MemberOffsetCommitRequest('', 't', {0: 0},
          generationId: 1, memberId: 'm'),
      () => MemberOffsetCommitRequest('g', '', {0: 0},
          generationId: 1, memberId: 'm'),
      () => MemberOffsetCommitRequest('g', 't', {},
          generationId: 1, memberId: 'm'),
      () => MemberOffsetCommitRequest('g', 't', {-1: 0},
          generationId: 1, memberId: 'm'),
      () => MemberOffsetCommitRequest('g', 't', {2147483648: 0},
          generationId: 1, memberId: 'm'),
      () => MemberOffsetCommitRequest('g', 't', {0: -1},
          generationId: 1, memberId: 'm'),
      () => MemberOffsetCommitRequest('g', 't', {0: 0},
          generationId: 1, memberId: 'm', metadata: 'x' * 32768),
    ];
    for (final operation in operations) {
      expect(operation, throwsArgumentError);
    }
  });

  test('member commit returns raw partial and membership rejection codes', () {
    final request = MemberOffsetCommitRequest('g', 't', {0: 123, 1: 456},
        generationId: 7, memberId: 'm');
    final bytes = [
      ...int32(request.correlationId),
      ...int32(0),
      ...int32(1),
      ...string('t'),
      ...int32(2),
      ...int32(0),
      ...int16(0),
      ...int32(1),
      ...int16(22),
    ];
    final response = request.parseResponse(bytes);
    expect(response.topics.single.partitions.map((p) => p.errorCode), [0, 22]);
    for (var length = 0; length < bytes.length; length++) {
      expect(() => request.parseResponse(bytes.sublist(0, length)),
          throwsFormatException);
    }
    expect(() => request.parseResponse([...bytes, 0]), throwsFormatException);
    expect(
        () => request.parseResponse([
              ...int32(request.correlationId + 1),
              ...bytes.sublist(4),
            ]),
        throwsFormatException);
  });

  test(
      'member commit rejects omitted duplicate or unexpected response coverage',
      () {
    final request = MemberOffsetCommitRequest('g', 't', {0: 123, 1: 456},
        generationId: 7, memberId: 'm');
    List<int> response(String topic, List<int> partitions) => [
          ...int32(request.correlationId),
          ...int32(0),
          ...int32(1),
          ...string(topic),
          ...int32(partitions.length),
          for (final partition in partitions) ...[
            ...int32(partition),
            ...int16(0)
          ],
        ];
    for (final bytes in [
      [...int32(request.correlationId), ...int32(0), ...int32(0)],
      response('t', [0]),
      response('other', [0, 1]),
      response('t', [0, 0]),
      response('t', [0, 2]),
      response('t', [0, 1, 2]),
    ]) {
      expect(() => request.parseResponse(bytes), throwsFormatException);
    }
    final complete = request.parseResponse(response('t', [1, 0]));
    expect(
        complete.topics.single.partitions.map((p) => p.partitionIndex), [1, 0]);
  });

  test('raw manual request parsing remains unchanged by member coverage checks',
      () {
    final request = GroupOffsetCommitRequest('g', 't', {0: 1});
    final response = request.parseResponse([
      ...int32(request.correlationId),
      ...int32(0),
      ...int32(0),
    ]);
    expect(response.topics, isEmpty);
  });
}
