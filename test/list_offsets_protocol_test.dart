import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';

import 'support/list_offsets_peer.dart';

void main() {
  for (final position in KafkaOffsetPosition.values) {
    test('ListOffsets v2 exact $position wire and consumer isolation', () {
      final request =
          ListOffsetsRequest('tπ', partition: 2147483647, position: position);
      expect(request.apiKey, 2);
      expect(request.apiVersion, 2);
      expect(
          request.toBytes(),
          offsetFrame([
            ...offsetI16(2),
            ...offsetI16(2),
            ...offsetI32(request.correlationId),
            ...offsetString('dart_kafka'),
            ...offsetI32(-1),
            0,
            ...offsetI32(1),
            ...offsetString('tπ'),
            ...offsetI32(1),
            ...offsetI32(2147483647),
            ...offsetI64(position == KafkaOffsetPosition.earliest ? -2 : -1),
          ]));
    });
  }

  test('request validates UTF-8 STRING and partition bounds', () {
    for (final topic in [
      '',
      List.filled(32768, 'a').join(),
      List.filled(16384, 'π').join(),
      String.fromCharCode(0xd800),
    ]) {
      expect(
          () =>
              ListOffsetsRequest(topic, position: KafkaOffsetPosition.earliest),
          throwsArgumentError);
    }
    for (final partition in [-1, 2147483648]) {
      expect(
          () => ListOffsetsRequest('topic',
              partition: partition, position: KafkaOffsetPosition.latest),
          throwsArgumentError);
    }
    expect(
        ListOffsetsRequest(List.filled(32767, 'a').join(),
                position: KafkaOffsetPosition.earliest)
            .topic
            .length,
        32767);
  });

  for (final offset in [-1, 0, 9223372036854775807]) {
    test('decoder retains offset $offset and unavailable timestamp', () {
      final response = ListOffsetsResponse.fromBytes(
          offsetResponseBytes(7, throttle: 123, topics: [
        offsetTopicBytes(name: 'tπ', partitions: [
          offsetPartitionBytes(partition: 3, offset: offset),
        ]),
      ]));
      expect(response.correlationId, 7);
      expect(response.throttleTimeMs, 123);
      expect(response.topics.single.name, 'tπ');
      final result = response.topics.single.partitions.single;
      expect(result.partition, 3);
      expect(result.errorCode, 0);
      expect(result.timestamp, -1);
      expect(result.offset, offset);
      expect(response.toJson()['topics'], [
        {
          'name': 'tπ',
          'partitions': [
            {
              'partition': 3,
              'errorCode': 0,
              'timestamp': -1,
              'offset': offset,
            }
          ],
        }
      ]);
    });
  }

  test('decoder preserves broker errors and their unavailable sentinels', () {
    for (final code in [-1, 5, 6, 9, 29, 35, 78]) {
      final result =
          ListOffsetsResponse.fromBytes(offsetResponseBytes(4, topics: [
        offsetTopicBytes(partitions: [
          offsetPartitionBytes(error: code, offset: -1),
        ]),
      ])).topics.single.partitions.single;
      expect(result.errorCode, code);
      expect(result.offset, -1);
      expect(result.timestamp, -1);
    }
  });

  test('immutable response copies lists and blocks correlation mutation', () {
    final partitions = [const ListOffsetsPartitionResponse(0, 0, -1, 0)];
    final topics = [ListOffsetsTopicResponse('topic', partitions)];
    final response = ListOffsetsResponse(1, 0, topics);
    partitions.clear();
    topics.clear();
    expect(response.topics.single.partitions.single.offset, 0);
    expect(() => response.topics.clear(), throwsUnsupportedError);
    expect(() => response.topics.single.partitions.clear(),
        throwsUnsupportedError);
    expect(() => response.correlationId = 9, throwsUnsupportedError);
    expect(response.correlationId, 1);
  });

  test('all truncated prefixes and trailing data fail', () {
    final bytes = offsetResponseBytes(1);
    for (var end = 0; end < bytes.length; end++) {
      expect(() => ListOffsetsResponse.fromBytes(bytes.sublist(0, end)),
          throwsFormatException,
          reason: 'truncation at $end');
    }
    expect(() => ListOffsetsResponse.fromBytes([...bytes, 0]),
        throwsFormatException);
  });

  test('negative/excessive counts, invalid bytes and topic strings fail', () {
    final malformed = <List<int>>[
      [-1, ...offsetResponseBytes(1).skip(1)],
      [256, ...offsetResponseBytes(1).skip(1)],
      [...offsetI32(1), ...offsetI32(0), ...offsetI32(-1)],
      [...offsetI32(1), ...offsetI32(0), ...offsetI32(2147483647)],
      offsetResponseBytes(1, throttle: -1),
      offsetResponseBytes(1, topics: [offsetTopicBytes(name: null)]),
      offsetResponseBytes(1, topics: [offsetTopicBytes(name: '')]),
      [
        ...offsetI32(1),
        ...offsetI32(0),
        ...offsetI32(1),
        ...offsetString('topic'),
        ...offsetI32(-1),
      ],
      [
        ...offsetI32(1),
        ...offsetI32(0),
        ...offsetI32(1),
        ...offsetString('topic'),
        ...offsetI32(2147483647),
      ],
      [
        ...offsetI32(1),
        ...offsetI32(0),
        ...offsetI32(1),
        ...offsetI16(1),
        255,
        ...offsetI32(0),
      ],
    ];
    for (final bytes in malformed) {
      expect(() => ListOffsetsResponse.fromBytes(bytes), throwsFormatException);
    }
  });

  test('invalid negative partition, timestamp and offset fail decoding', () {
    for (final partition in [
      offsetPartitionBytes(partition: -1),
      offsetPartitionBytes(timestamp: -2),
      offsetPartitionBytes(offset: -2),
    ]) {
      expect(
          () => ListOffsetsResponse.fromBytes(offsetResponseBytes(1, topics: [
                offsetTopicBytes(partitions: [partition])
              ])),
          throwsFormatException);
    }
  });

  test('request parser checks response correlation identity', () {
    final request =
        ListOffsetsRequest('topic', position: KafkaOffsetPosition.earliest);
    expect(
        request
            .parseResponse(offsetResponseBytes(request.correlationId))
            .correlationId,
        request.correlationId);
    expect(
        () => request
            .parseResponse(offsetResponseBytes(request.correlationId + 1)),
        throwsFormatException);
  });
}
