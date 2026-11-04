import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

typedef _Reply = List<int>? Function(int key, int id, KafkaBytesReader reader);

class _Peer {
  final ServerSocket server;
  final sockets = <Socket>[];
  final keys = <int>[];
  late _Reply reply;

  _Peer(this.server) {
    server.listen((socket) {
      sockets.add(socket);
      final buffer = <int>[];
      socket.listen((data) {
        buffer.addAll(data);
        while (buffer.length >= 4) {
          final length = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (buffer.length < length + 4) break;
          final reader =
              KafkaBytesReader.fromBytes(buffer.sublist(4, length + 4));
          buffer.removeRange(0, length + 4);
          final key = reader.readInt16();
          reader.readInt16();
          final id = reader.readInt32();
          reader.readString();
          keys.add(key);
          final response = reply(key, id, reader);
          if (response != null) socket.add(KafkaRequest.frame(response));
        }
      }, onError: (Object _) {});
    });
  }

  static Future<_Peer> start() async =>
      _Peer(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
  }
}

class _Cluster {
  final _Peer bootstrap, leader, alternate;
  late MetadataResponse metadata;
  final produceSeen = Completer<void>();
  int offset = 42, throttle = 0, appendTime = -1, errorCode = 0;

  _Cluster(this.bootstrap, this.leader, this.alternate) {
    metadata = MetadataResponse(0, [
      broker()
    ], [
      topic([partition()])
    ]);
    for (final peer in peers) {
      peer.reply = (key, id, reader) {
        final b = KafkaBytesBuilder()..addInt32(id);
        if (key == 18) {
          b
            ..addInt16(0)
            ..addInt32(3);
          for (final entry in const {3: 0, 0: 3, 1: 4}.entries) {
            b
              ..addInt16(entry.key)
              ..addInt16(entry.value)
              ..addInt16(entry.value);
          }
        } else if (key == 3) {
          b.addInt32(metadata.brokers.length);
          for (final broker in metadata.brokers) {
            b
              ..addInt32(broker.nodeId)
              ..addString(broker.host)
              ..addInt32(broker.port);
          }
          b.addInt32(metadata.topics.length);
          for (final topic in metadata.topics) {
            b
              ..addInt16(topic.errorCode)
              ..addString(topic.name)
              ..addInt32(topic.partitions.length);
            for (final partition in topic.partitions) {
              b
                ..addInt16(partition.errorCode)
                ..addInt32(partition.partitionIndex)
                ..addInt32(partition.leaderId)
                ..addInt32(partition.replicaNodes.length);
              for (final id in partition.replicaNodes) {
                b.addInt32(id);
              }
              b.addInt32(partition.isrNodes.length);
              for (final id in partition.isrNodes) {
                b.addInt32(id);
              }
            }
          }
        } else if (key == 0) {
          if (!produceSeen.isCompleted) produceSeen.complete();
          expect(reader.readString(), isNull);
          final acks = reader.readInt16();
          if (acks == 0) return null;
          b
            ..addInt32(1)
            ..addString('topic')
            ..addInt32(1)
            ..addInt32(0)
            ..addInt16(errorCode)
            ..addInt64(offset)
            ..addInt64(appendTime)
            ..addInt32(throttle);
        } else if (key == 1) {
          b
            ..addInt32(0)
            ..addInt32(1)
            ..addString('topic')
            ..addInt32(1)
            ..addInt32(0)
            ..addInt16(0)
            ..addInt64(0)
            ..addInt64(0)
            ..addInt32(-1)
            ..addBytes([]);
        } else {
          fail('Unexpected API $key');
        }
        return b.takeBytes();
      };
    }
  }

  List<_Peer> get peers => [bootstrap, leader, alternate];

  BrokerMetadata broker({int id = 7, String host = '127.0.0.1', int? port}) =>
      BrokerMetadata(nodeId: id, host: host, port: port ?? leader.server.port);

  PartitionMetadata partition({int id = 0, int leader = 7, int code = 0}) =>
      PartitionMetadata(
          errorCode: code,
          partitionIndex: id,
          leaderId: leader,
          replicaNodes: [7],
          isrNodes: [7]);

  TopicMetadata topic(List<PartitionMetadata> partitions, {int code = 0}) =>
      TopicMetadata(errorCode: code, name: 'topic', partitions: partitions);

  static Future<_Cluster> start() async =>
      _Cluster(await _Peer.start(), await _Peer.start(), await _Peer.start());

  KafkaClient client() => KafkaClient(
      brokers: [Broker('127.0.0.1', bootstrap.server.port)],
      maxReadRetries: 3,
      retryBaseDelay: Duration.zero);

  Future<void> close() async {
    for (final peer in peers) {
      await peer.close();
    }
  }

  void expectDispatchedOnce(int key) {
    expect(bootstrap.keys, [18, 3]);
    expect(leader.keys, [18, key]);
    expect(alternate.keys, isEmpty);
    expect(alternate.sockets, isEmpty);
  }
}

void main() {
  final ambiguous = <String, void Function(_Cluster)>{
    'same broker endpoint': (c) => c.metadata.brokers.add(c.broker()),
    'different broker endpoint': (c) =>
        c.metadata.brokers.add(c.broker(port: c.alternate.server.port)),
    'valid then invalid broker endpoint': (c) =>
        c.metadata.brokers.add(c.broker(host: '')),
    'invalid then valid broker endpoint': (c) =>
        c.metadata.brokers.insert(0, c.broker(port: 0)),
    'three broker endpoints': (c) => c.metadata.brokers
        .addAll([c.broker(port: c.alternate.server.port), c.broker(host: '')]),
    'same topic': (c) => c.metadata.topics.add(c.topic([c.partition()])),
    'conflicting topic': (c) =>
        c.metadata.topics.add(c.topic([c.partition(leader: 8)])),
    'successful then failed topic': (c) =>
        c.metadata.topics.add(c.topic([c.partition()], code: 29)),
    'same partition': (c) =>
        c.metadata.topics.single.partitions.add(c.partition()),
    'conflicting partition': (c) =>
        c.metadata.topics.single.partitions.add(c.partition(leader: 8)),
    'warning then successful partition': (c) {
      c.metadata.topics.single.partitions
        ..clear()
        ..addAll([c.partition(code: 9), c.partition()]);
    },
  };
  for (final operation in ['Produce', 'Fetch']) {
    for (final scenario in ambiguous.entries) {
      test('$operation rejects ambiguous ${scenario.key} before dispatch',
          () async {
        final c = await _Cluster.start();
        final client = c.client();
        try {
          scenario.value(c);
          final action = operation == 'Produce'
              ? client.produce('topic', [
                  KafkaRecord(value: [1])
                ])
              : client.fetch('topic');
          await expectLater(action, throwsFormatException);
          // Nonzero read retry allowance must not replay malformed discovery.
          expect(c.bootstrap.keys, [18, 3]);
          for (final peer in [c.leader, c.alternate]) {
            expect(peer.keys, isEmpty);
            expect(peer.sockets, isEmpty);
          }
        } finally {
          await client.close();
          await c.close();
        }
      });
    }
    for (final code in [0, 9]) {
      test('$operation accepts unique live leader with partition code $code',
          () async {
        final c = await _Cluster.start();
        final client = c.client();
        try {
          c.metadata.topics.single.partitions
            ..clear()
            ..add(c.partition(code: code));
          if (operation == 'Produce') {
            expect(
                await client.produce('topic', [
                  KafkaRecord(value: [1])
                ]),
                isNotNull);
          } else {
            expect(
                (await client.fetch('topic'))
                    .topics
                    .single
                    .partitions
                    .single
                    .records,
                isEmpty);
          }
          c.expectDispatchedOnce(operation == 'Produce' ? 0 : 1);
        } finally {
          await client.close();
          await c.close();
        }
      });
    }
  }

  test('raw Metadata preserves duplicate routing diagnostics', () async {
    final c = await _Cluster.start();
    final client = c.client();
    try {
      ambiguous['different broker endpoint']!(c);
      ambiguous['same partition']!(c);
      ambiguous['same topic']!(c);
      final result = await client.metadata({'topic'});
      expect(
          result.toJson(),
          MetadataResponse(
                  result.correlationId, c.metadata.brokers, c.metadata.topics)
              .toJson());
      expect(c.bootstrap.keys, [18, 3]);
      expect(c.leader.keys, isEmpty);
      expect(c.alternate.keys, isEmpty);
    } finally {
      await client.close();
      await c.close();
    }
  });

  for (final acks in [1, -1]) {
    final malformed = <String, void Function(_Cluster)>{
      'offset -1': (c) => c.offset = -1,
      'offset -2': (c) => c.offset = -2,
      'minimum INT64 offset': (c) => c.offset = -9223372036854775808,
      'throttle -1': (c) => c.throttle = -1,
      'minimum INT32 throttle': (c) => c.throttle = -2147483648,
    };
    for (final scenario in malformed.entries) {
      test('acks=$acks rejects successful ${scenario.key} without replay',
          () async {
        final c = await _Cluster.start();
        final client = c.client();
        try {
          scenario.value(c);
          await expectLater(
              client.produce(
                  'topic',
                  [
                    KafkaRecord(value: [1])
                  ],
                  acks: acks),
              throwsFormatException);
          c.expectDispatchedOnce(0);
        } finally {
          await client.close();
          await c.close();
        }
      });
    }
    for (final values in [
      [0, 0, -1],
      [42, 1, 0],
      [9223372036854775807, 2147483647, 1700000000000],
    ]) {
      test('acks=$acks accepts offset/throttle/append-time $values', () async {
        final c = await _Cluster.start();
        final client = c.client();
        try {
          c
            ..offset = values[0]
            ..throttle = values[1]
            ..appendTime = values[2];
          final result = (await client.produce(
              'topic',
              [
                KafkaRecord(value: [1])
              ],
              acks: acks))!;
          expect(result.responses.single.partitionResponses.single.baseOffset,
              values[0]);
          expect(result.throttleTimeMs, values[1]);
          expect(
              result.responses.single.partitionResponses.single.logAppendTimeMs,
              values[2]);
          c.expectDispatchedOnce(0);
        } finally {
          await client.close();
          await c.close();
        }
      });
    }
    for (final code in [6, 9]) {
      test('acks=$acks retains broker error $code and sentinels without replay',
          () async {
        final c = await _Cluster.start();
        final client = c.client();
        try {
          c
            ..errorCode = code
            ..offset = -1
            ..appendTime = -1;
          await expectLater(
              client.produce(
                  'topic',
                  [
                    KafkaRecord(value: [1])
                  ],
                  acks: acks),
              throwsA(isA<KafkaBrokerException>()
                  .having((e) => e.errorCode, 'code', code)));
          c.expectDispatchedOnce(0);
        } finally {
          await client.close();
          await c.close();
        }
      });
    }
  }

  test('acks=0 needs no offset or throttle response', () async {
    final c = await _Cluster.start();
    final client = c.client();
    try {
      expect(
          await client.produce(
              'topic',
              [
                KafkaRecord(value: [1])
              ],
              acks: 0),
          isNull);
      await c.produceSeen.future;
      c.expectDispatchedOnce(0);
    } finally {
      await client.close();
      await c.close();
    }
  });

  test('raw Produce parser retains unavailable offset and CreateTime sentinel',
      () {
    final b = KafkaBytesBuilder()
      ..addInt32(5)
      ..addInt32(1)
      ..addString('topic')
      ..addInt32(1)
      ..addInt32(0)
      ..addInt16(9)
      ..addInt64(-1)
      ..addInt64(-1)
      ..addInt32(0);
    final response = ProduceResponse.fromBytes(b.takeBytes());
    final partition = response.responses.single.partitionResponses.single;
    expect(partition.errorCode, 9);
    expect(partition.baseOffset, -1);
    expect(partition.logAppendTimeMs, -1);
  });
}
