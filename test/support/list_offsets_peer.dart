import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

// Independent wire oracle: do not use the production byte reader or builder.
List<int> offsetI16(int value) =>
    (ByteData(2)..setInt16(0, value)).buffer.asUint8List();
List<int> offsetI32(int value) =>
    (ByteData(4)..setInt32(0, value)).buffer.asUint8List();
List<int> offsetI64(int value) =>
    (ByteData(8)..setInt64(0, value)).buffer.asUint8List();
List<int> offsetString(String? value) => value == null
    ? offsetI16(-1)
    : [...offsetI16(utf8.encode(value).length), ...utf8.encode(value)];
List<int> offsetFrame(List<int> body) => [...offsetI32(body.length), ...body];
List<int> offsetPartitionBytes(
        {int partition = 0,
        int error = 0,
        int timestamp = -1,
        int offset = 42}) =>
    [
      ...offsetI32(partition),
      ...offsetI16(error),
      ...offsetI64(timestamp),
      ...offsetI64(offset),
    ];
List<int> offsetTopicBytes(
        {String? name = 'topic', List<List<int>>? partitions}) =>
    [
      ...offsetString(name),
      ...offsetI32((partitions ?? [offsetPartitionBytes()]).length),
      for (final partition in partitions ?? [offsetPartitionBytes()])
        ...partition,
    ];
List<int> offsetResponseBytes(int id,
        {int throttle = 0, List<List<int>>? topics}) =>
    [
      ...offsetI32(id),
      ...offsetI32(throttle),
      ...offsetI32((topics ?? [offsetTopicBytes()]).length),
      for (final topic in topics ?? [offsetTopicBytes()]) ...topic,
    ];

class OffsetWireReader {
  final List<int> bytes;
  int cursor = 0;
  OffsetWireReader(this.bytes);
  int i16() {
    final value =
        ByteData.sublistView(Uint8List.fromList(bytes), cursor).getInt16(0);
    cursor += 2;
    return value;
  }

  int i32() {
    final value =
        ByteData.sublistView(Uint8List.fromList(bytes), cursor).getInt32(0);
    cursor += 4;
    return value;
  }

  int i64() {
    final value =
        ByteData.sublistView(Uint8List.fromList(bytes), cursor).getInt64(0);
    cursor += 8;
    return value;
  }

  String string() {
    final size = i16();
    final value = utf8.decode(bytes.sublist(cursor, cursor + size));
    cursor += size;
    return value;
  }
}

class OffsetWireRequest {
  final int key, version, id;
  final String clientId;
  final List<int> body;
  OffsetWireRequest(this.key, this.version, this.id, this.clientId, this.body);
}

/// Loopback protocol peer only. It never starts or connects to a Kafka broker.
class ListOffsetsPeer {
  final ServerSocket server;
  final sockets = <Socket>[];
  final requests = <OffsetWireRequest>[];
  final errors = <Object>[];
  List<List<int>> versions = [
    [2, 2, 2],
    [3, 0, 0],
  ];
  String topic = 'topic';
  int partition = 0;
  int metadataError = 0;
  late int leaderPort = server.port;
  List<int>? Function(OffsetWireRequest, Socket)? onListOffsets;
  List<int>? Function(OffsetWireRequest, Socket)? onMetadata;

  ListOffsetsPeer._(this.server) {
    server.listen((socket) {
      sockets.add(socket);
      final buffered = <int>[];
      socket.listen((data) {
        try {
          buffered.addAll(data);
          while (buffered.length >= 4) {
            final size = OffsetWireReader(buffered).i32();
            if (buffered.length < size + 4) break;
            final reader = OffsetWireReader(buffered.sublist(4, size + 4));
            buffered.removeRange(0, size + 4);
            final key = reader.i16(), version = reader.i16(), id = reader.i32();
            final clientId = reader.string();
            final request = OffsetWireRequest(key, version, id, clientId,
                reader.bytes.sublist(reader.cursor));
            requests.add(request);
            List<int>? response;
            if (key == 18) {
              response = [
                ...offsetI32(id),
                ...offsetI16(0),
                ...offsetI32(versions.length),
                for (final api in versions)
                  for (final value in api) ...offsetI16(value),
              ];
            } else if (key == 3) {
              response = onMetadata == null
                  ? metadataBytes(id)
                  : onMetadata!(request, socket);
            } else if (key == 2) {
              response = onListOffsets == null
                  ? offsetResponseBytes(id, topics: [
                      offsetTopicBytes(name: topic, partitions: [
                        offsetPartitionBytes(partition: partition),
                      ]),
                    ])
                  : onListOffsets!(request, socket);
            } else {
              throw StateError('Unexpected API $key');
            }
            if (response != null) socket.add(offsetFrame(response));
          }
        } catch (error) {
          errors.add(error);
          socket.destroy();
        }
      }, onError: (Object _) {});
    });
  }

  List<int> metadataBytes(int id) => [
        ...offsetI32(id),
        ...offsetI32(1),
        ...offsetI32(7),
        ...offsetString('127.0.0.1'),
        ...offsetI32(leaderPort),
        ...offsetI32(1),
        ...offsetI16(metadataError),
        ...offsetString(topic),
        ...offsetI32(1),
        ...offsetI16(0),
        ...offsetI32(partition),
        ...offsetI32(7),
        ...offsetI32(1),
        ...offsetI32(7),
        ...offsetI32(1),
        ...offsetI32(7),
      ];

  static Future<ListOffsetsPeer> start() async => ListOffsetsPeer._(
      await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
  }
}
