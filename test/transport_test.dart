import 'dart:async';
import 'dart:io';
import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

class EchoRequest extends KafkaRequest {
  final bool failParse;
  EchoRequest({this.failParse = false});
  @override
  List<int> toBytes() => KafkaRequest.frame(
      (KafkaBytesBuilder()..addInt32(correlationId)).takeBytes());
  @override
  int parseResponse(List<int> bytes) {
    if (failParse) throw FormatException('test parser error');
    final r = KafkaBytesReader.fromBytes(bytes)..readInt32();
    return r.readInt32();
  }
}

List<int> response(int id, int value) => KafkaRequest.frame((KafkaBytesBuilder()
      ..addInt32(id)
      ..addInt32(value))
    .takeBytes());

void main() {
  late ServerSocket server;
  late KafkaClient client;
  final sockets = <Socket>[];
  setUp(() async {
    server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    client = KafkaClient(
        brokers: [Broker('127.0.0.1', server.port)],
        requestTimeout: Duration(milliseconds: 150));
  });
  tearDown(() async {
    await client.close();
    for (final s in sockets) {
      s.destroy();
    }
    sockets.clear();
    await server.close();
  });
  void serve(void Function(Socket, List<int>) onFrame) {
    server.listen((s) {
      sockets.add(s);
      final buffer = <int>[];
      s.listen((data) {
        buffer.addAll(data);
        while (buffer.length >= 4) {
          final size = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (buffer.length < size + 4) break;
          final frame = buffer.sublist(4, size + 4);
          buffer.removeRange(0, size + 4);
          onFrame(s, frame);
        }
      });
    });
  }

  test('fragmented prefix/body and coalesced responses correlate out of order',
      () async {
    final ids = <int>[];
    serve((s, b) async {
      ids.add(KafkaBytesReader.fromBytes(b).readInt32());
      if (ids.length == 2) {
        final bytes = [...response(ids[1], 22), ...response(ids[0], 11)];
        for (final byte in bytes.take(7)) {
          s.add([byte]);
          await s.flush();
        }
        s.add(bytes.sublist(7));
      }
    });
    expect(
        await Future.wait(
            [client.send(EchoRequest()), client.send(EchoRequest())]),
        [11, 22]);
  });
  test('deadline completes instead of hanging', () async {
    serve((s, b) {});
    await expectLater(
        client.send(EchoRequest()), throwsA(isA<TimeoutException>()));
  });
  test('disconnect fails every pending request', () async {
    serve((s, b) {
      s.destroy();
    });
    final futures = [client.send(EchoRequest()), client.send(EchoRequest())];
    await Future.wait(futures
        .map((f) => expectLater(f, throwsA(isA<KafkaConnectionException>()))));
  });
  test('close fails pending and prevents reuse', () async {
    final received = Completer<void>();
    serve((s, b) {
      if (!received.isCompleted) received.complete();
    });
    final pending =
        expectLater(client.send(EchoRequest()), throwsA(isA<StateError>()));
    await received.future;
    await client.close();
    await pending;
    await expectLater(client.send(EchoRequest()), throwsA(isA<StateError>()));
  });
  test('response parser errors are delivered, not unhandled', () async {
    serve((s, b) {
      s.add(response(KafkaBytesReader.fromBytes(b).readInt32(), 1));
    });
    await expectLater(
        client.send(EchoRequest(failParse: true)), throwsFormatException);
    expect(await client.send(EchoRequest()), 1);
  });
  for (final length in [-1, 0, 3, 16777217]) {
    test('invalid response frame size $length disconnects', () async {
      serve((s, b) {
        s.add((KafkaBytesBuilder()..addInt32(length)).takeBytes());
      });
      await expectLater(client.send(EchoRequest()), throwsFormatException);
    });
  }
  test('acks zero finishes without response', () async {
    serve((s, b) {});
    expect(
        await client
            .send(ProduceRequest(acks: 0, timeoutMs: 100, topicData: [])),
        isNull);
  });
  test('same request cannot overwrite pending completer', () async {
    serve((s, b) {});
    final request = EchoRequest();
    final first =
        expectLater(client.send(request), throwsA(isA<TimeoutException>()));
    await expectLater(client.send(request), throwsA(isA<StateError>()));
    await first;
  });
  test('timed-out request object cannot reuse its correlation ID', () async {
    serve((s, b) {});
    final request = EchoRequest();
    await expectLater(client.send(request), throwsA(isA<TimeoutException>()));
    await expectLater(client.send(request), throwsA(isA<StateError>()));
  });
  test('timeout retires connection and a fresh request reconnects', () async {
    var first = true;
    serve((s, b) {
      final id = KafkaBytesReader.fromBytes(b).readInt32();
      if (first) {
        first = false;
      } else {
        Future.delayed(
            Duration(milliseconds: 60), () => s.add(response(id, 2)));
      }
    });
    await expectLater(
        client.send(EchoRequest()), throwsA(isA<TimeoutException>()));
    expect(await client.send(EchoRequest()), 2);
  });
  test('connection refused is delivered', () async {
    await server.close();
    await expectLater(
        client.send(EchoRequest()), throwsA(isA<SocketException>()));
  });
}
