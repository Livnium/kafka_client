import 'dart:async';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

class LifecycleProbe extends KafkaRequest {
  @override
  List<int> toBytes() => KafkaRequest.frame(
      (KafkaBytesBuilder()..addInt32(correlationId)).takeBytes());
  @override
  int parseResponse(List<int> bytes) =>
      KafkaBytesReader.fromBytes(bytes).readInt32();
}

// A deterministic scheduling hook at application-request serialization, after
// version validation but before transport dispatch.
class SerializationHookRecord extends KafkaRecord {
  void Function()? hook;
  SerializationHookRecord(this.hook) : super(value: [1], timestamp: 1000);
  @override
  int get timestamp {
    final callback = hook;
    hook = null;
    callback?.call();
    return 1000;
  }
}

void main() {
  final servers = <ServerSocket>[];
  final sockets = <Socket>[];
  late KafkaClient client;
  Future<Broker> serve(void Function(Socket, List<int>) onFrame,
      {void Function()? onClose}) async {
    final server = await ServerSocket.bind('127.0.0.1', 0);
    servers.add(server);
    server.listen((socket) {
      sockets.add(socket);
      final buffer = <int>[];
      socket.listen((data) {
        buffer.addAll(data);
        while (buffer.length >= 4) {
          final size = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (buffer.length < size + 4) break;
          final frame = buffer.sublist(4, size + 4);
          buffer.removeRange(0, size + 4);
          onFrame(socket, frame);
        }
      }, onDone: onClose, onError: (Object _) {});
    });
    return Broker('127.0.0.1', server.port);
  }

  tearDown(() async {
    await client.close();
    for (final socket in sockets) {
      socket.destroy();
    }
    for (final server in servers) {
      await server.close();
    }
    sockets.clear();
    servers.clear();
  });

  test('idle LRU eviction allows sequential endpoint churn within limit',
      () async {
    final closedB = Completer<void>();
    var aClosed = false;
    final a = await serve((s, b) => s.add(KafkaRequest.frame(b)),
        onClose: () => aClosed = true);
    final b = await serve((s, b) => s.add(KafkaRequest.frame(b)), onClose: () {
      if (!closedB.isCompleted) closedB.complete();
    });
    final c = await serve((s, b) => s.add(KafkaRequest.frame(b)));
    client = KafkaClient(brokers: [a], maxConnections: 2);
    for (final broker in [a, b, a, c]) {
      final request = LifecycleProbe();
      expect(await client.send(request, broker: broker), request.correlationId);
      expect(client.connectionCount, lessThanOrEqualTo(2));
      expect(client.pendingRequestCount, 0);
    }
    await closedB.future.timeout(Duration(seconds: 1));
    expect(aClosed, isFalse);
    for (final broker in [a, b, c]) {
      expect(await client.send(LifecycleProbe(), broker: broker), isA<int>());
      expect(client.connectionCount, 2);
    }
  });

  test('pool pressure cannot evict a connection awaiting a response', () async {
    final received = Completer<void>();
    final a = await serve((s, b) => received.complete());
    final b = await serve((s, b) => s.add(KafkaRequest.frame(b)));
    client = KafkaClient(brokers: [a], maxConnections: 1);
    final token = KafkaCancellationToken();
    final pending = expectLater(
        client.send(LifecycleProbe(), cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    await received.future;
    await expectLater(client.send(LifecycleProbe(), broker: b),
        throwsA(isA<KafkaResourceLimitException>()));
    expect(client.connectionCount, 1);
    token.cancel();
    await pending;
    expect(await client.send(LifecycleProbe(), broker: b), isA<int>());
  });

  test('pool pressure cannot evict pending ApiVersions negotiation', () async {
    final received = Completer<void>();
    final a = await serve((s, bytes) {
      final reader = KafkaBytesReader.fromBytes(bytes);
      expect(reader.readInt16(), 18);
      if (!received.isCompleted) received.complete();
    });
    final b = await serve((s, b) => s.add(KafkaRequest.frame(b)));
    client = KafkaClient(brokers: [a], maxConnections: 1);
    final pending = expectLater(client.metadata(), throwsStateError);
    await received.future;
    await expectLater(client.send(LifecycleProbe(), broker: b),
        throwsA(isA<KafkaResourceLimitException>()));
    expect(client.connectionCount, 1);
    await client.close();
    await pending;
  });

  test('checked send never reconnects without validating replacement socket',
      () async {
    final probeReceived = Completer<void>();
    final negotiated = <Socket>{};
    var versionCalls = 0;
    var metadataCalls = 0;
    var produceCalls = 0;
    final broker = await serve((socket, bytes) {
      if (bytes.length == 4) {
        probeReceived.complete();
        return;
      }
      final r = KafkaBytesReader.fromBytes(bytes);
      final key = r.readInt16();
      r.readInt16();
      final id = r.readInt32();
      final response = KafkaBytesBuilder()..addInt32(id);
      if (key == 18) {
        versionCalls++;
        negotiated.add(socket);
        response
          ..addInt16(0)
          ..addInt32(2)
          ..addInt16(3)
          ..addInt16(0)
          ..addInt16(0)
          ..addInt16(0)
          ..addInt16(3)
          ..addInt16(3);
      } else if (key == 3) {
        expect(negotiated.contains(socket), isTrue,
            reason: 'Metadata must use a version-checked physical connection');
        metadataCalls++;
        response
          ..addInt32(1)
          ..addInt32(1)
          ..addString('127.0.0.1')
          ..addInt32(socket.port)
          ..addInt32(1)
          ..addInt16(0)
          ..addString('topic')
          ..addInt32(1)
          ..addInt16(0)
          ..addInt32(0)
          ..addInt32(1)
          ..addInt32(1)
          ..addInt32(1)
          ..addInt32(1)
          ..addInt32(1);
      } else {
        produceCalls++;
        fail('Retired connection must never transmit Produce');
      }
      socket.add(KafkaRequest.frame(response.takeBytes()));
    });
    client = KafkaClient(brokers: [broker], maxReadRetries: 0);
    await client.metadata();
    final token = KafkaCancellationToken();
    final pending = expectLater(
        client.send(LifecycleProbe(), cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    await probeReceived.future;
    // Retire the validated socket exactly as Produce is serialized. A record
    // getter preserves this post-negotiation hook even though input collections
    // are now snapshotted before discovery or version negotiation.
    await expectLater(
        client.produce('topic', [SerializationHookRecord(token.cancel)]),
        throwsA(isA<KafkaConnectionException>()));
    await pending;
    expect(versionCalls, 1);
    expect(metadataCalls, 2);
    expect(produceCalls, 0);
    await client.metadata();
    expect(versionCalls, 2);
    expect(metadataCalls, 3);
  });

  test('concurrent close callers share pending connection teardown', () async {
    final broker = await serve((s, b) {});
    client = KafkaClient(brokers: [broker]);
    final pending =
        expectLater(client.send(LifecycleProbe()), throwsStateError);
    final first = client.close();
    final second = client.close();
    expect(identical(first, second), isTrue);
    await second;
    await first;
    await pending;
    expect(client.connectionCount, 0);
    expect(client.pendingRequestCount, 0);
    expect(client.queuedRequestBytes, 0);
  });

  test('unsettled canceled dispatch and response wait retain separate slots',
      () async {
    final received = Completer<void>();
    final a = await serve((s, b) {
      if (!received.isCompleted) received.complete();
    });
    final b = await serve((s, b) => s.add(KafkaRequest.frame(b)));
    client = KafkaClient(brokers: [a], maxInFlightRequests: 2);
    final first = expectLater(client.send(LifecycleProbe()), throwsStateError);
    await received.future;
    await Future<void>.delayed(Duration.zero);
    final token = KafkaCancellationToken();
    final canceled = expectLater(
        client.send(LifecycleProbe(), broker: b, cancellationToken: token),
        throwsA(isA<KafkaCancelledException>()));
    token.cancel();
    expect(client.pendingRequestCount, 1);
    expect(client.admittedRequestCount, 2);
    await expectLater(client.send(LifecycleProbe()),
        throwsA(isA<KafkaResourceLimitException>()));
    await canceled;
    // Let the canceled connect settle and release its admission slot.
    await Future<void>.delayed(Duration(milliseconds: 20));
    expect(await client.send(LifecycleProbe(), broker: b), isA<int>());
    expect(client.pendingRequestCount, 1);
    expect(client.admittedRequestCount, 1);
    await client.close();
    await first;
    expect(client.queuedRequestBytes, 0);
    expect(client.admittedRequestCount, 0);
  });
}
