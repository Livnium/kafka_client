import 'dart:async';
import 'dart:collection';
import 'dart:io';
import 'dart:typed_data';

import 'package:kafka_client/src/reliability.dart';
import 'package:kafka_client/src/transport.dart';
import 'package:test/test.dart';

class FakeTlsSocket extends Stream<RawSocketEvent> implements RawSecureSocket {
  final events = StreamController<RawSocketEvent>();
  final Queue<int> writePlan = Queue<int>();
  final Queue<Uint8List> incoming = Queue<Uint8List>();
  final written = <int>[];
  bool failWrite = false;
  bool failRead = false;
  int closeCalls = 0;
  @override
  bool readEventsEnabled = true;
  @override
  bool writeEventsEnabled = true;
  @override
  int write(List<int> buffer, [int offset = 0, int? count]) {
    if (failWrite) throw StateError('must-not-escape');
    final remaining = count ?? buffer.length - offset;
    final n = writePlan.isEmpty ? remaining : writePlan.removeFirst();
    if (n >= 0 && n <= remaining) {
      written.addAll(buffer.sublist(offset, offset + n));
    }
    return n;
  }

  @override
  Uint8List? read([int? length]) {
    if (failRead) throw StateError('must-not-escape');
    return incoming.isEmpty ? null : incoming.removeFirst();
  }

  @override
  Future<RawSecureSocket> close() async {
    closeCalls++;
    unawaited(events.close());
    return this;
  }

  @override
  StreamSubscription<RawSocketEvent> listen(
          void Function(RawSocketEvent)? onData,
          {Function? onError,
          void Function()? onDone,
          bool? cancelOnError}) =>
      events.stream.listen(onData,
          onError: onError, onDone: onDone, cancelOnError: cancelOnError);
  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

Future<void> tick() => Future<void>.delayed(Duration.zero);
void main() {
  test('zero-length write flushes immediately and next write works', () async {
    final raw = FakeTlsSocket();
    final transport = RawTlsKafkaTransport(raw);
    transport.stream.listen((_) {});
    transport.add([]);
    await transport.flush();
    transport.add([1, 2]);
    await transport.flush();
    expect(raw.written, [1, 2]);
    expect(raw.writeEventsEnabled, isFalse);
    transport.destroy();
    await tick();
  });
  test('partial writes plus repeated zero writes preserve bytes and rearm',
      () async {
    final raw = FakeTlsSocket()..writePlan.addAll([2, 0, 0, 1, 0, 2]);
    final transport = RawTlsKafkaTransport(raw);
    transport.stream.listen((_) {});
    transport.add([1, 2, 3, 4, 5]);
    var flushed = false;
    final done = transport.flush().then((_) => flushed = true);
    expect(raw.written, [1, 2]);
    expect(raw.writeEventsEnabled, isTrue);
    expect(() => transport.add([99]), throwsStateError);
    raw.events.add(RawSocketEvent.write);
    await tick();
    expect(flushed, isFalse);
    expect(raw.written, [1, 2]);
    raw.events.add(RawSocketEvent.write);
    await tick();
    expect(raw.written, [1, 2, 3]);
    expect(flushed, isFalse);
    raw.events.add(RawSocketEvent.write);
    await done;
    expect(raw.written, [1, 2, 3, 4, 5]);
    expect(raw.writeEventsEnabled, isFalse);
    transport.destroy();
    await tick();
  });
  test('destroy fails blocked flush and force-closes once', () async {
    final raw = FakeTlsSocket()..writePlan.add(0);
    var forced = 0;
    final transport =
        RawTlsKafkaTransport(raw, closeUnderlying: () => forced++);
    final ended = Completer<void>();
    transport.stream.listen((_) {}, onDone: ended.complete);
    transport.add([1, 2, 3]);
    final pending = expectLater(
        transport.flush(), throwsA(isA<KafkaConnectionException>()));
    transport.destroy();
    transport.destroy();
    await pending;
    await ended.future;
    expect(raw.closeCalls, 1);
    expect(forced, 1);
    expect(() => transport.add([1]), throwsA(isA<KafkaConnectionException>()));
    await expectLater(
        transport.flush(), throwsA(isA<KafkaConnectionException>()));
  });
  test('reads drain chunks, respect pause and allow destroy in data callback',
      () async {
    final raw = FakeTlsSocket();
    final transport = RawTlsKafkaTransport(raw);
    expect(raw.readEventsEnabled, isFalse);
    final seen = <int>[];
    late StreamSubscription<List<int>> sub;
    sub = transport.stream.listen((bytes) {
      seen.addAll(bytes);
      sub.pause();
    });
    raw.incoming.addAll([
      Uint8List.fromList([1]),
      Uint8List.fromList([2])
    ]);
    raw.events.add(RawSocketEvent.read);
    await tick();
    expect(seen, [1]);
    expect(raw.readEventsEnabled, isFalse);
    sub.resume();
    expect(raw.readEventsEnabled, isTrue);
    raw.events.add(RawSocketEvent.read);
    await tick();
    expect(seen, [1, 2]);
    sub.resume();
    await sub.cancel();
    await tick();
    expect(raw.closeCalls, 1);
    final raw2 = FakeTlsSocket();
    final t2 = RawTlsKafkaTransport(raw2);
    final ended = Completer<void>();
    t2.stream.listen((_) => t2.destroy(), onDone: ended.complete);
    raw2.incoming.add(Uint8List.fromList([1]));
    raw2.events.add(RawSocketEvent.read);
    await ended.future;
  });
  for (final event in [RawSocketEvent.readClosed, RawSocketEvent.closed]) {
    test('$event closes stream and fails pending write', () async {
      final raw = FakeTlsSocket()..writePlan.add(0);
      final transport = RawTlsKafkaTransport(raw);
      final ended = Completer<void>();
      transport.stream.listen((_) {}, onDone: ended.complete);
      transport.add([1]);
      final pending = expectLater(
          transport.flush(), throwsA(isA<KafkaConnectionException>()));
      raw.events.add(event);
      await pending;
      await ended.future;
    });
  }
  test('raw write error is sanitized on flush and stream', () async {
    final raw = FakeTlsSocket()..failWrite = true;
    final transport = RawTlsKafkaTransport(raw);
    final errors = <Object>[];
    final ended = Completer<void>();
    transport.stream
        .listen((_) {}, onError: errors.add, onDone: ended.complete);
    transport.add([1]);
    await expectLater(
        transport.flush(), throwsA(isA<KafkaConnectionException>()));
    await ended.future;
    expect(errors.single, isA<KafkaConnectionException>());
    expect(errors.single.toString(), isNot(contains('must-not-escape')));
  });
  test('raw event error fails pending flush without leaking server data',
      () async {
    final raw = FakeTlsSocket()..writePlan.add(0);
    final transport = RawTlsKafkaTransport(raw);
    final errors = <Object>[];
    final ended = Completer<void>();
    transport.stream
        .listen((_) {}, onError: errors.add, onDone: ended.complete);
    transport.add([1]);
    final pending = expectLater(
        transport.flush(), throwsA(isA<KafkaConnectionException>()));
    raw.events.addError(StateError('must-not-escape'));
    await pending;
    await ended.future;
    expect(errors.single, isA<KafkaConnectionException>());
  });
  test('plain socket adapter preserves request response bytes', () async {
    final server = await ServerSocket.bind('127.0.0.1', 0);
    final peers = <Socket>[];
    server.listen((s) {
      peers.add(s);
      s.listen(s.add);
    });
    final socket = await Socket.connect('127.0.0.1', server.port);
    final transport = SocketKafkaTransport(socket);
    final bytes = transport.stream.first;
    transport.add([1, 2, 3]);
    await transport.flush();
    expect(await bytes, [1, 2, 3]);
    transport.destroy();
    for (final s in peers) {
      s.destroy();
    }
    await server.close();
  });
}
