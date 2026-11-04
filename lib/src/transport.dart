import 'dart:async';
import 'dart:io';

import 'reliability.dart';

/// Internal byte transport. The caller serializes add/flush pairs and owns
/// request byte limits. Completion means local acceptance, not Kafka delivery.
abstract class KafkaTransport {
  Stream<List<int>> get stream;
  void add(List<int> bytes);
  Future<void> flush();
  void destroy();
}

/// Plain TCP adapter retaining Socket's native stream and sink semantics.
class SocketKafkaTransport implements KafkaTransport {
  final Socket _socket;
  SocketKafkaTransport(this._socket);
  @override
  Stream<List<int>> get stream => _socket;
  @override
  void add(List<int> bytes) => _socket.add(bytes);
  @override
  Future<void> flush() => _socket.flush();
  @override
  void destroy() => _socket.destroy();
}

/// Adapts an already authenticated TLS socket without a write queue.
///
/// Only one add/flush operation may be outstanding. The caller must not mutate
/// its byte list until flush completes. Reads respect subscriber pause state.
/// [closeUnderlying] should force-close the retained TCP socket: TLS close alone
/// can wait for encrypted buffers to drain on older Dart runtimes.
class RawTlsKafkaTransport implements KafkaTransport {
  final RawSecureSocket _socket;
  final void Function()? _closeUnderlying;
  late final StreamController<List<int>> _controller;
  StreamSubscription<RawSocketEvent>? _subscription;
  List<int>? _writeBytes;
  int _writeOffset = 0;
  Completer<void>? _writeCompleter;
  Future<void> _flushFuture = Future<void>.value();
  bool _closed = false;
  bool _paused = true;
  bool _pumping = false;

  RawTlsKafkaTransport(this._socket, {void Function()? closeUnderlying})
      : _closeUnderlying = closeUnderlying {
    _socket.readEventsEnabled = false;
    _socket.writeEventsEnabled = false;
    _controller = StreamController<List<int>>(
        sync: true,
        onListen: _resumeReads,
        onPause: _pauseReads,
        onResume: _resumeReads,
        onCancel: destroy);
    _subscription = _socket.listen(_onEvent,
        onError: (Object _, StackTrace st) =>
            _terminate(error: true, stack: st),
        onDone: () => _terminate(),
        cancelOnError: true);
  }

  @override
  Stream<List<int>> get stream => _controller.stream;

  void _resumeReads() {
    if (_closed) return;
    _paused = false;
    _socket.readEventsEnabled = true;
  }

  void _pauseReads() {
    if (_closed) return;
    _paused = true;
    _socket.readEventsEnabled = false;
  }

  void _onEvent(RawSocketEvent event) {
    if (_closed) return;
    try {
      if (event == RawSocketEvent.read) {
        while (!_closed && !_paused) {
          final bytes = _socket.read(64 * 1024);
          if (bytes == null || bytes.isEmpty) break;
          _controller.add(bytes);
        }
      } else if (event == RawSocketEvent.write) {
        _pumpWrite();
      } else if (event == RawSocketEvent.readClosed ||
          event == RawSocketEvent.closed) {
        _terminate();
      }
    } catch (_, st) {
      _terminate(error: true, stack: st);
    }
  }

  @override
  void add(List<int> bytes) {
    if (_closed) throw KafkaConnectionException();
    if (_writeBytes != null) {
      throw StateError('Only one TLS write may be pending');
    }
    final completer = Completer<void>();
    _writeCompleter = completer;
    _flushFuture = completer.future;
    // A raw write may fail synchronously before the caller reaches flush().
    // Mark the future handled without changing the original flush result.
    unawaited(
        _flushFuture.then<void>((_) {}, onError: (Object _, StackTrace __) {}));
    _writeBytes = bytes;
    _writeOffset = 0;
    _pumpWrite();
  }

  void _pumpWrite() {
    if (_closed || _pumping) return;
    final bytes = _writeBytes;
    if (bytes == null) return;
    _pumping = true;
    try {
      while (_writeOffset < bytes.length && !_closed) {
        final remaining = bytes.length - _writeOffset;
        final written = _socket.write(bytes, _writeOffset, remaining);
        if (written < 0 || written > remaining) {
          throw StateError('Invalid raw TLS write result');
        }
        _writeOffset += written;
        if (written == 0) {
          _socket.writeEventsEnabled = true; // Raw write events are one-shot.
          return;
        }
      }
      if (_closed) return;
      _socket.writeEventsEnabled = false;
      _writeBytes = null;
      _writeOffset = 0;
      final completer = _writeCompleter;
      _writeCompleter = null;
      completer?.complete();
    } catch (_, st) {
      _terminate(error: true, stack: st);
    } finally {
      _pumping = false;
    }
  }

  @override
  Future<void> flush() =>
      _closed ? Future<void>.error(KafkaConnectionException()) : _flushFuture;

  @override
  void destroy() => _terminate();

  void _terminate({bool error = false, StackTrace? stack}) {
    if (_closed) return;
    _closed = true;
    _writeBytes = null;
    _writeOffset = 0;
    final completer = _writeCompleter;
    _writeCompleter = null;
    if (completer != null && !completer.isCompleted) {
      completer.completeError(KafkaConnectionException(), stack);
    }
    // Never emit stream events reentrantly when a data listener destroys us.
    scheduleMicrotask(() {
      if (error && !_controller.isClosed) {
        _controller.addError(KafkaConnectionException(), stack);
      }
      if (!_controller.isClosed) unawaited(_controller.close());
    });
    try {
      _closeUnderlying?.call();
    } catch (_) {/* Cleanup must not leak errors or credentials. */}
    try {
      unawaited(_socket
          .close()
          .then<void>((_) {}, onError: (Object _, StackTrace __) {}));
    } catch (_) {/* The socket may already have closed. */}
    final subscription = _subscription;
    if (subscription != null) {
      unawaited(subscription
          .cancel()
          .then<void>((_) {}, onError: (Object _, StackTrace __) {}));
    }
  }
}
