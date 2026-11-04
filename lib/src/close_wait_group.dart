import 'dart:async';

/// Package-internal, removable delays released when their owner closes.
///
/// Completed waits are removed immediately; no listener is attached to a
/// long-lived close Future. This bounds retained waiters to active delays.
class CloseWaitGroup {
  final Set<Completer<void>> _active = {};
  bool _closed = false;

  int get activeCount => _active.length;

  Future<void> wait(Duration duration) async {
    if (_closed) return;
    final waiter = Completer<void>();
    _active.add(waiter);
    Timer? timer;
    try {
      timer = Timer(duration, () {
        if (!waiter.isCompleted) waiter.complete();
      });
      await waiter.future;
    } finally {
      timer?.cancel();
      _active.remove(waiter);
    }
  }

  void close() {
    _closed = true;
    for (final waiter in _active) {
      if (!waiter.isCompleted) waiter.complete();
    }
    _active.clear();
  }
}
