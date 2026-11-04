import 'dart:io';

enum RelayLeg { downstream, upstream }

/// Test-only accounting. It never changes production client behavior or socket
/// routing. Expected EPIPE classification is restricted to one explicitly armed
/// quiescent client-facing connection, after the second real88 flush gate.
class TransactionRelayLifecycle {
  final int connectionId;
  final Set<int> _pending = {};
  int requests = 0, responses = 0;
  bool second88Flushed = false;
  bool intentionalClientClose = false;
  bool requestAfterCloseArm = false;
  int expectedCloseEvents = 0;

  TransactionRelayLifecycle(this.connectionId);
  int get pendingResponses => _pending.length;

  void requestReceived(int correlation) {
    if (intentionalClientClose) requestAfterCloseArm = true;
    if (!_pending.add(correlation)) {
      throw StateError('Duplicate pending relay correlation');
    }
    requests++;
    if (intentionalClientClose) {
      throw StateError('New request after intentional client-close arm');
    }
  }

  void responseForwarded(int correlation) {
    if (!_pending.remove(correlation)) {
      throw StateError('Unmatched relay response');
    }
    responses++;
  }

  void markSecond88Flushed() {
    if (pendingResponses != 0) {
      throw StateError('Second88 gate has pending relay responses');
    }
    second88Flushed = true;
  }

  void armIntentionalClientClose() {
    if (!second88Flushed ||
        pendingResponses != 0 ||
        requestAfterCloseArm ||
        intentionalClientClose) {
      throw StateError('Cannot arm non-quiescent or ungated client close');
    }
    intentionalClientClose = true;
  }

  Map<String, Object?> observeError(
    Object error,
    RelayLeg leg, {
    required String callback,
  }) {
    final code = error is SocketException ? error.osError?.errorCode : null;
    final expected = leg == RelayLeg.downstream &&
        error is SocketException &&
        code == 32 &&
        second88Flushed &&
        intentionalClientClose &&
        pendingResponses == 0 &&
        !requestAfterCloseArm &&
        expectedCloseEvents == 0;
    if (expected) expectedCloseEvents++;
    return {
      'event': expected
          ? 'expected-client-close-transport-event'
          : 'unexpected-relay-transport-event',
      'connectionId': connectionId,
      'leg': leg.name,
      'callback': callback,
      'errorType': error.runtimeType.toString(),
      'errno': code,
      'second88Flushed': second88Flushed,
      'intentionalClientClose': intentionalClientClose,
      'pendingResponses': pendingResponses,
      'requestAfterCloseArm': requestAfterCloseArm,
      'requests': requests,
      'responses': responses,
      'expectedCloseEvents': expectedCloseEvents,
    };
  }
}
