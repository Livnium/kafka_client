import 'dart:io';
import 'package:test/test.dart';
import 'support/transaction_relay_lifecycle.dart';

final epipe = SocketException(
  'injected EPIPE',
  osError: OSError('broken pipe', 32),
);
TransactionRelayLifecycle gated() {
  final state = TransactionRelayLifecycle(7);
  state.requestReceived(41);
  state.responseForwarded(41);
  state.requestReceived(43);
  state.responseForwarded(43);
  state.markSecond88Flushed();
  return state;
}

void main() {
  test(
    'only explicit quiescent downstream close EPIPE is retained as expected',
    () {
      final state = gated()..armIntentionalClientClose();
      final event = state.observeError(
        epipe,
        RelayLeg.downstream,
        callback: 'stream-onError',
      );
      expect(
        event,
        containsPair('event', 'expected-client-close-transport-event'),
      );
      expect(event, containsPair('requests', 2));
      expect(event, containsPair('responses', 2));
      expect(event, containsPair('errno', 32));
      expect(event, containsPair('connectionId', 7));
      expect(event, containsPair('leg', 'downstream'));
      expect(event, containsPair('pendingResponses', 0));
      expect(event, containsPair('second88Flushed', true));
      expect(event, containsPair('intentionalClientClose', true));
    },
  );
  test('same EPIPE on active connection is unexpected', () {
    final state = gated();
    expect(
      state.observeError(
        epipe,
        RelayLeg.downstream,
        callback: 'stream-onError',
      )['event'],
      'unexpected-relay-transport-event',
    );
  });
  test('upstream EPIPE remains unexpected after intentional close', () {
    final state = gated()..armIntentionalClientClose();
    expect(
      state.observeError(
        epipe,
        RelayLeg.upstream,
        callback: 'stream-onError',
      )['event'],
      'unexpected-relay-transport-event',
    );
  });
  test(
    'wrong errno and TLS authentication protocol errors remain failures',
    () {
      for (final error in <Object>[
        SocketException('reset', osError: OSError('reset', 104)),
        SocketException('unknown errno'),
        HandshakeException('TLS failure'),
        StateError('authentication rejection'),
        FormatException('malformed protocol'),
      ]) {
        final state = gated()..armIntentionalClientClose();
        expect(
          state.observeError(
            error,
            RelayLeg.downstream,
            callback: 'stream-onError',
          )['event'],
          'unexpected-relay-transport-event',
        );
      }
    },
  );
  test('no gate cannot arm and EPIPE stays unexpected', () {
    final state = TransactionRelayLifecycle(1);
    expect(state.armIntentionalClientClose, throwsStateError);
    expect(
      state.observeError(
        epipe,
        RelayLeg.downstream,
        callback: 'stream-onError',
      )['event'],
      'unexpected-relay-transport-event',
    );
  });
  test('pending response cannot gate or arm and EPIPE stays unexpected', () {
    final state = gated();
    state.requestReceived(45);
    expect(state.markSecond88Flushed, throwsStateError);
    expect(state.armIntentionalClientClose, throwsStateError);
    expect(
      state.observeError(
        epipe,
        RelayLeg.downstream,
        callback: 'stream-onError',
      )['event'],
      'unexpected-relay-transport-event',
    );
  });
  test(
    'new request after arm fails and permanently disqualifies later EPIPE',
    () {
      final state = gated()..armIntentionalClientClose();
      expect(() => state.requestReceived(45), throwsStateError);
      expect(
        state.observeError(
          epipe,
          RelayLeg.downstream,
          callback: 'stream-onError',
        )['event'],
        'unexpected-relay-transport-event',
      );
      state.responseForwarded(45);
      expect(
        state.observeError(
          epipe,
          RelayLeg.downstream,
          callback: 'stream-onError',
        )['event'],
        'unexpected-relay-transport-event',
      );
    },
  );
  test('duplicate expected shutdown errors are not broadly suppressed', () {
    final state = gated()..armIntentionalClientClose();
    expect(
      state.observeError(
        epipe,
        RelayLeg.downstream,
        callback: 'stream-onError',
      )['event'],
      'expected-client-close-transport-event',
    );
    expect(
      state.observeError(
        epipe,
        RelayLeg.downstream,
        callback: 'stream-onError',
      )['event'],
      'unexpected-relay-transport-event',
    );
  });
  test(
    'duplicate request or unmatched response remains instrumentation failure',
    () {
      final state = TransactionRelayLifecycle(1)..requestReceived(10);
      expect(() => state.requestReceived(10), throwsStateError);
      expect(() => state.responseForwarded(11), throwsStateError);
    },
  );
}
