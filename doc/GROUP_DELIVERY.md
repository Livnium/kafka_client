# Bounded single-generation group delivery

Import `package:kafka_client/kafka_group.dart`. After `session.start()` establishes
one classic generation, `session.subscribe` creates this session's sole delivery
handle. Supply inclusive absolute offsets for exactly every nonempty assigned
topic/partition set. For an empty assignment, use `{}`; no Fetch is sent.
Invalid configuration is rejected synchronously before data I/O and does not
consume the one permitted delivery. Offsets and broker lists are copied.

```dart
import 'dart:async';
import 'package:kafka_client/kafka_group.dart';

KafkaGroupDelivery? delivery;
StreamSubscription<KafkaGroupRecord>? listener;
try {
  final token = await session.start();
  // Application chooses positions, including retention/out-of-range policy.
  delivery = session.subscribe(
    assignment: token,
    startOffsets: applicationPositions,
  );
  listener = delivery.records.listen((event) {
    // This direct callback is admitted synchronously under event.assignment.
    // Work started here is NOT acknowledged, committed or externally fenced.
    inspectRecord(event.topic, event.partition, event.offset, event.record);
  }, onError: handleReadFailure);
  await session.revoked;
  final dataCleanup = await delivery.stopped;
  if (dataCleanup.cleanupError != null) reportCleanupFailure(dataCleanup);
} finally {
  try {
    final dataCleanup = await delivery?.cancel();
    if (dataCleanup != null && dataCleanup.cleanupError != null) {
      reportCleanupFailure(dataCleanup);
    }
  } finally {
    try {
      await listener?.cancel();
    } finally {
      // Includes failed start or invalid subscribe configuration.
      final membershipCleanup = await session.close();
      inspectMembershipCleanup(membershipCleanup);
    }
  }
}
```

This illustrates lifetime handling, not a processing/commit algorithm. Nothing
starts polling before listen; an unlistened delivery still stops when its session
revokes. Cancellation or terminal Fetch failure closes membership too. A session
cannot attach another delivery, even after the first one stops. A new explicit
session is a new operation, not a safe replay of previously delivered effects.

## Exact local admission boundary

One synchronous single-subscription controller has one producer pump. Listen and
resume handlers never emit records. Before each Fetch admission and each original
record callback, the pump checks its exact assignment identity and monotonic local
expiry, with no intervening await before dispatch. A response and its buffer carry
the captured assignment token. All records in a response are validated before any
is emitted. Positive gaps and validated compacted/control-only batch ends advance
that partition's cursor; an empty poll's high watermark never does.

Pause prevents new Fetch and data admission. An admitted Fetch may still complete,
and one validated response can remain buffered. Remaining records live in an
internal queue, not the stream controller's asynchronous event queue. Synchronous
local revocation invalidates the token, clears that queue, wakes pause/backoff
waits, and begins owned data-client closure before the session's revoked future is
completed. Late Fetch successes/errors cannot revive delivery. The pump yields
between records so callbacks, pause, cancellation and heartbeat work can run.

**This is not broker ownership fencing.** A broker may have changed ownership
before the client learns of it. The token check establishes local validity only
at the original callback's admission. Already-started async callbacks, user-added
stream transformations, user buffers and external effects can outlive revocation.
The same boundary limitation applies to `await for` and `StreamIterator`: their
asynchronous consumer handoff is outside the original direct listener.
An async `listen` callback does not apply processing backpressure. Explicit
pause/resume can limit subsequent delivery but cannot undo an admitted effect.

## Three separate completion signals

- `session.revoked`: local assignment and delivery admission have been invalidated;
  membership/data cleanup can still be in flight.
- `delivery.stopped`: the data pump and actually admitted Fetch have settled, the
  internal buffer is empty, and owned data-client closure has settled. Check its
  `cleanupError`; failure does not prove transport shutdown. It can resolve while
  the stream listener remains paused, before stream `done` is delivered. It never
  awaits application callback Futures and has no independent hard deadline.
- `session.closed` / `session.close()`: membership cleanup, including the separate
  Leave evidence and control-client cleanup status. It does not await the data
  barrier. Await both cleanup results when both channels were created.

Owned data-client close is initiated before waiting for the pump or Fetch, so
pending I/O can be interrupted. No `Future.any` drops ownership of the Fetch.
Cancellation is idempotent; cancelling from the original data callback or through
`take(1)` does not reentrantly add/close the stream. A paused listener may retain a
terminal error/done notification, but never a stale queued data record from this
producer. Stream cancellation itself has no result object; inspect `stopped` for
cleanup evidence.

## Budgets and failure policy

The data client uses the same immutable broker list, configured strict TLS/SASL objects and
configured transport/resource/retry settings as the control client. It has its
own four-request transport admission budget; delivery admits one high-level Fetch
at a time across assigned topics/partitions in round-robin order. This separates
request budgets, not Dart isolate CPU, broker or network capacity. A long
synchronous listener can delay heartbeats; the next admission still checks expiry
synchronously even if the watchdog timer has not run.

`maxBytes` is Kafka's soft requested limit; the first batch may exceed it. Existing
frame/wire-buffer limits still apply. `maxBufferedRecords` and
`maxBufferedPayloadBytes` are checked after decoding the complete response and
before delivery. Payload bytes count keys, values, UTF-8 header keys and header
values, not object/list overhead. These bounds are not a whole-heap/RSS cap or a
pre-decoding allocation guarantee. At most one decoded response is retained by
the delivery pump. Defaults: 10,000 records and 16 MiB payload.

By default the data client retains ordinary read-safe Fetch retries. A session
may separately opt into `tlsFetchRecovery`, requiring TLS and explicit bootstrap
fallback. Only its dedicated data client receives that bounded policy; the control
client does not. The policy has one finite Fetch round budget and attempt-admission
window, not a hard cancellation deadline. Revocation still closes the data client
and discards late results. See [GROUP_FETCH_RECOVERY.md](GROUP_FETCH_RECOVERY.md).

After the selected bounded Fetch policy is exhausted, any
Fetch error terminates delivery and closes membership; there is no outer
indefinite retry. Positive Fetch throttle is respected, but this does not add the
unsupported membership quota-throttling policy. No Produce, OffsetFetch or
OffsetCommit is issued by delivery, and no automatic commit replay is added.

## Remaining work and evidence

Raw `subscribe` intentionally has no processing acknowledgement or commit policy.
A paused raw consumer can retain membership indefinitely while heartbeats succeed.
Use the separate opt-in `subscribeProcessing` for an acknowledged-prefix ledger,
explicit generation commits and processing-stall rules; see
[GROUP_PROCESSING.md](GROUP_PROCESSING.md). Neither mode adds automatic initial
position lookup/reset, automatic rejoin, external-effect fencing or an exactly-once
claim.

The checkpoint receipt is under `verification/group_delivery/`. Evidence remains
same-host native Linux x64 on the stated Dart/Kafka versions. It does not establish
Flutter embedding, other platforms, AOT runtime performance, independent-host
fault recovery or long-duration group processing. No publication is performed.

Later [headless Flutter engine evidence](RUNTIME_COMPATIBILITY.md) covers nine
specified cases on the newer checkpoint. It does not expand this historical
checkpoint's tests or establish packaged-app or real OS lifecycle support.
