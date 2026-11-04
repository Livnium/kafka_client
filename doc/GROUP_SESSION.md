# Single-generation managed classic membership

The experimental `package:kafka_client/kafka_group.dart` library adds
`KafkaGroupSession`. It manages one dynamic classic-group generation: coordinator
lookup, version checks, Join, eager range assignment, Sync, a timely initial
Heartbeat, periodic heartbeats, local expiry and terminal cleanup.

The control client never sends Fetch, Produce or OffsetFetch. The additive
`session.subscribe` method can create one separate owned data client for bounded
assignment-tagged delivery; see [GROUP_DELIVERY.md](GROUP_DELIVERY.md).
Alternatively, `subscribeProcessing` adds explicit acknowledgements, bounded
processing stalls and generation-aware OffsetCommit on the owned control client;
see [GROUP_PROCESSING.md](GROUP_PROCESSING.md). There is no application-callback
draining or automatic rejoin. Existing `KafkaClient` methods, manual `subscribe()`
and manual commits are unchanged.

## Starting and observing a session

```dart
import 'package:kafka_client/kafka_group.dart';

final session = KafkaGroupSession(
  brokers: [Broker('127.0.0.1', 9092)],
  groupId: 'dedicated-experimental-group',
  topics: {'pre-created-topic'},
);
try {
  final assignment = await session.start();
  print(assignment.partitions);
  // Observe membership only. This does not start a record-processing loop.
  final loss = await session.revoked;
  print(loss.reason);
} finally {
  final cleanup = await session.close();
  print(cleanup.leaveOutcome);
}
```

Construction is lazy. `start()` can be called once and grants an immutable
assignment only after successful Sync, assignment validation and a timely initial
Heartbeat. Empty assignments are valid and designate zero partitions. No listener
or event stream is required to keep heartbeats running.

The assignment contains a controller-issued local epoch, Kafka generation,
member ID and deeply immutable topic/partition map. `isCurrent(token)` and
`assertCurrent(token)` accept only this session's exact live token and check
monotonic expiry even if a delayed timer has not run yet. Revocation invalidates
it synchronously, before notification or awaited cleanup. Late successful replies
cannot restore it, and another session's token is never interchangeable.

**A guard is true only at the instant checked.** It is not a broker ownership
lock, partition fence or authorization to finish asynchronous effects. Membership
may change immediately after the check. Guards cannot cancel outstanding Fetches,
stop async listeners, undo external effects or make a commit atomic with work.
Wrapping the manual `KafkaClient.subscribe()` stream in a guard does not give
the stronger original-listener admission boundary of `session.subscribe()`.

## Deliberately conservative lifecycle

- Join error 79 permits one continuation using its nonempty assigned dynamic
  member ID. This handshake response itself grants no assignment.
- Any later nonzero Join/Sync/Heartbeat response terminates the session. This
  includes rebalance-in-progress 27, illegal-generation 22, unknown-member 25,
  coordinator changes and authorization failures.
- Timeout, disconnection, malformed response and unconfirmed mutation outcomes
  terminate without replay or automatic rejoin. A broker-side member can remain
  until expiry, especially when a Join response carrying its ID was lost.
- Invalid assignment or unsupported versions fail before granting local validity.
  Only eager dynamic `range` is advertised. Outgoing metadata is version 0;
  supported incoming metadata is decoded strictly. When this session is leader,
  visible non-null peer rack IDs are rejected rather than implying rack-aware
  assignment. A follower cannot inspect other members' subscriptions. Join v4
  does not expose peer static identity; this is not a detector of all static
  peers. This client has no static membership, cooperative assignment or newer
  consumer protocol support.
- Negative group-response throttle values are malformed. Positive values fail
  closed as an explicitly unsupported quota policy instead of silently ignoring
  client-side throttling or sleeping past a freshness deadline. Quota-throttled
  membership is not supported by this increment.
- The leader uses fresh metadata for the union of all members' topic
  subscriptions, not just its own. It divides sorted partition IDs into contiguous
  ranges among sorted eligible member IDs. Received local assignments are checked
  against subscribed topics and fresh partition metadata. Metadata changes after
  stable start are not automatically detected.

On a terminal path, `revoked` resolves with the original cause before cleanup.
`closed` and `close()` provide the bounded cleanup result. A non-null
`cleanupError` means shutdown failed or could not be confirmed within its budget;
local state `closed` is not proof that every underlying resource has settled. Explicit close first
invalidates local validity, then may attempt one bounded Leave only from a stable
state without a pending membership request. A valid successful response confirms
Leave; rejection and an unreceived/malformed response remain distinct. Closing
transport alone does not confirm departure. Cleanup problems do not replace the
original reason. The generic `failure` reason and its original typed error are
not proof of non-execution, rollback or safe retry; failures before dispatch and
unknown outcomes after dispatch can share that classification. `leaveOutcome`
describes departure only, not the outcome of earlier membership operations. `close()` is idempotent, including during startup.

This fail-closed policy sacrifices availability. For example, a cold coordinator
or rebalance can end an otherwise usable session. There is no retry of membership
mutations and no coordinator migration, automatic rejoin, reacquisition or
application-work draining. A new explicit session is a new operation, not a safe
retry of old processing.

## Independent control traffic and deadlines

Each session owns a dedicated control `KafkaClient`. Its
four-request application admission budget is isolated from application Fetch and
Produce. Broker endpoints share strict TLS and optional SASL PLAIN-over-TLS
settings. This does not reserve broker/network/event-loop capacity or prevent a
process pause. If `session.subscribe()` is used, a separate owned data client
is closed when delivery stops; its separate stopped barrier must also be awaited.

Defaults are session 10 seconds, rebalance 20 seconds, heartbeat interval 2
seconds, local expiry 8 seconds, startup 60 seconds, Leave 3 seconds and request
35 seconds. Local expiry must be shorter than the requested broker session
and longer than the heartbeat interval. Request timeout must exceed rebalance
timeout. Connection/resource bounds remain explicit. Broker limits may reject
these requested values; acceptance is not assumed.

Only one heartbeat is outstanding. The local freshness deadline is based on the
successful heartbeat's request start, **not** its reply time. An independent
monotonic watchdog can revoke and close the owned transport while a heartbeat
still waits under the longer request deadline. Startup has a separate overall
budget. Initial Sync is not enough to grant a token; its following heartbeat must
also complete before the local deadline. No timer promises hard real-time
execution, especially while the Dart isolate is blocked.

## Remaining coordinated-consumer work

The bounded delivery layer now supplies per-epoch Fetch/buffer admission and
local revocation barriers. Opt-in processing now adds an acknowledged-prefix
ledger, bounded pending records, monotonic application-stall rules and explicit
single-topic generation commits with partial/unknown evidence. Remaining work
includes application-owned async work draining/fencing, retention/reset choices,
durable application work and deliberate rejoin/recovery.
The broker's generation checks do not prove partition ownership or make effects
exactly once. None of those properties follows from this membership session.

Membership evidence is recorded under `verification/group_session/`. The
additive delivery checkpoint has its own `verification/group_delivery/` receipt.
The bounded native tests do not establish long-duration coordinated processing,
independent-host failover, other Kafka versions/platforms, Flutter embedding or
production readiness. No package or repository publication is performed.

Later [headless Flutter engine evidence](RUNTIME_COMPATIBILITY.md) covers nine
specified cases on the newer checkpoint. It does not expand this historical
checkpoint's tests or establish packaged-app or real OS lifecycle support.
