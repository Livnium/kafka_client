# Bounded serial consumer with fresh positions and rejoin

`KafkaGroupConsumer` is a new opt-in runner in `kafka_group.dart`. Existing
single-generation lifecycle, raw delivery, processing handles and manual commit
rules remain intact. The separately documented Metadata-v0 replica-warning
compatibility correction also applies to single-generation startup. This runner owns a serial application callback and offers a
narrow, finite automatic rejoin policy. It is still experimental native Dart.

## Explicit setup choices

```dart
final consumer = KafkaGroupConsumer(
  brokers: [Broker('127.0.0.1', 9092)],
  groupId: 'my-dedicated-group',
  topics: {'pre-created-topic'},
  commitAfterProcessing: true, // Required choice; no hidden default.
  missingOffsets: {'pre-created-topic': {0: 0}}, // Literal fallback, not earliest.
  maxRejoins: 3,
);

try {
  final end = await consumer.run((record) async {
    // Return ALL work for this record. Detached Futures are not owned or drained.
    await applyIdempotentApplicationWork(record);
  });
  inspectConsumerTermination(end);
} finally {
  consumer.stop();
  inspectConsumerTermination(await consumer.closed);
}
```

The application must arrange when to call `stop`, for example on its shutdown
signal. `run` stays active until explicit stop, a terminal failure or the rejoin
limit. The example is a lifetime sketch, not a transaction or exactly-once recipe.
If a callback decides to stop, call the synchronous `consumer.stop()` and return.
**Do not await `consumer.closed` inside that callback:** it waits for that callback
itself to settle. Await it from the outside owner instead.

`commitAfterProcessing: true` acknowledges and commits each successfully processed
record before admitting the next callback. It trades throughput for a simple
serial boundary. False still acknowledges locally for backpressure but sends no
commits. In that mode a later generation can replay work from an older stored
offset or the same explicit fallback. Neither mode commits after explicit stop or
a callback that failed or completed after losing local validity.

The same copied broker list, topic set and configured TLS/SASL objects are used
for every cycle and position reader. Existing strict TLS and SASL-over-TLS policy
remains intact. Session, transport, Fetch and resource settings are validated at
construction without network activity. Native clients remain exclusively owned.

`tlsFetchRecovery` is a separate nullable opt-in for each generation's dedicated
data client. It requires TLS and `allowTlsBootstrapFallback: true`; it is not
forwarded to position or control clients. It does not change rejoin eligibility,
revocation authority, callback draining or commit replay rules. See
[GROUP_FETCH_RECOVERY.md](GROUP_FETCH_RECOVERY.md).

## Starting positions for every generation

After Join/Sync and the confirming heartbeat establish an assignment, a separate
owned client reads broker-stored next offsets for exactly the nonempty assigned
topic/partition sets. Every topic is read afresh on every replacement generation.
No old Fetch cursor, local acknowledged candidate or prior confirmation snapshot
is reused as a starting position. Empty assignments issue no position read or
Fetch and can maintain membership.

- A valid stored next offset wins over `missingOffsets`.
- A successful response with no stored offset uses the copied numeric fallback
  for that topic/partition, subject to the opt-in replacement consistency guard.
  Without a numeric entry, separately opting into `missingOffsetPosition` looks
  up earliest/latest; the default null still fails before processing. See
  [missing-position resolution](GROUP_MISSING_OFFSETS.md) for precedence and risks.
- Lookup failures, malformed/incomplete responses and invalid offsets never
  become a missing-offset fallback.
- Earliest/latest lookup is limited to that explicit missing-position option.
  There is no retention reset, seek, log-truncation recovery or interpretation of
  stored leader epochs. Literal zero can be out of range for retained logs.
- Assignment validity is checked before and after every awaited read and again
  before processing is attached. Stale lookup results never reach the callback.
- The position client must finish its actual close before data processing starts.
  Revocation/stop initiates its close immediately to interrupt an admitted read.
  A close failure is terminal, not evidence that the client was drained.

Default position reads use OffsetFetch v5, which lacks RequireStable. A separate
`requireStableOffsets: true` option selects stable v7 reads for initial and every
replacement generation. It can wait for broker stability and requires advertised
v7 support; no downgrade, null retry or offset substitution occurs. Optional
`stableOffsetRetry: KafkaStableOffsetRetry(...)` adds a separate bounded error-88
read retry policy and requires `requireStableOffsets: true`. Its deadline applies
to each position read, not the whole generation or membership lifetime. See
[bounded unstable-offset retries](GROUP_UNSTABLE_OFFSETS.md) and
[stable reads](GROUP_STABLE_OFFSETS.md) for the migrated-position visibility gap
and limitations. Revocation while loading still terminates that cycle before any
callback, even if its stable read later succeeds.

Readback remains a starting-position observation, not proof of an earlier
ambiguous commit's outcome or a broker ownership lease. Other writers can overwrite the same group. Starting from an explicitly
older fallback can rewind later stored progress when commits are enabled.

## Owned serial callback boundary

The runner listens directly to the existing synchronous processing stream. It
pauses the listener before invoking application code, checks the exact token, and
invokes the callback with no intervening await. It owns the exact returned Future.
No second callback starts while that Future or its configured commit is pending.
The processing ledger has capacity one, while the existing one-response buffer
retains its independent post-decode bounds.

Only successful completion while still locally current permits acknowledgement.
When commits are enabled, the runner awaits the actual generation/member commit
and inspects its partition evidence. An unknown or rejected commit is terminal,
even if an eligible heartbeat failure revoked the generation first. Applying `Future.timeout` to
`run` or `closed` does not cancel the runner or make an admitted request complete.
Use `stop()` to begin explicit shutdown and inspect `closed` afterward.

Revocation stops new original callbacks and invalidates receipts, but an already
invoked callback can continue. Its Future must settle before a new generation can
start. The processing ledger's cleared pending count is **not** used as application
drain evidence. Detached Futures, externally queued work, external transactions
and effects not represented by the returned callback Future remain outside this
ownership boundary. Application effects still need idempotency or an independent
fencing/transaction design.

A processing timeout ends membership and owned network admission, but does not
pretend to cancel the application Future. If that Future never settles, `closed`
remains pending and no rejoin occurs. This preserves safety rather than abandoning
old work to make a timeout look like successful draining. No hard overall shutdown
deadline or external-effect cancellation is claimed.

## Exact automatic rejoin policy

A replacement session is created only when all of these hold:

1. The prior cycle reached processing, including an empty assignment.
2. Its membership ended through a heartbeat broker error 27
   (`REBALANCE_IN_PROGRESS`), or the separately enabled coordinator-loss
   policy recognizes an actual stable-heartbeat failure described below.
3. Application processing settled successfully, with no failed, rejected or
   unknown commit hidden behind the earlier revocation.
4. The actual data/commit barrier, position work/closure and membership/control
   cleanup settled with no reported cleanup failure.
5. The runner was not stopped and still has a rejoin allowance.

The old session and token are never revived. A new dynamic member/session is
created after a close-aware monotonic delay. `maxRejoins` defaults to 3, accepts
0 through 100 and is a total lifetime budget, not a counter that resets after a
successful record. The maximum attempted generations is one plus that budget.
`rejoinDelay` defaults to 250 ms and must be between 1 ms and 1 minute. Early timer
wakeups do not permit early rejoin, and stop wakes the backoff without another Join.

Startup and position-loading revocations/failures are terminal in this increment.
So are local expiry, ineligible heartbeat errors, Fetch failures, processing stalls
or exceptions, commit rejection/uncertainty, malformed responses and cleanup
errors. There is no general coordinator failover, retry-until-success loop,
automatic Produce/commit replay, or recovery of uncertain startup mutations.

A rebalance rejection does not prove the old member departed. Cleanup need not
have a confirmed Leave for this narrowly eligible transition, and broker expiry
or further group coordination can delay the new Join. The normal session startup
budget applies; a failed replacement startup is terminal. Static membership,
cooperative assignment and transparent coordinator recovery are not implemented.

## Separate coordinator-loss opt-in

`rejoinOnCoordinatorLoss: true` extends the eligible causes only for failures
originating in an actual stable heartbeat. It accepts explicit coordinator broker
errors 15/16 and the typed connection/socket/timeout failures described in
[GROUP_COORDINATOR_RECOVERY.md](GROUP_COORDINATOR_RECOVERY.md). It is false by
default and shares the existing lifetime `maxRejoins` and `rejoinDelay` bounds.

This starts a fresh generation after all the same drains, rather than retrying
old membership or commit requests. It does not extend local expiry or enable
TLS fallback for group discovery. Replacement startup can still fail, including
when a dead first bootstrap accepts TCP and closes during TLS negotiation.
Explicit TLS/certificate/hostname and authentication failures remain terminal;
when TLS is configured, every new connection continues strict validation. Established transport errors
already normalized to `KafkaConnectionException` cannot be distinguished further
by this option. This remains a limited availability policy, not a general broker
crash-recovery guarantee.

When this option is enabled, a replacement read that is missing or below a known
previous broker-observed/confirmed position fails before fallback or callbacks.
The comparison floor is never used as a replacement starting offset. Only the
previous assignment is retained; new or returning partitions may have no floor.
This in-memory check applies only to overlapping partitions of the immediately
preceding generation in this same consumer object. It is not persisted or carried
into a newly created consumer. A halt does not diagnose permanent loss or its
cause. See the [reliability contract](RELIABILITY_CONTRACT.md) and policy document
for commit, external-reset and history limits.

## Stop and result evidence

`stop()` synchronously disables future callbacks, acknowledgements, commits and
new joins. It initiates owned cleanup in joining, position lookup, processing,
drain and backoff states. It can be called repeatedly or before `run`.

`closed` waits for the actual admitted callback, data/commit and position barriers,
plus the bounded membership cleanup attempt. A membership-cleanup timeout can
return while underlying shutdown remains unconfirmed; its cleanup error blocks
rejoin and must not be treated as successful closure. The result distinguishes cancelled, rejoin-limit and failure termination, and
includes generation-attempt count, primary error, cleanup errors, last membership
result, last processing result and last commit evidence. Inspect those fields,
not just the reason. A valid late commit confirmation remains evidence but never
restores local validity or permits new work by itself.

A pending startup or position operation can fail while explicit stop is underway.
That observation is retained as `interruptedOperationError` without claiming stop
necessarily caused the failure. Clean cancellation is not mislabeled as an
application failure; genuine callback/commit and cleanup failures remain failures.
There is no implicit final commit. Local socket closure cannot prove broker
rollback or departure.

`state`, `generationsStarted`, `assignment` and `isCurrent(token)` are diagnostic
snapshots, not leases. Checking one before an arbitrary later async effect cannot
fence that effect. The runner is single-use; construct another instance for an
explicit new operation after reviewing the previous termination evidence.

## Evidence and limits

The source-specific receipt is in `verification/group_rejoin/`. Controlled local
peers establish wire/send-count behavior; real Kafka fixtures separately establish
stored-offset and membership behavior. Same-host short tests do not establish
long-duration, high-throughput, cross-platform, other-Kafka-version or
independent-host reliability. Separate [headless Flutter evidence](RUNTIME_COMPATIBILITY.md)
covers only its nine stated cases, not Flutter endurance or OS lifecycle support.
No production certification or exactly-once claim is made. Publication and release support remain separate decisions.
