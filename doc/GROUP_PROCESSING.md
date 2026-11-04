# Bounded single-generation application processing

Import `package:kafka_client/kafka_group.dart`. After `session.start()`, choose
`session.subscribeProcessing(...)` instead of `session.subscribe(...)` to enable
an acknowledgement ledger, finite processing stalls and explicit generation-aware
commits. Each session still permits exactly one delivery handle. Raw delivery,
manual subscriptions and manual `commitOffsets` retain their existing behavior.

Supply explicit inclusive offsets for exactly the assigned partitions. There is
no implicit OffsetFetch, reset, seek or rejoin. The chosen start position can be
older than an existing stored offset; committing the resulting acknowledged work
can therefore intentionally rewind stored progress. The library does not read or
compare external stored offsets for you. An empty assignment uses `{}`.

## Acknowledgement is application evidence, not persistence

Call `processing.acknowledge(event)` only after application processing succeeds.
The exact object must have been admitted by this handle's original synchronous
stream. Copying its public fields, token, offset or even its `KafkaRecord` object
cannot fabricate a receipt. A duplicate acknowledgement returns false while the
handle remains locally current; any acknowledgement after revocation throws.

A per-partition FIFO retires only its acknowledged prefix. For delivered offsets
`10, 12, 15`, acknowledging 12 and 15 first advances nothing. Acknowledging 10 then
advances the candidate next offset to 16. Kafka offsets need not be consecutive.
Neither Fetch cursor movement, high watermarks, filtered control-only batches nor
batch-end gaps create acknowledged progress. A last delivered record at 10 whose
batch ends at 15 permits a candidate of 11, not 16. This can conservatively reread
non-application gaps after a restart; it does not skip unacknowledged work.

`acknowledgedNextOffsets` and `confirmedNextOffsets` are independent immutable
snapshots. The latter contains only this handle's successful broker response
evidence. Initial positions and delivered records create neither confirmation nor
a durable checkpoint. Another client can overwrite the same group's stored state.

## Bounded admission and processing stalls

`maxPendingRecords` defaults to 1,000 and bounds all unretired entries, including
acknowledged records blocked behind an unfinished head. At capacity the existing
single delivery pump waits before its next Fetch or original callback. One
already-decoded response may remain buffered under the existing delivery bounds.
Acknowledging a retired prefix wakes capacity waits. A global window can stall
all partitions behind one slow partition; this is deliberate bounded backpressure,
not per-partition fairness or a worker pool.

The ledger uses weak object-identity receipts and retains offsets/times, not
record payloads. The count does not bound application-held records, async jobs,
external buffers or total heap. Existing record-count/payload bounds apply after
response decoding, not before allocations. See [GROUP_DELIVERY.md](GROUP_DELIVERY.md).

`processingTimeout` defaults to 30 seconds, must be positive and at most one hour.
Every unfinished partition head keeps its original monotonic admission deadline.
Heartbeats, later acknowledgements and unrelated partition progress cannot extend
it. After a prefix retires, its successor retains that successor's original time.
A timer and synchronous admission/acknowledgement/commit checks enforce expiry;
early timer wakes are rearmed. Blocking the isolate still prevents hard-real-time
execution. No outstanding work means no application-processing deadline.

Expiry ends record admission, clears the ledger, cancels pending commit work and
closes membership without another membership mutation. It never commits on close.
An application task already started may still finish and needs its own duplicate,
fencing and cancellation policy. Independent heartbeats do not mean application
processing is healthy.

## Explicit commits, one topic at a time

`await processing.commitAcknowledged(topic)` captures that assigned topic's
advanced candidate offsets. It sends no request when nothing advanced. One call
is allowed in flight across the whole handle, including other topics. Overlap is
rejected rather than queued, and a caller's `Future.timeout` cannot release that
gate or cancel the underlying request. New acknowledgements during a commit remain
candidates for a later explicit operation. No automatic commit schedule exists.

A commit uses OffsetCommit v6 on the prepared coordinator, the exact session
generation/member, unknown leader epoch and null metadata. Support is checked
against the coordinator ApiVersions snapshot obtained during membership setup.
There is no coordinator rediscovery, automatic replay or generation -1 fallback.
The control client owns commits alongside heartbeats with its existing fixed
request budget; it never sends Fetch, Produce or OffsetFetch. A changed/rejected
coordinator ends this bounded session rather than starting a recovery workflow.

Results preserve one outcome for every partition in the requested snapshot:

- `confirmed`: a complete, valid broker response returned error code zero.
- `rejected`: a complete, valid response returned a nonzero partition error code.
- `unknown`: after handing the request to the native driver, a lost/malformed
  response, transport error, timeout or cancellation prevents confirmation. This
  includes conservative cases where transmission cannot be established.
- `notAttempted`: a final local admission guard failed before invoking the driver.

A mixed valid response preserves successes and rejections separately. A missing,
duplicate, unexpected or otherwise malformed response is never partially applied.
Negative throttle is malformed. Positive throttle preserves valid partition
response evidence but ends the session because quota continuation is unsupported.
Any rejection, unknown outcome or other commit failure is terminal here. There is
no automatic retry, rollback inference or switch to manual commits. Multiple
topics require separate explicit calls and are never atomic with each other.

The exact token is checked immediately before native request admission with no
intervening await. Native transport may still be awaiting a connection, TLS/SASL
or a prior flush. Revocation synchronously cancels the request and initiates owned
transport cleanup, preventing future admission where cancellation can still take
effect. It cannot retract bytes already accepted by a socket or broker. Closing
while a commit is outstanding suppresses graceful Leave; failure cleanup also
sends no Leave. A valid late response can still provide confirmation evidence,
but `locallyCurrent` remains false and the session cannot revive.

Broker generation checks are not a partition-ownership lease, monotonicity rule
against other clients or atomic transaction with application effects. Applications
must handle replay and duplicate effects; doing so alone is not a blanket
at-least-once certification. See the [reliability contract](RELIABILITY_CONTRACT.md).
This increment makes no exactly-once or external-side-effect fencing claim.

## Usage and shutdown

The following callback shows only the successful processing step. Integrate it
with the application's failure policy and lifetime handling; do not catch errors
and silently continue with a false checkpoint.

```dart
final processing = session.subscribeProcessing(
  assignment: token,
  startOffsets: applicationPositions,
  maxPendingRecords: 1,
  processingTimeout: const Duration(seconds: 30),
);

// In your application's work handler for an originally delivered event:
await applyApplicationWork(event); // Can outlive revocation; make effects safe.
processing.acknowledge(event);     // Throws if the local generation has ended.
final result = await processing.commitAcknowledged(event.topic);
inspectEveryPartitionOutcome(result);
```

This is not a guarantee that `applyApplicationWork` is cancelled or fenced by the
following acknowledgement. An async `listen` callback is not awaited. Supply an
error handler, track its Future yourself and observe `processing.stopped` even
when no stream callback is running. The original listener boundary from
[GROUP_DELIVERY.md](GROUP_DELIVERY.md) also applies here; transformations,
`await for`, `StreamIterator` and application queues can move processing beyond it.

Use `try/finally` around session startup and delivery creation so invalid
configuration cannot leave membership running. On shutdown:

1. `processing.cancel()` synchronously invalidates new admission and receipts.
2. Await `processing.stopped` for the actual Fetch/data cleanup and any admitted
   commit to settle. Inspect `error` and `cleanupError`.
3. Cancel the listener and separately await `session.close()`/`session.closed` for
   membership/control cleanup, even if delivery creation failed.
4. Handle or drain application work under the application's own explicit policy.

The processing barrier has no independent hard deadline. It is separate from
paused stream error/done delivery and does not wait for application Futures.
Processing failures produce a terminal stream error only after the pump is safe
to emit, never reentrantly from acknowledgement or a revocation hook. The stopped
result remains usable even if the listener is paused. If a commit cancellation
hook fails, data and membership cleanup still run;
`KafkaGroupProcessingCleanupException` preserves that error and any data-close
error. Neither absence of a late callback nor local transport closure proves
broker rollback or departure. There is never an implicit final commit.

## Evidence limits and remaining work

Source-specific receipts live in `verification/group_processing/`. These cover
bounded same-host native Linux tests on the stated Dart SDKs and Kafka 4.0.2,
not production certification. Controlled socket peers prove wire/send boundaries,
not durable Kafka storage. No new independent-host, long-duration coordinated
processing, Flutter embedding or cross-platform claim is made.

This single-generation handle still has no automatic position lookup/reset or
rejoin/recovery. A separate opt-in serial runner now supplies fresh committed
positions and narrowly bounded heartbeat-rebalance rejoin after callback draining;
see [GROUP_CONSUMER.md](GROUP_CONSUMER.md). Still absent: implicit offset resets,
general recovery, durable application work storage, transactions, idempotent external effects, leader-epoch
truncation recovery, cooperative/static membership, and a hard overall shutdown
or processing deadline for user-owned asynchronous work. Publication and release
support decisions remain separate.

Later [headless Flutter engine evidence](RUNTIME_COMPATIBILITY.md) covers nine
specified cases on the newer checkpoint. It does not expand this historical
checkpoint's tests or establish packaged-app or real OS lifecycle support.
