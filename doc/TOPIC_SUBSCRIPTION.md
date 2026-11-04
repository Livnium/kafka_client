# Explicit topic subscriptions

`KafkaClient.subscribe` adds a native Dart, single-listener stream over the
existing high-level Fetch API. It reads one topic and an explicit, fixed set of
partitions. It is experimental and uncoordinated: this is not a consumer-group
implementation or a complete consumer position-management API.

The client still requires native `dart:io` sockets. This feature does not expand
the broker, SDK or operating-system compatibility claims in the README.

## API and defaults

```dart
Stream<KafkaTopicRecord> subscribe(
  String topic, {
  required Map<int, int> startOffsets,
  Duration pollInterval = const Duration(milliseconds: 100),
  Duration retryDelay = const Duration(milliseconds: 500),
  int maxWaitMs = 100,
  int maxBytes = 1048576,
});
```

- `startOffsets` maps a partition number to its **inclusive absolute starting
  offset**. For example, `{0: 120, 2: 85}` selects only partitions 0 and 2, starting
  their reads at offsets 120 and 85 respectively. Those are example positions,
  not a way to discover the start of either partition.
- The map is copied when `subscribe` is called. Later changes to the caller's map
  do not change the subscription. Newly added topic partitions are not picked up.
- There is no default offset. Literal `0` means offset zero, not "earliest retained
  record". Negative earliest/latest sentinels are not accepted. Supply positions
  obtained from your own checkpoints, acknowledged Produce responses or another
  application-managed source.
- `pollInterval` is the delay after a response with no validated batch progress.
  A larger broker throttle takes precedence. Responses that do make progress
  observe the broker throttle without adding the empty-poll delay.
- `retryDelay` separates subscription-level retries after a high-level Fetch
  exhausts its own read-safe retry policy. Both delays must be between one
  millisecond and one minute, inclusive.
- `maxWaitMs` is a broker-side Fetch wait, not a total request or cancellation
  deadline. `maxBytes` is a soft requested Fetch limit; Kafka may return an
  oversized first batch. These have the same meaning as on `fetch`.

The topic and offset map must be nonempty. Partition numbers and `maxWaitMs` must
be nonnegative signed INT32 values; `maxBytes` must be a positive signed INT32
value. Offsets must be nonnegative native signed INT64 values. Invalid
configuration throws `ArgumentError` synchronously, before network work. A valid
call on a closed client returns a stream that reports `StateError`. A stream
created while open but first listened to after client close also reports an
error without starting Fetch work.

The stream emits `KafkaTopicRecord` values with:

- `topic`: the selected topic name.
- `partition`: the partition number.
- `record`: the decoded `KafkaRecord`, including bytes, headers and timestamp.
- `offset`: a convenience getter for `record.offset`, an absolute log offset.

Keys and values remain bytes; a null value is a tombstone, distinct from an empty
value. Records are read-uncommitted, with no transactional visibility guarantee.
Control records are filtered out. Compression remains unsupported.

## Run the example

Pre-create a dedicated `dart-subscribe-example` topic with partition 0 on a trusted
local plaintext Kafka broker, then run from the source checkout:

```sh
dart run example/subscribe_example.dart
```

Optional environment settings are `KAFKA_HOST` (default `127.0.0.1`),
`KAFKA_PORT` (default `9092`), `KAFKA_TOPIC` (default `dart-subscribe-example`) and
`KAFKA_PARTITION` (default `0`). Use only a topic where appending a demonstration
record is acceptable.

The example produces one unique record using `acks=1`, obtains the acknowledged
base offset, subscribes from that exact offset and checks the returned record.
It awaits processing before computing a demonstration next-offset checkpoint,
then cancels the stream and closes its owned client. The checkpoint is in memory
only. A 30-second stream inactivity timeout ends the example's read loop; cleanup
can take longer while an admitted Fetch settles. A Produce failure is not
replayed, since an uncertain acknowledgement may still mean the record was stored.

The plaintext configuration is for a trusted local fixture only. For real
credentials, configure strict TLS and optional SASL PLAIN as described in the
README and `example/kafka_client_secure_example.dart`; the subscription uses those
same client settings. No credentials or TLS bypass are added by this example.

## Ordering, cursor progress and partition ownership

No I/O starts until the stream is listened to. Each stream has one listener and
at most one high-level Fetch in flight. Selected partitions are polled
sequentially in round-robin order. A retryable failure leaves that partition's
cursor unchanged and rotates to the next partition after the retry delay, so one
transiently failing partition does not monopolize every subsequent attempt. An
admitted Fetch and its internal retries can still delay the other partitions;
there is no parallel-read or zero head-of-line latency guarantee. Separate streams
have independent cursors and share the client's connection/request budgets.

Offsets strictly increase within each partition during a subscription. There is
no ordering guarantee across partitions and no promise that offsets are
contiguous: compaction, control batches and other log gaps may leave holes.
Records below the requested offset are filtered, and invalid or non-increasing
response offsets terminate the stream. A batch ending at signed INT64_MAX is
also terminal before its records are delivered because a next offset cannot be
represented; the stream never wraps its cursor.

The cursor advances from validated returned batch/record offsets, not from the
number of records emitted. A control-only or fully compacted batch can advance
internal progress without emitting an application record. An empty response with
no such progress does not advance the cursor. The high watermark alone never
moves it, and an empty poll does not prove that the log has ended.

The stream does not discover earliest/latest positions, use ListOffsets, reset an
out-of-range offset or follow a changed partition assignment. Broker out-of-range
errors terminate it. Retention recovery requires an application decision and a
new subscription with explicit positions. Subscribing twice to the same partition
creates independent readers, not load balancing or coordinated ownership.

## Backpressure and cancellation

Use `await for`, `asyncMap`, or explicit `StreamSubscription.pause()` and
`resume()` when processing is asynchronous. For example, with application-provided
processing and checkpoint functions:

```dart
final nextOffsets = await loadNextOffsets(topic);
await for (final event in client.subscribe(topic, startOffsets: nextOffsets)) {
  await processRecord(event);
  await saveNextOffset(event.topic, event.partition, event.offset + 1);
}
```

Dart's [`asyncMap` documentation](https://api.dart.dev/dart-async/Stream/asyncMap.html)
describes how asynchronous mapping pauses the source subscription. An `async`
callback passed directly to `listen` is not awaited by Dart's stream machinery
and does **not** apply processing backpressure. It can launch overlapping work
even though the subscription has only one Fetch in flight.

Pausing stops admission of further subscription Fetch calls and record events.
An already admitted Fetch is allowed to settle under its existing policy,
including any internal read retries. Its decoded response may remain retained
while paused. The pump checks for pause/cancellation between records. Wire-buffer
budgets and the one-Fetch limit are not total decoded-heap caps; your listener can
also retain records or create unbounded asynchronous work.

`await subscription.cancel()` stops only that stream. It interrupts its poll and
retry delays, stops new subscription Fetch admissions, and waits for an already
admitted Fetch to settle. It does not cancel that Fetch's shared transport
requests or close the shared `KafkaClient`. Cancellation has no immediate-network
or single overall deadline guarantee: existing per-phase connection/request
limits and finite Fetch retry budgets still apply. Cancellation does not wait
for asynchronous listener callbacks you started independently.

`await client.close()` stops all active subscriptions and closes the client using
its existing terminal teardown semantics. Closing a shared client also affects
unrelated work, including potentially ambiguous pending Produce operations.
Cancel an individual subscription when other client users must continue.
`activeSubscriptionCount` reports listened pumps that have registered and not yet
finished, including paused ones; it is not a record-processing or acknowledgement
counter. Unlistened streams do not start work or count as active.

## Errors, retries and security

Each read delegates to the existing high-level `fetch` implementation. That call
retains its configured finite retry policy. After it fails, the subscription
retries only read-safe transient failures: `SocketException`, `TimeoutException`,
`KafkaConnectionException`, and broker error codes 5, 6 and 9. The subscription
waits `retryDelay` and rotates to the next selected partition, retaining the
failed partition's position for a later turn. Retries continue while active,
without a total subscription retry count or overall recovery deadline. These
transient failures are not emitted as stream errors while being retried.

TLS, authentication, malformed protocol/records, unsupported versions,
resource-limit errors and broker offset-out-of-range errors are terminal after
that existing Fetch policy. A terminal failure emits an error and ends the stream;
there is no automatic restart or offset reset. Other non-retryable errors also
end the stream. Handle the stream's error path rather than treating silence as
successful processing or proof of recovery.

When TLS is configured, validation remains strict. `allowTlsBootstrapFallback` stays
default-off and `tlsFetchRecovery` stays default-null. If explicitly configured,
the existing bounded Fetch TLS recovery policy applies to each Fetch; the
subscription does not broaden it or weaken certificate, hostname or authentication
validation. Review `TLS_BOOTSTRAP_FALLBACK.md` and `TLS_FETCH_RECOVERY.md` before
opting in. A terminal TLS failure is not reclassified as a transient socket error.
Produce and low-level send retain their existing non-replay behavior and defaults.

## Checkpoints are an application responsibility

Delivery to a stream listener is **not** acknowledgement of completed processing,
a durable checkpoint, or a Kafka offset commit. The internal polling cursor tracks
what the stream has admitted for delivery, not your application's successful work.
No checkpoint store or cursor-restoration mechanism is implicit in the stream.
Separate [explicit offset APIs](GROUP_OFFSETS.md) can store/read next offsets in
Kafka; the application must call them and supply `startOffsets` itself.

Persist the next offset for each partition only after successful processing, then
pass that map as `startOffsets` on restart. Use the actual record's absolute offset
plus one, not a count of delivered records. Your checkpoint store must preserve
partition identity and avoid advancing past unfinished work. A restart between
processing and checkpoint persistence can replay records; checkpointing too early
can skip unprocessed work. Idempotent processing or application-specific atomicity
is needed if those outcomes matter.

This stream provides no coordinated groups, rebalancing, automatic Kafka commits,
automatic partition discovery, ListOffsets, read-committed isolation, transactions,
idempotent delivery, or exactly-once processing guarantee. Existing `client.fetch`
remains available for application-managed reads; its defaults and stateless
behavior are unchanged.

## Reliability observations

See [SUBSCRIPTION_RELIABILITY.md](SUBSCRIPTION_RELIABILITY.md) for the paced
endurance and secured topology/network-fault gates, exact duration settings,
checkpoint verification, cleanup requirements and evidence limits.
