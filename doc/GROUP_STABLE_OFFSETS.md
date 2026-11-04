# Explicit stable committed-position reads

`KafkaClient.committedOffsets` can explicitly request Kafka's stable-read behavior:

```dart
final offsets = await client.committedOffsets(
  groupId,
  topic,
  partitions: {0, 1},
  requireStable: true,
);
```

Managed consumers can apply the same choice to every initial and replacement
position lookup:

```dart
KafkaGroupConsumer(
  // Existing required broker, group, topic and processing settings...
  requireStableOffsets: true,
);
```

Both options default to false. The existing default sends OffsetFetch v5 exactly
as before. Explicit true selects OffsetFetch v7 with `RequireStable=true`; the
connection must advertise support for v7. There is no downgrade to v6/v5, no retry
of successful missing values, and no locally remembered offset substitution.
Merely selecting a newer wire version without the true flag is not equivalent.

## What the choice means

A stable read asks the broker for stable committed-offset visibility. It may wait
or return an error rather than immediately return a value from an older visible
snapshot. It can still legitimately return no offset for a group or partition
without committed progress. Successful absence is not an instruction to reset.

The observed Kafka 4.0.2 new coordinator can finish replay after migration while
its committed snapshot is behind its replayed state. In a controlled experiment
with `replica.fetch.wait.max.ms=5000`, official Java v5 and v7(false) both returned
successful absence for an acknowledged offset already replayed by the new leader.
A single v7(true) request waited for the existing committed boundary to advance,
then returned that offset. Concurrent v5 reads changed only after that advance.
The server did not append another record during the stable request's wait.

Kafka 4.0.2 computes the response, including its pending-transaction check,
before deferring completion behind pending writes. It does not first wait and
then look up the offset. Thus a precomputed error-88 response can arrive after
the transaction settles. See the versioned sources and internal-boundary
explanation in the [reliability contract](RELIABILITY_CONTRACT.md).

This establishes the mechanism and stable-request behavior in that controlled
fixture. Earlier failures with default fixture timing remain preserved; their
exact internal state was not captured. Other broker implementations/versions and
arbitrary failures are not certified by this experiment. The implementation does
not repair Kafka replication, force a commit, or establish a durable monotonic
history of every group's positions.

## Bounds, errors and security

Without a separate retry policy, existing connection/request deadlines, resource
limits and finite read/discovery retry budgets remain. These are per-phase bounds,
not a new overall deadline. An explicit `stableOffsetRetry` policy adds a single
operation deadline and bounded error-88 retries; see
[bounded unstable-offset retries](GROUP_UNSTABLE_OFFSETS.md).
Coordinator errors 14/15/16 and permitted transport failures retain their existing
read retry behavior. Successful null, unsupported v7, malformed data, explicit
TLS/authentication failures and other terminal broker errors are not newly
retryable. `UNSTABLE_OFFSET_COMMIT` (88) remains an error, never a missing value.

Every new TLS connection still validates certificates and hostnames. Metadata or
data Fetch fallback options do not change group discovery/position TLS behavior.
Closing the client interrupts its pending read and retry waits. Applying a timeout
to an application Future is not a cancellation primitive.

## Comparison with the official Java client

Kafka 4.0.2's Java `ConsumerCoordinator` requests stable offsets and retries
`UNSTABLE_OFFSET_COMMIT` with its timer/backoff. Its request builder can clear the
stable flag when negotiating below v7 unless its strict unsupported-stable option
is enabled. This implementation deliberately differs: opting in requires exact
v7 support and never downgrades stability. Error 88 remains terminal under the
existing read policy unless the separate `stableOffsetRetry` policy is provided.
That policy adds bounded read-only retries, not full Java-client liveness or
compatibility parity, and cannot confirm an ambiguous commit.

Sources: [Java committed-offset retry loop](https://github.com/apache/kafka/blob/4.0.2/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java#L922-L960),
[stable request and response handling](https://github.com/apache/kafka/blob/4.0.2/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java#L1436-L1527),
and [OffsetFetch request builder](https://github.com/apache/kafka/blob/4.0.2/clients/src/main/java/org/apache/kafka/common/requests/OffsetFetchRequest.java#L139-L155).

## Managed ownership remains separate

`requireStableOffsets` is independent of `rejoinOnCoordinatorLoss`. Stable reads
do not grant assignment authority or add a rejoin reason. Revocation during
initial position loading still stops that cycle permanently; it closes the owned
position client and rejects late results before data Fetch, callbacks or commits.
A longer broker wait does not extend the local membership deadline.

When coordinator-loss recovery is also enabled, its previous-assignment position
floor checks remain in force. Missing or lower fresh positions behind previously
observed/confirmed progress still fail closed. Stable reads do not turn ambiguous
commits into confirmed commits, and readback cannot authorize their replay.

No transaction producer/commit workflow, application-effect atomicity, exactly-once
processing, external fencing or general coordinator failover is added.

## Wire and verification

The v7 request uses request header v2 and a compact/tagged body; its response uses
response header v1. The correlation ID remains the first response-header field.
Unknown well-formed tags are skipped, while malformed lengths, ordering, counts,
UTF-8, correlations, partition sets and trailing bytes remain rejected.

The checkpoint's verification report separates official-codec golden vectors,
strict-TLS/lifecycle tests, controlled real-broker observations, default-timing
non-reproductions and retained failing receipts. A successful later stable read
never converts the original successful-null assertion into a pass.

See [explicit offsets](GROUP_OFFSETS.md), [managed consumers](GROUP_CONSUMER.md)
and [coordinator-loss recovery](GROUP_COORDINATOR_RECOVERY.md).
