# Opt-in missing committed-position resolution

`KafkaGroupConsumer` can look up an earliest/latest boundary only when its fresh
committed-position read succeeds with no stored offset and no numeric fallback
applies. This option is separate from Fetch error recovery or offset reset.

```dart
final consumer = KafkaGroupConsumer(
  brokers: [Broker('127.0.0.1', 9092)],
  groupId: 'my-dedicated-group',
  topics: {'pre-created-topic'},
  commitAfterProcessing: true,
  requireStableOffsets: true, // Separate choice; requires exact OffsetFetch v7.
  missingOffsetPosition: KafkaOffsetPosition.earliest,
);
```

Choose `KafkaOffsetPosition.latest` explicitly to start at the exclusive
read-uncommitted high watermark. Earliest can replay retained records. Latest
skips records below the boundary observed by that lookup. The option covers all
assigned partitions lacking numeric entries, including newly assigned partitions.

## Precedence and validation

For every initial or replacement generation:

1. Use the successful broker-stored next offset, including zero.
2. If it is missing, use `missingOffsets[topic]?[partition]` when present.
3. Otherwise use `missingOffsetPosition` when non-null.
4. Otherwise fail before processing, as before.

The default is null, preserving numeric-only fallback. Numeric entries win even
when the boundary option is set; those partitions need no ListOffsets support.
Exact topic/partition response coverage and raw committed-position values are
validated before any boundary query. Results are snapshotted before new awaits.
OffsetFetch errors, unstable error 88, unsupported versions, omitted results and
malformed data never become missing values. No partial positions reach Fetch.

Existing replacement consistency guards run on the raw committed result before
either kind of fallback. With `rejoinOnCoordinatorLoss` enabled, a missing or lower
fresh commit behind an applicable previous-generation floor still stops the
consumer. A larger latest boundary cannot repair that conflict. Boundary and
numeric starts establish no floor; actual broker-observed commits and wholly
confirmed commits retain their existing meaning. The history still covers only
the immediately preceding assignment, not restarts or all past owners.

## Lookup and ownership

Each unresolved partition uses the existing exact ListOffsets v2 API. Earliest
is the retained beginning, not necessarily zero or an existing record. Latest
is exclusive; do not subtract one. It is not a transaction-stable boundary.
Successful unavailable offsets and all lookup failures remain errors, with no
second fallback. This read does not commit a starting position.

Queries run serially through the generation's same owned positions client.
Assignment identity and local expiry are checked before and after every awaited
read. Stop or revocation closes that client and interrupts pending requests and
retry waits. Late results cannot start another query, Fetch, callback, commit or
replacement generation. Position-client closure must settle before processing.
Loading failures and revocation during loading remain terminal for that cycle;
the option adds no rejoin reason and extends no membership authority.

ListOffsets retains its existing bounded retries, connection/request phase limits
and explicit TLS bootstrap fallback policy. Fetch-only TLS recovery is not applied.
There is no new whole-loading deadline or scoped cancellation token. Applying
`Future.timeout` does not cancel work. Each missing partition adds its own lookup
and potential retries, so large assignments can take longer to start.

## Stability and out-of-range limits

Default OffsetFetch v5 may successfully report transient absence after coordinator
migration. Null describes that response, not proof of durable nonexistence. The
separate `requireStableOffsets: true` option requests exact stable v7 and never
downgrades. Optional `stableOffsetRetry` can retry error 88 within its own bounds.
Its deadline applies to that committed read, not subsequent ListOffsets queries.
Neither stable mode nor this boundary policy confirms an ambiguous commit.

Retention, deletion, leadership and new writes can move boundaries between lookup
and Fetch. An out-of-range Fetch remains terminal, whether its start came from a
commit, numeric fallback or boundary. No automatic retention/truncation reset,
seek, leader-epoch validation or application-effect fencing is added. Raw
single-generation sessions, subscriptions and manual reads keep their explicit
numeric start APIs.

## Verification scope

Deterministic controller and loopback protocol tests cover precedence, mixed
assignments, strict validation, snapshots, fresh generation reads, position-floor
guards, cancellation, late results, close barriers and stable-read composition.
They do not establish broker persistence, transaction stability, independent-host
failover or a new real-Kafka qualification.

See [ListOffsets](LIST_OFFSETS.md), [stable reads](GROUP_STABLE_OFFSETS.md),
[bounded unstable retries](GROUP_UNSTABLE_OFFSETS.md),
[coordinator recovery](GROUP_COORDINATOR_RECOVERY.md) and the
[reliability contract](RELIABILITY_CONTRACT.md).
