# Explicit broker-stored offsets

The client can discover a group's coordinator, read committed offsets and
explicitly commit application-processed next offsets. These APIs build on the
existing manually assigned subscription. They do **not** implement coordinated
consumer membership, partition assignment, heartbeats, rebalancing, fencing or
automatic commits.

Use a dedicated group ID and external single-writer coordination for each
partition. A group ID alone does not give a client exclusive ownership. Concurrent
manual writers are last-write-wins and can overwrite newer positions with older
ones. Sequentially await commits; the client does not serialize them into a
monotonic sequence or fence another process. Do not use this manual API to modify
an active managed group's progress. Kafka can reject it with membership errors.

## Restore, process, explicitly commit

```dart
final saved = await client.committedOffsets(
  groupId,
  topic,
  partitions: {0},
);
final committed = saved[0];
if (committed == null && applicationStartOffset == null) {
  throw StateError('Choose an explicit initial offset before subscribing');
}
final start = committed?.nextOffset ?? applicationStartOffset!;

await for (final event in client.subscribe(topic, startOffsets: {0: start})) {
  await processRecord(event); // Application-defined successful processing.
  await client.commitOffsets(
    groupId,
    topic,
    nextOffsets: {event.partition: event.offset + 1},
    metadata: 'application checkpoint',
  );
}
```

The application supplies `processRecord` and a deliberate initial offset. Literal
zero is not an earliest-retained lookup. A null result means only a successful
partition response with no stored offset; errors, malformed responses and omitted
partitions never become null. Unknown groups may legitimately have no offsets.
After coordinator migration, a nonstable read can observe an older committed
snapshot and return missing even when the new leader has replayed the offset.
Explicit `requireStable: true` requests the broker stability barrier; it does not
make genuinely absent offsets exist or authorize resetting a known position.
No automatic retention/out-of-range recovery or earliest/latest resolution occurs.

The committed value is the **next** position to read. Commit only after completed
processing, considering gaps and out-of-order work. Stream delivery and its internal
cursor do not acknowledge business processing. A crash between processing and
commit can replay records; committing too early can skip work. Application effects
and offsets are not atomic, and these methods do not provide exactly-once behavior.
Kafka retention/expiry policies can remove stored offsets; this is not a permanent
application database. The subscription never reads or commits offsets implicitly.

## API and wire contract

- `findGroupCoordinator(groupId)` returns the discovered `Broker` without joining
  the group. Success requires a usable advertised endpoint.
- `committedOffsets(groupId, topic, partitions: {...})` returns an immutable map
  whose values are nullable `KafkaCommittedOffset`. Optional `requireStable: true`
  selects an explicit stable v7 read; the default remains v5. A present value has
  `nextOffset`, `leaderEpoch` (-1 means unknown), and nullable `metadata`.
  Optional `stableOffsetRetry` requires stable reads and enables bounded error-88
  read retries; its operation deadline and cancellation differ from the default
  per-phase bounds. See [bounded retries](GROUP_UNSTABLE_OFFSETS.md).
- `commitOffsets(groupId, topic, nextOffsets: {...}, metadata: ...)` completes
  only after every requested partition returns broker code zero. It permits
  explicit backwards positions but does not protect against accidental rewind.
  Metadata applies to every partition in this one-topic request.

Protocol selection is fixed and checked on the actual connection:

| API | Key | Version |
| --- | ---: | ---: |
| FindCoordinator | 10 | 2 |
| OffsetFetch (default) | 9 | 5 |
| OffsetFetch (`requireStable: true`) | 9 | 7 |
| OffsetCommit | 8 | 6 |

Default requests use non-flexible request header v1 and responses use header v0.
Explicit stable OffsetFetch uses flexible request header v2 and response header v1,
with compact fields and tagged-field validation. Unsupported v7 is terminal,
without downgrade. The
selected schema definitions are embedded in the installed official Kafka 4.0.2
client JAR. The original unsupported ZooKeeper-backed OffsetFetch v0 source stays
unexported and excluded from the prospective publication payload.

Commits use generation -1, empty member ID and leader epoch -1. This supports
manual simple-group offsets, not classic JoinGroup/SyncGroup participation or
the newer consumer-group protocol. Default OffsetFetch v5 has no `RequireStable`
flag. Explicit v7 stable reads request broker stability semantics but do not add
transactional offset commit workflows. See [stable reads](GROUP_STABLE_OFFSETS.md)
for the observed migrated-coordinator visibility gap, bounds and limitations.
Returning a leader epoch does not implement truncation handling or epoch fencing.

Group/topic IDs must be nonempty, valid UTF-8 protocol strings of at most 32,767
encoded bytes. Sets/maps must be nonempty. Partition IDs must fit nonnegative
INT32 and committed next offsets nonnegative INT64. Nullable metadata must fit
the same protocol STRING limit. The broker's configured metadata limit may be
smaller and can reject an otherwise wire-valid string. Inputs are snapshotted
and checked before I/O; invalid calls deliver errors through their Futures.

The advanced `kafka_protocol.dart` import exposes these three explicit request
types and their typed responses. Those retain throttle/error diagnostics.
By default, high-level methods use per-phase request/connection deadlines and
resource limits, without an overall-operation deadline or high-level cancellation
token. The separate `stableOffsetRetry` policy adds an overall read deadline and
allows `KafkaCancellationToken` for that read; see [bounded retries](GROUP_UNSTABLE_OFFSETS.md).
Neither path adds client-side quota scheduling. Applying `Future.timeout` does
not cancel underlying work. Closing the client interrupts outstanding work.

## Reads, rejection and unknown commit outcomes

Coordinator discovery and offset reads retry transport failures and coordinator
codes 14/15/16 within `maxReadRetries`, refreshing discovery on every read round.
Each discovery traversal tries distinct configured bootstraps; terminal errors
stop it. Delays use existing bounded jitter and are interrupted by client close.
Authentication, protocol, unsupported-version and resource errors remain terminal.
All partition errors are inspected before returning an offset map; a terminal
partition error is not hidden behind a retryable error from another partition.

TLS/authentication stays strict for bootstrap and advertised coordinator endpoints.
The existing optional TLS recovery policies apply only to Metadata/Fetch. They do
not cause certificate/handshake rejection or authentication rejection during
group discovery or coordinator connection to traverse other peers. Transport
deadlines, including a stalled TLS handshake timeout, remain eligible for the
bounded read/discovery retry policy; every new connection still validates TLS.
There is no plaintext or weaker-authentication fallback.

**The commit request is dispatched once, never automatically replayed.** Only
its pre-send coordinator discovery may retry. This also applies when an actual
commit response reports coordinator errors: the caller receives that result.

`KafkaOffsetCommitException.partitionErrors` is an immutable map of every
requested partition's returned code, including successful zeros.
`successfulPartitions` identifies those acknowledged by the broker. A partial
failure does not roll back successful partitions or authorize replaying them.

Timeout, connection loss, client closure, malformed response or response-side
resource failure can leave a commit's outcome **unknown**. Those preserve their
original exception rather than being mislabeled as definite broker rejection.
An older commit may have been applied or may still finish. Blindly replaying it
can overwrite newer progress. Even matching stable readback observes the state
returned by that read, not which historical write caused it, the outcome of an
ambiguous attempt, or once-only
application effects. See the [reliability contract](RELIABILITY_CONTRACT.md).

A separate advanced `MemberOffsetCommitRequest` carries an explicit classic
member/generation tuple. It does not change these manual APIs or supply a managed
consumer. See [CLASSIC_GROUP_PROTOCOL.md](CLASSIC_GROUP_PROTOCOL.md) for its
requirements and the distinction between generation checks and partition ownership.

## Verification scope

The source-specific receipt and commands are retained under
`verification/group_offsets/`. Synthetic peers exercise request counts and error
policies; they do not prove Kafka persistence. Actual disposable-broker tests
are recorded separately, including independent official Java-client checks. A
separate synthetic TLS matrix covers strict group-operation boundaries with both Metadata/Fetch
recovery options enabled and a separately advertised coordinator.
Historical subscription endurance belongs to its recorded runtime snapshot and
is not a fresh long-soak result for this additive code.

Coordinated membership/assignment, rebalancing, commits during a rebalance,
coordinator failover across independent hosts, transactional offset stability,
long-duration mixed workloads and additional target platforms remain separate
steps. No publication, platform expansion or production certification is implied.
