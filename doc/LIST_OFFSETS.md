# Explicit earliest/latest lookup

`KafkaClient.listOffsets(topic, partition: 0, position: KafkaOffsetPosition.earliest)`
reads one selected partition. Choose `KafkaOffsetPosition.latest` explicitly for
the other boundary. The result exposes `throttleTimeMs`, the requested topic and
partition, its `offset`, `timestamp` and successful Kafka error code.

- Earliest is the retained log beginning, not literal zero. Retention may have
  advanced it, and compaction means that no record need exist at that exact offset.
- Latest is the exclusive read-uncommitted high watermark. It is not the last
  record's offset, a record count, a read-committed boundary or unreplicated leader
  log end. Fetching from it starts at that boundary without subtracting one.
- An empty partition can have equal earliest/latest boundaries. Returned offset
  zero is valid. Boundary timestamps are -1, meaning unavailable.
- A subsequent Fetch can race retention, new records, deletion or leader changes.
  Read retries look up a fresh boundary. This is not an atomic log snapshot.

## Wire and validation contract

The fixed codec uses API key 2/version 2, request header v1, response header v0,
consumer replica ID -1 and read-uncommitted isolation byte 0. Earliest/latest use
timestamp selectors -2/-1. Only the selected version is admitted after checking
the actual leader connection's ApiVersions table. Unsupported v2 fails without
fallback. v0, flexible versions, timestamp searches, leader epochs and special
local/tiered offsets are outside this API.

Topic names must be nonempty lossless UTF-8 fitting Kafka STRING. Partition
indices must fit nonnegative INT32. Invalid arguments fail before discovery;
closed clients report StateError first. The response parser checks sizes,
counts, UTF-8, scalar sentinels and complete consumption. High-level lookup
requires exactly the selected topic and partition. Extra, missing or duplicate
results are errors, including an empty response with positive throttle metadata.

The low-level decoder preserves broker error codes and unavailable -1 offsets.
High-level calls surface broker errors before interpreting successful values.
A successful unavailable offset or a boundary timestamp other than -1 is
malformed, not permission to substitute zero or a fallback. Topic/partition lists
and result fields, including correlation identity, are immutable.

## Retries, shutdown and group boundaries

Existing `maxReadRetries` bounds transport/deadline and leader errors 5/6/9.
Each round rediscovers the leader and creates a fresh single-use request. TLS,
authentication, unsupported-version, protocol and resource failures remain
terminal. Explicit TLS bootstrap fallback retains its existing policy; the
Fetch-only `tlsFetchRecovery` option does not apply. ListOffsets v2 is throttled
by the broker before its response, so it does not add a v3+ client-throttle wait.

Like ordinary high-level Fetch, this API has no scoped cancellation token or
operation-wide deadline. Connection/request phase limits still apply.
`Future.timeout` does not cancel the underlying operation. Closing the client
interrupts requests and retry waits and also stops unrelated work on that client.
The low-level `send` API retains its existing cancellation support and caveats.

Lookup changes no consumer cursor, assignment, stored offset or reset policy. It
does not join a group or commit. Fetch, subscription and managed-consumer defaults
are unchanged. Managed consumers continue to read committed positions, with
numeric-only missing-position fallbacks by default. Their separate nullable
[`missingOffsetPosition` option](GROUP_MISSING_OFFSETS.md) can resolve successful
missing values after numeric fallback and position-floor checks. It never
converts errors into missing values or resets out-of-range Fetch.
Applications coordinating a lookup with a group must still validate their exact
current assignment before using its result; lookup grants no membership authority.

## Evidence and limits

Independent ByteData wire fixtures and loopback protocol peers cover exact
selectors and headers, retained nonzero/empty boundaries, scalar/malformed
results, immutable snapshots, exact API admission, terminal errors, bounded
leader/transport/deadline retry and shutdown. These peers do not run Kafka.
No new real-broker, security-interoperability, retention or failover qualification
is implied. Those gates need explicit coordinated execution for the final source.

Primary references: Apache Kafka [request schema](https://raw.githubusercontent.com/apache/kafka/4.0/clients/src/main/resources/common/message/ListOffsetsRequest.json),
[response schema](https://raw.githubusercontent.com/apache/kafka/4.0/clients/src/main/resources/common/message/ListOffsetsResponse.json),
[broker boundary selection](https://raw.githubusercontent.com/apache/kafka/4.0.2/core/src/main/scala/kafka/cluster/Partition.scala),
and [broker error handling](https://raw.githubusercontent.com/apache/kafka/4.0.2/core/src/main/scala/kafka/server/ReplicaManager.scala).
The existing official Kafka 4.4.0 client JAR's embedded schemas were also inspected
to confirm the same v2 layout. No external package is added to the Dart library.
