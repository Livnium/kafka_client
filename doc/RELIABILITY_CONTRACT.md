# Reliability contract and application boundaries

These are the existing client's safety boundaries, not new defaults or a delivery
certification. See [explicit offsets](GROUP_OFFSETS.md),
[managed consumers](GROUP_CONSUMER.md) and
[runtime evidence](RUNTIME_COMPATIBILITY.md) for API and platform scope.

## Commit evidence is not effect atomicity

Managed commit results distinguish `notAttempted`, `confirmed`, `rejected` and
`unknown`. Once a snapshot reaches the driver, an unusable or missing response
can leave its outcome unknown. Unknown means neither rejection nor rollback.
**The client never automatically replays the commit or rejoins to hide uncertainty.**
A valid late confirmation remains evidence but does not restore revoked authority.
Another writer can overwrite a confirmed group's offset.

A later stable read reports the broker state returned by that read. Even a matching
position cannot identify which historical write produced it, resolve the causal
outcome of an ambiguous attempt, or prove callbacks or external effects ran once.
Preserve uncertain outcomes and choose application-specific reconciliation.

This conservative no-write-replay contract is not a claim that all write-retry
algorithms are unsafe. The [Java 4.0.2 synchronous commit implementation](https://github.com/apache/kafka/blob/4.0.2/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java#L1101-L1136)
has its own retry, ordering and membership rules; they are not this client's policy.

## Stable reads and unstable-offset retries are separate opt-ins

- Manual `requireStable: true` and managed `requireStableOffsets: true` select
  exact OffsetFetch v7 with RequireStable. Unsupported v7 fails without downgrade.
  The default remains nonstable v5.
- Stable mode alone does not retry error 88 (`UNSTABLE_OFFSET_COMMIT`). The
  separate nullable `stableOffsetRetry: KafkaStableOffsetRetry(...)` requires
  stable mode and enables bounded read-only retries.
- Policy defaults are 10 total attempts, a 5-second absolute monotonic deadline
  and 150-ms delay. Discovery, connection, authentication and reads share that
  budget. Terminal errors take precedence over unstable results. These are this
  client's defaults, not Java defaults.
- Close, cancellation, expiry and revocation fence subsequent phases/results.
  An obsolete generation cannot install a position. Underlying DNS/OS work may
  settle later; this is not instantaneous physical cancellation of every operation.
- The policy does not retry Produce or OffsetCommit, substitute a local offset,
  reset to earliest/latest, or expand rejoin eligibility.

See [bounded retries](GROUP_UNSTABLE_OFFSETS.md). The
[Java 4.0.2 classic response handler](https://github.com/apache/kafka/blob/4.0.2/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java#L1480-L1538)
fails the read when any requested partition is unstable and retries the requested
set, rather than returning successful entries as a partial result. This reference
does not imply identical defaults, protocol versions or recovery behavior.

## Explicit API choices

This helper performs only a read on a caller-owned client. The caller must handle
errors and close that client after its intended work. A returned null remains
successful absence, not permission to reset or replay a write.

```dart
import 'package:kafka_client/kafka_client.dart';

Future<Map<int, KafkaCommittedOffset?>> readStablePositions({
  required KafkaClient client,
  required String groupId,
  required String topic,
  required Set<int> partitions,
}) {
  return client.committedOffsets(
    groupId,
    topic,
    partitions: partitions,
    requireStable: true,
    stableOffsetRetry: KafkaStableOffsetRetry(
      maxAttempts: 10,
      timeout: const Duration(seconds: 5),
      retryDelay: const Duration(milliseconds: 150),
    ),
  );
}
```

This separate helper constructs, but does not run, a managed consumer. TLS is
required by the helper; authentication is optional. The caller explicitly chooses
whether successful processing should commit. The three recovery/stability choices
are independent: coordinator-loss replacement, stable v7, and error-88 read retries.
No missing-offset fallback is supplied, so successful absence stops startup.
Before invoking `run`, read the [callback and shutdown contract](GROUP_CONSUMER.md).

```dart
import 'package:kafka_client/kafka_group.dart';

KafkaGroupConsumer createStableConsumer({
  required List<Broker> brokers,
  required String groupId,
  required Set<String> topics,
  required bool commitAfterProcessing,
  required KafkaTlsConfig tls,
  KafkaSaslPlain? sasl,
}) {
  return KafkaGroupConsumer(
    brokers: brokers,
    groupId: groupId,
    topics: topics,
    commitAfterProcessing: commitAfterProcessing,
    tls: tls,
    sasl: sasl,
    maxRejoins: 3,
    rejoinOnCoordinatorLoss: true,
    requireStableOffsets: true,
    stableOffsetRetry: KafkaStableOffsetRetry(
      maxAttempts: 10,
      timeout: const Duration(seconds: 5),
      retryDelay: const Duration(milliseconds: 150),
    ),
  );
}
```

## Kafka 4.0.2 stable-read execution order

The new coordinator computes the response from the latest timeline state,
including the pending-transaction check, **before deferring completion behind
pending writes**. It does not wait for the committed boundary and then perform
the lookup. A precomputed error-88 response can arrive after a transaction has
settled, so another bounded read may still be needed. See the versioned
[response generation](https://github.com/apache/kafka/blob/4.0.2/coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java#L1376-L1402),
[completion barrier](https://github.com/apache/kafka/blob/4.0.2/coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java#L924-L966)
and [transaction check](https://github.com/apache/kafka/blob/4.0.2/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java#L861-L924).

A controlled migration fixture observed a replayed internal offset record at
log position r=2, written boundary W=3 and committed boundary C=2. Nonstable
Dart and Java reads returned absence; stable v7 returned prior confirmed progress
after C advanced to 3, about 4.559 seconds later. The fixture deliberately used
a 5-second follower fetch wait. This is a measured reproduction, not a recovery
SLA or an explanation of every historical anomaly. These internal log positions
are not application-topic offsets; visibility requires C > r.

[KAFKA-16973 / PR #16367](https://github.com/apache/kafka/pull/16367) corrected the
empty-write caught-up test to compare written and committed boundaries. That
protection is already present in 4.0.2, not an unfixed 4.0.2 defect. RequireStable
appears in the [Kafka 2.5.0 schema](https://github.com/apache/kafka/blob/2.5.0/clients/src/main/resources/common/message/OffsetFetchRequest.json);
protocol availability does not establish client testing on that broker version.
This client uses classic JoinGroup/SyncGroup, not KIP-848 membership. The new broker
coordinator can serve classic groups; v9 member-ID/epoch fields must not be
attributed to its [v7 requests](https://github.com/apache/kafka/blob/4.0.2/clients/src/main/resources/common/message/OffsetFetchRequest.json#L37-L76).

## Exact scope of the replacement position floor

Only `rejoinOnCoordinatorLoss: true` enables this **in-memory, same-consumer**
guard. It applies to otherwise eligible replacements, including heartbeat error
27, and creates no additional retry or rejoin reason. After successful owned
callback, commit and cleanup drains, the floor takes the higher of the previous
generation's broker-observed start and confirmed commits for each partition.
Fallback starts and uncommitted application progress do not establish a floor.
An unknown commit blocks eligible recovery.

Only partitions overlapping the immediately preceding assignment are compared.
The history is replaced each generation, not accumulated as a lifetime maximum.
It is not persisted across process restarts or inherited by a new consumer object,
and cannot fence another client or external effect.

A successful missing/lower fresh read behind an applicable floor stops recovery
before fallback or callbacks. The client neither retries that successful result
nor substitutes its floor. This detects a conflict with tracked evidence; it does
**not** diagnose permanent loss, durable regression, a specific cause or the
outcome of an ambiguous commit. Outside an applicable floor, successful absence
is valid and requires an explicit managed fallback or the consumer stops.

## Delivery limits

There is no blanket at-least-once certification, process-wide read-your-own-writes,
zero-replay or exactly-once guarantee. Processing before committing can repeat
callbacks after interruption. Availability and durability depend on broker policy,
retention, leadership, application handling and operation outcomes. Stable reads
and floors cannot reconstruct lost records.

Local assignment tokens fence admission at checked boundaries, not arbitrary
external effects already running. Atomic local effect/checkpoint storage does
not make an unrelated HTTP request atomic. Kafka-to-Kafka exactly-once work
requires a complete transactional/fencing workflow, not a test transaction helper.
