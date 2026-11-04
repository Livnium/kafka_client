# Bounded retries for unstable committed offsets

This is a separate opt-in policy for read-only OffsetFetch operations. It does not
add transaction production to the Dart client or retry any write.

```dart
final positions = await client.committedOffsets(
  groupId,
  topic,
  partitions: {0, 1},
  requireStable: true,
  stableOffsetRetry: KafkaStableOffsetRetry(
    maxAttempts: 10,
    timeout: const Duration(seconds: 5),
    retryDelay: const Duration(milliseconds: 150),
  ),
);
```

Managed consumers accept the same policy as `stableOffsetRetry`, together with
`requireStableOffsets: true`. A policy without stable reads is rejected before
I/O. Omitting the policy preserves the previous behavior: default reads use v5;
explicit stable reads use exact v7 and error 88 remains terminal.

## Read-only recovery and bounds

Kafka can return `UNSTABLE_OFFSET_COMMIT` (88) while transactional offsets are
pending. Kafka 4.0.2 computes that response before deferring completion, so error
88 can arrive even after the transaction settles; see the
[reliability contract](RELIABILITY_CONTRACT.md). Retrying the read can return the committed
position. Error 88 is never converted into a missing position or a local fallback.
A successful missing position remains null and is never retried merely for being
missing. No partial position map is returned when any requested partition fails.

The policy accepts 1–20 total attempts, a positive timeout of at most one minute,
and a retry delay from zero through one second. It has one total round budget
and a monotonic timeout for the whole read,
including coordinator discovery, connection establishment, authentication, version
negotiation, OffsetFetch and backoff. It replaces `maxReadRetries` for this read;
it does not nest another retry loop around that budget. A round attempts one
coordinator discovery and, when discovery succeeds, one exact-v7 stable read.
Distinct configured bootstrap endpoints rotate across permitted rounds.

Retryable coordinator errors 14/15/16 and permitted transport failures consume
the same bounds. Error 88 is retryable only when returned by this enabled
OffsetFetch operation. Terminal partition errors take precedence over retryable
errors, including mixed group/partition responses. Malformed results, unsupported
v7, resource limits, and explicit TLS/authentication rejection remain terminal.
No attempt downgrades to v5 or clears the stable flag.

The last broker/transport error is preserved when the attempt budget runs out.
Expiry of the overall timeout is terminal. A retry wait cannot reset or extend
that deadline; zero-delay retries still yield rather than spinning indefinitely.

## Cancellation, ownership and resource accounting

An enabled read uses connections owned by that operation, while retaining the
parent client's aggregate connection, request and byte limits. It may need an
additional connection when ordinary traffic is active on the same endpoint.
Resource exhaustion fails closed; the policy does not create a hidden child
client with a separate budget or cancel unrelated active connections.

`committedOffsets` also accepts a `KafkaCancellationToken` when this policy is
provided. Cancellation and client close stop the operation and prevent later
phases or late successful results from being admitted. Cancellation cannot undo
bytes already sent. Scoped sockets are closed when the operation settles.

Dart may still be resolving a hostname before `startConnect` returns a cancellable
connection task. The operation deadline fences all follow-on phases and disposes
late task/socket results, but does not claim to stop an OS/DNS lookup physically
at the exact deadline. Synchronous application/runtime scheduling can also delay
when a timeout is observed. Do not substitute an application `Future.timeout` for
the operation's own cancellation and ownership rules.

For managed consumers, the policy applies independently to each topic's
committed-position read within each generation. Multi-topic startup can therefore
consume multiple separate deadlines. It does not extend membership or processing
deadlines. Revocation
closes the owned positions client; late offsets cannot authorize data Fetch,
callbacks, commits or a replacement generation. Existing previous-generation
position-floor checks still reject missing/lower fresh positions where required.

Startup and membership-expiry watchdogs rearm if a timer wakes before the
monotonic target. Rounding a wakeup delay never extends the original authority
deadline; stale callbacks from renewal, completion or closure are ignored.
This shared controller correction also applies when the retry policy is omitted.

This is not readback confirmation of an ambiguous commit. Produce, OffsetCommit,
JoinGroup and other write-like operations do not gain retries from this policy.

## Official Java comparison and verification scope

Kafka 4.0.2's Java consumer requests stable offsets and retries error 88 within its
caller timer/backoff. This policy adopts bounded read retry, but retains different
explicit choices: stable reads and retries are opt-in, exact v7 is required, and
unsupported stability never silently downgrades. It does not claim full Java
consumer compatibility or liveness parity.

The checkpoint report separates protocol-peer tests, strict-TLS managed lifecycle
tests and genuine pending transactional offsets created by the official Java
producer in an owned Kafka fixture. Synthetic offsets are metadata observations,
not proof of application record delivery or exactly-once processing. Broker,
platform, independent-host and transaction-workflow coverage are bounded by the
reported executed cases.

See [stable committed-position reads](GROUP_STABLE_OFFSETS.md),
[managed consumers](GROUP_CONSUMER.md), and
[explicit offsets](GROUP_OFFSETS.md).
