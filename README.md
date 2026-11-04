# kafka_client - native Dart Kafka foundation

A continuation of the original Dart TCP Kafka implementation, now with a tested
produce/fetch foundation with validated TLS and optional SASL PLAIN authentication.
Explicit partition subscriptions build on the same read path.
**Experimental; not a production-ready client.**

## Verified compatibility

Published **0.1.1 remains experimental**. This source checkout includes later
native request-snapshot and response-validation hardening and version-selectable
fixtures. It is not byte-for-byte the published package, and its retained `0.1.1` pubspec is not a
new release. See the [native v1 scope and gates](doc/NATIVE_V1_SCOPE.md).

On Linux x64 with **Dart 3.12.0**, native source **6f828d3** passed **4 plaintext and
68 TLS/SASL-over-TLS cases per exact Kafka version**: **4.0.0, 4.0.2, 4.1.2,
4.2.2, 4.3.2 and 4.4.0**. Independent official Java producer/consumer oracles
also passed. These are single-node loopback results, not every 4.x release or
the full fault/endurance matrix. See [broker qualification](doc/BROKER_QUALIFICATION.md)
for source identity, case lists, reproduction and limits.

That pre-response-hardening library/test snapshot passed **1,058 tests with 360
external-fixture skips on each of Dart 3.2.6 and 3.12.0**, with clean analyzers.
Skipped tests are not real-broker evidence. Historical Kafka 4.0.2 operational
and replicated three-node results on both SDKs remain evidence for their own
source snapshots, not automatic validation of every later change.

After response-validation hardening, source **0d0c800** separately passed
**4 plaintext and 9 secure cases on each of Kafka 4.0.0 and 4.4.0**, plus four
official Java consume-back checks in total, on Dart 3.12.0/Linux x64. These
representative checks do not extend the earlier six-version or 68-case secure
matrix to the newer source. See [broker qualification](doc/BROKER_QUALIFICATION.md).

The runtime SDK constraint remains `^3.2.6`; other SDK versions have not been
individually tested. All public entrypoints require native `dart:io` sockets.
Browser targets, including Flutter Web, are unsupported and deferred. Nine
historical real-broker cases passed in a Linux x64 headless Flutter 3.44.0 test
engine with Dart 3.12.0. This is not packaged desktop/mobile or real OS lifecycle
validation. See [runtime compatibility](doc/RUNTIME_COMPATIBILITY.md).

Historical release preparation on macOS arm64 with Dart 3.12.0 passed 1,044 tests; 360
external-fixture cases were skipped. Six initial TLS fixture failures were
resolved by adding the macOS-required `serverAuth` certificate extension to the
owned test certificate. Untrusted-certificate and wrong-hostname rejection tests
also pass; client TLS verification was not changed. This unit-test run does not
establish real-broker, packaged-app or production support on macOS.

Implemented high-level APIs deliberately use these versions:

| API | Version | Notes |
| --- | --- | --- |
| ApiVersions | 0 | Negotiation per broker connection |
| Metadata | 0 | Bootstrap fallback; discover partition leaders |
| Produce | 3 | Uncompressed magic-2 batches, `acks=-1/0/1` |
| Fetch | 4 | Explicit partition and offset; read-uncommitted |
| FindCoordinator | 2 | Group coordinator discovery, no membership |
| OffsetFetch | 5 / opt-in 7 | Broker-stored next offsets; v7 requires stable reads |
| OffsetCommit | 6 | Explicit manual commit, no automatic replay |
| JoinGroup / SyncGroup | 4 / 2 | Separate single-generation membership session |
| Heartbeat / LeaveGroup | 2 / 2 | Separate single-generation membership session |

Security negotiation uses fixed SaslHandshake v1 and SaslAuthenticate v0 codecs.
This does not expand the tested broker/version matrix above.

The client checks that a broker supports the version its codec actually encodes;
it does not blindly select the broker's highest version. Kafka 4 may advertise
old Produce versions while rejecting them. Legacy OffsetFetch/DeleteTopics
source is retained in the development repository but excluded from the
publication payload and **not part of the supported Kafka 4 API surface**.

## Example

Pre-create a topic named `dart-example` on a trusted local plaintext broker:

```dart
import 'dart:convert';
import 'package:kafka_client/kafka_client.dart';

final client = KafkaClient(brokers: [Broker('127.0.0.1', 9092)]);
try {
  final produced = await client.produce('dart-example', [
    KafkaRecord(key: utf8.encode('key'), value: utf8.encode('Hello 🌍')),
  ]);
  final offset = produced!.responses.single.partitionResponses.single.baseOffset;
  final fetched = await client.fetch('dart-example', offset: offset);
  for (final record in fetched.topics.single.partitions.single.records) {
    print('${record.offset}: ${record.value}');
  }
} finally {
  await client.close();
}
```

Keys/values are bytes; null values are tombstones. Empty and null values stay
distinct. Headers and millisecond timestamps are supported. Omitted timestamps
use the current time. `Record.fromString` remains a compatibility convenience.
The package's example is runnable with `dart run example/kafka_client_example.dart`.

Fetch is inclusive and stateless: its default offset is literal zero, and calls
do not maintain a cursor. Track positions separately for each partition from
successfully processed records' absolute offsets, not from the number returned.
An empty response does not itself advance a position or prove the end of the log.
The client does not assign partitions, look up earliest/latest offsets or manage
retention/out-of-range recovery. Separate explicit committed-offset methods are
available; Fetch itself neither restores nor commits progress automatically.

## Subscribe to explicit partitions

`subscribe` returns a native Dart, single-listener `Stream<KafkaTopicRecord>`.
Supply a nonempty partition-to-**inclusive absolute offset** map, using your own
processing and checkpoint functions:

```dart
// Use positions supplied by your application, for example a saved next-offset
// checkpoint. Literal zero is not an "earliest retained" lookup.
final records = client.subscribe(topic, startOffsets: savedNextOffsets);
await for (final event in records) {
  await processRecord(event.topic, event.partition, event.record);
  await saveNextOffset(event.topic, event.partition, event.offset + 1);
}
```

The map is copied and fixes the selected partitions. Work starts on listen, with
one high-level Fetch in flight per stream and ordering within each partition.
Pausing stops new Fetch calls and event admission; an admitted Fetch may finish
and retain one decoded response. `await for`, `asyncMap`, or explicit pause/resume
apply backpressure; an `async` callback passed directly to `listen` does not.

Canceling a subscription waits for its admitted Fetch under existing per-phase
deadlines and does not close the shared client. Client close stops all streams.
Read-safe transient failures retry while active; TLS/authentication, protocol,
resource-limit and offset-out-of-range failures remain terminal after the
existing Fetch policy. Strict TLS and all prior client defaults are unchanged.

This is read-uncommitted consumption, without coordinated groups, automatic commits,
ListOffsets, automatic partition discovery or retention recovery. Delivery is not
processing acknowledgement. Persist application checkpoints after successful
work and supply them on restart; replays are possible. Empty polls never jump to
the high watermark.

See `doc/TOPIC_SUBSCRIPTION.md` in the source checkout for defaults, error and
cancellation semantics. Run `dart run example/subscribe_example.dart` against a
pre-created dedicated local topic: it produces one record and starts from its
acknowledged base offset rather than assuming offset zero is valid.

## Explicit broker-stored checkpoints

Use `committedOffsets(groupId, topic, partitions: {0})` to read saved next
positions, and `commitOffsets(groupId, topic, nextOffsets: {0: nextOffset})`
only after successful application processing. These are manual group-offset
operations, not consumer-group assignment, heartbeats or rebalancing. Use a
dedicated group ID and external single-writer coordination per partition.

Commit is never automatically replayed. A timeout or disconnect can leave its
outcome unknown; partial broker errors preserve successful partition results in
`KafkaOffsetCommitException`. Null read results mean successful absence in the
broker's selected snapshot, never an error or automatic earliest/latest choice.
After coordinator migration, default nonstable reads can lag replayed progress.
Explicit `requireStable: true` selects stable OffsetFetch v7 without changing the
default. Managed consumers expose the independent `requireStableOffsets: true`
option for their position reads. A separate nullable `stableOffsetRetry` policy
adds bounded read-only retries of unstable-offset error 88, requiring stable
reads; see `doc/GROUP_UNSTABLE_OFFSETS.md`. See `doc/GROUP_STABLE_OFFSETS.md` for waiting,
capability and safety limits, and read `doc/GROUP_OFFSETS.md` before using these APIs.

## Classic-group protocol requests

The advanced `kafka_protocol.dart` import now includes dynamic-member classic
JoinGroup, SyncGroup, Heartbeat and LeaveGroup requests, consumer subscription/
assignment metadata versions 0–3, and `MemberOffsetCommitRequest` for an explicit
member/generation tuple. These are single-use protocol primitives, not a managed
consumer or automatic group subscription.

The caller must handle coordinator changes, assignment, heartbeats, processing
progress and safe revocation. Generation validation does not prove partition
ownership. Existing `subscribe()` and `commitOffsets()` behavior is unchanged.
Read `doc/CLASSIC_GROUP_PROTOCOL.md` for required lifecycle and timeout handling
before building a controller with these requests.

## Single-generation membership and bounded delivery

The separate `kafka_group.dart` entrypoint adds `KafkaGroupSession`: one classic
Join/Sync generation, eager range assignment, independent heartbeats on an owned
control client, and immediately invalidated local assignment tokens on loss.
It stops permanently on rebalance or uncertain outcomes; it does not rejoin.
An explicit nullable `tlsFetchRecovery` option is available for managed data
Fetches only; it requires TLS and bootstrap fallback and leaves strict defaults
and generation authority unchanged. See managed Fetch recovery (`doc/GROUP_FETCH_RECOVERY.md`).

After `start()`, `session.subscribe(assignment: token, startOffsets: positions)`
can deliver the assigned partitions through a separate owned data client. Each
record carries the exact local assignment token. The original direct stream
listener is checked synchronously at admission; revocation discards buffered and
late Fetch results. Await `delivery.stopped` for data cleanup and `session.closed`
separately for membership cleanup.

For opt-in application processing, use `session.subscribeProcessing(...)` instead.
Its identity-bound acknowledgements retire per-partition completed prefixes;
`commitAcknowledged(topic)` explicitly stores that topic's snapshot using the
current generation/member. A finite pending-record window and oldest-work deadline
bound admission and processing stalls independently of heartbeats. Rejections,
unknown commit outcomes and revocation end the session without automatic replay.

This single-generation interface is **not a complete coordinated consumer**.
Start positions remain explicit; it has no automatic offset lookup/commit, rejoin,
transaction or fencing of asynchronous effects. Existing raw subscriptions and manual commits are unchanged.
See `doc/GROUP_PROCESSING.md`, `doc/GROUP_DELIVERY.md` and `doc/GROUP_SESSION.md`
for outcome/cleanup contracts, evidence boundaries and remaining work.

For an opt-in serial callback runner, `KafkaGroupConsumer` reloads committed
positions for every generation and supports bounded rejoin after a confirmed
heartbeat rebalance rejection. It waits for the actual old callback Future and
all owned cleanup before replacing a generation. Missing offsets require an
explicit numeric fallback; `commitAfterProcessing` is a required true/false
choice. Uncertain commits remain terminal; other failures remain terminal by default. See
`doc/GROUP_CONSUMER.md` for the narrow policy and shutdown contract.
A separate default-off coordinator-loss policy (`doc/GROUP_COORDINATOR_RECOVERY.md`)
allows narrowly classified fresh-generation replacement with the same drain and
commit-safety requirements; it does not enable group-discovery TLS fallback.

## API surface

Import `kafka_client.dart` for the small application-facing API: `KafkaClient`,
`Broker`, `KafkaRecord`, `KafkaTopicRecord`, `KafkaCommittedOffset`, headers,
`KafkaOffsetCommitException`, and typed results. The separate
`kafka_protocol.dart` entrypoint exposes low-level, single-use requests for
advanced protocol tooling. The `kafka_group.dart` entrypoint adds experimental
`KafkaGroupSession`, `KafkaGroupDelivery` and assignment-tagged `KafkaGroupRecord`.
Internal `src/` libraries are not stable API.

## TLS and authentication

| Connection mode | This milestone |
| --- | --- |
| Plaintext | Retained for compatibility/local fixtures; never sends SASL credentials |
| TLS, server authenticated | System roots or explicit custom CA; hostname validation mandatory |
| TLS + SASL PLAIN | Qualified in the exact single-node broker matrix above |
| SASL over plaintext | Rejected before connection/authentication |
| SCRAM, OAuth, Kerberos, mTLS, SASL reauthentication | Not implemented |

For a runnable secure-only example, configuration and failure handling, see
`doc/RUNTIME_COMPATIBILITY.md` in the source checkout
and `example/kafka_client_secure_example.dart`. It appends one record to a
pre-created dedicated topic and checks the acknowledged offset.

```dart
import 'dart:io';
import 'package:kafka_client/kafka_client.dart';

// Load from your runtime secret provider. Never hard-code or log credentials.
final client = KafkaClient(
  brokers: [Broker('broker.example.internal', 9093)],
  tls: KafkaTlsConfig(
    trustedCertificates: await File(caFilePath).readAsBytes(),
    withTrustedRoots: false, // trust this CA only
  ),
  sasl: KafkaSaslPlain(username: runtimeUsername, password: runtimePassword),
);
// ... use produce/fetch, then await client.close() in finally.
```

Use `KafkaTlsConfig()` for the platform's trusted roots. Explicit CA bytes augment
system roots unless `withTrustedRoots: false` is selected. Trust/hostname/auth
failure never falls back to plaintext or a weaker mechanism. There is no
bad-certificate override or TLS key logging hook. Connect using a hostname present
in the broker certificate, including every advertised partition-leader hostname.
Authentication finishes on that same verified TLS connection before application
requests are sent. Authentication diagnostics discard broker-provided text and
redact credentials. Credentials remain in process memory; Dart cannot guarantee
secure memory erasure. The library never logs or persists them.

## Reliability and failure semantics

Read the reliability contract (`doc/RELIABILITY_CONTRACT.md`) before relying on
commit outcomes, stable reads or replacement position floors. Matching readback
cannot resolve an ambiguous historical write or prove once-only effects. The floor
is an in-memory previous-generation overlap check, not a persisted checkpoint or
a guarantee across newly created consumers. No blanket at-least-once or
exactly-once guarantee is made.

- Connection deadlines cover TCP plus the complete TLS handshake; stalled TLS
  peers are actively closed, and `close()` interrupts a handshake in progress.
  Request deadlines are bounded. Timeout retires the affected
  connection and fails its other pending requests, avoiding stalled flush chains.
- Default budgets: 100 admitted requests, 8 connections, 16 MiB per frame,
  32 MiB queued request bytes, 32 MiB aggregate buffered response bytes. Configure
  `maxInFlightRequests`, `maxConnections`, `maxFrameBytes`,
  `maxPendingRequestBytes`, and `maxBufferedResponseBytes` explicitly as needed.
  Idle established connections are evicted least-recently-used when a new
  endpoint needs space. Active requests, authentication and version checks are
  protected; if every connection is busy, the new request fails with
  `KafkaResourceLimitException`. No unbounded wait queue is created. Byte budgets are wire-buffer bounds, **not a total heap cap**.
- `metadata()` retries transient transport failures and returns broker topic and
  partition error fields unchanged; callers must inspect them. Produce's pre-send
  leader lookup and Fetch also retry eligible routing/Fetch error codes 5/6/9:
  at most `maxReadRetries` (default 2) after the initial attempt, with
  bounded exponential full jitter (default base 50 ms; maximum delay 1 second).
  Each fetch retry refreshes metadata. Metadata v0 can report partition error 9
  for an unavailable replica even when a valid leader is advertised; routing may
  use that leader while the original metadata error remains visible to callers.
  Election can outlast one finite retry budget, so an application may need a later
  read attempt. Authentication, malformed-response, unsupported-version and
  resource-limit errors remain terminal. TLS failures are fail-fast by default.
  A first bootstrap that accepts TCP then terminates its TLS handshake can stop
  fallback to otherwise healthy brokers; see `doc/TLS_BOOTSTRAP_LIMIT.md`.
  The experimental, default-off `allowTlsBootstrapFallback` option tries the next
  distinct configured bootstrap during metadata discovery after a TLS handshake
  failure, including certificate rejection. Every peer still needs strict TLS
  validation before SASL or Kafka data. Exhaustion after any TLS failure fails
  closed, without outer read retries. Bootstrap fallback alone leaves leader and
  low-level TLS failures terminal, so it does not provide transparent failover. The observable
  `bootstrapTlsFallbackCount` counts traversal advances, not successful recovery.
  Review `doc/TLS_BOOTSTRAP_FALLBACK.md` in the source checkout before opting in.
- A separate experimental `tlsFetchRecovery` policy (default `null`) opts only
  high-level Fetch into operation-local exclusion of TLS-rejected endpoints and
  bounded metadata refresh. It requires TLS and `allowTlsBootstrapFallback`.
  Its total-round budget replaces ordinary Fetch retry settings; its monotonic
  window controls admission, not cancellation of already-admitted phases.
  Previously rejected peers are never reconnected by that Fetch, and replacement
  leaders must independently validate. This is not a hard overall deadline or
  a change to Produce/low-level send. See `doc/TLS_FETCH_RECOVERY.md` before use.
- Completed retry delays are removed from client-owned wait bookkeeping, and
  close cancels pending delay timers. Retry counts and error eligibility are
  unchanged; see `doc/RETRY_WAITERS.md` for the scoped before/after evidence.
- High-level calls reject invalid numeric wire bounds before discovery or
  transport work, through failed Futures. This does not validate topic existence
  or record delivery; previously documented closed-client errors remain.
- **Produce is never automatically replayed.** Only its pre-send leader lookup is
  safe to retry. A timeout, disconnect or response-side resource-limit error may
  mean records were stored; blind retries can duplicate them. A resource-limit
  exception alone does not prove rejection before transmission. No idempotence
  or exactly-once claim.
- `acks=0` returns `null` after local socket flush, **not a delivery/persistence ack**.
  The default is `acks=1` (leader acknowledgement); `acks=-1` requests in-sync
  replica acknowledgements. Produce's `timeoutMs` is a broker acknowledgement
  timeout. Fetch's `maxWaitMs` is a broker wait and `maxBytes` a soft requested
  byte limit, not a heap cap; an oversized first batch can exceed it.
- Low-level request objects are single-use. `send` itself never retries.
  `KafkaCancellationToken` can cancel a local `send` wait. If already assigned to a
  socket, cancellation retires that connection, also failing its other pending
  requests. It **cannot undo records already sent**. Canceling before dispatch
  prevents the send. Admission remains reserved until both the caller wait and
  asynchronous dispatch have settled, even when cancellation finishes first.
  `close()` cancels waits and closes live connections; overlapping callers await
  the same teardown. Closing is terminal and does not gracefully drain delivery
  acknowledgements. Await intended operations first; a pending Produce can remain
  ambiguous. Create a new client to resume.
- Typed `KafkaAuthenticationException`, `KafkaConnectionException`,
  `KafkaBrokerException`, `KafkaResourceLimitException`, and
  `KafkaCancelledException` distinguish common failures. TLS errors remain the
  platform's `HandshakeException`/TLS errors; deadlines use `TimeoutException`.
- Default deadlines apply per low-level request, not to one overall multi-request
  operation. The separate stable-offset retry policy adds an overall read deadline
  and scoped cancellation; see bounded retries (`doc/GROUP_UNSTABLE_OFFSETS.md`).
  High-level produce/fetch do not accept a cancellation token. Wrapping their
  Future with `timeout` does not cancel the underlying work.
  Bootstrap candidates and read retry attempts are finite. Metadata v0 follows
  the broker's auto-topic-creation policy; disable broker auto-creation when
  implicit topic creation is not wanted. Local multi-node process failover is
  exercised with explicit retry budgets; no universal or long-running recovery
  guarantee is claimed.
- CRC32C, record/frame lengths, nullable fields and UTF-8 are checked. Unsupported
  compression fails explicitly. Truncated record batches are rejected; Kafka
  control records are filtered from application data.

## Verification

```sh
dart pub get
dart analyze --fatal-infos
dart test
```

The ordinary test run skips real-broker tests. Run those explicitly using Java 17+
and a SHA512-verified official Kafka **4.0.2** binary distribution:

```sh
export KAFKA_HOME=/absolute/path/to/kafka_2.13-4.0.2
# Optional, place ephemeral fixture data on a disk with enough space:
export KAFKA_TEST_RUN_ROOT=/absolute/path/to/broker-runs
bash tool/run_broker_integration.sh
```

The fixture binds loopback ports 19092/19093 by default, creates isolated topics,
checks byte/Unicode/header/tombstone/offset roundtrips, `acks=0`, broker errors,
and verifies **both directions against official Java console clients**. It stops
its own broker and removes broker data on success/failure. Diagnostic logs remain.
No Docker or external cluster credentials are needed. See the script for overrides.

For secured integration, OpenSSL and Python 3 are also required:

```sh
bash tool/run_secure_broker_integration.sh
```

It creates one-day local CA/server certificates and random synthetic credentials
in a private temporary directory, checks TLS trust/hostname/authentication failures
and Java interoperability, then deletes keys, credentials, broker configuration
and data on success/failure. Only sanitized test summaries remain. Never point
these fixtures at production infrastructure or provide real credentials.

## Operational recovery checks

Python 3 is also needed for the disposable crash/restart and cluster fixtures:

```sh
python3 tool/run_operational_integration.py
KAFKA_SOAK_SECONDS=300 python3 tool/run_operational_integration.py
python3 tool/run_guarded_soak.py
python3 tool/run_cluster_integration.py
python3 tool/run_secure_cluster_integration.py
python3 tool/run_network_fault_integration.py
python3 tool/run_tls_network_fault_integration.py
```

They exercise same-client process-crash recovery, real long-poll cancellation and
request deadlines, repeated close, concurrent record/offset verification, and a
replicated three-node leader change. The cluster uses explicit recovery budgets
and bounded application-level read polling; it does not promise uninterrupted
service or that one default-budget call spans an election. Secured cluster tests
cover both TLS-only and SASL-over-TLS client traffic; local controller/inter-broker
fixture traffic remains plaintext. A separate plaintext proxy verifies lost
Produce acknowledgements, blackholed traffic, forced disconnects and delayed,
fragmented replies. An opaque ciphertext proxy repeats those faults for both
TLS-only and SASL-over-TLS clients without decrypting traffic. Lost-acknowledgement
checks use unique stored markers and contiguous sentinel offsets; they do not
claim the encrypted proxy can count Kafka requests. No Produce is replayed.
The guarded Linux soak defaults to 30 minutes, records minute-by-minute progress,
and stops on sampled RSS/host-resource thresholds. These are safety stops, not
hard memory limits or a capacity certification.
For a framework-free memory control, use the standalone probe described in
`doc/V1_READINESS.md` in the source checkout. Source and cached test-runner entrypoints
have materially different RSS overhead; record the runner when comparing results.
See `doc/V1_READINESS.md` in the source checkout for setup, precise scope, remaining gates
and the current verification receipt. Never use these fixtures on a live cluster.

## Not implemented / not verified

SCRAM/OAuth/Kerberos, mutual TLS, SASL reauthentication and ACL-management
workflows; general/unbounded group recovery and complete processing/commit management;
compression;
transactions/idempotence;
automatic Produce replay; independent-host/network-partition and kernel packet-loss
tests, load/capacity certification;
other Kafka releases; packaged Flutter desktop/mobile applications, real OS
suspension/termination, macOS/iOS/Android/Windows or non-x64 runtime verification.
The headless Flutter evidence is limited to the cases in
runtime compatibility (`doc/RUNTIME_COMPATIBILITY.md`). Browser targets are unsupported.
Server-side ACLs still determine what an authenticated principal may do.
Do not expose plaintext listeners publicly. The runtime/platform matrix is only
what the verification receipt explicitly tested. Run `dart doc` for current API
documentation; the old generated snapshot is excluded from publication.

## Protocol sources

- [Kafka 4.0 record format](https://kafka.apache.org/40/implementation/message-format/)
- [Official versioned protocol schemas](https://github.com/apache/kafka/tree/4.0.2/clients/src/main/resources/common/message)
- [Kafka 4.0 quickstart](https://kafka.apache.org/40/getting-started/quickstart/)
- [Kafka SASL security](https://kafka.apache.org/40/security/authentication-using-sasl/)
- [Dart SecureSocket](https://api.dart.dev/dart-io/SecureSocket/connect.html)
- [Dart Socket API](https://api.dart.dev/dart-io/Socket-class.html)

## License

The original client code is copyright Livnium and licensed under the Apache
License, Version 2.0. See `LICENSE`, `NOTICE` and `THIRD_PARTY_NOTICES.md` in the
source/package. Third-party material retains its own ownership and license.

This experimental `0.1.0` release is maintained at
https://github.com/Livnium/kafka_client. Report issues at
https://github.com/Livnium/kafka_client/issues. See `doc/RELEASING.md` for the
release checklist and the support limitations above before using the package.
