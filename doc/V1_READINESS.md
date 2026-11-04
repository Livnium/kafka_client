# Operational readiness and remaining v1 gates

This is an experimental native Dart client, not a production certification.
A version number alone does not change that status. Publication is a separate,
author-approved action; see [RELEASING.md](RELEASING.md).

## Contract being hardened

- Native Dart VM sockets on the explicitly tested Linux/SDK matrix.
- Kafka 4.0.2; Produce v3, Fetch v4, Metadata v0 and ApiVersions v0.
- Plaintext local connectivity, strict server-authenticated TLS, and SASL PLAIN
  only over verified TLS. No weak-certificate or plaintext-auth fallback.
- Uncompressed magic-2 records, explicit partitions/offsets, read-uncommitted.
- Finite retries within each Fetch and bounded wire resources. The additive
  subscription stream repeats eligible transient reads while active, with one
  Fetch at a time and explicit fixed partition offsets. It has no durable
  processing acknowledgments. See [TOPIC_SUBSCRIPTION.md](TOPIC_SUBSCRIPTION.md).
- Explicit manual broker-stored next-offset read/commit APIs, without group
  membership, ownership or automatic commits. See [GROUP_OFFSETS.md](GROUP_OFFSETS.md).
- Advanced dynamic classic-group wire primitives and versioned consumer metadata
  support controlled protocol work; see [CLASSIC_GROUP_PROTOCOL.md](CLASSIC_GROUP_PROTOCOL.md).
- A separate single-generation membership session handles eager range assignment,
  independent heartbeats and immediate local token revocation. It permanently
  stops on rebalance/loss. One optional owned data channel now supplies bounded
  epoch-tagged record admission and a separate local revocation/cleanup barrier,
  with a separate opt-in acknowledgement ledger, bounded processing stalls and
  explicit single-topic generation commits with partial/unknown outcomes. There
  is no application-work fencing; see [GROUP_SESSION.md](GROUP_SESSION.md),
  [GROUP_DELIVERY.md](GROUP_DELIVERY.md) and [GROUP_PROCESSING.md](GROUP_PROCESSING.md).
- An opt-in serial runner now reloads committed positions and can rejoin a finite
  number of times after a processing-stage heartbeat rebalance rejection, but only
  after the old returned callback Future and all owned work settle cleanly.
  The separate default-off [coordinator-loss policy](GROUP_COORDINATOR_RECOVERY.md)
  also allows selected stable-heartbeat failures under the same drain/budget
  gates. See [GROUP_CONSUMER.md](GROUP_CONSUMER.md). Other failures remain terminal.
- No automatic Produce/commit replay, idempotence, transactions, general group
  recovery, implicit offset resets or detached-application-work fencing.
- Request-level deadlines, low-level cancellation and shared client shutdown.
  High-level operations normally have per-phase bounds. The separately enabled
  [stable-offset retry policy](GROUP_UNSTABLE_OFFSETS.md) provides an overall
  read deadline and scoped cancellation, without adding write retries.

## Evidence that must stay reproducible

1. Minimum and current tested SDK: analyzer, formatting, every unit regression.
2. Actual Kafka plaintext and secured roundtrips, including independent official
   Java producer/consumer interoperability and trust/authentication failures.
3. Actual broker process crashes and persisted-log restarts on the same client.
4. Actual long-poll deadline, cancellation, reconnect and repeated shutdown.
5. Replicated three-node tests with plaintext, TLS and SASL-over-TLS client
   traffic: dead bootstrap fallback, multiple leaders, leader loss, surviving
   leader reads/writes, restart and full ISR recovery. The secured fixture's
   local controller/inter-broker traffic remains plaintext.
6. Concurrent soak: exact acknowledged payload/key/header/offset checks, complete
   offset coverage, and zero settled admission/queued/buffered counters afterward.
7. Owned plaintext and opaque encrypted TCP proxies: one stored marker after a
   lost Produce ack, concurrent blackholed reads, forced disconnect/recovery,
   fragmented replies and settled counters. TLS and SASL-over-TLS clients retain
   strict trust/authentication through reconnects. This is not a kernel packet-loss
   or cross-host fault model.
8. Every disposable fixture: only owned processes, loopback listeners, private
   control files, and verified cleanup on success and intentional failure.
9. Tested-source identity and retained logs. Historical receipts are evidence for
   their own source snapshot, not automatically the current tree.

The development checkout retains the current receipt and logs in
`verification/operations/`, `verification/secure_recovery/` and
`verification/tls_fault_soak/`, with controlled memory follow-up in
`verification/memory_isolation/` and the longer minimum-SDK follow-up in
`verification/minimum_soak/`. Local native-runtime and secure-example checks
are recorded in `verification/runtime_compatibility/`. Evidence logs are excluded from the prospective
publication payload; use the source checkout to inspect them.

The subsequent ordinary retry wait fix removes completed close-listener history
without broadening retry policy. Direct count, shutdown, security and operational
regressions are described in [RETRY_WAITERS.md](RETRY_WAITERS.md) and retained in
`verification/retry_waiters/`. It does not attribute the older RSS failure to this
mechanism or establish a general heap-leak fix.

Subscription-specific endurance and secured topology/network-fault gates are
described in [SUBSCRIPTION_RELIABILITY.md](SUBSCRIPTION_RELIABILITY.md), with
source-specific receipts in `verification/subscription_endurance/`. Their duration
variable is `KAFKA_SUBSCRIPTION_ENDURANCE_SECONDS`; the older `KAFKA_SOAK_SECONDS`
does not set the new test duration. Strict-default terminal TLS behavior and
explicitly configured recovery remain separate expected outcomes.

## Interpreting failover correctly

Kafka Metadata v0 can report partition error 9 (`REPLICA_NOT_AVAILABLE`) because
one replica endpoint is missing while a usable leader is still advertised. The
client preserves that diagnostic in raw metadata. Routing may use only a
nonnegative leader ID that resolves to a nonempty host and valid advertised port.
It does not suppress topic errors, missing leaders or other partition errors.

Election and high-watermark visibility can outlast one retry call. The cluster
fixture uses a 500 ms broker heartbeat, 3 s broker session timeout, up to ten
additional internal read attempts with a 1 s jitter base, and a bounded 45 s
application-level read polling window. A successful Fetch can temporarily be
empty during recovery. These results do not establish a default-budget,
single-call or zero-downtime guarantee. Produce is called only once per marker;
a write failure is not converted into a silent retry.

## Still needed before claiming a broadly dependable v1

- Longer, repeated and varied sustained observations. The recorded 30-minute
  four-worker run supplies bounded evidence, not a capacity or leak guarantee.
  The Dart 3.2.6 60-second guarded smoke did not complete: whole-test-process RSS
  exceeded the fixed 768 MiB ceiling after about three seconds. Subsequent
  controlled 60-second source/cached-runner and framework-free runs passed at the
  same limit. No-broker controls isolate substantial source-runner overhead, but
  the exact earlier crossing was not reproduced or attributed to specific heap
  allocations. A subsequent 30-minute minimum-SDK framework-free run also passed
  with the fixed ceiling and zero settled counters. Keep the original failure and
  runner provenance visible; longer/repeated varied workloads remain necessary
  before broad claims.
- Broader combined topology/fault coverage. The original secured three-node
  simultaneous opaque-transport fault gate recorded a strict-default
  TLS-bootstrap recovery limit; that availability limitation remains. A
  still-listening proxy of the crashed first bootstrap can terminate TLS before negotiation and prevent fallback to a
  healthy endpoint. The isolated secure-proxy and direct-endpoint cluster tests
  do not close this gap. See [TLS_BOOTSTRAP_LIMIT.md](TLS_BOOTSTRAP_LIMIT.md).
  A separate default-off candidate supports independently validated bootstrap
  fallback and application-coordinated recovery after metadata convergence.
  Immediate Fetch can still reach a stale leader and fail TLS terminally. These
  are separate result categories, not a closed transparent-failover gate; see
  [TLS_BOOTSTRAP_FALLBACK.md](TLS_BOOTSTRAP_FALLBACK.md). Adoption of this explicit
  security/availability policy remains a maintainer decision. The additional
  default-disabled Fetch policy in [TLS_FETCH_RECOVERY.md](TLS_FETCH_RECOVERY.md)
  is a separate candidate, with operation-local rejection and bounded refresh;
  its explicit configuration is not a default-policy or hard-deadline claim.
  The later subscription fault gate separately verifies the expected strict
  terminal outcome and recovery of the same explicitly opted-in stream, on
  both recorded SDKs in TLS and SASL-over-TLS modes. That closes this bounded
  stream scenario, not transparent default failover or the broader fault matrix.
  See [SUBSCRIPTION_RELIABILITY.md](SUBSCRIPTION_RELIABILITY.md).
- Expand the network-fault matrix to independent-host partitions, kernel-level
  loss/latency, repeated leader changes and constrained resources under sustained
  load. The local proxy covers selected stalled/cut/fragmented TCP behavior only.
- Independent-host deployment and any additional Kafka or operating-system
  versions the author intends to support. Same-host processes are not independent
  failure domains. Broker process crashes do not simulate host power loss.
- A clear decision on whether high-level cancellation/overall deadlines belong
  in v1; current low-level semantics are documented and tested. Subscription
  cancellation stops new Fetch admission and awaits the current Fetch under its
  existing phase deadlines; it does not add immediate scoped transport abort.
- Subscription product decisions: supplied versus earliest/latest offsets, fixed
  versus automatically discovered partitions, and coordinated consumer-group
  membership/assignment/rebalancing. Broker-stored offsets are now explicit
  manual APIs; durable processing and single-writer coordination are still the
  application's responsibility. The stream restores/commits nothing implicitly.
- An explicit OS/architecture/runtime support policy. Local Linux x64 source
  and native smoke evidence does not establish other target support. Additional
  headless Flutter test-engine evidence covers nine cases, not packaged apps or
  real OS lifecycle behavior. See [RUNTIME_COMPATIBILITY.md](RUNTIME_COMPATIBILITY.md).
- An automated release gate that reruns these fixtures in the maintainer's actual
  CI environment; no CI account or workflow publication is assumed here.
- Verify the selected Apache 2.0 license and separate third-party notices in the
  final payload. Repository metadata and final version still need author decisions,
  followed by explicit permission for external package validation and publication.

## Running operational evidence

After ordinary `dart pub get`, with Java 17+, Python 3, Dart, and an independently
SHA512-verified official Kafka 4.0.2 distribution:

```sh
export KAFKA_HOME=/absolute/path/to/kafka_2.13-4.0.2
python3 tool/run_operational_integration.py
KAFKA_SOAK_SECONDS=300 python3 tool/run_operational_integration.py
python3 tool/run_guarded_soak.py
python3 tool/run_cluster_integration.py
python3 tool/run_secure_cluster_integration.py
python3 tool/run_network_fault_integration.py
python3 tool/run_tls_network_fault_integration.py
python3 -m unittest discover -s tool -p 'test_*.py'
```

The operational fixture uses loopback ports 19292/19293 by default; override
`KAFKA_OPERATIONAL_PORT` and `KAFKA_OPERATIONAL_CONTROLLER_PORT` if occupied.
The cluster fixture allocates seven free loopback ports, including a reserved
non-listening bootstrap endpoint. Run roots can be moved with
`KAFKA_OPERATIONAL_RUN_ROOT` and `KAFKA_CLUSTER_RUN_ROOT`.

To verify failure cleanup without a test-suite edit:

```sh
python3 tool/run_operational_integration.py -- /bin/false
python3 tool/run_cluster_integration.py -- /bin/false
python3 tool/check_guarded_soak_cleanup.py
```

The two `/bin/false` commands are expected to exit nonzero. The guard cleanup
check deliberately interrupts an active run after positive progress, then exits
zero only if the real guard interruption and owned cleanup are verified. It
injects a synthetic resource reading; it does not exhaust system memory.
Inspect each run's `result.json`
and verify its ports are reusable. Logs and non-secret configuration remain;
owned broker data and control files are removed. The retention setting is a
broker cleanup policy, not an instantaneous disk cap. Ensure sufficient free
space before a long run. RSS metrics describe the whole Dart test process,
including JIT, assertions and bookkeeping, not an isolated client heap guarantee.

The guarded runner is Linux-specific (`/proc/meminfo`). By default it observes
1,800 seconds of sustained work, samples test-process RSS every 100 ms with a
768 MiB ceiling, and samples free disk/host available memory every five seconds.
It stops below 2 GiB free disk or 1 GiB available memory; sampling failures fail
closed. `KAFKA_SOAK_SECONDS` and `KAFKA_SOAK_MAX_RSS_BYTES` configure duration and
the optional RSS ceiling (zero disables the RSS check). These are sampled safety
stops, not hard resource limits, so values may overshoot between observations.
Host reports are logged every minute; reported minima refer to retained samples.
The runner accepts an explicit command after `--`, including a direct cached
Dart VM/test entrypoint when dependency resolution is deliberately unavailable.

## Separating client memory from the test runner

The diagnostic `tool/measure_client_memory.dart` has no test-framework
import. It provides `idle`, `codec` and `live` modes, a fixed 768 MiB sampled RSS
ceiling, and a duration of 10–1,800 seconds. Live mode uses the operational fixture's
four-worker payload/Produce/Fetch sequence with manual validation and explicit
shutdown. Codec mode excludes networking, metadata/Fetch envelopes and broker
read amplification; idle mode constructs no client. Neither is a production
heap benchmark. Offset coverage is merged rather than retained per record.

With existing resolved packages and `DART` set to the SDK executable:

```sh
KAFKA_MEMORY_MODE=codec KAFKA_SOAK_SECONDS=10 "$DART" --disable-dart-dev \
  --packages=.dart_tool/package_config.json tool/measure_client_memory.dart
KAFKA_MEMORY_MODE=live KAFKA_SOAK_SECONDS=60 python3 tool/run_guarded_soak.py -- \
  "$DART" --disable-dart-dev --packages=.dart_tool/package_config.json \
  tool/measure_client_memory.dart
```

These direct VM commands do not resolve packages. For framework-based resource
gates, an already-compatible cached runner can be supplied after `--`; verify and
record its SDK, dependency configuration and snapshot hash. Do not automatically
select an arbitrary stale snapshot or compare it with a source-runner result as
though their process overhead were equivalent. The retained controlled results
demonstrate this difference without changing the client or raising its RSS guard.
