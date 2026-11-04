# Reproducing subscription reliability gates

These are bounded observations of the explicit-partition API in
[TOPIC_SUBSCRIPTION.md](TOPIC_SUBSCRIPTION.md), not production certification.
They preserve the existing security policies, Produce non-replay behavior and
application-owned checkpoint contract. Neither test implements consumer groups,
a durable checkpoint store or exactly-once processing.

Use a disposable local environment with Java 17+, Python 3, a supported Dart SDK,
resolved test dependencies, and a separately SHA512-verified official Kafka 4.0.2
distribution. Set `KAFKA_HOME` to that distribution. Never point these destructive
fixtures at a live cluster. The runners create and control only their own brokers.

## Subscription endurance

The default test duration is **30 seconds**, suitable for a basic smoke only.
The duration variable is **KAFKA_SUBSCRIPTION_ENDURANCE_SECONDS**, not the older
operational suite's `KAFKA_SOAK_SECONDS`. Use 65 seconds to exercise a checkpoint
restart, or 1,800 seconds for the longer observation:

```sh
KAFKA_SUBSCRIPTION_ENDURANCE_SECONDS=65 python3 tool/run_guarded_soak.py -- \
  dart test test/topic_subscription_endurance_integration_test.dart \
  --concurrency=1 --reporter expanded

KAFKA_SUBSCRIPTION_ENDURANCE_SECONDS=1800 python3 tool/run_guarded_soak.py -- \
  dart test test/topic_subscription_endurance_integration_test.dart \
  --concurrency=1 --reporter expanded
```

The supported duration range is 1–1,800 seconds. For an environment where package
resolution is deliberately unavailable, replace `dart test` with an explicit
already-compatible cached test-runner command. Record the SDK, dependency config
and snapshot identity; do not select an arbitrary stale runner. The retained
verification receipt records the exact cached commands used for these observations.

Each batch contains four records, each with a 512-byte value, plus byte keys,
timestamps and ordered duplicate/null/empty headers. Produce is called once, with `acks=-1`.
The application synchronously verifies every acknowledged record before advancing
its in-memory next-offset checkpoint. It retains only the current batch and probe,
not a growing record/offset history, and pauses at each drained batch boundary.

Every 60 seconds, at a drained boundary, the test cancels the subscription,
verifies shared-client reuse by fetching the last processed record, then creates
a subscription from the saved next offset. Final assertions require the expected
restart count and verified delivery through every replacement: **29 verified
restarts in a completed 1,800-second run**. Construction alone is not counted as
successful restart coverage.

Inspect `SUBSCRIPTION_ENDURANCE_PROGRESS` and `SUBSCRIPTION_ENDURANCE_RESULT`:

- The final `requested_seconds` must match the intended duration, and elapsed
  time must be at least that duration. An unchanged default 30-second pass is not
  a long observation.
- Acknowledged, verified and delivered record totals must agree, as must
  acknowledged and verified content bytes. Require one Produce call per
  acknowledged batch and the expected next-offset checkpoint.
- Intermediate samples can contain an outstanding batch or a not-yet-acknowledged
  Produce. Final assertions require no outstanding records and settled resources.
- Active subscriptions, connections, requests and queued/buffered bytes must be
  zero after final client close. The runner's cleanup receipt must separately
  confirm stopped processes and removed broker data/control files.

The test's **768 MiB sampled RSS ceiling is fixed**. It cannot be disabled with
`KAFKA_SOAK_MAX_RSS_BYTES`. The host guard remains 2 GiB free disk and 1 GiB
MemAvailable. RSS is sampled every 100 ms and at work boundaries; host resources
every five seconds. Wire counters and connections are checked against configured
client limits. These are sampled safety stops, not OS-enforced hard caps, and
reported peaks may miss brief spikes. RSS includes the Dart VM, test framework and
assertions; it does not isolate client heap or certify absence of leaks.

The listener pauses after each expected batch, and cancellation does not drain
additional buffered events. The gate verifies the expected delivery prefix, not
absence of trailing records after its final checkpoint.

This is paced, single-partition correctness with per-batch backpressure. It does
not measure maximum throughput, offline retention recovery, durable processing,
application process crashes or broker host-power failure. Broker retention is a
cleanup policy, not an instantaneous disk cap. Ensure free space before running.

## Secured topology and network fault overlap

Run both security modes through the existing owned three-node fixture:

```sh
KAFKA_COMBINED_MODES=tls,sasl python3 tool/run_combined_secure_fault_integration.py -- \
  dart test test/topic_subscription_combined_fault_integration_test.dart \
  --concurrency=1 --reporter expanded
```

An explicit compatible cached command can be substituted as above. The runner
stops at the first failed mode; an isolated TLS or SASL run does not replace a
complete two-mode result. All nodes are on one host. Controller, admin and
inter-broker plumbing remain local plaintext; client traffic uses strict TLS or
SASL PLAIN over TLS through opaque proxies. Test certificates/credentials are
synthetic and ephemeral, and the fixture removes them after the run.

Two independent consumers read the same three explicitly assigned partitions:

1. **Strict defaults:** no TLS bootstrap fallback or Fetch recovery option. The
   stream must emit one terminal HandshakeException during the combined outage,
   with no fallback. This is the preserved security policy, not transparent
   default failover.
2. **Explicit recovery:** bootstrap fallback plus Fetch recovery with 20 total
   rounds, a 10-second per-Fetch admission window and 500 ms retry delay. Every
   peer is still validated independently. The same stream must drain its
   pre-acknowledged backlog after transport restoration, without an application
   metadata-convergence wait or replacement stream.

The fixture overlaps a leader crash with three bidirectional proxy blackholes,
then forces disconnect and fragments ciphertext. Both streams admit reads under
the blackholes and remain active at the crash overlap; those reads may be in
metadata discovery. The gate does not reassert
each subscription request remains pending after the crash. A separate, warmed
surviving-node raw long-poll remains positively pending across the overlap, and
another raw read helps prove upstream and downstream ciphertext drops. Proxies
neither decrypt traffic nor identify Kafka frames. The test does not attribute an exact TLS failure stage without evidence.

Six markers are acknowledged before the outage, three already processed and three
left as backlog. No Produce is attempted during the outage. After the crashed node
and full ISR are restored, three further markers are produced. Fresh clients
resume from each application's in-memory checkpoints, and each independent
consumer must verify offsets 0, 1 and 2 once per partition. The gate makes nine
explicit Produce calls and checks delivery of those nine acknowledged markers
at their exact offsets. It stops after the final expected
record rather than querying the log end, so it does not exclude a trailing
duplicate beyond that record. This is not an independent no-replay proof and
does not add idempotent production or durable/exactly-once processing.

Consumers allow three connections so healthy peers can stay authenticated before
the fault; the producer/proof client allows two. Clients configure explicit wire
and connection limits. Selected boundary checks and final zero-resource assertions supplement the sampled process/host guards;
wire counters are not observed continuously through fault recovery. The boundary
connection assertion permits three for all clients, so it does not independently
verify the producer/proof client's configured two-connection limit. Final client
counters and proxy workers/connections must settle. Inspect the sanitized JSON receipt for both
completed modes, no guard/cleanup failure, stopped owned processes/threads,
removed private fixture material and reusable ports. Do not count an assertion,
cleanup failure, skipped mode or incomplete run as a pass.

## Evidence and limits

Historical development receipts refer to `verification/subscription_endurance/`;
they are not present in every source checkout and apply to their own snapshots.
At `a9c5d12`, the existing plaintext paced profile completed 1,800.011 seconds
on Kafka 4.0.2/Dart 3.12.0 with 308,040 acknowledged/verified/delivered records,
29 verified subscription replacements and settled counters. Its separately
attempted four-worker concurrent operational soak was resource-guard stopped
and remains incomplete. The paced pass is not equivalent to completing that
profile. See [additional observations](QUALIFICATION_OBSERVATIONS.md) for exact
source, guard, setup and cleanup scope. Raw receipts are retained separately.
None broadens runtime support or changes default security behavior.

Independent-host failures, kernel packet loss, longer/repeated mixed workloads,
real-broker transaction/compaction progress and intended additional runtime forms
remain separate evidence gaps. Passing a finite observation is not a promise of
100% reliability or a reason to remove those release gates.
