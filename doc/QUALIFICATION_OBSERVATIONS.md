# Additional native qualification observations, 2026-11-04

These are source-specific observations, not a cumulative certification of the
latest checkout or the published experimental 0.1.1 package. The documentation
baseline is `4701dd688ddd8aed43a64085b29f8c7d33754bfa` (the same source tree as
remote `54d8b9c`). Earlier results below do not automatically qualify changes
made after their recorded commits. The separate
[six-version and response-hardening matrices](BROKER_QUALIFICATION.md) retain
their original source attribution.

All observations below used Linux x64 and Dart 3.12.0 with owned loopback
fixtures. Java/distribution provenance remains scoped to each original receipt;
it is not inferred from another run on the same source. Source captures matched
their stated commits and remained unchanged through the recorded runs. Source/SDK identities, assertion probes,
raw receipts and setup failures are retained separately, not shipped here.
Counts use the units stated below; scratch assertion probes are not
`package:test` cases, and scenarios/assertions must not inflate test totals.

## ApiVersions-hardened source: coordinator and network faults

Source: `a9c5d1210892519f1d5637ade9999aec303e78ef`. The coordinator receipt and
network-fault runtime record identify OpenJDK 21.0.12.1.

- **Coordinator loss: four top-level tests passed, containing eight scenarios**
  across Kafka 4.0.2 and 4.4.0. Each broker version ran TLS and SASL_SSL/PLAIN,
  with recovery disabled/enabled. These are owned three-node RF3 fixtures with
  opaque TCP proxies and test-specific bounded election/recovery settings.
- The actual non-first-bootstrap coordinator was crashed while a callback was
  held and Fetch was paused. Disabled recovery stopped after one generation;
  enabled recovery revoked the old token, drained that callback, read preserved
  confirmed progress and processed/committed in one replacement generation on
  survivors. The victim stayed down through result and commit readback. Actual
  Metadata-v0 warning 9 with usable survivor leaders was required.
- A preliminary 4.0.2 source-harness attempt was SIGKILLed before test output or
  proxy traffic. No guard violation was reported; the cause remains unknown.
  The passing source-identical retry used a prebuilt `package:test` app-jit
  harness, separating compilation from broker execution. Its no-fixture prewarm
  was skipped and is not qualification evidence.
- **Network faults: 12 tests passed on Kafka 4.0.2 only**, four each for
  plaintext, TLS and SASL_SSL/PLAIN. Cases cover a lost Produce acknowledgement,
  three concurrent-read blackhole cycles, forced in-flight disconnect/recovery
  and three delayed-fragmentation cycles. Settled request/byte counters were zero.
- The lost-ack storage check found one marker and its contiguous acknowledged
  sentinel using an independent direct Fetch oracle, not an official Java
  consumer. Only plaintext counted a Produce frame; encrypted proxies remained
  opaque. This establishes no automatic replay in that scenario, not general
  idempotence or exactly-once effects.

The retained coordinator/network JSON receipts confirm cleanup, reusable ports
and removal of private fixture material. Fixed guards
were unchanged: at least 2 GiB free disk, 1 GiB MemAvailable and, where sampled
for the test process, a 768 MiB RSS ceiling. Admin/interbroker/controller traffic
was local plaintext; these are not fully encrypted or independent-host
deployment tests. Fault-injection-to-proxy-restoration timing is not end-to-end
coordinator recovery latency. See [coordinator recovery](GROUP_COORDINATOR_RECOVERY.md)
for the narrow opt-in policy, not a default-budget or single-call guarantee.

## Same source: two distinct sustained profiles

Both observations used `a9c5d1210892519f1d5637ade9999aec303e78ef`, one loopback
Kafka 4.0.2 broker, plaintext and an identified cached `package:test` app-jit
harness. Neither qualifies secure endurance, other broker versions/platforms,
replicated failures or maximum throughput.

### Paced subscription: passed

The existing single-stream explicit-partition profile ran **1,800.011 seconds**
with `KAFKA_SUBSCRIPTION_ENDURANCE_SECONDS=1800`. It completed 77,010 batches/
Produce calls; acknowledged, verified and delivered counts all equalled
**308,040 records**, covering 179,365,100 verified content bytes. All **29 of 29
subscription replacements** verified subsequent delivery. These were stream
replacements, not broker/process restarts, durable checkpoints or group commits.

The workload used four 512-byte records per batch, synchronous content/header/
offset verification, a fixed 10 ms pause per drained batch, fixed 10 ms polling
and application-owned in-memory next offsets. Sampled whole-test RSS peaked at
126,697,472 bytes. Final outstanding records, subscriptions, connections,
requests, queued bytes and buffered bytes were zero. No host guard stopped the
run; cleanup and independent port reuse passed.

### Four-worker operational soak: incomplete

The separate four-worker run was **resource-guard stopped, not a 30-minute
pass**. The first four operational cases completed, covering persisted-log
crash/restarts, long-poll deadline/reconnection, cancellation/reconnection and
repeated close. Its latest sustained checkpoint was **660.603 seconds and
2,616,800 verified records**; no final sustained receipt was emitted. The guard
stopped it when free disk fell below 2 GiB. Sampled test RSS peaked at
135,241,728 bytes. Cleanup and independent port reuse passed.

The paced pass does not complete or replace this concurrent profile. Both kept
the original fixed resource guards and workload settings. Resource observations
are sampled whole-test-process values, not isolated client-heap or hard-limit
guarantees. Neither finite observation proves leak freedom, capacity, absence
of trailing records after the final checkpoint or general production readiness.
The concurrent completion gate remains open; the resource stop does not
identify a client leak. See [subscription reliability](SUBSCRIPTION_RELIABILITY.md).

## Explicit ListOffsets: plaintext boundary comparisons

Source: `39a02aa998e200061ec51735071fc247d33a6997`. Kafka 4.0.2 and 4.4.0 were
tested serially, one owned partition per fixture, using an external compiled
assertion probe in the unchanged guarded operational fixture. This receipt
identifies OpenJDK 21.0.12.1 and retained SHA512 distribution-verification
receipts and observed broker runtime versions.

The public earliest/latest results matched each selected distribution's official
`kafka-get-offsets.sh` numeric results at every stage:

| Owned log stage | Earliest | Latest |
| --- | ---: | ---: |
| Empty | 0 | 0 |
| Eight acknowledged records | 0 | 8 |
| Official DeleteRecords advances logical start | 3 | 8 |
| Two more acknowledged records | 3 | 10 |

**16 boundary comparisons passed** across the two brokers, with 16 successful
official GetOffsetShell commands and two successful DeleteRecords commands.
Response topic/partition, successful error code, timestamp -1 and nonnegative
throttle metadata also matched expectations. All five exposed connection/
request/byte counters were zero after close; guards, cleanup and port reuse passed.
Two pre-broker scratch compilation setup failures were corrected and retained;
no library source changed to address them.

DeleteRecords tests a deterministic logical retained start, not automatic
time-based retention or physical segment reclamation. No secure ListOffsets,
multiple partitions, timestamp queries, broker errors/retries/failover or other
broker versions were qualified by this probe. See [ListOffsets](LIST_OFFSETS.md).

## Managed missing-position policy: plaintext precedence cases

Source: `4701dd688ddd8aed43a64085b29f8c7d33754bfa`. The same two exact broker
versions ran serially with one owned partition and an independent group per case,
using an external compiled assertion probe in the unchanged guarded fixture.
This receipt identifies OpenJDK 21.0.12.1 and retained SHA512 distribution
receipts and observed broker runtime versions.

**14 cases passed**: seven per version, including two expected default-policy
terminal cases and 12 clean processing/stop cases. They checked absent+earliest,
absent+latest, numeric fallback overriding either boundary choice, stored next
offset 7 overriding numeric 2/latest, no-fallback failure, and earliest after
DeleteRecords advanced logical log start to 3. There were **66 exact ordered
record deliveries** across independent groups and 20 unique acknowledged records
across the final fixtures; replay between groups was intentional.

Every case used `commitAfterProcessing=false`. Independent OffsetFetch reads
before/during/after processing found missing positions still missing and stored
7 still 7; managed `lastCommit` stayed null. This establishes no broker-stored
offset change in these cases, not a raw OffsetCommit-request count. The latest
case waited for public processing readiness, saw no old callback before append
and then delivered only new offsets 8–9. Record bytes, timestamps, ordered
duplicate/null/empty headers, absolute offsets and callback assignment were checked.

Both final fixtures passed guards, cleanup, reusable-port checks and confirmed
LeaveGroup cleanup. A preliminary scratch timeout configuration was rejected by
constructor validation before any case/join, after seeding eight synthetic
records; it cleaned successfully and remains a setup failure, not a pass.

This does not qualify secure transport, multiple partitions/members,
rejoin/failover, stable OffsetFetch v7, automatic retention races, lookup errors
or out-of-range recovery. Default fallback behavior and Fetch out-of-range
failure remain unchanged. See [missing-position policy](GROUP_MISSING_OFFSETS.md).
