# Experimental Fetch TLS leader recovery

This is an isolated, default-disabled candidate built on the sealed strict
bootstrap-fallback candidate `d8f0f06`. It is not adoption, a release or a general
transparent-failover guarantee. The previous immediate-Fetch failure remains a
reproducible result of that prior policy.

## Why bootstrap fallback alone is insufficient

A healthy bootstrap can briefly return metadata that still names a crashed
leader. In the owned combined-fault fixture, a proxy for that leader remains
listening but closes because its upstream broker is absent. Dart reports a TLS
handshake failure. The bootstrap-only candidate successfully discovers metadata,
then stops at that leader's failed TLS connection. This reproduced independently
with TLS and SASL-over-TLS on the current SDK before the new policy was applied.

The new policy must not classify TLS rejection as an ordinary network retry or
reconnect to a rejected endpoint just because stale metadata still names it.
Neither TLS exception text nor a null OS error reliably identifies harmless EOF.

## Explicit configuration

The candidate configuration is `KafkaClient(tls: ..., allowTlsBootstrapFallback:
true, tlsFetchRecovery: KafkaTlsFetchRecovery(...), brokers: ...)`.
`tlsFetchRecovery` defaults to null. Both TLS and the existing explicit bootstrap
fallback option are required when the Fetch policy is supplied.

The immutable policy contains a finite total `maxAttempts` (1–20, default 10),
a monotonic `window` (positive, at most one minute, default five seconds), and a
bounded `retryDelay` (zero through one second, default 150 ms). It applies only to high-level Fetch. Its total round
budget replaces the ordinary `maxReadRetries` budget for that Fetch; the budgets
are not nested or multiplied. Standalone metadata, Produce and low-level send do
not acquire the new recovery behavior.

## Rejected endpoints remain rejected during the read

Each enabled Fetch owns an initially empty set of rejected literal host:port
endpoints. Scoped handshake failures during initial or later metadata traversal
are recorded when the explicit bootstrap policy allows continuation. A discovered
leader's handshake failure is recorded too. Later metadata traversal skips those
endpoints. If metadata still names a rejected leader, the read may refresh again
within its remaining budget, but never reconnects to that endpoint.

A newly advertised endpoint must independently pass the same strict TLS and SASL
checks before Kafka application data. The policy never sends Fetch to an arbitrary
bootstrap in place of its advertised leader, changes the trust store, substitutes
a hostname, accepts a bad certificate, or downgrades to plaintext. A valid
certificate still does not prove Kafka cluster membership; configure acceptable
bootstrap destinations and trust settings.

An endpoint stays excluded for this Fetch even if its process recovers or its
certificate is repaired. Another Fetch starts fresh; concurrent reads have
independent state even when they share connection futures. There is no global
blacklist, DNS canonicalization or mutation of the configured bootstrap list.

## Error and time bounds

One dedicated loop owns the entire round budget, including ordinary transport,
election and stale-route failures. TLS remains absent from generic `_retryable`.
Metadata exhaustion with new or carried TLS rejection fails closed immediately.
Stale-route/round exhaustion after TLS rejection is a fixed terminal handshake
error, not a generic transport error that silently restarts discovery.
Authentication, local CA loading, unsupported mechanisms/APIs, malformed responses,
resource limits and client closure remain terminal and are not hidden behind an
earlier TLS failure.

The monotonic window is an **attempt-admission/polling window**, starting at Fetch
entry. It is checked before metadata endpoints and checked sends, after version
negotiation before application request enqueue, before leader dispatch and after
close-aware polling delays. Once it expires, no new recovery phase is admitted.
After a TLS rejection, expiry fails with a terminal handshake error; without one,
expiry can report a timeout.

This is **not a hard overall cancellation deadline**. Shared connection,
authentication and API-version work already admitted may finish under the existing
connection/request deadlines. The policy does not use `Future.timeout` to orphan
underlying work, and does not add a new high-level cancellation API. A hard
operation-wide cancellation contract needs separate ownership and propagation
design. Client close interrupts polling delays and remains terminal.

Produce still performs only its existing pre-send leader lookup and one dispatch.
No ambiguous write is replayed. Low-level send remains single-use and non-retrying.

## Evidence and interpretation

The new strict real-TLS synthetic fixture passes 49 cases on Dart 3.12.0 and
3.2.6, with complete allowlisted case coverage, successful exit and private cleanup.
It covers the four certificate/EOF rejection categories, initial/later bootstrap
exclusions, stale and alternating leaders, mixed transport failures, exact round
limits, terminal security/protocol/resource errors, window admission and close,
independent calls, shared failed handshakes and shared API-version futures,
authentication-window expiry, and unchanged non-Fetch behavior. Synthetic Fetch
responses are intentionally empty; their exact send counts verify that successful
empty reads are returned rather than retried. These are routing/validation tests,
not durability evidence.

The first 46-case execution reported every case passing but failed overall when
RSS sampling raced child exit. That failure is retained. A controlled Python
regression reproduced the issue before a minimal runner fix: missing RSS can be
accepted only after a fresh poll confirms the owned child exited, preserving its
actual exit code and all matrix/cleanup gates. Live or uncertain sampling failure
still stops the run. Guard thresholds were not changed.

The real combined fixture's initial ten-round / five-second / 150-ms configuration
failed closed at its count budget. In a diagnostic repeat, ten rounds ended after
1,684 ms; separate post-failure metadata still named the crashed leader through
2,817 ms after restoration. Those diagnostic reads did not turn the failed Fetch
into a pass. The default policy limits are unchanged. A distinct test configuration
uses at most 20 rounds, a ten-second admission window and 500-ms polling. Election
timing varies; different runs are not a controlled latency comparison.

The real-broker gate calls Fetch once immediately after fault restoration, with
no application metadata wait or outer read polling for that call. Its first
current-SDK TLS/SASL run passed and verified all six markers. A minimum-SDK SASL
run returned a valid empty Fetch response but failed the test's stronger immediate
marker assumption. That receipt is retained. The revised gate reports initial
Fetch success, record count and watermark separately; if empty, a later explicitly
counted application poll verifies marker visibility. The client does not gain
empty-read retries. A successful first Fetch therefore proves routing recovery,
not immediate availability of its requested record. Full marker integrity,
restart/ISR recovery and cleanup remain separate assertions.

The revised final gate passes TLS and SASL on both tested SDKs. In the minimum
SDK SASL run, the initial successful Fetch reported watermark 0 and no records;
two later visibility calls found the marker after another 423 ms. The other
three final cases returned the marker in the first recovered Fetch. All four
verified six markers, restart/ISR3 recovery and settled resources. These are
single local-fixture observations, not latency or availability guarantees.

Final real-broker matrix receipts and remaining limits are recorded in
`verification/stale_leader_candidate/RESULTS.md` in the development checkout.
They do not replace the earlier bootstrap-only red gate or turn this optional
configuration into a default-policy, independent-host or hard-deadline guarantee.

## Delay ownership

The new polling path uses a package-internal group of active waiters. Timer
completion unregisters its waiter; close releases all currently registered waits;
timer construction failure and timer/close races also unregister safely. It does
not attach a new losing listener to a permanently pending close Future on each
completed polling delay. The installed SDK `Future.any` implementation retains
those losing listeners until its other input completes, so that earlier prototype
was replaced before sealing.

This conclusion follows source reachability, not a heap/RSS measurement or a
claim that Fetch payloads were retained. Tests cover repeated timer completion,
concurrent close, timer/close races and timer-creation failure. A subsequent
checkpoint also moved ordinary read retries onto the same removable group,
without changing either retry policy. See [RETRY_WAITERS.md](RETRY_WAITERS.md)
for the direct before/after listener measurements and their limits.

## Resolver-free reproduction

Use an already-resolved package configuration, matching cached test runner and
installed SDK. These commands do not resolve or download packages. The local
fixtures generate only ephemeral test certificates and synthetic credentials.
Never point them at a live service or supply real credentials.

```sh
export DART=/absolute/path/to/matching/dart
export PACKAGE_CONFIG=/absolute/path/to/resolved/package_config.json
export TEST_RUNNER=/absolute/path/to/matching/test.dart-SDK_VERSION.snapshot

"$DART" --disable-dart-dev --packages="$PACKAGE_CONFIG" "$TEST_RUNNER" \
  test/tls_fetch_recovery_policy_test.dart --reporter=expanded

python3 tool/run_tls_fetch_recovery_integration.py -- \
  "$DART" --disable-dart-dev --packages="$PACKAGE_CONFIG" "$TEST_RUNNER" \
  test/tls_fetch_recovery_integration_test.dart --reporter=json

export KAFKA_HOME=/absolute/path/to/SHA512-verified/kafka_2.13-4.0.2
KAFKA_COMBINED_MODES=tls,sasl \
  python3 tool/run_combined_secure_fault_integration.py -- \
  "$DART" --disable-dart-dev --packages="$PACKAGE_CONFIG" "$TEST_RUNNER" \
  test/combined_secure_fault_fetch_recovery_test.dart --reporter=expanded
```

The synthetic runner requires all 49 unique expected cases, no skips/failures,
successful reporter and process exit, and cleanup. Without its fixture environment
the Dart suite skips and supplies no security evidence. The combined fixture needs
Linux, Python 3, OpenSSL, Java and the specified local Kafka distribution. Its
resource guards are sampled stops, not hard limits. Modes run serially and stop at
the first failure; an unreached mode is not a pass.

## Timer granularity correction

The bounded group-delivery verification found a pre-existing retry-backoff edge on
the minimum SDK: a clipped wait with microseconds could be rounded down by the VM
Timer, allowing another metadata round before the intended retry target. In the
250-ms-window/500-ms-retry diagnostic, 5 of 20 runs admitted a second metadata
request. All still terminated with the expected window error, and none contacted
the replacement leader. This was a backoff-admission defect, not a certificate
bypass or evidence of post-window phase admission.

The delay now keeps one fixed monotonic target, rounds positive timer waits up,
and rechecks lifetime/window and the target after every wake. Early timer wakes
cannot by themselves grant another retry. Zero-delay waits still yield once so
close work can run. Global wait behavior, security defaults, maximum rounds and
Produce non-replay remain unchanged. An already-admitted phase can still finish
after the window; this does not create a hard operation deadline. Evidence and
deterministic early-wake regressions are in `verification/group_delivery/`.

## Explicit forwarding into managed groups

`KafkaGroupSession` and `KafkaGroupConsumer` separately accept the nullable
`tlsFetchRecovery` policy. Only the dedicated data client for each generation
receives it. Control, position lookup and membership/commit behavior do not change.
Strict defaults remain null/disabled, and TLS plus explicit bootstrap fallback
are required. See [GROUP_FETCH_RECOVERY.md](GROUP_FETCH_RECOVERY.md) for the
revocation and ownership boundaries and separately scoped validation.
