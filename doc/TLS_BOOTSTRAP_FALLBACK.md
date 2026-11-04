# Experimental TLS bootstrap fallback

This document records the bootstrap-only policy and evidence sealed at
`d8f0f06`. Its default behavior is preserved. A separate, explicit Fetch recovery
policy is under evaluation in [TLS_FETCH_RECOVERY.md](TLS_FETCH_RECOVERY.md);
do not apply its new behavior or evidence retroactively to this checkpoint.

This isolated candidate adds an explicitly opt-in availability policy. It is
**off by default** and is not a claim of transparent failover or production
readiness. The original fail-fast checkpoint and its red combined-fault receipt
remain evidence of the original policy; see [the original limit](TLS_BOOTSTRAP_LIMIT.md).

## API and trust decision

Set `KafkaClient(allowTlsBootstrapFallback: true, tls: ..., brokers: ...)` to
enable the policy. Enabling it without a non-null TLS configuration is rejected.
Omitting the option, or setting it to false, preserves fail-fast TLS behavior.

The option permits metadata discovery to try the next distinct configured
bootstrap after a `HandshakeException` originating in TLS establishment. This
includes certificate, hostname and expiry rejection, not just interrupted
connections. The installed Dart 3.2.6 and 3.12.0 error interfaces do not provide a
stable typed EOF-only discriminator. The implementation does not inspect message
strings or treat a null `osError` as proof of a network-only failure.

Every attempted endpoint still independently passes the configured CA and
hostname checks. There is no plaintext downgrade, certificate-acceptance
callback, hostname substitution, or alternate trust store. SASL PLAIN and Kafka
application frames are sent only after TLS establishment succeeds; configured
SASL authentication must then succeed before ordinary application requests.

Enabling the option intentionally changes the meaning of one peer's TLS
rejection: it no longer necessarily stops discovery. Configure only bootstrap
endpoints that are acceptable destinations for this client's metadata requests
and, when configured, SASL credentials. A valid certificate authenticates the
configured peer identity; it does not by itself prove Kafka cluster membership.
Do not add unrelated endpoints merely because their certificates are trusted.

## Scope, errors and resource bounds

The policy is consumed only by metadata bootstrap traversal. High-level Fetch
and Produce use that traversal for leader discovery. Low-level `send` and direct
connections to discovered leaders do not traverse alternative bootstraps after
TLS failure. Discovered brokers are not appended to the configured fallback list.

Within one enabled traversal, configured order is preserved and each endpoint
key is attempted at most once. Identity uses the existing literal
`host:port` connection key. Duplicate entries with that key are skipped; DNS
aliases are not resolved or canonicalized into one identity. There is no global
blacklist, bootstrap rotation, or remembered exclusion across operations.

The private handshake marker is created only around TLS establishment, not
around local CA/context creation, SASL authentication or response parsing.
Invalid local certificate configuration remains terminal. Authentication,
unsupported API/mechanism, malformed Kafka responses, resource-limit,
cancellation and client-closure failures do not become fallback candidates.
Existing read-safe transport retry classifications remain unchanged.

If no endpoint succeeds and the traversal encountered a marked TLS failure,
it ends with a sanitized, non-retryable `HandshakeException`, even if the last
peer instead failed with an otherwise retryable transport error. This prevents
outer read retries from restarting that failed traversal. A terminal non-TLS
failure encountered during traversal still escapes immediately rather than being
hidden by the final TLS error. If no marked TLS failure occurred, existing
transport exhaustion/retry behavior applies.

For N distinct endpoints, each traversal makes at most N endpoint attempts.
If discovery succeeds but a later read-safe operation fails, an existing outer
retry can start a new traversal; metadata attempts for that operation can then
reach `(maxReadRetries + 1) * N`. Independent concurrent operations have their
own traversal state and may share an in-progress connection attempt. Existing
connection, admission, buffer and per-request limits still apply.

These are attempt and resource bounds, **not an overall operation deadline**.
The high-level methods do not acquire a new overall timeout or cancellation API.
An application `Future.timeout` does not cancel underlying work.

Produce remains single-attempt after leader discovery. The option can help its
pre-send metadata lookup, but never replays Produce after a failed or ambiguous
acknowledgement, reconnects it to an arbitrary bootstrap, or establishes whether
an uncertain write was stored. Idempotent/transactional production is not added.

## Compatibility and adoption

The new optional named constructor argument preserves ordinary callers' default
behavior. Adding public members can require changes in external classes that
`implements KafkaClient` directly; source compatibility with such implementations
has not been established. Maintainers must decide whether to adopt the option,
how to expose it in configuration, and the appropriate release/version policy.
The supplied secure example still uses the default-disabled behavior.

## Diagnostics

`bootstrapTlsFallbackCount` is a cumulative, read-only count of metadata
traversal advances to another distinct configured endpoint immediately after a
marked TLS handshake failure. It is not a count of physical sockets, rejected
certificates, completed recoveries, or all TLS failures. A failure at the last
distinct endpoint adds no advance. Skipped duplicate entries add none. Concurrent
traversals count independently even when they share the failed connection.

When enabled, handshake failures are replaced with fixed-text errors without
the original native message, OS error, certificate details or nested cause.
This sanitization also applies when such an error escapes a low-level or leader
connection, although those operations do not gain fallback. Exhausted bootstrap
TLS discovery has its own fixed-text `HandshakeException`. Default-disabled
clients retain native fail-fast TLS exceptions. Sanitization is scoped to these
handshake diagnostics, not a promise that every possible application error or
log is safe to publish.

## Evidence and remaining limits

At this documentation checkpoint, the disposable strict-TLS fixture passed all
22 required cases on Dart 3.12.0 and Dart 3.2.6. Both sanitized runner receipts
reported zero failures/skips, complete case coverage, successful reporter
completion, process exit zero and fixture cleanup. Its cases exercise EOF,
untrusted CA, hostname mismatch and expired certificates followed by a valid
peer; default-off rejection; all-invalid exhaustion; terminal authentication,
mechanism, API, malformed-frame and resource failures; and Produce send counts.
These are owned synthetic Kafka peers using real TLS, not storage/durability or
independent-host validation. The focused tests additionally cover local CA
failure, mixed TLS/transport exhaustion, duplicate endpoints, close/cancellation
and concurrent traversals. Their execution receipts are maintained separately.

The enabled immediate-Fetch combined gate is still **red**: on the observed
Dart 3.12.0 TLS run, metadata discovery progressed but Fetch then attempted a
stale crashed leader and failed during that leader's TLS establishment. The
SASL mode was not reached in that failed combined run. Leader TLS remains
terminal; this candidate does not fix that boundary by expanding retries.

A separately identified metadata-converged combined scenario **passed TLS and
SASL modes on Dart 3.12.0 and Dart 3.2.6**. Its application waits, using the same client and
bootstrap order, for surviving leaders and at least two surviving ISR members
before Fetch. Both modes verified six markers at offsets 0 and 1 across three
partitions, restart/rejoin to ISR 3, settled counters and clean fixture teardown.
Observed metadata-convergence / subsequent Fetch / total recovery times were
539 / 103 / 642 ms for TLS and 435 / 412 / 847 ms for SASL. These are individual
fixture observations, not latency guarantees.

The equivalent Dart 3.2.6 observations were 540 / 104 / 644 ms for TLS and
444 / 844 / 1,288 ms for SASL. Its strict 22-case synthetic fixture pass is a
separate result.
The converged scenario demonstrates application-coordinated recovery after
observed metadata convergence, not transparent immediate-Fetch failover.
Metadata convergence does not guarantee a later independent lookup cannot be
stale. Preserve the immediate-Fetch red receipt rather than replacing it with
the converged result.

In combined result JSON, `recovery_tls_fallback_count` is sampled at scenario
completion as a delta from recovery start. It includes later reads, writes and
rejoin checks; it is not a snapshot at the initial recovered Fetch. The separate
metadata-convergence event reports the counter at that earlier observation.

## Resolver-free reproduction

Run from the candidate repository root. Prerequisites are an already installed
compatible Dart SDK, its already-resolved package configuration and matching
cached test runner, Python 3, OpenSSL and Linux. No command below runs `pub get`
or downloads dependencies. The focused and synthetic fixture commands do not
contact a Kafka service. Package resolution must point
`kafka_client` at this candidate's source, not a stale source copy. Do not reuse a
runner snapshot compiled with a different SDK.

Set these paths to the pre-existing artifacts for each SDK under test:

```sh
export DART=/absolute/path/to/matching/dart
export PACKAGE_CONFIG=/absolute/path/to/resolved/package_config.json
export TEST_RUNNER=/absolute/path/to/matching/test.dart-SDK_VERSION.snapshot

"$DART" --disable-dart-dev --packages="$PACKAGE_CONFIG" "$TEST_RUNNER" \
  test/tls_bootstrap_boundary_test.dart test/bootstrap_tls_fallback_test.dart \
  --concurrency=1 --reporter=expanded

python3 tool/run_bootstrap_tls_fallback_integration.py -- \
  "$DART" --disable-dart-dev --packages="$PACKAGE_CONFIG" "$TEST_RUNNER" \
  test/bootstrap_tls_fallback_integration_test.dart \
  --concurrency=1 --reporter=json
```

The strict fixture runner creates temporary private CA/server material and
synthetic credentials, checks the intended certificate defects, and removes
fixture material afterward. It drains raw child output without retaining it.
Only allowlisted case names and sanitized status fields are reported. Passing
requires all 22 unique expected cases, no skipped/failed cases, a successful JSON
reporter completion and successful child exit. Running the integration test
without its fixture environment skips it and is not validation.

The runner also applies sampled host/process resource guards and an execution
timeout. These safeguards do not turn the library's operations into hard
wall-clock-bounded operations. Never supply real credentials or redirect this
fixture toward a live service. Use the separately recorded combined-gate
instructions and receipts for real owned Kafka validation; strict synthetic
fixture success does not replace them.

For the distinct converged scenario, additionally provide an independently
SHA512-verified Kafka 4.0.2 distribution and Java 17+, then use the same resolved
Dart artifacts:

```sh
export KAFKA_HOME=/absolute/path/to/kafka_2.13-4.0.2
KAFKA_COMBINED_MODES=tls,sasl \
  python3 tool/run_combined_secure_fault_integration.py -- \
  "$DART" --disable-dart-dev --packages="$PACKAGE_CONFIG" "$TEST_RUNNER" \
  test/combined_secure_fault_converged_test.dart --concurrency=1 --reporter=expanded
```

`combined_secure_fault_candidate_test.dart` is the distinct enabled
immediate-Fetch gate. `combined_secure_fault_integration_test.dart` retains the
default-disabled entrypoint. Do not substitute the converged entrypoint when
reporting an immediate-Fetch result. Combined modes run serially and stop at the
first failure, so an unreached second mode is not a pass.
