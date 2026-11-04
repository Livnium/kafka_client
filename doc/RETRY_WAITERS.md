# Removable ordinary retry delays

This checkpoint changes internal delay ownership, not Kafka retry policy. The
sealed `52e09f8` snapshot remains the before-fix reference. The optional TLS
bootstrap and Fetch policies remain default-disabled and experimental.

## What was reproduced

The original ordinary `_safeRetry` loop raced a new delayed Future against one
client-wide close Future with `Future.any`. In both tested Dart SDKs, each race
attaches a listener to the close Future and does not detach it when the delay
wins. Direct inspection of a still-open client found exactly 10,000 reachable
close listeners after 10,000 completed retries. Closing the client cleared them.

The standalone diagnostic calls the real private retry method with a synthetic
operation that fails once with `SocketException`, then returns a fixed scalar.
It checks two calls per operation, no connections or application requests, and
listener counts at 0, 10, 100, 1,000 and 10,000 completed retries. This is a focused
ownership measurement, not a network, payload, sustained-throughput or heap test.

The diagnostic deliberately uses `dart:mirrors` and private SDK fields in the
recorded Dart 3.12.0 and 3.2.6 JIT VMs. It validates the expected Future state and
listener shape, bounds traversal and fails on unknown layouts. It is not an AOT,
Flutter, browser or general SDK compatibility claim. No VM service or credentials
are needed. RSS observations are sampled safety readings, not evidence of which
objects consume memory or proof that total process memory should fall.

## Internal change

Both ordinary retries and opt-in Fetch recovery now use one package-internal
`CloseWaitGroup`. Each active delay owns a completer and timer, unregisters after
completion, and cancels its timer in cleanup. Close marks the client closed first,
then releases the active waits. Their continuations cannot admit another retry.
The old permanently pending close Future is no longer needed.

The fixed diagnostic reports zero active waiters after every completed sequential
retry and after close. The retained history is removed; currently active waits
still consume resources. Their number is not bounded by `maxInFlightRequests`,
which limits admitted transport requests rather than every logical retrying call.
Applications should continue to bound their own operation concurrency.

The implementation preserves:

- `maxReadRetries` additional-attempt semantics and retryable error categories.
- Integer full-jitter exponential delay bounds, capped at one second, including
  asynchronous zero-duration timers. Tests check bounds, not random distribution.
- Pre-send leader discovery retries, followed by a single Produce dispatch.
- Existing low-level request cancellation and client-close behavior.
- The separate opt-in Fetch round budget and admission-window semantics.

A high-level Future timeout still does not cancel underlying work. No per-call
high-level cancellation token, hard overall deadline, TLS retry expansion or
Produce replay has been added. Close releases waiters asynchronously; timer
cleanup is verified after the operation and close settle, not instantaneously at
the call to `close()`.

## Regressions and evidence

The loopback cases cover attempt budgets 0/1/5/10, delay envelopes, zero delay,
retryable and terminal errors, raw metadata diagnostics, one Produce after
pre-send discovery retries, timer-creation failure, close-only interruption,
both synchronous callback orders, and simultaneous retrying calls canceled by
close. The synthetic timer used to force callback ordering is intentionally not
a clock or a real one-shot timer; those cases prove ordering safety and explicit
cancel invocation, not three different real timer leaks.

The existing helper tests exercise completed waits, concurrent close, 200 forced
callback orderings and failed timer construction. Security and operational
regressions are recorded with the final runtime source in
`verification/retry_waiters/RESULTS.md` in the development checkout. Prior
checkpoint reports remain historical evidence for their own source revisions.

No conclusion about retained request payloads, a whole-client heap leak, the
older minimum-SDK RSS guard failure, or general memory capacity follows from this
listener-count result. It does not close the independent-host/platform/release
decisions listed in [V1_READINESS.md](V1_READINESS.md).

## Resolver-free reproduction

Use an already-resolved package configuration and the matching installed SDK.
The fixed probe runs no broker and opens no sockets:

```sh
python3 tool/run_retry_waiter_probe.py /new/private/probe-run -- \
  /absolute/path/to/dart --disable-dart-dev \
  --packages=.dart_tool/package_config.json \
  tool/measure_retry_waiters.dart removable 10000
```

The runner requires the complete probe report, exact counts, successful exit,
final cleanup and fixed sampled guards: 120 seconds, 768 MiB process-group RSS,
2 GiB free disk and 1 GiB available host memory. It is Linux-specific and starts
only the explicitly supplied command. Use only the standalone no-child probe
command above; process-leader exit is not a general descendant-cleanup proof.
The output directory must not already exist. The before-fix mode is `legacy` and
requires the frozen before-fix library source with this diagnostic copied in.
Running legacy mode on the fixed source, or vice versa, fails rather than quietly
assuming missing instrumentation means zero.
