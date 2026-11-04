# Separate graceful-stop source qualification

Observed 4 November 2026 for implementation commit
`693f509a0705dab30a23f832cb2899b44ea8cda2`, tree
`c81006f0d3f530e5bbcf57157c26160a8ebf3fe5`. The accompanying
[structured receipt](graceful_stop_qualification.json) records source hashes,
SDK dependency snapshots, exact Kafka distribution and aggregate outcomes.
Later documentation-only commits do not broaden these observations.
The later Kafka 4.4.0 checks below qualify their own exact source and are not
part of that `693f509` structured receipt.

This qualifies the separate opt-in
[`KafkaGroupConsumer.drainAndStop` contract](GROUP_CONSUMER.md#separate-opt-in-graceful-stop),
not a stable release or production-readiness claim. Immediate `stop()`, commit
non-replay and the session's existing heartbeat/commit Leave gate are unchanged.
No runtime dependencies were added.

## Ordinary and independent checks

On Linux x64 / Debian 13, both Dart **3.2.6** and **3.12.0** passed:

- Formatting and fatal-info analysis.
- The complete ordinary suite: **1,454 passed, zero failed, 382 skipped** each.
  Twelve new skips are the explicitly opt-in graceful-stop broker cases. A skip
  is not broker qualification.
- The 47 new deterministic graceful-stop tests, covering synchronous reentrancy,
  callback/commit ownership, independent heartbeats, fixed and exact-boundary
  deadlines, early/delayed/stale timers, repeated requests, immediate-stop
  supersession, startup/positions/backoff, late buffered Fetch, revocation/stalls,
  commit uncertainty and cleanup barriers distinct from the grace report.

The fixture-tool Python aggregate passed **283** cases without starting brokers.
An independent lifecycle/code review found no remaining blocker, independently
reran all 47 new tests and the existing consumer/coordinator-loss regressions,
and checked additional deadline, pending-commit and cleanup probes. The tests
found a consumed watchdog callback could replace its rearmed successor; a timer
version guard fixes that and preserves the original deadline.

The first full Dart 3.12.0 attempt failed during compilation when the shared
`/tmp` filesystem ran out of space. That attempt is retained as failed/incomplete.
A rerun using an owned workspace temporary directory passed without a client
code change for that environment failure. It does not explain or supersede any
unrelated historical failure.

## Separate owned Kafka 4.0.2 checks

Both SDKs independently passed **12/12** executed cases each, 24 total:
4 plaintext, 4 strictly verified TLS and 4 SASL_SSL PLAIN per SDK. Each mode checks:

1. Grace requested synchronously inside the original callback. Its exact Future
   remains pending for 7 seconds, beyond the configured 4-second local expiry and
   6-second broker session timeout. Independent heartbeats preserve the current
   generation, then its configured commit is confirmed. Fresh-transport readback
   observes the next offset, and the buffered next record is never admitted.
2. Disabled commit drains the callback without creating a stored offset.
3. Grace expiry reports the pending callback, leaves actual `closed` pending until
   that Future settles, and does not admit a late commit.
4. A genuine competing member causes heartbeat rebalance rejection during grace.
   The old callback can settle later, but admits no commit or replacement session.

The single-node fixtures used OpenJDK **21.0.12.1**, OpenSSL **3.5.7**, and the
explicit Kafka **4.0.2** distribution. Its archive matched the retained,
independently verified official SHA512 recorded in the structured receipt. The
secure runner's Java clients perform security smoke checks; the custom graceful
suite does **not** execute the default bidirectional Java application oracle.

Existing runners were used unchanged, with the new test file and concurrency 1:

```sh
python3 tool/run_operational_integration.py -- "$DART" test \
  test/group_consumer_graceful_stop_integration_test.dart \
  --concurrency=1 --reporter expanded
bash tool/run_secure_broker_integration.sh "$DART" test \
  test/group_consumer_graceful_stop_integration_test.dart \
  --concurrency=1 --reporter expanded
```

Select the verified `KAFKA_HOME`, exact `KAFKA_VERSION`, intended SDK and its
matching dependency snapshot first. Use only the disposable owned fixture, never
real credentials or production infrastructure. Separate plaintext/secure runs
skip the other mode's cases; those skips are not counted among the 24 executions.

All four runner exits were zero. Both plaintext broker processes stopped, their
data/control directories were removed, and no cleanup errors were reported.
Both secure private directories, including generated identities, credentials,
configs and raw broker logs, were removed. All seven owned ports were checked
closed after the final run. Only sanitized summaries and ordinary test receipts
were retained locally.

## Limits of the Kafka 4.0.2 observations

These are short, same-host, single-partition observations on Kafka 4.0.2. They do
not qualify another Kafka release, multi-node/independent-host behavior, sustained
workloads, mTLS, SCRAM, packaged mobile apps, OS lifecycle or web support.
Deterministic tests cover late unknown/rejected commits and Leave suppression;
this work adds no real-broker commit response-loss fault qualification. Historical
security/failover/version results remain separate.

The grace deadline bounds the attempt to retain application/commit authority,
not arbitrary code execution or actual closure. A `drained` report does not prove
cleanup, broker departure or external-effect safety. No exactly-once, external
fencing, guaranteed immediate partition handoff, publication or release follows.

## Separate Kafka 4.4.0 checks at 46c6355

Exact clean source `46c6355f193d1675ac680680cc462dd3674e5c02`, tree
`3fb6fa6268e5c6f2d2a535a8de0282d38cab91ff`, passed **24 requested real-broker
cases with zero failures** on Linux x64:

| Dart SDK | Plaintext | Server-authenticated TLS | SASL_SSL/PLAIN | Total |
| --- | ---: | ---: | ---: | ---: |
| 3.2.6 | 4 | 4 | 4 | 12 |
| 3.12.0 | 4 | 4 | 4 | 12 |

Each mode/SDK ran the same four existing cases described above: reentrant grace
with independent heartbeats and confirmed admitted-record commit, disabled
commit with missing stored offset, grace expiry without late commit, and a real
competing member revoking grace without late commit or rejoin. In the expiry
case, 250 ms grace reported `deadlineExpired`, `callbackPending: true` and
`commitPending: false`; the immutable report did not complete the held callback.
Every report was followed by waiting for actual `closed` and owned cleanup.

All four serial fixture invocations independently observed Kafka 4.4.0. The
tests used one owned broker and partition, explicit numeric missing-offset
fallbacks, strict server-authenticated TLS and SASL_SSL PLAIN where selected.
**Client-certificate authentication was not enabled**: these are not mTLS-plus-
grace tests. Local plaintext admin/controller plumbing is fixture-only.
The secure fixture's Java security smoke/producer setup succeeded; the selected
grace suite adds no Java consumer consume-back oracle.

Each plaintext invocation passed four and skipped eight unconfigured secure
definitions; each secure invocation passed eight and skipped four plaintext
definitions. Those **24 gated other-mode skips** are excluded from the 24 real
passes. Two broker-free prewarms each skipped all 12 definitions, another 24
excluded skips. No requested mode failed or was missing.

Explicit offline package configurations resolved the exact candidate. The
minimum SDK used `test` 1.25.2; the modern SDK used `test` 1.33.0 and a
SHA256-verified cached runner, compiling candidate library/tests freshly. Exact
HEAD, clean status and all captured source hashes matched before and after each
invocation. The library/pubspec source-map SHA256 was
`2ee85ae687a763e08479655a199eeff837fea41ffc5e846fb81cffe138a2e759`.
These checks do not rerun the ordinary SDK suites on this newer source.

The unchanged resource guard required 2 GiB free disk and 1 GiB MemAvailable,
sampled every five seconds; additional read-only samples observed compile/run
footprints. All four guarded invocations exited zero with no resource violation.
Both plaintext brokers stopped and owned data/control directories were removed.
Both secure brokers stopped and ephemeral keys, credentials and private
directories were removed. Seven selected ports independently rebound after
cleanup, owned temporary directories were empty, and no observed descendants
remained. An initial dispatch to a not-yet-created task directory did not run
and is not counted as a test. Raw evidence is retained separately, not shipped
here; secure output was sanitized by the existing fixture.

This is a short Kafka 4.4.0/Linux/two-SDK observation, separate from the earlier
`693f509`/4.0.2 results. A competing-member rebalance is not broker failover.
Neither result qualifies mTLS-plus-grace, sustained load, endurance, production
PKI, other platforms, producer recovery, exactly-once effects or published
package bytes. A grace report bounds authority/admission, not arbitrary
application work, actual closure or external-effect completion.
