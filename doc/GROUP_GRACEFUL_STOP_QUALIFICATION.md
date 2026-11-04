# Separate graceful-stop source qualification

Observed 4 November 2026 for implementation commit
`693f509a0705dab30a23f832cb2899b44ea8cda2`, tree
`c81006f0d3f530e5bbcf57157c26160a8ebf3fe5`. The accompanying
[structured receipt](graceful_stop_qualification.json) records source hashes,
SDK dependency snapshots, exact Kafka distribution and aggregate outcomes.
Later documentation-only commits do not broaden these observations.

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

## Limits

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
