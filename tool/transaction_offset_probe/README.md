# Genuine pending transactional-offset evidence (Kafka 4.0.2)

This fixture creates only synthetic group IDs derived from the existing owned
single-broker secure fixture topic. It does not test application message delivery,
no-gap processing, failover, or exactly-once correctness. Offsets 10 and 20 are
synthetic metadata, not claims about records in the topic.

Run through `tool/run_secure_broker_integration.sh` with `bash
tool/transaction_offset_probe/run.sh` as its custom test command. Set verified
`KAFKA_HOME` and `JAVA_HOME`. The existing broker runner provisions loopback TLS
and SASL_SSL, removes ephemeral credentials/configuration/logs, and retains
sanitized test output even on failure. Check resource floors before starting a
broker. The helper compiles into a temporary directory and removes its classes.

Both Java sources required by `run.sh` live in this directory and are included
in the package payload. `OffsetFetchWireOracle.java` is the unchanged fixture
oracle from the stable-offset development checkpoint; its historical copy under
`verification/` remains evidence, not a runtime dependency. Kafka 4.0.2's external
JARs and a suitable JDK are still required. The offline launcher regressions in
`tool/test_transaction_offset_package.py` check source closure and cleanup without
starting a broker; they do not replace the real transaction integration tests.

The official KafkaProducer initializes a transaction and synchronously calls
`sendOffsetsToTransaction` for a synthetic group, without completing that
transaction. This ACK precedes all pending-state observations. Official
OffsetFetch codecs emit complete request and response hex and hashes:

1. Admin seeds baseline offset 10, verified with stable v7 OffsetFetch.
2. Producer stages offset 20; v5 and non-stable v7 still see baseline 10.
3. Three stable v7 requests must each return partition-level error 88.
4. Producer commits or aborts once (no application-level commit retry).
5. Read-only stable OffsetFetch retries settle to 20 (commit) or 10 (abort).

Default execution covers SSL and SASL_SSL, each with commit and abort. All TLS
connections use CA verification and hostname verification. SASL credentials
come from the ephemeral fixture environment and are never included in wire
receipts (authentication frames are deliberately excluded).

For coordination with a Dart retry test, pass `--hold SSL commit` (or SASL_SSL,
abort). The helper emits `awaiting-resolution-command` after the explicit raw 88
observations and holds the transaction open. Its event includes the synthetic
group ID. The controller can start the Dart read for that group, capture its
error-88 retry, then write the exact line `commit` (or `abort`) to the helper's
stdin. Use the fixture topic and partition 0. Transaction timeout is 60 seconds;
finish the controlled handshake within that interval. A wrong command/EOF fails
rather than silently releasing as a different outcome.

`pending-not88`, unexpected control offsets/errors, a post-resolution timeout,
or other failed assertions must remain failures with retained raw receipts;
they must not be relabeled no-gap evidence. `all-cases-pass` proves only this
single-broker transaction visibility experiment. The production client remains
outside this helper; Dart's retry integration requires its own assertions.

Source rationale (development-checkout evidence, excluded from publication):
the checked-in official Kafka 4.0.2 source capture
`verification/stable_offsets/evidence/source-review/OffsetMetadataManager-4.0.2.java`
lines 865–902 checks open transactional offsets when requireStable is true and
returns `UNSTABLE_OFFSET_COMMIT`. `GroupCoordinatorService`'s stable-read write
barrier handles ordinary pending log writes, but does not remove this separate
transaction-specific check.

## Dart policy integration

Run `python3 tool/run_transaction_offset_integration.py -- <dart>
--disable-dart-dev --packages=.dart_tool/package_config.json
.dart_tool/pub/bin/test/test.dart-<version>.snapshot
test/group_stable_offsets_transaction_integration_test.dart --reporter=expanded
--concurrency=1` with verified `KAFKA_HOME`, `JAVA_HOME`, and optionally
`KAFKA_TRANSACTION_RUN_ROOT`. No implicit Dart resolver invocation is accepted. This
wrapper enforces 1 GiB available memory / 2 GiB free disk floors before and
during its owned broker session and a 768 MiB aggregate owned-Dart RSS cap,
retains resource samples and failed output, independently decodes raw wire
receipts, checks twelve passed/zero skipped in the test reporter, verifies
owned process exit and port reuse 19192–19196, and verifies the full
12-case matrix before producing successful evidence.

The Dart test uses a fixture-only TLS-terminating relay with the same ephemeral
CA-signed localhost certificate. TLS hostname and CA verification remain active
on both hops. The relay rewrites only FindCoordinator v2's returned endpoint so
Dart OffsetFetch traverses it; all OffsetFetch bytes are forwarded unchanged.
The Java helper connects directly to the broker, never to this relay. Therefore
relay wire observations belong to the policy-enabled Dart operation, not the
precondition oracle. Authentication frames are not logged or retained.

Commit/abort release is gated on the **second distinct** Dart OffsetFetch v7
response containing broker error 88, proving that a retry occurred while the
transaction remained open. The wrapper retains request/response hex, decoded
errors/offsets, correlation/version/RequireStable and computed SHA256 hashes.
Other cases leave the transaction unresolved until attempts/deadline exhaust,
or cancel/close the operation and verify no additional reads for more than two
retry-delay intervals. All cases check cleared client resource counters.

This controlled routing fixture is not transparent network-failover evidence,
full Java Admin API parity, or a security certification. Synthetic metadata
visibility says nothing about end-to-end application record processing gaps.
