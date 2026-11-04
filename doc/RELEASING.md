# Next native release checklist

This checklist is preparation, **not permission to publish**.

## Established release facts

1. `kafka_client` is the published package name. Experimental 0.1.1 was published
   on 2026-11-04; publication did not certify production readiness.
2. Apache License 2.0 is selected for the original client code, with Livnium
   designated as copyright owner and original authorship confirmed. Preserve
   third-party ownership and notices in `../THIRD_PARTY_NOTICES.md`; this choice
   does not relicense upstream material or clear every historical generated asset.
3. The repository and issue tracker are
   [Livnium/kafka_client](https://github.com/Livnium/kafka_client) and its
   [issues](https://github.com/Livnium/kafka_client/issues). Keep final metadata
   accurate; no placeholder URLs should appear.
4. Published 0.1.1 changed the README title, with no client code changes from
   0.1.0. Later source adds native request-snapshot/response-validation hardening
   and version-selectable fixture work, with separate source-specific
   qualification results. The approved next version is experimental 0.1.2;
   its version metadata is not evidence that pub.dev contains these changes.

## Decisions still required for the next release

- The selected next version is 0.1.2 and remains experimental. Publication is
  pending a successful final package dry-run and the authorized publishing flow.
  The original imported `1.0.0` value was never proof of a published or stable release.
- Select supported OS/architecture/runtime and exact broker coverage. Static
  native/Flutter tags and broad SDK constraints are not executed target tests.
  See [RUNTIME_COMPATIBILITY.md](RUNTIME_COMPATIBILITY.md) and
  [NATIVE_V1_SCOPE.md](NATIVE_V1_SCOPE.md).
- Decide the supported public API and default/recovery/cancellation promise.
  Do not infer feature expansion from a stable version number. Web remains deferred.

## Checks against the final candidate

These remain unchecked for a future release until verified against its exact
candidate; historical publication or qualification is not a substitute.

- [ ] Set the approved version and keep imports/examples/metadata consistent.
  Record the exact candidate and dependencies used by every gate.
- [ ] Verify the approved root `LICENSE`, `NOTICE` and third-party license files are
  present in the exact release payload. Review any newly incorporated code and
  keep historical generated docs excluded until separately audited.
- [ ] Read README as a new user: honest scope, setup, example, failure semantics,
  tested compatibility, limitations, and issue-reporting destination.
- [ ] Run `dart format --output=none --set-exit-if-changed lib test example`.
- [ ] Run `dart analyze --fatal-infos` and `dart test` on the chosen minimum and
  current SDKs. Identify external-fixture skips separately from executed tests.
- [ ] Run `python3 -m unittest discover -s tool -p 'test_*.py'`, including the
  fixture CLI/version-selection and cleanup regressions.
- [ ] Run the exact plaintext and secure matrix promised by the release using
  independently verified distributions. The recorded six-version matrix ran on
  Dart 3.12.0 only for `6f828d3`; the later `0d0c800` has representative checks
  on two versions only. Subsequent fault/endurance and ListOffsets/missing-policy
  observations remain scoped to their own source; see
  [additional observations](QUALIFICATION_OBSERVATIONS.md). See
  [BROKER_QUALIFICATION.md](BROKER_QUALIFICATION.md) for the
  eight-file secure selection and independent Java backcheck. Do not label the
  secure runner's default nine cases as the full 68-case selection.
- [ ] Refresh operational and replicated-cluster fixtures on the selected
  source/SDK/broker combinations; record unexecuted combinations rather than
  extrapolating. The historical broader fixtures used Kafka 4.0.2. Never use a
  live broker. Verify resource counters,
  owned-process cleanup, secure-fixture secret cleanup and failure diagnostics.
  Include subscription ordering, pause/backpressure, cancel/close, secured
  failures and broker-restart tests. Review [V1_READINESS.md](V1_READINESS.md)
  and the current verification receipt.
- [ ] Run `dart doc --validate-links`; require zero warnings/errors in its output,
  not only exit zero. This tool can return success while reporting broken links.
- [ ] Compile and run the secure example on each intended native target, including
  trust/hostname/authentication rejection. Repeat the full operational suite for
  any runtime form the release promises, rather than extrapolating from a smoke.
- [ ] Obtain any needed approval for external package validation, then run
  `dart pub publish --dry-run` and review **every published path**. A previously
  authorized publication does not authorize a new one. Ensure no
  credentials, private source-transfer notes, IDE state, logs, broker data, old
  generated docs, binaries, lockfiles, or unrelated artifacts are included.
- [ ] Run package scoring tools only on an isolated copy; their local results are
  diagnostics, not a promise of pub.dev points or a production endorsement.
- [ ] Obtain explicit approval for publication, publisher identity and final version.
  Never substitute an actual publish for dry-run verification.

## Intended support contract

Native Dart VM TCP; exact qualified broker coverage is recorded separately in
[BROKER_QUALIFICATION.md](BROKER_QUALIFICATION.md), without extending the full
historical fault/endurance suite to every version. No browser transport;
Flutter Web and web gateway work are deferred. Plaintext or validated
TLS, optional SASL PLAIN over TLS, uncompressed magic-2 records, manual partitions
and offsets, read-uncommitted. Separate opt-in classic-group sessions provide
eager range assignment, heartbeats, local processing acknowledgements and explicit
member commits. The serial managed runner adds fresh position reads and narrowly
bounded replacement after eligible heartbeat failures, with separate coordinator-loss,
stable-read and unstable-offset retry opt-ins. These do not fence external effects
or resolve unknown commits; see the [reliability contract](RELIABILITY_CONTRACT.md).
Optional source-level mTLS uses the [identity contract](MTLS.md) and requires
separate qualification; historical server-only TLS results do not cover it.
No SCRAM/OAuth/Kerberos/reauthentication, compression, idempotence, transactional
workflow or general group recovery is provided. Each Fetch has bounded read-safe
retries; explicit subscriptions repeat eligible transient reads while active,
without a total lifetime retry budget. No automatic Produce/commit replay or
production-capacity claim.

Development verification receipts are excluded by `.pubignore`. Before release,
check every shipped tool's source dependencies as well as documentation links.
In particular, `tool/transaction_offset_probe/run.sh` compiles its two sibling
Java sources, including the packaged `OffsetFetchWireOracle.java`; it must not
depend on the historical oracle under excluded `verification/`. Run the offline
`tool/test_transaction_offset_package.py` regressions and compile both sources
from the unpacked candidate with the required external Kafka 4.0.2 JARs/JDK.
This closes that source dependency only, not the full publication checklist or
the live-broker transaction validation gate.

## Maintainer references

- https://dart.dev/tools/pub/publishing
- https://dart.dev/tools/pub/package-layout
- https://dart.dev/tools/pub/writing-package-pages
- https://pub.dev/help/scoring
