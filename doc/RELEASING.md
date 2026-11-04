# Pre-publication checklist

This checklist is preparation, **not permission to publish**.

## Author decisions required

1. The author selected `kafka_client` for any future release. Recheck availability
   immediately before an authorized publication; no name has been reserved.
2. Apache License 2.0 is selected for the original client code, with Livnium
   designated as copyright owner and original authorship confirmed. Preserve
   third-party ownership and notices in `../THIRD_PARTY_NOTICES.md`; this choice
   does not relicense upstream material or clear every historical generated asset.
3. Supply an actual authorized repository URL and optional issue tracker/homepage.
   No placeholder URLs should appear in package metadata.
4. The selected first release version is experimental `0.1.0`. The original
   imported `1.0.0` value was not a published release or stability certification.
   Keep the documented experimental scope and verify the exact approved version.
5. Choose the supported OS/architecture/runtime matrix and platform metadata.
   Static native/Flutter tags do not establish execution on those targets; see
   [RUNTIME_COMPATIBILITY.md](RUNTIME_COMPATIBILITY.md).

## Checks after those decisions

- Rename pubspec/imports/examples consistently if approved; rerun every check.
- Verify the approved root `LICENSE`, `NOTICE` and third-party license files are
  present in the exact release payload. Review any newly incorporated code and
  keep historical generated docs excluded until separately audited.
- Read README as a new user: honest scope, setup, example, failure semantics,
  tested compatibility, limitations, and issue-reporting destination.
- Run `dart format --output=none --set-exit-if-changed lib test example`.
- Run `dart analyze --fatal-infos` and `dart test` on minimum SDK and current stable SDK.
- Run plaintext, secure, operational and replicated-cluster fixtures with verified
  Kafka 4.0.2 on both SDKs; never use a live broker. Verify resource counters,
  owned-process cleanup, secure-fixture secret cleanup and failure diagnostics.
  Include subscription ordering, pause/backpressure, cancel/close, secured
  failures and broker-restart tests. Review [V1_READINESS.md](V1_READINESS.md)
  and the current verification receipt.
- Run `dart doc --validate-links`; require zero warnings/errors in its output,
  not only exit zero. This tool can return success while reporting broken links.
- Compile and run the secure example on each intended native target, including
  trust/hostname/authentication rejection. Repeat the full operational suite for
  any runtime form the release promises, rather than extrapolating from a smoke.
- Obtain approval for external package validation before rerunning the previously
  blocked dry-run. Then run `dart pub publish --dry-run` and review **every published path**. Ensure no
  credentials, private source-transfer notes, IDE state, logs, broker data, old
  generated docs, binaries, lockfiles, or unrelated artifacts are included.
- Run package scoring tools only on an isolated copy; their local results are
  diagnostics, not a promise of pub.dev points or a production endorsement.
- Obtain explicit approval for publication, publisher identity and final version.
  Never substitute an actual publish for dry-run verification.

## Intended support contract

Native Dart VM TCP; Kafka 4.0.2 tested; no browser transport. Plaintext or validated
TLS, optional SASL PLAIN over TLS, uncompressed magic-2 records, manual partitions
and offsets, read-uncommitted. Separate opt-in classic-group sessions provide
eager range assignment, heartbeats, local processing acknowledgements and explicit
member commits. The serial managed runner adds fresh position reads and narrowly
bounded replacement after eligible heartbeat failures, with separate coordinator-loss,
stable-read and unstable-offset retry opt-ins. These do not fence external effects
or resolve unknown commits; see the [reliability contract](RELIABILITY_CONTRACT.md).
No SCRAM/OAuth/Kerberos/mTLS/reauthentication, compression, idempotence, transactional
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
