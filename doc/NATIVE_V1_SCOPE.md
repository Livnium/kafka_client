# Native v1 scope and release gates

This is a proposed boundary for a narrowly scoped native v1, not a declaration
that v1 is ready or a new maintainer support commitment. Published `kafka_client`
0.1.1 is experimental. The qualified source contains later native hardening;
the retained pubspec version does not publish those changes.

## Boundary to approve before selecting a v1 version

- Native Dart VM networking on an explicitly selected OS/architecture and SDK
  matrix. Current candidate evidence is Linux x64; other deployment targets
  need their own qualification if promised.
- Exact broker versions and executed coverage, rather than all Kafka 4.x.
  [The recorded six-version matrix](BROKER_QUALIFICATION.md) contains 72
  single-node core/secure Dart cases per exact version on Dart 3.12.0 for
  `6f828d3`. Later `0d0c800` has only the separate 4.0.0/4.4.0 representative
  checks recorded there. Neither result extends every operational or failure-mode
  claim to those versions or to newer source.
- Plaintext or strictly verified server TLS, optional SASL PLAIN over TLS,
  uncompressed magic-2 records, read-uncommitted Fetch, explicit partitions and
  offsets, manual broker checkpoints and the documented bounded classic-group
  APIs. Public API compatibility, defaults and experimental group status need
  an explicit maintainer decision before a stable version promise.
- Preserve no automatic Produce/commit replay, visible partial/unknown commit
  outcomes, fail-closed authentication/trust handling and application-owned
  external-effect safety. See [the reliability contract](RELIABILITY_CONTRACT.md).
- Preserve current phase deadlines and documented shutdown semantics unless a
  different operation-wide cancellation/deadline contract is deliberately
  implemented and tested. A callback that never settles can keep managed
  shutdown pending; `Future.timeout` does not cancel its work.

## Concrete gates before a dependable native v1 claim

- [ ] Approve the boundary above, supported targets, exact broker scope, public
  API/default policy and whether optional group APIs remain experimental.
- [ ] Freeze the release candidate and retain its exact source identity,
  dependencies, SDK/runtime identities and verified broker distributions.
  Publishable descriptions must distinguish historical and final-source tests.
- [ ] Run formatting, analyzer, full Dart regressions and fixture-runner Python
  regressions on the selected SDKs. The pre-response-hardening `6f828d3`
  library/test snapshot passed 1,058 tests with 360 external-fixture skips on
  each of 3.2.6 and 3.12.0;
  skipped cases need their named fixtures, not a blanket pass label.
- [ ] Refresh final-source plaintext and secure Java interoperability plus
  the selected operational, subscription, managed-group, stable-offset and
  transaction-boundary fixtures. Rerun replicated leader/coordinator loss,
  restart, proxy-fault overlap, cancellation and repeated cleanup where those
  behaviors are in the support promise. A six-version smoke matrix is not this
  complete gate. Document exclusions instead of extrapolating.
- [ ] Agree and execute repeated, varied sustained workloads with explicit
  payload/offset correctness, bounded counters, memory/disk observations and
  owned cleanup criteria. Keep the historical minimum-SDK RSS failure and
  subsequent controlled passes visible. One 30-minute pass is not a leak or
  capacity guarantee; see [operational readiness](V1_READINESS.md).
- [ ] Qualify independent-host failure domains and realistic constrained
  network/resource conditions before making broad deployment/failover claims.
  If the release remains limited to local-test evidence, state that limit and
  do not describe it as general production qualification.
- [ ] Refresh native executable/secure-example checks for the final source on
  every promised target and runtime form. Qualify actual packaged Flutter apps
  and OS lifecycle/trust behavior only if those are in the release promise.
  A Linux headless engine smoke does not qualify mobile or desktop apps.
- [ ] Put the selected fixtures and result/cleanup checks in the maintainer's
  actual CI and establish how required gates are enforced. An
  [ordinary native CI baseline](NATIVE_CI.md) is defined for the two tested SDKs;
  it discloses fixture skips and does not run real brokers. Remote workflow
  success, CI account setup and required-check enforcement are not established
  by adding the workflow files.
- [ ] Complete the [next-release payload checklist](RELEASING.md), review
  documentation and license/notices, choose the next version, then obtain
  explicit authorization for external validation and publication.

## Deferred features, not mandatory additions for the boundary above

Browser transport, Flutter Web and a WebSocket gateway are deferred. No web
prototype results count as native qualification or permission to ship web APIs.

Compression, SCRAM/OAuth/Kerberos/mTLS/reauthentication, idempotent production,
transactions/exactly-once workflows, read-committed consumption, automatic
out-of-range reset, automatic partition discovery, KIP-848 membership,
general automatic group recovery and application-effect fencing are not provided.
They become release blockers only if deliberately added to the approved scope.
Their absence must remain clear to users.

Explicit [ListOffsets](LIST_OFFSETS.md) and opt-in managed
[missing-position resolution](GROUP_MISSING_OFFSETS.md) are implemented in source.
Neither changes default fallback behavior or establishes new real-broker qualification.

Strict-default TLS termination during some failover sequences is a documented
availability limit, not a claim that transparent failover passed. Default-off
bootstrap/Fetch/coordinator recovery policies have narrower, separate contracts.
Maintainers may keep these policies explicit rather than broaden defaults;
test the selected policy and report terminal versus recovered results separately.

## Planning and uncertainty

The evidence does not establish a defensible v1 completion date. The recorded
pre-response-hardening Dart suites each took about 40–43 seconds, excluding
environment setup and all skipped external fixtures. That timing cannot estimate the remaining integration,
CI, deployment, sustained-observation or failure-diagnosis work. Define the scope
and acceptance workloads first, then estimate engineering effort separately from
elapsed observation time. Passing a rerun does not explain a historical failure.
