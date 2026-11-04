## 0.1.1 (experimental)

- Replace the README title em dash with a hyphen. No client code changes.

## 0.1.0 (experimental)

First experimental release candidate. The development milestones below are all
included in this version; they are not separate published versions.

- Provide native Dart Kafka 4.0.2 produce/fetch, explicit subscriptions, strict
  TLS/SASL PLAIN, broker checkpoints and opt-in bounded classic-group processing.
- Preserve explicit delivery/commit uncertainty and the documented recovery
  limits. Packaged Android/iOS applications are not yet qualified.
- License original client code under Apache 2.0 with Livnium attribution and
  separate third-party notices.
- Package both Java transaction-probe sources so the helper no longer depends
  on excluded development evidence.
- Retain experimental status. Publication and any wider support claims require
  their own completed checks; this changelog is not proof of publication.

### reliability documentation and headless Flutter evidence

- Clarify commit uncertainty, independent stable-read/retry opt-ins and the
  in-memory previous-generation position floor without changing APIs or runtime.
- Correct Kafka 4.0.2 stable-response execution order and default versus opt-in
  read deadline/cancellation documentation.
- Record nine passing Linux x64 Flutter test-engine cases, with no packaged-app,
  mobile, real OS lifecycle or broader delivery-guarantee claim.

### bounded unstable-offset read retries

- Add separately opt-in `KafkaStableOffsetRetry` for stable committed-position
  reads and managed consumers, with a total attempt budget, monotonic operation
  timeout and close/cancellation-aware backoff.
- Retry explicit unstable-offset error 88 only on enabled read-only OffsetFetch;
  preserve strict v7/no downgrade, terminal error precedence, genuine-null
  semantics, assignment revocation, position floors and write non-replay.
- Scope connections to the operation within existing aggregate resource budgets.
  Default v5 and stable reads without the policy retain their previous behavior.
- Rearm startup and membership-expiry watchdogs after early timer wakeups against
  their unchanged absolute targets; stale callbacks cannot replace new deadlines.

### explicit stable committed-position reads

- Add default-off `requireStable` to committed-offset reads and independent
  `requireStableOffsets` to managed consumers, selecting exact OffsetFetch v7
  with RequireStable=true while leaving default v5 requests unchanged.
- Retain strict TLS, capability checks, finite read budgets, revocation/cleanup,
  previous-position guards and terminal ambiguous-commit handling. No silent
  version fallback, retry of null, offset substitution or commit replay is added.
- Validate flexible headers, compact fields and tagged fields with independent
  official Kafka codec fixtures. Stable reads can wait or fail; genuine absence
  remains possible. No broker durability or exactly-once guarantee is introduced.

### opt-in managed coordinator-loss replacement

- Add a default-off consumer policy for fresh-generation replacement after a
  proven stable-heartbeat coordinator rejection or selected transport failure.
- Reuse the finite lifetime rejoin budget and preserve all callback, commit,
  data, position and control drain requirements. Unknown commits remain terminal.
- With the option enabled, reject missing/lower replacement positions behind
  prior broker-observed or confirmed progress instead of silently using fallback;
  local evidence is a comparison only, never a substituted starting offset.
- Keep strict TLS, group discovery retry/fallback and default rejoin behavior
  unchanged. No in-place membership or commit replay is introduced.
- Correct native assignment interpretation of Metadata-v0 partition warning9
  only with a unique valid leader endpoint, preserving the full partition list
  and all other errors/checks. This also affects single-generation startup.

### explicit managed-group Fetch TLS recovery

- Expose a separate nullable Fetch recovery policy on managed sessions and consumers,
  requiring configured TLS and explicit bootstrap fallback. Strict defaults stay unchanged.
- Apply the existing bounded policy only to each generation's dedicated data client;
  position lookup, control traffic and membership/commit replay rules do not change.
- Preserve local revocation, callback draining and generation boundaries. Recovery
  cannot extend membership authority or provide a hard overall cancellation deadline.

### bounded serial group consumer

- Add an opt-in serial callback runner that owns the returned application Future,
  reloads exact assigned committed positions and uses only explicit missing-offset
  fallbacks. Commit-after-processing is a required explicit true/false choice.
- Permit finite automatic rejoin only after processing-stage heartbeat rebalance
  rejection and successful application/commit/data/position/control draining.
- Keep unknown/rejected commits, startup/read/processing/cleanup failures terminal;
  stop suppresses later callbacks/joins without pretending to cancel application work.
- Preserve existing single-generation and manual APIs. No implicit offset reset,
  general failover, detached-work fencing or exactly-once guarantee is implied.

### bounded single-generation application processing

- Add opt-in identity-bound acknowledgements and per-partition acknowledged-prefix
  tracking, separate from delivery and broker-confirmed offsets.
- Bound pending work including out-of-order completions, with an oldest-work
  monotonic processing deadline independent of heartbeats.
- Add explicit single-topic generation/member commits, serialized snapshots,
  per-partition confirmed/rejected/unknown evidence and no automatic replay.
- Cancel pending commits on revocation, preserve late evidence without reviving
  authority, suppress Leave during pending/failed commits, and settle owned work.
- Preserve raw/manual defaults. No automatic positions/rejoin, transactions,
  external-effect fencing, whole-heap cap or production certification is implied.

### bounded single-generation group delivery

- Add one optional assignment-tagged delivery channel per managed session, using
  a separately owned data client with the same frozen broker list and TLS/SASL configuration.
- Check local token/expiry synchronously at each original listener admission;
  clear paused buffers and discard late Fetch results on revocation.
- Separate the data stopped barrier from paused stream done and membership cleanup;
  preserve cleanup errors and settle admitted work rather than abandon it.
- Validate exact explicit offsets, whole responses, record counts and payload
  bounds before delivery. Preserve manual subscriptions, commits and transport.
- No automatic rejoin, processing acknowledgements, managed commits, hard overall
  deadline, external-effect fencing or production certification is implied.

### monotonic TLS recovery backoff

- A retained minimum-SDK gate exposed a pre-existing early-timer edge: rounding a
  clipped retry wait down could admit another metadata round before the intended
  backoff target. Keep a fixed monotonic retry target and recheck after each wake.
- Preserve zero-delay yielding, close-aware wait ownership, strict TLS validation,
  total round/window limits, default-off recovery and Produce single dispatch.

### single-generation managed membership

- Add a separate experimental group session with owned control traffic, eager
  dynamic range assignment, independent heartbeats and conservative local expiry.
- Invalidate exact immutable assignment tokens synchronously on close, rebalance,
  loss or uncertain outcomes; never revive them from late responses or rejoin.
- Keep explicit Leave outcomes separate from local transport cleanup.
- Preserve existing clients, streams and manual commits. No record delivery,
  processing acknowledgements, commits, async-effect fencing or production-ready
  coordinated consumer is claimed.

### classic-group protocol primitives

- Add dynamic-member JoinGroup v4, SyncGroup/Heartbeat/LeaveGroup v2 to the
  advanced protocol surface, without an automatic membership controller.
- Encode/decode immutable consumer subscription and assignment metadata v0–3.
- Add a separate generation-bound member OffsetCommit v6 request with exact
  response coverage checks; preserve manual commit bytes and high-level defaults.
- Reject inconsistent successful Join identities and retain single-dispatch,
  unknown-outcome behavior. No high-level group subscription is implied.

### explicit group offsets

- Add coordinator discovery and explicit broker-stored offset reads/commits for
  manually assigned partitions, with fixed protocol versions checked against
  each connection's advertised support.
- Validate/snapshot inputs before discovery, preserve partial broker responses,
  and never automatically replay a commit with an uncertain outcome.
- Keep existing subscription and security defaults. Group membership, assignment,
  rebalancing, fencing and automatic commits remain separate missing layers.
- Harden local verification fixtures with bounded confirmed-exit RSS handling
  and read-only topic-serving checks before a restarted broker is called ready.

### subscription reliability gates

- Add a guarded, paced subscription endurance test with verified application
  checkpoints, repeated cancel/recreate cycles and explicit duration/restart gates.
- Add secured three-node leader-crash/opaque-network-fault overlap coverage,
  distinguishing strict-default termination from explicitly configured recovery.
- Preserve runtime code, security defaults, Produce non-replay and existing features.
  Document sampled resource limits, owned cleanup and remaining evidence gaps.

### explicit topic subscriptions

- Add lazy, single-listener `KafkaClient.subscribe` streams and `KafkaTopicRecord`
  events for a fixed partition-to-inclusive-offset map, with one Fetch in flight,
  per-partition ordering, pause/backpressure and application-supplied checkpoints.
- Advance cursors using validated batch/record progress, including filtered
  control batches; never infer progress from an empty poll's high watermark.
- Retry only read-safe transient failures while active. Preserve terminal
  TLS/authentication/protocol/resource/out-of-range failures and existing Fetch
  policies, strict TLS defaults, and Produce/low-level non-replay behavior.
- Cancel one stream without closing its shared client, awaiting admitted Fetch
  work under existing per-phase deadlines. Client close stops all subscriptions.
- Document read-uncommitted delivery without consumer groups, commits,
  ListOffsets, automatic partition discovery or processing acknowledgements. Add
  a runnable example starting at an acknowledged Produce offset, not assumed zero.

### high-level argument preflight

- Reject invalid Fetch/Produce scalar bounds before metadata or transport work,
  using Future-delivered errors and preserving prior closed-client precedence.
- Keep valid INT32/INT64 boundaries, acknowledgments, retry policy, strict TLS
  defaults and one Produce dispatch unchanged.
- Clarify the secure example's retained broad `verification` category without
  changing output labels, and identify source/AOT smoke as historical evidence.

### removable ordinary retry waits

- Remove completed ordinary retry delays from client-owned wait bookkeeping,
  instead of retaining a losing close-Future listener until client close.
- Reuse the existing removable waiter group and cancel pending retry timers on
  close. Preserve retry counts, eligibility, jitter limits and single Produce
  dispatch; no new public policy or high-level cancellation API.
- Add direct SDK-private listener-count reproduction and loopback regressions.
  Listener accumulation is demonstrated; retained payloads or an RSS leak are not.

### bounded Fetch TLS recovery candidate

- Add a separate default-disabled Fetch policy with one total round budget,
  operation-local exclusions and a monotonic phase-admission window.
- Preserve strict TLS on replacement leaders, terminal security/protocol/resource
  errors, and unchanged Produce and low-level single-send semantics.
- Keep bootstrap-only stale-leader failures and bounded candidate failures visible;
  single-call actual-broker recovery and synthetic regression evidence are separate.
- The window does not cancel an admitted connection/authentication/request phase.
  No hard overall deadline or production adoption is implied.

### opt-in TLS bootstrap candidate

- Add default-off metadata-only bootstrap traversal after a scoped TLS handshake
  failure, with independent strict validation, deduplication, observable advance
  count and terminal fail-closed exhaustion. No TLS downgrade or Produce replay.
- Add trust, hostname, expiry, authentication, protocol, resource and bounded
  traversal regressions using disposable local TLS peers.
- Keep immediate-Fetch failure visible. Separate application-coordinated
  metadata-converged recovery from any transparent-failover claim.
- This is an isolated experimental candidate, not a publication or default-policy
  change. Prior runtime and operational receipts apply to their own snapshots.

### combined secured-fault diagnosis

- Add an owned three-node secured cluster with bounded opaque client proxies,
  causal blackhole/leader-crash checks, fixed guards and verified private cleanup.
- Preserve the failing recovery gate: a still-listening dead-upstream proxy can
  surface a non-retryable TLS handshake error and block healthy bootstrap fallback.
- Document the policy decision needed; do not relabel generic TLS failures as
  retryable transport errors or change client library executable behavior.

### runtime scope and secure example

- Make standalone Linux x64 evidence distinct from Flutter, other architectures
  and static platform tags. Keep the exact tested Kafka release explicit.
- Add a runnable secure-only example with validated runtime settings, one Produce,
  exact-offset Fetch checks, awaited close and sanitized error categories.
- Add unit coverage and an owned-fixture source/native example checker for TLS,
  SASL-over-TLS, untrusted CA, hostname mismatch and invalid credentials.
- Correct stale Broker API documentation that denied implemented TLS/SASL.
  No client library executable behavior changes in this checkpoint.

### minimum-SDK observation and API contracts

- Extend the framework-free diagnostic to a bounded 30-minute duration without
  changing its sampled RSS ceiling; test the accepted duration boundaries.
- Correct resource-limit documentation: a response-side limit can fail after
  Produce transmission, so the exception does not imply safe replay. Add a
  fake-peer regression proving that failure remains non-replayed.
- Clarify raw metadata error handling and add a no-retry regression. Document
  high-level acknowledgement/wait/deadline parameters, manual offset ownership,
  terminal cancellation on close, and the fixed SASL wire versions.
- Client library executable behavior is unchanged; library edits are comments.

### memory isolation controls

- Add a framework-free idle/codec/live memory probe with fixed sampled RSS guard,
  exact record validation, bounded bookkeeping and six focused regressions.
- Compare source versus existing cached test runners, fresh test-cache copies,
  and standalone clients under unchanged safety limits. Retain the earlier
  minimum-SDK resource failure; do not infer a client leak or make an unsupported
  runtime fix from whole-process RSS.

### encrypted faults and extended observations

- Repeat transport fault cases through a bounded opaque TLS proxy for TLS-only
  and SASL-over-TLS clients, with strict reconnect authentication and stored-record
  proof that ambiguous Produce acknowledgements are not silently replayed.
- Add minute-by-minute sustained-run observations and opt-in RSS/resource guards;
  guard failures are visible and trigger owned-fixture cleanup.
- Correct the plaintext fault harness's two-thread admission boundary and add a
  shared regression. No client runtime changes in this checkpoint.

### secured recovery and network fault evidence

- Add real three-node TLS and SASL-over-TLS failover tests with two connection
  slots, authenticated reconnection, marker/offset checks and ISR restoration.
- Add an owned loopback fault proxy for lost Produce acknowledgements,
  bidirectional blackholes, connection cuts, and delayed/fragmented responses.
- Prove one stored marker and no implicit replay after an actual lost Produce ack.
- Add private-material cleanup and failure-reporting regression for secured
  fixtures. No client runtime changes or publication in this checkpoint.

### operational recovery hardening

- Reuse bounded connection capacity by evicting only idle established endpoints.
  Active requests, authentication and version negotiation remain protected.
- Bind checked sends to the physical connection whose API versions were validated;
  never silently reconnect between negotiation and dispatch.
- Route Metadata v0 partition error 9 only when a usable leader is advertised,
  preserving raw diagnostics. Retry safe-read replica errors without write replay.
- Make overlapping `close()` calls await the same connection teardown.
- Correct admission accounting for canceled dispatches and independent pending
  response waits; each request retains exactly one slot until both settle.
- Add disposable process-crash/restart, long-poll cancellation/deadline, repeated
  close, and configurable concurrent produce/fetch soak integration fixtures.
- Add actual replicated three-node leader-change and bootstrap-fallback coverage.
  Verification results and remaining release gates are recorded separately.

### TLS/authentication and reliability

- Add strict server/hostname-validated TLS with system roots or custom CA bytes.
- Add SASL PLAIN over TLS only, sanitized failures, no logging or weaker fallback.
- Bound in-flight work, connections, queued wire bytes and response buffering.
- Add cancellation that retires assigned sockets; release payloads and prevent
  abandoned writes from bypassing admission budgets.
- Retry only read-safe metadata/fetch transport/leader failures with bounded jitter;
  never replay an ambiguous Produce. Add typed failure categories.
- Test actual secured Kafka with ephemeral certificates/credentials and Java
  interoperability; preserve the foundation milestone receipt separately.

### Kafka 4 foundation

- Preserve original package/API intent; remove obsolete commented duplicates.
- Correct object-array encoding, request framing, strict binary bounds and CRC32C.
- Implement uncompressed magic-2 Produce v3 / Fetch v4, nullable payloads and headers.
- Add broker-version checks, leader routing, bootstrap fallback and explicit errors.
- Bound requests; fail pending work on disconnect/close; handle acks=0 without hang.
- Add codec, malformed-input, transport, version-negotiation and routing regressions.
- Verify Kafka 4.0.2 roundtrip plus bidirectional official Java-client interoperability.
- Replace template README/example; retain legacy APIs as explicitly unverified.
- Development dependency lock refreshed for the tested Dart SDK; runtime has no
  third-party dependencies. No package publication or production-readiness claim.

### Original import (unpublished template version 1.0.0)

Original author implementation (imported without original Git history).
