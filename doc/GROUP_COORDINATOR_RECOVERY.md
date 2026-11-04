# Explicit managed coordinator-loss replacement

`KafkaGroupConsumer` can opt into a narrow whole-generation replacement policy:

```dart
rejoinOnCoordinatorLoss: true,
maxRejoins: 3,
rejoinDelay: const Duration(seconds: 1),
```

The policy is disabled by default. These are example settings, not a guaranteed
coordinator-election delay. It shares the existing lifetime rejoin allowance and
close-aware monotonic delay; it does not add an independent nested retry loop.
Raw sessions and explicit delivery/processing handles still end permanently when
their generation is revoked.

## Eligible evidence

The previous generation must have reached processing. The added eligibility is
limited to the actual stable heartbeat path, after its local deadline check:

- Broker error 15 (`COORDINATOR_NOT_AVAILABLE`) or 16 (`NOT_COORDINATOR`).
- A typed `KafkaConnectionException`, `SocketException` or `TimeoutException`.

The controller records immutable provenance for that original heartbeat failure.
The same-looking error from startup, initial heartbeat, application work, Fetch,
position lookup, commit or cleanup does not qualify. Existing heartbeat error 27
rebalance eligibility remains unchanged.

Explicit `HandshakeException`/`TlsException`, certificate/hostname rejection,
authentication rejection, malformed responses and resource failures do not qualify.
Some established transport errors already become `KafkaConnectionException` in
the shared transport. This policy cannot reconstruct their original TLS-layer
cause. When TLS is configured, every replacement connection still performs strict
certificate and hostname validation; there is no trust bypass or plaintext downgrade.

## Replacement and ownership

Revocation synchronously invalidates the old assignment and processing receipts,
stops new callbacks and closes owned data work. Recovery never revives that token
or extends the local membership deadline. All existing replacement gates remain:

- The exact returned application Future settles successfully.
- Every admitted commit settles with wholly confirmed evidence. Unknown, rejected,
  malformed or failed commits remain terminal even if heartbeat failure came first.
- Data/commit, position, listener and membership/control cleanup finish without a
  reported cleanup error. A cleanup timeout is not a successful drain.
- Explicit stop has not occurred, and the lifetime rejoin allowance remains.

After the backoff, a fresh client/session rediscovers the coordinator, sends a new
dynamic Join/Sync/confirming Heartbeat sequence, and reads assigned committed
positions afresh. It never replays the old Heartbeat, Join, Sync or commit request.
A failed or ambiguous replacement startup is terminal. The old member can remain
at Kafka until broker expiry, so a fresh Join may wait or fail within its normal
startup budget. Local cleanup does not prove broker departure.

Local expiry, a stalled or failed callback, Fetch/position failure and cleanup
failure remain terminal. A delayed heartbeat completion cannot change an already
expired generation into a recoverable one. Application Futures are drained, not
cancelled or externally fenced; detached work remains outside this ownership.

## Fresh-position consistency guard

With `rejoinOnCoordinatorLoss` enabled, every replacement, including an ordinary
heartbeat-27 replacement, also checks the fresh broker position against evidence
from the immediately preceding assignment. Its comparison floor is the maximum
of that cycle's non-null broker-observed starting position and all wholly
confirmed commits for the partition, including confirmations received while
revocation was draining. Explicit fallback positions, application work and local
acknowledgements are not committed evidence.

For a partition present in both assignments, a missing or lower fresh broker
position is terminal before fallback, Fetch or callbacks. A larger configured
fallback cannot override this rejection. Equal or higher fresh broker positions
remain the actual starting positions. The local floor is never substituted,
committed, or used to resolve an ambiguous commit; the read is not retried to get
a preferred answer. Default-off behavior is unchanged.

This floor is in memory within the same consumer object; it is not persisted
across restarts or inherited by a new consumer. A missing/lower halt detects a
conflict with tracked evidence, not permanent loss, a durable regression, a
specific cause or an ambiguous commit's outcome. See the
[reliability contract](RELIABILITY_CONTRACT.md).

History is limited to the previous assignment. A partition that leaves and later
returns has no retained floor. New partitions retain the normal explicit fallback
rules. Legitimate external resets or offset expiry can also make the check fail;
this policy stops rather than inferring reset intent. It is not a durable global
monotonicity or offset-durability guarantee.

Retained real-fault attempts observed missing positions immediately after
migration despite earlier confirmation/readback. An exact-sequence diagnostic
observed successful missing replies followed about one second later by the
original confirmed position, with no writes, rejoin or restoration. That proves
transient absence in that reproduction, not its underlying cause. This guard
limits the consequence for known previous positions; it does not fix Kafka's
offset visibility. A separate default-off `requireStableOffsets` option requests
[stable v7 position reads](GROUP_STABLE_OFFSETS.md); it neither replaces these
floor checks nor changes coordinator-loss eligibility. Default v5 has no
`RequireStable` flag.

## Metadata-v0 replica warning compatibility

Assignment metadata now treats partition error 9 (`REPLICA_NOT_AVAILABLE`) as a
narrow Metadata-v0 warning only when the partition still names a nonnegative
leader with exactly one valid returned broker endpoint. Topic errors remain
terminal. All requested topics and all partition IDs still undergo the existing
completeness, uniqueness and assignment validation; no errored partition is omitted.
Other partition errors and missing, duplicate or invalid leader endpoints remain
terminal. The raw metadata response is not changed.

Kafka Metadata v0 can report error 9 when an unavailable follower endpoint is
removed from replica/ISR arrays, even though the leader and partition IDs remain
usable. This separately documented compatibility correction applies to native
single-generation sessions as well as managed consumers. It changes that specific
startup interpretation, not default coordinator-loss eligibility. It introduces
no retry, membership replay or TLS policy change. Sync and the confirming Heartbeat
must still succeed before an assignment is published. A broker-reported snapshot
cannot independently prove the topic's complete real-world partition set, just
as with error-zero metadata.

See the accepted [Metadata v1 change in KIP-4](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/50860476/KIP-4+-+Command+line+and+centralized+administrative+operations)
for the v0/v1 distinction. The recorded real-fault evidence must state whether
the crashed broker stayed down through replacement or was explicitly restored.

## Strict discovery remains separate

This policy does not change `findGroupCoordinator` or committed-position reads.
Their existing bounded read retries apply to transport failures and broker errors
14/15/16. Explicit TLS and authentication rejection remain terminal, including
when `allowTlsBootstrapFallback` or the data-only `tlsFetchRecovery` option is set.
A dead first bootstrap behind a live TCP proxy can therefore still prevent the
fresh cycle from starting when the proxy closes during TLS negotiation.

The policy is not general coordinator failover or automatic recovery from every
broker crash. It adds no retry-until-success behavior, offset reset, Produce replay,
commit reconciliation, exactly-once guarantee or external-effect fencing.
Successfully processed but uncommitted records can be seen again from a fresh
committed offset, as with the existing heartbeat-rebalance replacement path.

## Verification scope

At source `a9c5d12`, four top-level real-broker tests passed across Kafka 4.0.2
and 4.4.0, containing eight TLS/SASL_SSL/PLAIN opt-in disabled/enabled scenarios.
Enabled recovery replaced one generation only after old callback drain and
preserved confirmed progress while the actual coordinator remained down.
Disabled recovery validated terminal failure. These local three-node/proxy
observations, the retained preliminary unknown-cause SIGKILL and cleanup limits
are recorded in [additional observations](QUALIFICATION_OBSERVATIONS.md).
They do not establish the complete matrix on later source or general recovery.

Older development receipts used `verification/coordinator_recovery/RESULTS.md`;
that historical path is not present in every checkout. The later completed
observations above retain their raw evidence separately. Synthetic strict-TLS
peer evidence concerns routing and send counts. Separately executed owned real
Kafka faults can establish observed membership and stored-offset behavior;
reports must distinguish passed and failed scenarios.
Neither is evidence of production certification, independent-host behavior,
arbitrary broker versions or untested outage patterns.
