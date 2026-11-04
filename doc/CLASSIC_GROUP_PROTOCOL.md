# Classic-group protocol primitives

The advanced `kafka_protocol.dart` import provides dynamic-member classic-group
requests and strict consumer metadata codecs. These are tools for implementing
and testing a group controller. They are **not** a managed consumer, a live
membership lease, automatic assignment, heartbeat scheduling or safe rebalancing.
The existing `KafkaClient.subscribe()` remains manually assigned. Its delivery
cursor is not an application-processing acknowledgement.

## Available protocol surface

| Primitive | API key | Fixed version |
| --- | ---: | ---: |
| `JoinGroupRequest` | 11 | 4 |
| `SyncGroupRequest` | 14 | 2 |
| `HeartbeatRequest` | 12 | 2 |
| `LeaveGroupRequest` | 13 | 2 |
| `MemberOffsetCommitRequest` | 8 | 6 |

These use non-flexible request header v1 and response header v0, verified against
the installed official Kafka 4.0.2 schemas. They intentionally omit static member
identity. There is no cooperative assignment algorithm, newer consumer-group
protocol, or implicit downgrade to another version or to manual commits.

`ConsumerGroupSubscription` and `ConsumerGroupAssignment` encode and decode their
own version-prefixed payloads for metadata versions 0 through 3. This metadata
version is separate from the outer request version. Nullable user data stays
nullable, rather than becoming empty bytes. Subscription versions add owned
partitions (v1), generation (v2) and nullable rack (v3); accepting those fields
does not implement cooperative ownership, rack-aware assignment or fencing.
Assignment versions 0–3 share the same topic/partition and user-data body.

Inputs and nested collections are copied and immutable. Unsupported versions,
invalid UTF-8, malformed lengths/counts, duplicate identities, invalid partition
IDs, unexpected nulls and trailing data are rejected. Encoding an older metadata
version cannot silently discard supplied newer fields. Empty assignments are
valid; an empty assignment is not ownership of every partition.

## The caller owns routing and lifecycle

`KafkaClient.send()` sends one explicitly supplied request to the selected broker.
It does not discover a coordinator, negotiate a version, retry a rejected request,
join a group, interpret a rebalance or run a heartbeat loop automatically.

A classic controller must separately:

1. Discover the coordinator with `findGroupCoordinator(groupId)`, check the
   selected versions using ApiVersions, and route requests to that coordinator.
2. Join with an empty dynamic member ID. A MEMBER_ID_REQUIRED response (79)
   supplies an ID to use in a new Join request. This first response does not
   establish active membership or ownership.
3. Preserve the successful generation, actual member ID, leader ID and selected
   protocol. If leader, decode member subscriptions and implement the advertised
   assignment protocol correctly. Do not advertise cooperative-sticky unless
   implementing its ownership-transfer algorithm.
4. Sync with the leader's assignments or an empty assignment list for a follower.
   Validate the returned assignment before starting any assigned work.
5. Maintain heartbeats and processing-progress rules, handle membership and
   coordinator loss, and implement eager revocation before rejoining.
6. Explicitly leave when appropriate, treating an unreceived response as an
   uncertain outcome rather than confirmed departure.

Successful Join is not successful Sync. Successful Sync does not promise future
membership. REBALANCE_IN_PROGRESS (27), ILLEGAL_GENERATION (22) and
UNKNOWN_MEMBER_ID (25) need deliberate state transitions; they are not reasons
to replay old work or silently fall back to unfenced manual commits.

The broker carries classic assignment bytes. It does not compute or validate the
client's range assignment on its behalf. Consumer metadata support and a protocol
name alone are not an assignment implementation.

### Deadlines and admission

Join and Sync may wait for other members during a rebalance. The client's existing
request timeout begins at admission and defaults to ten seconds. An advanced
caller must choose a deadline appropriate for its configured rebalance timeout,
including setup/admission margin. The verification fixtures use a dedicated client
with a longer timeout; this does not change existing defaults or create an overall
lifecycle deadline.

Timeout or cancellation can disconnect the associated shared socket and fail
other requests on it. A heartbeat can also fail admission under existing resource
limits. A future managed controller must account for these effects, rather than
assuming a timer always sends on time or that cancellation is isolated.

Closing local sockets does not prove a broker-side member left. If a Join response
is lost, the broker may have a member whose assigned ID was never received.
There is no automatic lifecycle recovery or claimed cleanup of that unknown ID.

## Generation-bound offset requests

`MemberOffsetCommitRequest(groupId, topic, nextOffsets,
generationId: generation, memberId: member, metadata: metadata)` is an advanced
single-use OffsetCommit request. It requires a nonnegative INT32 generation and
nonempty member ID. Its response preserves per-partition error codes for the
caller to inspect, including partial successes. The member request validates exact
requested topic/partition coverage and rejects missing, duplicate or unexpected
entries rather than treating an empty response as success. The raw request does
not translate those codes into the high-level manual commit exception.

A valid member/generation tuple is **not proof of partition ownership**. The
broker's generation validation does not enforce the application's assignments,
make progress monotonic or make processing effects atomic. Commit only completed
application work, with deliberate handling of partial and unknown outcomes.

Membership-changing requests and commits are never automatically replayed by
`send()`. A timeout, disconnection, cancellation or malformed response can leave
an unknown outcome. A rejection response is distinct from a response that was
never reliably received. Each request object remains single-use.

The original `GroupOffsetCommitRequest` and `KafkaClient.commitOffsets()` remain
manual, generation -1 with an empty member ID, and unchanged defaults. Do not use
manual commits to bypass a managed-group rejection or modify an active group's
progress. `KafkaClient.subscribe()` never restores or commits offsets implicitly.

## Separate single-generation membership session

`kafka_group.dart` now supplies a separate experimental session over these
primitives. It performs one Join/Sync, maintains independent heartbeats and
revokes local assignment validity permanently on loss, without automatic rejoin.
Read [GROUP_SESSION.md](GROUP_SESSION.md) for the exact contract and limitations.
The raw primitives and existing manually assigned subscription are unchanged.

## Work still needed for a managed record consumer

The membership session now owns joining, syncing, stable, revoking, leaving and
closed states, with a local assignment epoch separate from the Kafka generation.
A future record controller must also tag pending Fetches, buffered records and
commits with that epoch. On
revocation or loss, new delivery/commit admission must stop and old response
epochs must be invalidated before rejoining. Already delivered asynchronous
application work requires an explicit bounded completion policy.

Heartbeats must remain independent of paused delivery and asynchronous processing,
with a separate limit for stalled processing and a policy for control-plane
capacity. Processing acknowledgements must be separate from delivered offsets.
These ownership and revocation guarantees are not supplied by wrapping the
existing manually assigned stream in Join and Heartbeat calls.

## Verification scope

The source-specific commands and results are recorded under
`verification/group_membership/`. Offline official Java codec fixtures verify
selected wire layouts and metadata versions. Dedicated owned-broker tests cover
explicit lifecycle operations, generation changes/rejections and Java classic
RangeAssignor coexistence. They exercise controlled low-level sequences, not a
production heartbeat scheduler or automatic rebalance controller.

Prior features remain subject to their own retained regression gates. No new
claim is made for other Kafka releases, independent-host coordinator failover,
other OS/architectures, Flutter embedding, long-duration coordinated consumption
or complete production readiness. Nothing is published by this increment.

Later [headless Flutter engine evidence](RUNTIME_COMPATIBILITY.md) covers nine
specified cases on the newer checkpoint. It does not expand this historical
checkpoint's tests or establish packaged-app or real OS lifecycle support.
