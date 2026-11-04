# Explicit managed-group Fetch TLS recovery

Managed sessions and serial consumers can explicitly select the existing bounded
high-level Fetch TLS recovery policy for their dedicated data clients. The policy
is nullable and disabled by default. It does not change membership operations,
position lookup, automatic rejoin eligibility or any write-replay rule.

## Configuration

Both `KafkaGroupSession` and `KafkaGroupConsumer` accept these options:

```dart
tls: KafkaTlsConfig(
  trustedCertificates: trustedCaBytes,
  withTrustedRoots: false,
),
allowTlsBootstrapFallback: true,
tlsFetchRecovery: KafkaTlsFetchRecovery(
  maxAttempts: 20,
  window: const Duration(seconds: 10),
  retryDelay: const Duration(milliseconds: 500),
),
```

These are explicit example budgets, not changed defaults or guaranteed election
bounds. The immutable policy validates its own bounds. Supplying it without TLS
or without explicit bootstrap fallback throws at construction before network
activity. Certificate-container parsing retains its existing lazy behavior; bad
CA data still fails closed when a connection is attempted. TLS trust, actual
hostname validation and SASL-over-TLS requirements are unchanged.

The same policy object is forwarded into each newly created generation's data
client. Control and committed-position clients do not receive the Fetch policy.
Raw delivery, explicit processing and serial managed consumers therefore use the
same bounded client implementation rather than adding nested retry loops.

## Authority and cleanup

A Fetch may try independently validated peers and refresh stale routing within
its finite attempt budget. An endpoint rejected by TLS is excluded for that
Fetch. No certificate bypass, hostname substitution, plaintext downgrade,
ambiguous Produce replay or membership/commit replay is introduced. The detailed
transport policy remains documented in [TLS_FETCH_RECOVERY.md](TLS_FETCH_RECOVERY.md).

Recovery is subordinate to the current local assignment. Revocation still stops
new original callbacks synchronously, clears buffered records, invalidates
processing receipts and closes the generation's data client. A late Fetch result
cannot restore the old token. Closing the data client interrupts pending recovery
waits and owned transport work; the delivery stopped barrier still waits for the
actual pump and close result rather than abandoning work with `Future.timeout`.

The policy window limits admission of new recovery phases, not a hard overall
cancellation deadline. Already admitted connection/authentication/request work
retains its existing deadlines. An application callback already invoked before
revocation is not cancelled or fenced. A managed consumer still drains its exact
returned Future and commit/data/position/control barriers before any permitted
replacement generation.

Only the existing processing-stage Heartbeat 27 path can trigger automatic
managed rejoin. Fetch exhaustion, authentication/protocol failures, uncertain
commits and other terminal membership failures do not become new rejoin reasons.
Fresh generations read fresh committed positions and receive independent data
clients and Fetch-local rejection state. No old Fetch cursor or callback is
replayed as a substitute for that transition.

## Evidence and limits

Validation is kept distinct between strict real-TLS synthetic routing/send-count
fixtures and owned real Kafka secured-fault fixtures. Synthetic responses do not
prove durable Kafka storage; real local fixtures do not establish independent-host
reliability, production readiness or arbitrary coordinator failover. The exact
executed cases, failed attempts and cleanup receipts are recorded separately in
`verification/group_fetch_recovery/RESULTS.md` when this checkpoint is sealed.

The original default combined-secure recovery failure remains preserved. An
explicit-policy pass does not convert that default-policy receipt into a pass.
No exactly-once processing, external-effect fencing, guaranteed immediate data
visibility, unbounded recovery or general automatic crash recovery is implied.
