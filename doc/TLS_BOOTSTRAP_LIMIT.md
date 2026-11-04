# Known TLS bootstrap recovery limit

This document retains the diagnostic findings from checkpoint `26ec5cd`. The
default policy is unchanged. A separate default-off candidate and its narrower
evidence are described in [TLS_BOOTSTRAP_FALLBACK.md](TLS_BOOTSTRAP_FALLBACK.md);
they do not turn the immediate-Fetch combined recovery gate into a pass.

The combined secured-cluster recovery gate is currently **failing**, not a
production-readiness pass. Its first real-broker observation exposed a boundary
in the client's existing fail-fast TLS policy. No runtime fix or weaker security
policy was applied in that diagnostic checkpoint.

## Observed sequence

The owned fixture places an opaque TCP proxy in front of each secured listener
of a three-node Kafka 4.0.2 cluster. All bootstrap and advertised leader endpoints
use those proxies. Client certificates/hostnames remain validated; SASL PLAIN,
when selected, runs only over verified TLS. Controller/inter-broker/admin traffic
is plaintext loopback fixture plumbing, not a secured deployment claim.

A warmed connection to a surviving broker has a pending long-poll. The fixture
blackholes all three client-facing proxies, then crashes a different partition
leader. It verifies the overlap, actual dropped encrypted bytes in both
directions, a bounded timeout and settled client counters. No Produce is issued
or replayed during that outage.

After traffic restoration, the crashed first bootstrap's proxy still accepts TCP
but closes because its upstream broker is unavailable. Dart reports a
`HandshakeException` for connection termination during TLS negotiation. The
client treats TLS failures as non-retryable and immediately stops bootstrap
selection, even though another configured broker remains healthy. Consequently
the intended same-client recovery while that first endpoint remains in this
state does not complete. Later marker writes/restart/ISR checks in the full gate
are not reached and must not be counted as passed.

This differs from a directly unavailable bootstrap that refuses the TCP
connection: a `SocketException` is eligible for bounded read-safe fallback. A
listening proxy changes the error category seen by the client. Same-host process
and proxy tests are not independent-host or kernel packet-loss validation.

## Why this is not an EOF-only retry patch

In the installed Dart 3.2.6 and 3.12.0 SDK sources, both premature EOF and generic
TLS filter errors can construct `HandshakeException` with a message and null
`osError`. There is no exposed typed reason code separating these paths. A null
OS error therefore does not establish that the problem is harmless transport
loss. Matching exception-message strings would couple behavior to implementation
text and would not create a stable security distinction.

The library still closes the failed raw socket and preserves the TLS exception.
It does not downgrade TLS, bypass hostname/trust validation, send credentials in
plaintext, reinterpret all TLS failures as network failures, or replay Produce.

## Decision required before closing this gate

Keep the present fail-fast behavior, with this availability limit documented,
or explicitly design a bounded fallback policy that tries another configured
bootstrap after a TLS failure. Such a policy could retain independent strict
validation on every endpoint, but would intentionally continue despite a TLS
failure at one endpoint. It is a public behavior/security-policy decision, not a
proven EOF-specific fix. It needs focused trust/hostname/authentication/fallback
regressions and visible diagnostic behavior before implementation is called safe.
No such policy change is part of this checkpoint.

Do not bypass certificate validation or blindly retry Produce as a workaround.
An ambiguous write can already be stored even when no success is reported.

## Reproducing the gate

Use only the disposable owned fixture, with an independently SHA512-verified
Kafka 4.0.2 distribution, Java 17+, OpenSSL, Python 3 and resolved Dart test
packages. Never point it at a live service or provide real credentials.

```sh
export KAFKA_HOME=/absolute/path/to/kafka_2.13-4.0.2
KAFKA_COMBINED_MODES=tls python3 tool/run_combined_secure_fault_integration.py
KAFKA_COMBINED_MODES=sasl python3 tool/run_combined_secure_fault_integration.py
```

Only `tls`, `sasl` and `tls,sasl` are accepted; the default is both, stopping at
the first failed mode. Mode selection allows independent diagnosis without
hiding the failed default gate. An explicit command may follow `--`. The default
uses an already-resolved source test entrypoint, not a cached snapshot; its
runner memory is included. Recorded runs use explicit existing cached snapshots
and retain their exact invocation/config/source hashes.

The Linux-only guards keep the existing thresholds: at least 2 GiB free disk
and 1 GiB host MemAvailable, sampled every five seconds; owned test-process RSS
at most 768 MiB, sampled every 100 ms. These are sampled safety stops, not hard
limits. Each broker retains a 256 MiB Java heap cap; proxy limits remain 16 active
connections, 32 workers and 16 KiB read chunks per proxy. Modes run serially.

Only sanitized results survive. The fixture stops its own processes/threads,
removes credentials, certificates, configuration and data, and verifies port
reuse on both success and failure. See `verification/combined_secure_faults/`
in the development checkout for precise outcomes and all unreached stages.
