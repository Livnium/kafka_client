# Optional native client certificate identities

`KafkaTlsConfig(clientIdentity: ...)` adds optional client authentication to the
existing native TLS connection. No runtime dependency, web API or insecure bypass
is added. Null preserves server-only TLS. SASL PLAIN still requires TLS and, if
configured alongside a client identity, follows that same verified connection.
Neither authentication method substitutes for the other when a broker requires both.

```dart
final identity = KafkaTlsClientIdentity.pem(
  certificateChain: await File(clientChainPath).readAsBytes(),
  privateKey: await File(clientKeyPath).readAsBytes(),
  privateKeyPassword: runtimeKeyPassword, // null for an unencrypted key
);
final tls = KafkaTlsConfig(
  withTrustedRoots: false,
  trustedCertificates: await File(serverCaPath).readAsBytes(),
  clientIdentity: identity,
);
// Or load a PKCS12 bundle containing BOTH the key and certificate chain:
final bundleIdentity = KafkaTlsClientIdentity.pkcs12(
  bytes: await File(identityBundlePath).readAsBytes(),
  password: runtimeBundlePassword,
);
```

Load keys and passwords from a runtime secret provider. Never commit, print or
include them in diagnostics. The example variable names are not credentials.

## Trust and ownership contract

- Server certificate and hostname checks remain mandatory. Platform trusted roots
  remain enabled by default; custom CA bytes augment them unless explicitly
  disabled. The client identity, including CAs bundled in PKCS12, is **not** added
  to server trust. Client identity alone cannot satisfy `withTrustedRoots: false`.
- The same immutable identity applies to all connections made by a client,
  including configured bootstraps, advertised leaders, and discovered coordinators.
  Only use an identity with endpoints you authorize to request it. There is no
  per-broker identity selector. Existing endpoint routing/recovery policies remain
  unchanged, including explicit opt-in bootstrap fallback.
- Each connection receives a fresh SecurityContext, without mutating the global
  default. Both ordinary connections and scoped stable-offset reads use it.
  Close, cancellation, connection/request deadlines and resource accounting retain
  their existing ownership contracts. This feature adds no request replay.
- Each input chain, key or bundle is copied and limited to 4 MiB. Empty inputs and
  integers outside 0–255 are rejected. An optional password is limited to 16 KiB
  in UTF-8; NUL, invalid Unicode and oversized values are rejected. Null and
  empty passwords are allowed because container formats may require them.
- The identity has no material/password getters and its string representation is
  redacted. Native identity-loading failures become a fixed TlsException without
  retaining the original error or cause. They occur before TCP connection and do
  not trigger bootstrap TLS fallback. Existing CA-loading behavior is unchanged.
- Loading and parsing are synchronous inside SecurityContext creation, with
  bounded input sizes. Dart does not guarantee secure memory erasure. Replacing
  files later does not rotate copied identities; create a new identity and client
  and close the old client for rotation. No live rotation API is provided.
- A broker rejection can appear as a TLS or connection failure depending on TLS
  version and platform. No broader fail-fast or retry guarantee is introduced.
  A successful client handshake alone is not proof that a broker accepted its
  identity; the real fixture checks authenticated application requests as well.

## SDK and platform restrictions

The API delegates parsing and cryptography to dart:io SecurityContext. It does
not convert formats, supply a crypto implementation, or bypass unsupported SDK
features. PEM is intended for SDK/platforms that support separate chain and key
loading. PKCS12 must contain the matching private key and certificate chain, with
algorithms supported by the running SDK/platform.

Dart's documented iOS restriction requires PKCS12 data containing **both** the
private key and certificate chain. The implementation invokes both chain and key
loading methods with that bundle; the SDK documents chain loading as a no-op on
iOS. Custom server CA loading has a separate iOS restriction: a single DER X509
certificate per call. The current config accepts one custom CA byte input and
does not split or convert a PEM CA bundle. Use platform-compatible bytes or the
platform's built-in roots. Format errors surface as loading failures rather than
a blanket platform ban.

This work does not qualify packaged Android/iOS apps, OS trust behavior,
background/lifecycle behavior, hardware-backed keys, device keychains, certificate
revocation or every PKCS12 encryption algorithm. Those remain separate checks.

Primary SDK references:
- [SecurityContext](https://api.dart.dev/dart-io/SecurityContext-class.html)
- [Private keys and iOS PKCS12](https://api.dart.dev/dart-io/SecurityContext/usePrivateKey.html)
- [Chain loading and iOS](https://api.dart.dev/dart-io/SecurityContext/useCertificateChain.html)
- [Custom CA loading and iOS DER](https://api.dart.dev/dart-io/SecurityContext/setTrustedCertificates.html)

## Separate qualification

Ordinary `dart test test/mtls_identity_test.dart` uses synthetic one-day loopback
certificates and no Kafka broker. It checks input bounds/copies, PEM/encrypted PEM/
PKCS12 loading, strict server trust/hostname behavior, missing/untrusted client
identities, PLAIN ordering, later endpoints, scoped stable reads, close ownership
and redacted pre-TCP loading failures. OpenSSL is a test tool, not a runtime
package dependency. All generated fixture material is removed afterward.

`bash tool/run_mtls_broker_integration.sh` is separate opt-in real-broker testing.
It uses the secure runner's explicitly selected, independently SHA512-verified
Kafka distribution. It sets `ssl.client.auth=required`, gives the owned broker a
private CA truststore, generates trusted/untrusted one-day client identities and
checks SSL and SASL_SSL produce/fetch, negative identity/trust/hostname cases and
a scoped stable-offset read. Official Java secure client smoke checks include
the client identity too. The underlying private fixture traps stop its broker and
remove keys, certificates, passwords, configs, raw logs and data on success or
failure. Only sanitized test summaries remain. Use no real credentials and never
point it at production infrastructure.

Historical server-only TLS/SASL version-matrix results do not qualify mTLS.
Results must identify exact source, SDK/OS and Kafka version. No publication,
release, production-readiness or mobile-support claim follows from this fixture.

### Implementation-source checks on 2026-11-04: Kafka 4.0.2

For exact implementation source `81761d2` on Linux x64:

- Dart 3.2.6 and 3.12.0 each passed the ordinary suite: 1,407 passed and 370
  external-fixture cases skipped. Analysis passed on both SDKs; formatting was
  clean. The Python fixture-tool aggregate passed 283 cases.
- A separate owned Kafka 4.0.2 fixture passed all 10 requiring-client-certificate
  cases on **each** SDK, with explicit required authentication on SSL and
  SASL_SSL. The selected official Kafka archive matched its SHA512. OpenJDK
  21.0.12.1 and OpenSSL 3.5.7 were used. These are single-node source observations,
  not an expansion of the historical multi-version matrix.
- The original server-only secure fixture also passed its 9 TLS/SASL cases on
  Dart 3.12.0/Kafka 4.0.2, including the existing bidirectional Java oracle. The
  separate mTLS fixture checks Java secure smoke only, not that bidirectional
  oracle. Neither fixture retained its ephemeral private directory, and owned
  broker ports were closed afterward.
- An independent review of the same commit found no blocker and independently
  passed the 35 focused identity/security-codec cases on Dart 3.2.6, plus all 6
  summary/cleanup regressions. Deliberately injected PEM, encodings, binary-like
  byte lists, passwords and paths did not survive into mTLS summaries. A simulated
  removal failure returned a failing status without claiming cleanup succeeded.

Those `81761d2` observations qualify no Kafka version other than 4.0.2, packaged
mobile app, real OS lifecycle, sustained mTLS workload, certificate rotation,
publication or release.

### Later exact-source checks on 2026-11-04: Kafka 4.4.0

Source `1199abdd2d9dff857eb936edcac2042d93f474bb`, tree
`863971cab2405899eed0bbf84db29467af79113a`, separately passed **20 real-broker
tests with zero qualification skips/failures** on Linux x64:

| SDK | Exact Kafka version | Required-client-auth cases passed |
| --- | --- | ---: |
| Dart 3.2.6 | 4.4.0 | 10 |
| Dart 3.12.0 | 4.4.0 | 10 |

Each SDK ran the existing ten mTLS cases: PEM SSL produce/fetch, PKCS12 SSL
produce/fetch, client identity plus PLAIN SASL_SSL produce/fetch, missing identity
rejection on SSL and SASL_SSL, untrusted client identity rejection, independent
server trust and hostname rejection, wrong PLAIN password rejection, and an
operation-scoped stable OffsetFetch read over mutual TLS. The private fixture
required client authentication, including explicit SASL_SSL listener overrides;
the capture independently observed those configuration flags and Kafka 4.4.0.
Wrong PLAIN password required `KafkaAuthenticationException`; other negative TLS
cases required non-timeout rejection without pinning native error strings/classes.

Java SSL/SASL_SSL CLI smoke and a Java producer succeeded during setup. The
selected mTLS tests did **not** add a Java consumer consume-back assertion.
The two no-broker prewarms intentionally skipped all ten cases each and are
excluded from the 20 real passes. Source hashes, clean Git status and package
resolution matched this exact candidate before and after each invocation. The
minimum SDK used compatible `test` 1.25.2; the modern SDK used `test` 1.33.0 and a
hash-verified cached runner. These are not published-package or later-source tests.

The original fixed guards remained at 2 GiB free disk and 1 GiB MemAvailable.
An initial resource preflight blocked correctly; it is not a qualification pass.
Both final broker commands and guards exited successfully. Fixture traps removed
ephemeral keys/credentials/private directories, all five selected ports rebound
independently after each cleanup, and owned compiler temporary directories were
empty. Minimum-SDK descendant monitoring was unavailable in this environment;
zero observed descendants is not proof that no subprocesses ran. Its cleanup is
supported by the trap and independent private-directory/port checks. The modern
read-only parent monitor observed 33 descendants with none remaining.

Raw test output and identity material were intentionally not retained. Separate
fixed-field receipts preserve source/config/runtime observations, distribution
verification, sampled resources and cleanup results; they are not shipped here.
The captured library/pubspec source-map SHA256 is
`ee889f3da2dc140c93f8c61444bdb40c3db79fbdaae7372fd20305e55aae0875`.

This qualifies only the stated Kafka 4.4.0/Linux/two-SDK mTLS scenarios. Synthetic
one-day PKI and local plaintext admin/controller plumbing do not qualify
production key storage, revocation, expiry, chain rotation, mobile/device trust,
failover/failure recovery or endurance. The run did not qualify graceful stop,
broader broker matrices, a release or general production readiness. Earlier
Kafka 4.0.2 checks remain attributed to `81761d2`, not a two-version rerun of
this newer source.
