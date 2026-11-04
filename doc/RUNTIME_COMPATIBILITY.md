# Runtime compatibility and secure example

This is an experimental client. An SDK constraint or static platform tag is not
an executed compatibility test. All three public entrypoints unconditionally depend
on `dart:io`; there is no browser transport, including for Flutter Web.

## Observed and untested environments

The retained operational evidence uses standalone Dart 3.2.6 and 3.12.0 on one
Linux x64 cloud host, with Apache Kafka 4.0.2. The SDK range `^3.2.6` permits
additional SDKs; those versions were not individually tested. The additional
headless Flutter test-engine evidence below is narrower than a packaged app test.

Historical secure-example validation passed TLS/SASL roundtrips and
trust/hostname/authentication rejection as direct source and Linux x64 native
executables on both SDKs. These native compilation and secure-example results
are recorded in `verification/runtime_compatibility/RESULTS.md` in the
development checkout, based on `54a13cb` and sealed in `b3ec170`. They are
historical evidence for that source, not validation of the latest source.
Refresh the source and native executable smoke on the final release source
before making runtime support claims.
A successful native executable smoke is narrower than rerunning the complete
unit, sustained-use, failover and network-fault suites under AOT. Compilation
for a different OS or architecture would not prove that output runs there.

Still unverified: packaged Flutter desktop/mobile applications;
Android/iOS/macOS/Windows; non-x64 targets;
other Kafka releases; another physical host or independent failure domain.
Custom certificate formats and platform trust stores vary. In particular, a
Linux PEM test does not establish mobile certificate-loading compatibility.
The fixed fixtures exercise an explicit local CA, not a real deployment's
system trust-store configuration. No unsupported target is claimed to work.

Before release, the author must choose the intended supported platforms and
package metadata. Earlier static scoring inferred broad native/Flutter tags;
those inferences are not the runtime matrix. Do not expand support from them.
See [V1_READINESS.md](V1_READINESS.md) and [RELEASING.md](RELEASING.md).

## Headless Flutter engine evidence

On 2026-11-04, nine real-broker cases passed against unchanged client checkpoint
`faf1f40b22ebd627422a3c9ba8ca5f908b6d3619` inside the Linux x64 Flutter 3.44.0
test engine with Dart 3.12.0, a genuine `WidgetsFlutterBinding` and a completed
widget frame. Framework revision: `559ffa3f75e7402d65a8def9c28389a9b2e6fe42`;
engine revision: `4c525dac5ebe5971c5708ef73558ed8edcf4a362`.
The cached frontend compiler and `flutter_tester` were invoked directly;
neither standard `flutter test` nor `flutter pub get` ran. The fixture was an
owned single-node Kafka 4.0.2 broker with ephemeral CA/certificate/credentials.

The nine passing cases were:

1. TLS managed processing, confirmed commits and fresh-transport stable readback.
2. The same under SASL PLAIN over TLS (`SASL_SSL`).
3. TLS wrong-CA rejection.
4. TLS hostname rejection.
5. SASL_SSL wrong-CA rejection before authentication payload access.
6. SASL_SSL hostname rejection before authentication payload access.
7. Terminal SASL bad-credential rejection, one payload read and no callback.
8. TLS framework-injected pause, callback drain, stop and new-consumer recreation.
9. The same lifecycle sequence under SASL_SSL.

Positive cases checked binary/null/empty values, duplicate headers and a
Java-produced Unicode oracle. They establish Java-to-Flutter interoperability
here, not Java readback of Flutter-produced records. Lifecycle cases used an
application-owned observer with injected `paused`/`resumed` notifications, not
actual OS suspension. A held callback drained without committing after revocation;
the old consumer closed before a distinct consumer read fresh stored positions
and processed that uncommitted record again. This demonstrates possible duplicate
callbacks, not exactly-once effects. The library does not manage app lifecycle.

Strict TLS remained enabled. TLS bootstrap fallback, TLS Fetch recovery and
write replay were disabled. Stable v7 and the independent read-retry policy were
enabled with 20 total attempts, an 8-second deadline and 150-ms delay; this run
does not establish that error 88 occurred. Automatic rejoins were zero and
coordinator migration was not retested. Cleanup checks found no owned process or
fixture secrets remaining; explicitly owned test clients reported zero resource
counters. Managed-consumer cleanup used public termination evidence.

An earlier attempt failed constructor validation before consumer creation because
its rebalance timeout exceeded its request timeout. Only the harness configuration
was corrected, and the successful run used a fresh disposable workload. These
nine passes are not evidence that the failed attempt passed.

No packaged desktop app, Android/iOS device or emulator, native plugin, release/AOT
app, device trust store, platform network configuration, real OS suspension or
termination, Flutter endurance or broker-fault matrix was tested. Linux PEM
certificate loading does not establish iOS compatibility. Platform qualification
still requires actual target-app tests, production-equivalent trust/network
configuration and interrupted-effect handling. See the
[reliability contract](RELIABILITY_CONTRACT.md) for the limits of fresh readback
and why a new consumer does not inherit the previous object's position floor.

This package-native summary records the executed scope; the external test harness
and raw receipts are not shipped as runnable package examples. Historical
`verification/` references elsewhere identify development evidence, excluded from
the publication archive, not prerequisites for understanding these contracts.

## Runnable secure example

`example/kafka_client_secure_example.dart` uses only the public application API.
It always enables verified TLS. Use a pre-created, dedicated topic whose
partition 0 you are authorized to write and read. Running it appends one record;
this is not a read-only connection check. The certificate must cover the bootstrap
hostname and every broker hostname advertised for partition leaders.

For an authorized TLS-only broker, after resolving package dependencies:

```sh
export KAFKA_HOST=broker.example.internal
export KAFKA_PORT=9093
export KAFKA_TOPIC=dedicated-example-topic
export KAFKA_CA_FILE=/absolute/path/to/trusted-ca.pem
dart run example/kafka_client_secure_example.dart
```

The endpoint and paths above are placeholders, not a public test service. Set
`KAFKA_CA_FILE` to an actual trusted CA file; when supplied it replaces platform
roots. Omit it to use platform roots. `KAFKA_PORT` defaults to 9093. Host and topic
are mandatory. Do not include a URI scheme or port in `KAFKA_HOST`.

For SASL PLAIN, inject both `KAFKA_USERNAME` and `KAFKA_PASSWORD` from your runtime
secret provider before the command. Set neither for TLS-only. Missing, empty or
invalid credential pairs fail before connecting. Do not put real credentials in
source, a checked-in environment file, shell history or command-line arguments.
Environment variables here are demonstration inputs, not a secret manager, and
may be visible to other sufficiently privileged processes. Credentials remain
in process memory; Dart cannot guarantee their erasure. No example settings or
credentials are written to disk or printed.

The example produces once with `acks=-1`, then checks its random key and exact
payload at the acknowledged partition-0 offset. Only Fetch is polled when empty,
for up to five attempts at that same offset. It ignores unrelated records rather
than printing their contents. Each low-level request has a five-second deadline;
this is not one overall-operation deadline. All operations are awaited and the
client is closed in `finally`, including on verification failure.

Success prints `SECURE_EXAMPLE_OK`, the offset and the security mode. Failure
prints a fixed category, no raw exception or credential data, and exits nonzero.
The `format` category can mean malformed local settings or malformed broker data;
it is not a diagnosis that configuration alone caused the failure. The retained
`verification` category covers every `StateError`, including ApiVersions errors,
missing leaders and a closed client, as well as record verification failures.
It does not establish that a record mismatch occurred.
A failure after transmission can leave a stored write even without a success
line. Do not blindly rerun or replay it. This example is not an idempotent producer,
consumer loop, cursor manager, deployment guide or capacity benchmark.

## Repeating the local native smoke

On the same target host, with already resolved dependencies:

```sh
dart compile exe --packages=.dart_tool/package_config.json \
  example/kafka_client_secure_example.dart -o /tmp/kafka-secure-example
```

The output is a host-specific executable, not a portable binary. Use the same
environment inputs described above to run it. For owned fixture validation,
`tool/check_secure_example.py` accepts a JSON list of local runtime entries:
`label`, `cwd`, and `command` (an argument list with no credentials). Invoke it
inside the existing disposable secure-broker fixture:

```sh
bash tool/run_secure_broker_integration.sh \
  python3 tool/check_secure_example.py /absolute/path/to/runtime-manifest.json
```

The fixture owns synthetic credentials and one-day certificates and deletes them
afterward. The checker maps those private inputs to each command without saving
them. It verifies TLS/SASL roundtrips and rejects an untrusted CA, hostname mismatch
and wrong password. Its retained output includes only fixed pass/fail fields.
Use only owned fixtures; never repoint this checker at a real service. This test
uses secured client traffic with loopback plaintext controller/admin plumbing.
