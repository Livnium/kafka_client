# Selecting broker releases for qualification

The [additional qualification observations](QUALIFICATION_OBSERVATIONS.md)
record later source-specific coordinator/network faults, a paced 30-minute
subscription pass, an incomplete concurrent soak, and plaintext ListOffsets/
missing-position probes. They do not rerun or extend the older matrices below
to the current `4701dd6` source.

Later [exact-source mTLS checks](MTLS.md#later-exact-source-checks-on-2026-11-04-kafka-440)
separately passed ten Kafka 4.4.0 cases on each tested SDK at `1199abd`.
They do not extend any historical server-only matrix or older mTLS source
results to a broader version, runtime or failure-mode claim.

Separate [graceful-stop observations](GROUP_GRACEFUL_STOP_QUALIFICATION.md#separate-kafka-440-checks-at-46c6355)
at `46c6355` passed 24 requested Kafka 4.4.0 cases across the two SDKs and
plaintext/server-authenticated TLS/SASL_SSL PLAIN. They do not combine mTLS with
grace, test broker failover or extend the earlier `693f509`/4.0.2 evidence to
the newer source.

## Recorded six-version native qualification, 2026-11-04

This evidence qualifies native source `6f828d306cb1f6c3adc0638f9ba7ac727d1cf368`
after the request-snapshot fixes, before response-validation hardening. It is not
the browser prototype or byte-for-byte published 0.1.1. Qualified library/pubspec
bytes match source tree `86d96bbf4b2a1701ed100ffa0d9528226a6df299`, available at
[the qualified public commit](https://github.com/Livnium/kafka_client/commit/c524daa41503dbdf583433dfafdaee051f9d0abe).
Later source changes do not inherit this matrix merely by descending from it.

Runtime: Linux x64, Dart 3.12.0, OpenJDK 21.0.12.1. Each selected official
distribution was independently SHA512-verified, and its runtime version was
checked in broker startup logs. Each final run retained source snapshots and
before/after fingerprints and verified fixture cleanup.

| Exact Kafka version | Plaintext Dart cases | TLS/SASL_SSL Dart cases | Official Java consume-back checks |
| --- | ---: | ---: | ---: |
| 4.0.0 | 4 passed | 68 passed | 2 passed |
| 4.0.2 | 4 passed | 68 passed | 2 passed |
| 4.1.2 | 4 passed | 68 passed | 2 passed |
| 4.2.2 | 4 passed | 68 passed | 2 passed |
| 4.3.2 | 4 passed | 68 passed | 2 passed |
| 4.4.0 | 4 passed | 68 passed | 2 passed |

Totals: 432 Dart cases and 12 official Java consume-back checks. Java-produced
records were also checked by Dart. Plaintext cases cover binary/Unicode,
tombstones, headers and offsets, `acks=0`, invalid partitions and out-of-range
handling. Secure cases cover verified TLS, SASL PLAIN over TLS, wrong trust/
hostname/authentication rejection, manual offsets, classic membership, session
revocation, delivery, acknowledged processing, the consumer runner and explicit
topic subscriptions. A SCRAM-only listener checks rejection of PLAIN, not working
SCRAM authentication.

These are disposable **single-node loopback** fixtures on **Dart 3.12.0 only**.
Replicated failover, fault proxies, transaction/stable-offset boundaries and
long endurance were not rerun across these versions. The independent Dart
3.2.6/3.12.0 unit-suite results are not a two-SDK broker matrix. Earlier
prototype-only runs are excluded. The final 4.3.2 secure rerun passed after a
preliminary broker-loss run failed; that earlier loss was not explained and is
not counted as a pass. A preliminary Kafka 4.4 CLI warning was resolved by
capability-aware consumer-config flag selection before the final Java check.

Raw captures are retained separately, not shipped in this repository/package.
The table summarizes those results; the commands below identify reproducible
public test inputs rather than implying raw receipts are included here.

## Representative response-hardening checks, 2026-11-04

Later native source `0d0c8005218ae4112f8d5e3d71db5a50e100c2be` rejects ambiguous
leader routes and malformed Produce success responses. It separately passed:

| Exact Kafka version | Plaintext Dart cases | Default TLS/SASL_SSL Dart cases | Official Java consume-back checks |
| --- | ---: | ---: | ---: |
| 4.0.0 | 4 passed | 9 passed | 2 passed |
| 4.4.0 | 4 passed | 9 passed | 2 passed |

Totals: 26 Dart cases and four official Java consume-back checks, including
bidirectional Java interoperability, binary/Unicode/tombstone/header/offset
handling, `acks=0`, partition/out-of-range errors and strict trust, hostname and
authentication rejection. Source was unchanged during each run. The previously
SHA512-verified official distributions were reused, exact broker runtime versions
were confirmed and fixture broker data/ephemeral keys/credentials were removed.
Runtime was Linux x64, Dart 3.12.0 and OpenJDK 21.0.12.1.

These checks used the default core and security runners, not the expanded
68-case secure selection below. Kafka 4.0.2, 4.1.2, 4.2.2 and 4.3.2 were not
rerun after this hardening. Neither were replicated failover, faults,
transaction/stable-offset boundaries or endurance. The earlier 432-case matrix
qualifies `6f828d3`, not this newer library. Raw response-hardening receipts are
also retained separately, not shipped here.

## Select an exact verified distribution

The fixture runners default to the historically tested Kafka 4.0.2 release.
To attempt a different stable 4.x release, set both variables explicitly:

```sh
export KAFKA_VERSION=4.4.0
export KAFKA_HOME=/absolute/path/to/kafka_2.13-4.4.0
bash tool/run_broker_integration.sh
bash tool/run_secure_broker_integration.sh
```

First independently verify the official binary distribution using its published
SHA512. The shared selection helper checks the exact version syntax and matching
client-jar filename, and rejects mixed client jars. It does not verify artifact
authenticity or prove the broker's runtime version. Do not rename jars to bypass
the check. Prereleases and releases outside 4.x are rejected.

The same variables apply to the operational, cluster, secure cluster, plaintext
and TLS network-fault, combined secure-fault and stable-offset migration runners.
Their result JSON includes `kafka_version`. The plaintext shell fixture retains
`kafka-version.txt`; the secure shell fixture records the version in its sanitized
result header. Historical test names or source comments may still mention 4.0.2;
they are not runtime version detection.

Selection enables a test attempt, not a compatibility claim. Keep results scoped
to the exact client source, selected distribution and checksum, SDK/runtime,
transport, fixture settings and tests actually executed. A unit-only or skipped
integration run is not broker evidence. Existing verification receipts are not
expanded by this change, and untested or future 4.x releases are not guaranteed.
All fixtures remain owned, disposable and loopback-only. Do not use a live cluster
or real credentials. Broker configuration/tool changes can require further runner
work before another release passes.

## Reproduce the recorded case selection

Resolve packages first, and export `DART` as the intended SDK executable if `dart`
on PATH is different. Run each version serially with its independently verified
matching `KAFKA_HOME`; record the source, SDK, distribution checksum, startup
version, output and cleanup for every attempt. Java 17+, Python 3 and OpenSSL
are required. Keep adequate free disk/memory for owned fixtures.

The plaintext runner's default executes `test/integration_test.dart` (four
cases) and its official Java produce/consume-back oracle. The secure runner's
default executes only `test/security_integration_test.dart` (nine cases),
followed by its Java oracle. A default secure invocation therefore does **not**
reproduce the recorded 68 cases.

The recorded secure selection consists of these eight files, run serially:

```sh
bash tool/run_secure_broker_integration.sh bash -c '
  set -euo pipefail
  "${DART:-dart}" test \
    test/security_integration_test.dart \
    test/group_offsets_security_integration_test.dart \
    test/group_membership_security_integration_test.dart \
    test/group_session_security_integration_test.dart \
    test/group_delivery_security_integration_test.dart \
    test/group_processing_security_integration_test.dart \
    test/group_consumer_security_integration_test.dart \
    test/topic_subscription_security_integration_test.dart \
    --concurrency=1 --reporter expanded
  private_dir="$(dirname "$KAFKA_SECURE_CA")"
  help_status=0
  "$KAFKA_HOME/bin/kafka-console-consumer.sh" --help \
    > "$private_dir/consumer-help.txt" 2>&1 || help_status=$?
  [[ "$help_status" == 0 || "$help_status" == 1 ]]
  grep -Eq "^Option[[:space:]]+Description[[:space:]]*$" "$private_dir/consumer-help.txt"
  if grep -Eq -- "^[[:space:]]*--command-config([[:space:]]|$)" "$private_dir/consumer-help.txt"; then
    config_option=--command-config
  elif grep -Eq -- "^[[:space:]]*--consumer[.]config([[:space:]]|$)" "$private_dir/consumer-help.txt"; then
    config_option=--consumer.config
  else
    exit 1
  fi
  "$KAFKA_HOME/bin/kafka-console-consumer.sh" \
    --bootstrap-server "localhost:$KAFKA_SECURE_SASL_PORT" \
    "$config_option" "$private_dir/sasl-client.properties" \
    --topic "$KAFKA_SECURE_ORACLE_TOPIC" --from-beginning \
    --max-messages 2 --timeout-ms 10000 \
    > "$private_dir/consumer.txt" 2> "$private_dir/consumer.log"
  printf "secure-java-to-dart-नमस्ते\nsecure-dart-to-java-नमस्ते\n" \
    > "$private_dir/expected.txt"
  diff -u "$private_dir/expected.txt" "$private_dir/consumer.txt"
  echo "Official Java SASL_SSL consumer verified Dart-produced record"
'
```

A custom secure command replaces the runner's default test/oracle step. The
explicit Java check above is therefore required in addition to the 68 Dart
cases. It uses only ephemeral fixture credentials and does not save real
credentials. The fixture removes its private directory afterward and retains
sanitized test output. Historical Dart group labels may still say 4.0.2;
verify the running broker version rather than relying on a test label.
