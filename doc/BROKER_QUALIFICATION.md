# Selecting broker releases for qualification

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
