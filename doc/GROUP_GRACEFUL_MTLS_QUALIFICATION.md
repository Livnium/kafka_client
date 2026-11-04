# Combined graceful-stop and mTLS observations

On 4 November 2026, exact local source
`b188ec5040d050147ed899819af4cbca10eaa66a`, tree
`f4f41a8040892c7df148fe00f6c4f07981474a51`, passed **32 selected real-broker
cases with zero failures or qualification skips** on Linux x64:

These are external copied-test-adapter observations, not an unmodified stock
fixture result. The adapter's exact scope and hashes are recorded below.

| Exact Kafka version | Dart SDK | Required-client-cert SSL | Required-client-cert SASL_SSL/PLAIN | Total |
| --- | --- | ---: | ---: | ---: |
| 4.0.2 | 3.2.6 | 4 | 4 | 8 |
| 4.0.2 | 3.12.0 | 4 | 4 | 8 |
| 4.4.0 | 3.2.6 | 4 | 4 | 8 |
| 4.4.0 | 3.12.0 | 4 | 4 | 8 |

Each invocation used one owned broker and partition with synthetic **PEM**
client identities. This is a separate combined observation, not an extension of
the earlier [server-authenticated grace checks](GROUP_GRACEFUL_STOP_QUALIFICATION.md)
or [standalone mTLS checks](MTLS.md) to another source or test profile.

## External copied-test adapter, not an unmodified stock test

The stock graceful-stop fixture has no client-identity hook. This qualification
used an external copied-test adapter that supplies the synthetic PEM identity in
its existing strict TLS getter, retargets helper imports to the exact candidate,
labels the secure modes as mTLS and requires the mTLS environment gate. The
entire original case/helper block remained byte-for-byte unchanged. No tracked
runtime, original test, broker fixture or resource guard was edited.

- Adapter SHA256:
  `4029bc4fe5f9de9d4dc77f512b40cb7e821c502689f220f2edd4e99bd00e41b0`.
- Unchanged case/helper block SHA256:
  `20a90f7da8507b88ba28a628489c3a5a7baa020c6acf1b260ee65057cdc55448`.

The external adapter/diff, receipts and source/configuration evidence are
retained separately, not shipped here. The command selected eight secure cases per invocation. Plaintext
definitions were filtered out, not qualification skips. Two broker-free compile
prewarms intentionally skipped eight selected cases each; those **16 skips** are
excluded from the 32 real passes. These results are not a claim that invoking
the unmodified stock grace test automatically exercises client identities.

## Four existing cases in every matrix cell

1. Reentrant grace holds the exact application Future for seven seconds, beyond
   the configured four-second local expiry and six-second broker session
   timeout. Independent heartbeats retain the current token and generation 1;
   no extra callback/early report occurs. The admitted-record commit is confirmed
   after callback completion and read back through a fresh client.
2. Commit-disabled grace drains its one callback without creating a stored
   offset. The report has no last commit, actual `closed` is awaited and fresh
   readback remains missing.
3. A 250 ms grace deadline reports `deadlineExpired`, callback pending and no
   pending commit. Authority is revoked while actual closure still waits for
   the callback; releasing it creates no late commit or extra callback.
4. A real competing member causes heartbeat error 27 and a revoked report.
   The old consumer remains at one generation, waits for callback release and
   admits no late commit, extra callback or rejoin; fresh readback remains missing.

The shared PEM identity covers readers, managed control/data/position clients
and the competitor. Explicit server CA trust, hostname checks and disabled TLS
bootstrap fallback remained unchanged. Runtime observations confirmed each
selected broker version and required client authentication, including the
SASL_SSL listener override. SASL_SSL additionally required PLAIN credentials.
Broker readbacks used fresh Dart clients; Java security smoke was setup, not an
additional Java consume-back oracle.

## Source, resource and cleanup evidence

Exact HEAD, clean status, candidate source hashes and adapter hash matched before
and after every invocation. The captured library/pubspec source-map SHA256 was
`2ee85ae687a763e08479655a199eeff837fea41ffc5e846fb81cffe138a2e759`.
Minimum SDK tooling used compatible `test` 1.25.2; the modern SDK used `test`
1.33.0 and a hash-verified cached harness, compiling candidate tests/library and
the adapter freshly. These checks do not rerun the ordinary SDK suites.

The original guards remained at 2 GiB free disk and 1 GiB MemAvailable. All four
invocations passed; additional read-only samples observed compile/run footprints.
All brokers stopped, private directories and one-day identities/random
credentials were removed, all five selected ports independently rebound after
each fixture and owned temporary directories were empty. No observed descendant
processes remained. Only fixed allowlisted summaries survived private-fixture
teardown; generated identities and raw TLS/test diagnostics were not retained.
Prior verified distribution receipts, SDK/config manifests and exact commands
remain with the separate evidence.

## Limits

This qualifies only the stated local source, two Kafka versions/SDKs, single
broker/partition and copied-adapter **PEM mTLS-plus-grace** scenarios. It does not
qualify PKCS12-plus-grace, mobile platforms, production PKI lifecycle,
certificate rejection/rotation/revocation, broker failover, sustained load or
endurance. The competing-member rebalance is not broker failover. Grace expiry
does not terminate application Futures or external effects; actual `closed` is
awaited separately. No exactly-once, published-package or production-readiness
claim follows, and earlier evidence retains its own source attribution.
