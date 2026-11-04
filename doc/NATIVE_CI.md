# Native CI baseline

`.github/workflows/native-ci.yml` defines an ordinary native Dart regression
baseline on Linux x64 (`ubuntu-22.04`), with exact SDKs **3.2.6** and **3.12.0**.
It runs on pushes, pull requests and manual dispatches. The actions are pinned
to commit IDs and have read-only repository permissions. No credentials are
retained by checkout, no package is published, and no broker is started.

The Dart jobs check formatting of active `lib`, `test`, `example` and `tool`
source, run `dart analyze --fatal-infos`, and run the ordinary `dart test` suite.
Historical `verification` receipts are not active formatter/analyzer inputs.
A separate Python job runs fixture-runner unit regressions. It does not run
the fixture integrations themselves.

## Dependency reproducibility

CI copies the appropriate `.github/ci/dependencies/dart-<sdk>.lock` into the
ignored root `pubspec.lock`, then runs `dart pub get --enforce-lockfile` and
checks that the lockfile did not change. The minimum SDK uses `test` 1.25.2
and analyzer 6.4.1; the 3.12.0 snapshot uses `test` 1.33.0 and analyzer 14.5.0.
Both retain `lints` 3.0.0 and the package's existing dev-dependency constraints.
The newer snapshot cannot be used on Dart 3.2.6. There are no runtime dependency
or package SDK-constraint changes.

These are CI-only test-tool snapshots, not a lockfile imposed on library
consumers. When deliberately updating them, resolve dependencies separately
with each exact SDK, copy each resulting lockfile to its matching snapshot,
and rerun the same checks on both SDKs. A changed pubspec that no longer matches
a snapshot must fail CI until the snapshot is deliberately reviewed and updated.
The hosted runner image and its system tools still receive upstream updates;
this is not a fully hermetic OS image.

## Skips and evidence

The ordinary Dart run leaves external fixtures unconfigured. The job summary
separates executed passes, failures, suite-load failures and skips, and lists
skipped counts by suite. Missing/incomplete reports, invalid result types,
orphan or duplicate test events, and events after the terminal result fail the
summary step. Errors emitted after a test's completion still count as failures,
including errors from previously hidden virtual setup/teardown tests.
JSON test events, the expanded log, exact dependency lockfile and source/SDK/OS
identity are retained as seven-day artifacts when their producing steps ran.
Do not label skipped tests as passed integrations.

A green result does **not** qualify Kafka versions, real Java interoperability,
replicated failover, sustained workloads, packaged Flutter apps, other operating
systems or browsers. The named real-broker and cleanup gates in
[native v1 scope](NATIVE_V1_SCOPE.md) remain separate, and required-check/branch
protection settings must be configured by the maintainer. Adding this file
does not establish that Actions is enabled or that a remote run has passed.

## Local reproduction

With the selected SDK on `PATH`, from a clean source checkout:

```sh
sdk=3.2.6 # alternatively 3.12.0
cp ".github/ci/dependencies/dart-$sdk.lock" pubspec.lock
dart --version
dart pub get --enforce-lockfile
cmp ".github/ci/dependencies/dart-$sdk.lock" pubspec.lock
dart format --output=none --set-exit-if-changed lib test example tool
dart analyze --fatal-infos
mkdir -p .dart_tool/ci
dart test --reporter expanded \
  --file-reporter json:.dart_tool/ci/test-results.jsonl
python3 tool/summarize_ci_tests.py .dart_tool/ci/test-results.jsonl
python3 -m unittest discover -s tool -p 'test_*.py'
```

Dependency downloads require pub.dev access. The default test suite needs
local loopback sockets and OpenSSL for its owned TLS tests; it does not need
external Kafka credentials. Keep sufficient temporary disk space available
for the Dart test compiler. On a sandbox with a read-only home or constrained
`/tmp`, use an owned writable `HOME`/`TMPDIR`; this is not needed on an ordinary
GitHub-hosted runner.
