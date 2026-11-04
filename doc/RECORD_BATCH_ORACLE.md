# Broker-free record-batch compatibility fixtures

`test/record_batch_java_oracle_test.dart` reads the checked-in synthetic fixtures
in `test/fixtures/record_batch_java_4_0_2.json`. Ordinary Dart tests need no Java,
Kafka installation, broker, network access, or new package dependency.

The project harness `tool/RecordBatchOracle.java` creates uncompressed magic-2
batches with the official Kafka 4.0.2 Java record APIs, validates their CRCs and
records, and obtains expected values through Kafka's own iterator. It does not
use the Dart encoder or decoder to create expected output. The Dart regression
compares strict decoding, strict decoding with batch progress, and Fetch-specific
decoding against those same independent values.

## Covered cases

- CreateTime, decreasing timestamps/negative deltas, binary payloads, duplicate
  Unicode/empty header names, and null versus empty keys, values, and headers.
- LogAppendTime rather than original record creation times.
- Sparse compacted batches with leading/trailing offset gaps.
- Delete-horizon batches with sparse records and a tombstone.
- A fully compacted empty batch that still carries validated offset progress.
- Real commit and abort control markers, omitted from application records.
- Transactional data visible under this client's read-uncommitted semantics.
- Offsets and timestamps above 2^53 and the -1 timestamp sentinel.

Offsets and timestamps are decimal strings in the JSON to avoid floating-point
rounding by JSON tooling. Payloads and UTF-8 header names are base64. The named
case list is asserted, so an accidentally empty or shortened fixture cannot pass.
This is a bounded codec oracle, not broker, failover, read-committed, compression,
or production-readiness qualification.

## Offline reproduction

Use Python 3, JDK 17+ (`java` and `javac`), and the separately installed official,
independently verified Kafka 4.0.2 distribution. The helper does not download or
install anything and does not start or contact a broker. From the repository root:

```sh
KAFKA_HOME=/path/to/kafka_2.13-4.0.2 \
  python3 tool/generate_record_batch_fixtures.py --check
```

The default mode is also check-only. A mismatch exits nonzero and leaves the
fixture untouched. To deliberately regenerate after reviewing the harness:

```sh
KAFKA_HOME=/path/to/kafka_2.13-4.0.2 \
  python3 tool/generate_record_batch_fixtures.py --write
```

Use `--java /path/to/java --javac /path/to/javac` when the JDK is not on PATH.
Only the following pinned JARs enter the classpath; a missing or different JAR
fails before any Java process starts:

- `kafka-clients-4.0.2.jar`, SHA256
  `90ef3a38696ee29e4f8dec2b2910bd31d8da0e69cb53a62a231a7d05fea32ad9`.
- `slf4j-api-1.7.36.jar`, SHA256
  `d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0`.

Compilation and execution have separate 60-second limits and 128 MiB Java heap
limits. Temporary class files are owned and removed by the helper. No class files
or JARs are checked in. Output contains no wall-clock timestamps or random data;
its provenance includes the generator's SHA256 with LF-normalized newlines.
The reference regeneration was repeated with Temurin 21.0.13+9. Ordinary tests
also run on the minimum Dart 3.2.6 and current tested Dart 3.12.0 SDKs.

## Attribution

The harness and synthetic fixtures are project test material under the root
Apache-2.0 license. They call separately installed Apache Kafka and SLF4J APIs,
without vendoring those implementations or their JARs. Apache Kafka's upstream
notice is retained at `third_party_licenses/kafka-4.0.2-NOTICE.txt`; see
`THIRD_PARTY_NOTICES.md` for distribution scope and external dependency notices.

Relevant Kafka 4.0.2 sources:

- [MemoryRecordsBuilder](https://github.com/apache/kafka/blob/4.0.2/clients/src/main/java/org/apache/kafka/common/record/MemoryRecordsBuilder.java)
- [DefaultRecordBatch](https://github.com/apache/kafka/blob/4.0.2/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java)
- [DefaultRecord](https://github.com/apache/kafka/blob/4.0.2/clients/src/main/java/org/apache/kafka/common/record/DefaultRecord.java)
