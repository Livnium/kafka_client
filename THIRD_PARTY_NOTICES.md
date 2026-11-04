# Third-party material and distribution scope

The original `kafka_client` code is copyright Livnium and is licensed under the
Apache License, Version 2.0 in `LICENSE`. This does not change the ownership or
license of third-party material. `NOTICE` intentionally does not invent a creation
year or a legal suffix for the designated copyright owner.

## Package payload

The native Dart library has no third-party runtime package dependencies in
`pubspec.yaml`. Its runtime imports use Dart SDK libraries and this package's own
code. The commented `kafkabr` line is not an enabled dependency. This is a source
inventory observation, not proof that every line was independently authored.

The package's Java fixture helpers call separately installed Kafka/Jackson APIs.
They are project test harnesses, not copies of the official Kafka implementation
merely because their names describe an official-codec oracle. Kafka/Jackson JARs,
the JDK, Dart/Flutter SDKs and resolved Dart development packages are not bundled
in the curated package. Those dependencies retain their own licenses; review
their notices separately if bundling them in another distribution.

The following retained development material is excluded by `.pubignore`:
`verification/` and `doc/api/`. Their notices are retained here to distinguish the
full development repository from the smaller package, not to imply that those
components are runtime dependencies of the client.

## Apache Kafka source captures

The files under
`verification/stable_offsets/evidence/source-review/` and
`verification/stable_offsets/evidence/official-client-review/` include Apache
Kafka Java, Scala and protocol-schema source. Their existing Apache headers remain
intact. The Apache License, Version 2.0 is included in the root `LICENSE`.

Upstream notices are preserved separately, without changing their wording:

- `third_party_licenses/kafka-3.9.0-NOTICE.txt`, from
  https://github.com/apache/kafka/blob/3.9.0/NOTICE
- `third_party_licenses/kafka-4.0.2-NOTICE.txt`, from
  https://github.com/apache/kafka/blob/4.0.2/NOTICE
- `third_party_licenses/kafka-4.1.0-NOTICE.txt`, from
  https://github.com/apache/kafka/blob/4.1.0/NOTICE
- `third_party_licenses/kafka-trunk-NOTICE.txt`, retrieved 2026-11-04 from
  https://github.com/apache/kafka/blob/trunk/NOTICE, blob
  `eaa0b8690d2269321ae0073dac4a3b8ac5c4564a`.

The complete upstream notices mention components of Kafka's own distribution,
including Jersey, jqwik or streams code where applicable. Keeping those notices
does not mean those components are bundled in this Dart package. The retained
source manifest identifies the captured source versions and hashes. The moving
trunk notice is a dated capture, not evidence that future trunk content is covered.

## Historical generated API documentation

`doc/api/` came with the original import and is excluded from package publication.
It contains identifiable third-party material:

- `static-assets/highlight.pack.js` identifies Highlight.js 11.8.0 and retains
  its Ivan Sagalaev/contributors attribution and BSD-3-Clause header. The complete
  upstream license is in `third_party_licenses/HIGHLIGHTJS-LICENSE.txt`, from
  https://github.com/highlightjs/highlight.js/blob/11.8.0/LICENSE.
- `static-assets/github.css` retains its Vasily Polovnyov theme attribution.
- The `docs.dart.js.map` source list identifies Dart SDK runtime sources and
  Dartdoc web/search sources. The Dart SDK BSD license is included in
  `third_party_licenses/DART-SDK-LICENSE.txt` and the Dartdoc BSD license in
  `third_party_licenses/DARTDOC-LICENSE.txt`. The latter was retrieved from
  https://github.com/dart-lang/dartdoc/blob/main/LICENSE, blob
  `000cd7becadd0c8369478f825174e164de9b8f91`.

The exact tool versions and provenance of every historical generated asset were
not recorded in the original import. The SDK license copy came from the retained
Dart 3.2.6 installation; this does not establish which SDK generated those assets.
The included licenses cover identified components, not a complete clearance of
the old generated output. Keep it excluded from public package releases. Before
publicly redistributing the full historical docs, finish its asset/version review
or generate a fresh, independently audited documentation distribution.

## Development dependencies and retained evidence

`lints` and `test` are development dependencies, with their transitive dependency
versions recorded in historical lockfiles. Those packages are resolved externally,
not vendored in the package payload. Historical evidence also contains small
project-generated class files and source snapshots. No rights in upstream code are
transferred to Livnium by the presence of those files or by the root license.

The original client source was confirmed by its author as their own work, with
Livnium designated as the copyright owner. The original import supplied no prior
Git history. This inventory is bounded by the reviewed repository and package
contents; it is not an independent legal title opinion or a claim of exhaustive
source-similarity checking. Review the exact final distribution before publication.
