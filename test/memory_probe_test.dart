import 'package:kafka_client/kafka_protocol.dart';
import 'package:test/test.dart';

import '../tool/measure_client_memory.dart' as probe;

void main() {
  test('standalone duration stays within ten seconds and thirty minutes', () {
    expect(probe.probeDurationSeconds(null), 10);
    for (final seconds in ['10', '60', '1800']) {
      expect(probe.probeDurationSeconds(seconds), int.parse(seconds));
    }
    for (final invalid in ['9', '1801', '0', '-1', 'invalid', '']) {
      expect(probe.probeDurationSeconds(invalid), isNull);
    }
  });
  group('standalone memory probe byte comparison', () {
    test('handles null, empty, lengths and byte order', () {
      expect(probe.bytesEqual(null, null), isTrue);
      expect(probe.bytesEqual(null, []), isFalse);
      expect(probe.bytesEqual([], null), isFalse);
      expect(probe.bytesEqual([], []), isTrue);
      expect(probe.bytesEqual([1, 2], [1, 2]), isTrue);
      expect(probe.bytesEqual([1], [1, 2]), isFalse);
      expect(probe.bytesEqual([1, 2], [2, 1]), isFalse);
    });
  });

  group('standalone memory probe offset coverage', () {
    test('merges adjacent out-of-order ranges, including a bridge', () {
      final coverage = probe.OffsetCoverage();
      coverage.add(16, 8);
      coverage.add(0, 8);
      expect(coverage.ranges, [
        [0, 7],
        [16, 23]
      ]);
      coverage.add(8, 8);
      expect(coverage.ranges, [
        [0, 23]
      ]);
      coverage.add(24, 8);
      expect(coverage.ranges, [
        [0, 31]
      ]);
    });
    test('rejects duplicate, nested and partial overlaps', () {
      for (final overlapping in [
        [8, 8],
        [9, 1],
        [0, 9],
        [15, 8]
      ]) {
        final coverage = probe.OffsetCoverage()..add(8, 8);
        expect(() => coverage.add(overlapping[0], overlapping[1]),
            throwsStateError);
        expect(coverage.ranges, [
          [8, 15]
        ]);
      }
    });
    test('rejects a 65th disjoint range rather than growing unbounded', () {
      final coverage = probe.OffsetCoverage();
      for (var i = 0; i < 64; i++) {
        coverage.add(i * 2, 1);
      }
      expect(coverage.ranges.length, 64);
      expect(() => coverage.add(128, 1), throwsStateError);
    });
  });

  group('standalone memory probe record validation', () {
    test('round-trips exact operational payloads, keys and worker headers', () {
      for (var worker = 0; worker < 4; worker++) {
        final records = probe.makeRecords(worker, 7);
        expect(records.length, 8);
        expect(records.first.value!.length, 512);
        expect(records.first.value!.take(3), [7, 8, 9]);
        final decoded = probe.codecRoundTrip('probe-topic', records);
        expect(decoded.length, 8);
        probe.validate(records, decoded, 0, worker);
      }
    });
    test('rejects missing records, offset, key, value and header mismatches',
        () {
      final records = probe.makeRecords(2, 3);
      final decoded = probe.codecRoundTrip('probe-topic', records);
      final original = decoded.first;
      expect(() => probe.validate(records, decoded.sublist(1), 0, 2),
          throwsStateError);
      expect(() => probe.validate(records, decoded, 1, 2), throwsStateError);
      final replacements = [
        KafkaRecord(
            key: [99], value: original.value, headers: original.headers),
        KafkaRecord(key: original.key, value: [99], headers: original.headers),
        KafkaRecord(key: original.key, value: original.value),
        KafkaRecord(key: original.key, value: original.value, headers: [
          KafkaRecordHeader('other', [2])
        ]),
        KafkaRecord(key: original.key, value: original.value, headers: [
          KafkaRecordHeader('worker', [1])
        ]),
        KafkaRecord(key: original.key, value: original.value, headers: [
          KafkaRecordHeader('worker', [2]),
          KafkaRecordHeader('worker', [2])
        ]),
      ];
      for (final replacement in replacements) {
        expect(
            () => probe.validate(
                records, [replacement, ...decoded.skip(1)], 0, 2),
            throwsStateError);
      }
    });
  });
}
