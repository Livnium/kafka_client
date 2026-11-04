import 'dart:convert';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart' hide KafkaType;
import 'package:test/test.dart';

void main() {
  test('object array elements have no spurious size prefixes', () {
    final b = KafkaBytesBuilder()
      ..addArray([1, 2], KafkaType.object,
          (int v) => (KafkaBytesBuilder()..addInt16(v)).takeBytes());
    expect(b.takeBytes(), [0, 0, 0, 2, 0, 1, 0, 2]);
  });
  test('fixed-width signed integer boundaries and UTF8 lengths', () {
    final b = KafkaBytesBuilder()
      ..addInt8(-128)
      ..addInt16(-32768)
      ..addInt32(-2147483648)
      ..addInt64(-9223372036854775808)
      ..addString('π🙂')
      ..addString(null)
      ..addBytes([])
      ..addBytes(null);
    final r = KafkaBytesReader.fromBytes(b.takeBytes());
    expect(r.readInt8(), -128);
    expect(r.readInt16(), -32768);
    expect(r.readInt32(), -2147483648);
    expect(r.readInt64(), -9223372036854775808);
    expect(r.readString(), 'π🙂');
    expect(r.readString(), isNull);
    expect(r.readBytes(), isEmpty);
    expect(r.readBytes(), isNull);
    expect(r.isEOF, isTrue);
  });
  test('zigzag golden vectors', () {
    final vectors = <int, List<int>>{
      0: [0],
      -1: [1],
      1: [2],
      -64: [127],
      64: [128, 1],
      2147483647: [254, 255, 255, 255, 15],
      -2147483648: [255, 255, 255, 255, 15]
    };
    for (final e in vectors.entries) {
      expect((KafkaBytesBuilder()..addVarInt(e.key)).takeBytes(), e.value);
      expect(KafkaBytesReader.fromBytes(e.value).readVarInt(), e.key);
    }
  });
  test('varlong signed extrema roundtrip without precision loss', () {
    for (final v in [-9223372036854775808, 9223372036854775807, -1, 0, 1]) {
      final b = KafkaBytesBuilder()..addVarLong(v);
      expect(KafkaBytesReader.fromBytes(b.takeBytes()).readVarLong(), v);
    }
  });
  test('malformed varints fail boundedly', () {
    for (final bytes in [
      [128],
      [128, 0],
      [255, 255, 255, 255, 16],
      [128, 128, 128, 128, 128, 0]
    ]) {
      expect(() => KafkaBytesReader.fromBytes(bytes).readVarInt(),
          throwsFormatException);
    }
    expect(() => KafkaBytesReader.fromBytes(List.filled(10, 255)).readVarLong(),
        throwsFormatException);
  });
  test('negative raw lengths and truncated primitives fail', () {
    expect(() => KafkaBytesReader.fromBytes([0]).readInt32(),
        throwsFormatException);
    expect(() => KafkaBytesReader.fromBytes([0]).readRaw(-1),
        throwsFormatException);
    expect(() => KafkaBytesReader.fromBytes([255, 254]).readString(),
        throwsFormatException);
    expect(() => KafkaBytesReader.fromBytes([0, 1, 255]).readString(),
        throwsFormatException);
    expect(() => KafkaBytesReader.fromBytes([2]).readBoolean(),
        throwsFormatException);
  });
  test('builder rejects overflow and overlong strings', () {
    expect(() => KafkaBytesBuilder().addInt16(32768), throwsArgumentError);
    expect(
        () => KafkaBytesBuilder().addVarInt(2147483648), throwsArgumentError);
    expect(
        () =>
            KafkaBytesBuilder().addString(utf8.decode(List.filled(32768, 97))),
        throwsArgumentError);
  });
}
