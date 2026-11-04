import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/src/group_offsets_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:kafka_client/src/api/abstract/kafka_request.dart';
import 'package:test/test.dart';

List<int> hex(String s) => [
      for (var i = 0; i < s.length; i += 2)
        int.parse(s.substring(i, i + 2), radix: 16)
    ];
final manifest = jsonDecode(
        File('test/support/offset_fetch_v7/manifest.json').readAsStringSync())
    as Map<String, dynamic>;
List<int> validResponse(Map<String, dynamic> c) {
  final bytes = hex(c['response']['hex'] as String).sublist(4);
  // Locate the deliberately wire-valid, semantically invalid signed minima.
  final marker = hex('800000000000000080000000');
  final position = List.generate(bytes.length - marker.length + 1, (i) => i)
      .singleWhere((i) =>
          List.generate(marker.length, (j) => bytes[i + j] == marker[j])
              .every((v) => v));
  bytes.replaceRange(position, position + marker.length, List.filled(12, 255));
  return bytes;
}

void main() {
  for (final raw in manifest['cases'] as List) {
    final c = raw as Map<String, dynamic>;
    final version = c['version'] as int;
    test('official ${c['name']} response: strict boundaries, UTF8, tags', () {
      expect(
          () => GroupOffsetFetchResponse.fromBytes(
              hex(c['response']['hex']).sublist(4),
              apiVersion: version),
          throwsFormatException);
      final response = GroupOffsetFetchResponse.fromBytes(validResponse(c),
          apiVersion: version);
      expect(response.correlationId, c['correlationId']);
      expect(response.throttleTimeMs, 123);
      expect(response.errorCode, c['decodedResponse']['errorCode']);
      final topic = response.topics.single;
      expect(topic.name, '主题-🙂-café');
      final ps = topic.partitions;
      expect(ps.map((p) => p.committedOffset),
          [-1, 0, 4294967297, 9007199254740993, 9223372036854775807, -1]);
      expect(ps.map((p) => p.metadata),
          [null, '', '非空-🙂-café', 'metadata', null, 'signed-wire-boundary']);
      expect(ps[5].errorCode, 23);
      for (var n = 0; n < validResponse(c).length; n++) {
        expect(
            () => GroupOffsetFetchResponse.fromBytes(
                validResponse(c).sublist(0, n),
                apiVersion: version),
            throwsFormatException,
            reason: 'truncation $n');
      }
      expect(
          () => GroupOffsetFetchResponse.fromBytes([...validResponse(c), 0],
              apiVersion: version),
          throwsFormatException);
    });
    if (c['tagged'] == false && (version == 5 || c['requireStable'] == true)) {
      test('exact official ${c['name']} request body and header', () {
        final request = GroupOffsetFetchRequest(
            '组-雪-🙂-café', '主题-🙂-café', {0, 1, 2, 3, 4, 5},
            requireStable: c['requireStable']);
        final reader =
            KafkaBytesReader.fromBytes(hex(c['request']['hex']).sublist(4));
        expect(reader.readInt16(), 9);
        expect(reader.readInt16(), request.apiVersion);
        reader.readInt32();
        reader.readString();
        final normalized = KafkaBytesBuilder.withRequestHeader(
            9, version, request.correlationId)
          ..addRaw(reader.readRaw(reader.remaining));
        expect(request.toBytes(), KafkaRequest.frame(normalized.takeBytes()));
      });
    }
  }
  test('default is v5; stable explicitly selects v7 and correlation matches',
      () {
    expect(GroupOffsetFetchRequest('g', 't', {0}).apiVersion, 5);
    final request = GroupOffsetFetchRequest('g', 't', {0}, requireStable: true);
    expect(request.apiVersion, 7);
    final c = (manifest['cases'] as List)[2] as Map<String, dynamic>;
    expect(
        () => request.parseResponse(validResponse(c)), throwsFormatException);
    expect(() => GroupOffsetFetchResponse.fromBytes([], apiVersion: 6),
        throwsArgumentError);
  });
  final prefix = hex('000000010000000000'); // ID, empty header tags, throttle.
  List<int> emptyBody(List<int> topics) => [...prefix, ...topics, 0, 0, 0];
  for (final bad in <List<int>>[
    [0], // Null topic array.
    [255, 255, 255, 255, 16], // uint32 overflow.
    [129, 0], // Noncanonical.
    [128, 128, 128, 128, 128], // Unterminated.
    [255, 255, 255, 255, 15], // Impossible bounded count.
    [2, 0, 1, 0], // Null topic name.
    [2, 2, 255, 1, 0], // Invalid UTF8 topic.
    [2, 1, 0, 0], // Null partition array.
  ]) {
    test(
        'reject malformed compact body $bad',
        () => expect(
            () => GroupOffsetFetchResponse.fromBytes(emptyBody(bad),
                apiVersion: 7),
            throwsFormatException));
  }
  for (final tags in <List<int>>[
    [2, 1, 0, 1, 0], // Duplicate.
    [2, 2, 0, 1, 0], // Unsorted.
    [1, 0, 100], // Truncated payload.
    [128, 0], // Noncanonical count.
    [1, 128, 0, 0], // Noncanonical tag.
    [1, 0, 128, 0], // Noncanonical size.
    [1, 255, 255, 255, 255, 16, 0], // Overflow tag.
    [1, 0, 255, 255, 255, 255, 16], // Overflow size.
  ]) {
    test('reject malformed header and body tags $tags', () {
      expect(
          () => GroupOffsetFetchResponse.fromBytes(
              [0, 0, 0, 1, ...tags, 0, 0, 0, 0, 1, 0, 0, 0],
              apiVersion: 7),
          throwsFormatException);
      expect(
          () => GroupOffsetFetchResponse.fromBytes(
              [...prefix, 1, 0, 0, ...tags],
              apiVersion: 7),
          throwsFormatException);
    });
  }
  test('multibyte compact request lengths use unsigned (not zigzag) varints',
      () {
    final request = GroupOffsetFetchRequest(
        'g' * 128, 't' * 128, Set.from(List.generate(128, (i) => i)),
        requireStable: true);
    final r = KafkaBytesReader.fromBytes(request.toBytes());
    r.readInt32();
    r.readInt16();
    r.readInt16();
    r.readInt32();
    r.readString();
    expect(r.readInt8(), 0);
    expect(r.readRaw(2), [129, 1]);
    r.readRaw(128);
    expect(r.readInt8(), 2);
    expect(r.readRaw(2), [129, 1]);
    r.readRaw(128);
    expect(r.readRaw(2), [129, 1]);
    for (var i = 0; i < 128; i++) {
      expect(r.readInt32(), i);
    }
    expect(r.readRaw(3), [0, 1, 0]);
    expect(r.isEOF, isTrue);
  });
  for (final tags in <List<int>>[
    [2, 1, 0, 1, 0],
    [2, 2, 0, 1, 0],
    [1, 0, 100],
    [128, 0],
    [1, 0, 128, 0]
  ]) {
    test('strict partition/topic struct tags $tags', () {
      final partition = (KafkaBytesBuilder()
            ..addInt32(0)
            ..addInt64(-1)
            ..addInt32(-1)
            ..addInt8(0)
            ..addInt16(0))
          .takeBytes();
      final head = [...prefix, 2, 2, 116, 2, ...partition];
      expect(
          () => GroupOffsetFetchResponse.fromBytes(
              [...head, ...tags, 0, 0, 0, 0],
              apiVersion: 7),
          throwsFormatException);
      expect(
          () => GroupOffsetFetchResponse.fromBytes(
              [...head, 0, ...tags, 0, 0, 0],
              apiVersion: 7),
          throwsFormatException);
    });
  }
}
