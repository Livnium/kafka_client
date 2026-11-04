import 'dart:convert';
import 'dart:typed_data';

import 'package:kafka_client/src/group_membership_protocol.dart';
import 'package:test/test.dart';

// Independent non-flexible fixtures. No production codec is used here.
List<int> i16(int n) => (ByteData(2)..setInt16(0, n)).buffer.asUint8List();
List<int> i32(int n) => (ByteData(4)..setInt32(0, n)).buffer.asUint8List();
List<int> str(String? s) =>
    s == null ? i16(-1) : [...i16(utf8.encode(s).length), ...utf8.encode(s)];
List<int> blob(List<int>? b) => b == null ? i32(-1) : [...i32(b.length), ...b];
List<int> frame(int key, int version, int id, List<int> body) {
  final payload = [
    ...i16(key),
    ...i16(version),
    ...i32(id),
    ...str('dart_kafka'),
    ...body
  ];
  return [...i32(payload.length), ...payload];
}

List<int> status(int id, {int error = 0}) =>
    [...i32(id), ...i32(23), ...i16(error)];
List<int> join(int id,
        {int error = 0,
        int generation = 7,
        String? protocol = 'range',
        String? leader = 'a',
        String? member = 'a',
        int count = 1,
        List<int>? entries}) =>
    [
      ...status(id, error: error),
      ...i32(generation),
      ...str(protocol),
      ...str(leader),
      ...str(member),
      ...i32(count),
      ...entries ??
          [
            ...str('a'),
            ...blob([0, 128, 255])
          ],
    ];
JoinGroupRequest joining(
        {String group = 'g',
        String member = '',
        int session = 1000,
        int rebalance = 2000,
        List<GroupJoinProtocol>? protocols}) =>
    JoinGroupRequest(group,
        sessionTimeoutMs: session,
        rebalanceTimeoutMs: rebalance,
        memberId: member,
        protocols: protocols ??
            [
              GroupJoinProtocol('range', [0, 128, 255])
            ]);

void main() {
  test('JoinGroup v4 exact framing and ordered protocol metadata', () {
    for (final member in ['', 'member-π']) {
      final r = joining(group: 'gπ', member: member, protocols: [
        GroupJoinProtocol('range', [0, 128, 255]),
        GroupJoinProtocol('roundrobin', [])
      ]);
      expect(r.apiKey, 11);
      expect(r.apiVersion, 4);
      expect(r.protocolType, 'consumer');
      expect(
          r.toBytes(),
          frame(11, 4, r.correlationId, [
            ...str('gπ'),
            ...i32(1000),
            ...i32(2000),
            ...str(member),
            ...str('consumer'),
            ...i32(2),
            ...str('range'),
            ...blob([0, 128, 255]),
            ...str('roundrobin'),
            ...blob([]),
          ]));
    }
  });

  test('SyncGroup v2 exact leader and follower requests', () {
    for (final assignments in <List<GroupAssignment>>[
      [],
      [
        GroupAssignment('a', [255, 0]),
        GroupAssignment('bπ', [])
      ]
    ]) {
      final r = SyncGroupRequest('g',
          generationId: 7, memberId: 'a', assignments: assignments);
      expect(r.apiKey, 14);
      expect(r.apiVersion, 2);
      expect(
          r.toBytes(),
          frame(14, 2, r.correlationId, [
            ...str('g'),
            ...i32(7),
            ...str('a'),
            ...i32(assignments.length),
            if (assignments.isNotEmpty) ...[
              ...str('a'),
              ...blob([255, 0]),
              ...str('bπ'),
              ...blob([])
            ],
          ]));
    }
  });

  test('Heartbeat and LeaveGroup v2 exact framing', () {
    final h = HeartbeatRequest('gπ', generationId: 0, memberId: 'a');
    final l = LeaveGroupRequest('gπ', memberId: 'a');
    expect(h.apiKey, 12);
    expect(h.apiVersion, 2);
    expect(l.apiKey, 13);
    expect(l.apiVersion, 2);
    expect(h.toBytes(),
        frame(12, 2, h.correlationId, [...str('gπ'), ...i32(0), ...str('a')]));
    expect(l.toBytes(),
        frame(13, 2, l.correlationId, [...str('gπ'), ...str('a')]));
  });

  test('request snapshots deeply freeze input lists', () {
    final metadata = [1, 2];
    final assignment = [3, 4];
    final protocols = [GroupJoinProtocol('range', metadata)];
    final assignments = [GroupAssignment('a', assignment)];
    final j = joining(protocols: protocols);
    final s = SyncGroupRequest('g',
        generationId: 1, memberId: 'a', assignments: assignments);
    final beforeJ = j.toBytes();
    final beforeS = s.toBytes();
    metadata[0] = 9;
    assignment.clear();
    protocols.clear();
    assignments.clear();
    expect(j.toBytes(), beforeJ);
    expect(s.toBytes(), beforeS);
    expect(() => j.protocols.clear(), throwsUnsupportedError);
    expect(() => j.protocols.first.metadata[0] = 9, throwsUnsupportedError);
    expect(() => s.assignments.clear(), throwsUnsupportedError);
    expect(
        () => s.assignments.first.assignment.clear(), throwsUnsupportedError);
  });

  test('validates request UTF8 strings by encoded length without replacement',
      () {
    final max = '${List.filled(16383, 'π').join()}x';
    final tooLong = List.filled(16384, 'π').join();
    for (final bad in ['', tooLong, '\ud800', '\udfff']) {
      expect(() => joining(group: bad), throwsArgumentError);
      expect(() => GroupJoinProtocol(bad, []), throwsArgumentError);
      expect(() => GroupAssignment(bad, []), throwsArgumentError);
      expect(() => SyncGroupRequest(bad, generationId: 0, memberId: 'a'),
          throwsArgumentError);
      expect(() => SyncGroupRequest('g', generationId: 0, memberId: bad),
          throwsArgumentError);
      expect(() => HeartbeatRequest(bad, generationId: 0, memberId: 'a'),
          throwsArgumentError);
      expect(() => HeartbeatRequest('g', generationId: 0, memberId: bad),
          throwsArgumentError);
      expect(() => LeaveGroupRequest(bad, memberId: 'a'), throwsArgumentError);
      expect(() => LeaveGroupRequest('g', memberId: bad), throwsArgumentError);
      if (bad.isNotEmpty) {
        expect(() => joining(member: bad), throwsArgumentError);
      }
    }
    expect(
        () => joining(
            group: max,
            member: max,
            protocols: [GroupJoinProtocol(max, [])]).toBytes(),
        returnsNormally);
    expect(
        () => SyncGroupRequest(max,
            generationId: 2147483647,
            memberId: max,
            assignments: [GroupAssignment(max, [])]).toBytes(),
        returnsNormally);
    expect(
        () => HeartbeatRequest(max, generationId: 2147483647, memberId: max)
            .toBytes(),
        returnsNormally);
    expect(
        () => LeaveGroupRequest(max, memberId: max).toBytes(), returnsNormally);
  });

  test('validates request timeout/generation bounds, bytes, and duplicate keys',
      () {
    for (final n in [-1, 0, 2147483648]) {
      expect(() => joining(session: n), throwsArgumentError);
      expect(() => joining(rebalance: n), throwsArgumentError);
    }
    // The schema does not impose a relation between the two timeouts.
    expect(() => joining(session: 2147483647, rebalance: 1).toBytes(),
        returnsNormally);
    for (final n in [-1, 2147483648]) {
      expect(() => SyncGroupRequest('g', generationId: n, memberId: 'a'),
          throwsArgumentError);
      expect(() => HeartbeatRequest('g', generationId: n, memberId: 'a'),
          throwsArgumentError);
    }
    for (final n in [-1, 256]) {
      expect(() => GroupJoinProtocol('range', [n]), throwsArgumentError);
      expect(() => GroupAssignment('a', [n]), throwsArgumentError);
      expect(() => JoinedGroupMember('a', [n]), throwsArgumentError);
      expect(() => SyncGroupResponse(0, 0, 0, [n]), throwsArgumentError);
    }
    expect(() => joining(protocols: []), throwsArgumentError);
    expect(
        () => joining(protocols: [
              GroupJoinProtocol('range', []),
              GroupJoinProtocol('range', [1])
            ]),
        throwsArgumentError);
    expect(
        () =>
            SyncGroupRequest('g', generationId: 1, memberId: 'a', assignments: [
              GroupAssignment('a', []),
              GroupAssignment('a', [1])
            ]),
        throwsArgumentError);
  });

  test(
      'JoinGroup parses success, followers, error sentinels and member-ID handshake',
      () {
    final r = joining();
    final response = r.parseResponse(join(r.correlationId));
    expect(response.throttleTimeMs, 23);
    expect(response.errorCode, 0);
    expect(response.generationId, 7);
    expect(response.protocolName, 'range');
    expect(response.leaderId, 'a');
    expect(response.memberId, 'a');
    expect(response.members.single.memberId, 'a');
    expect(response.members.single.metadata, [0, 128, 255]);
    expect(response.toJson()['members'], [
      {
        'memberId': 'a',
        'metadata': [0, 128, 255]
      }
    ]);
    expect(
        r
            .parseResponse(
                join(r.correlationId, member: 'b', count: 0, entries: []))
            .members,
        isEmpty);
    for (final error in [79, 22, 25, 27, -1]) {
      final result = r.parseResponse(join(r.correlationId,
          error: error,
          generation: -1,
          protocol: '',
          leader: '',
          member: error == 79 ? 'assigned' : '',
          count: 0,
          entries: []));
      expect(result.errorCode, error);
      expect(result.generationId, -1);
      expect(result.memberId, error == 79 ? 'assigned' : '');
    }
  });

  test('JoinGroup success-only invariants and offered protocol matching', () {
    final r = joining();
    final malformed = [
      join(r.correlationId, generation: -1),
      join(r.correlationId, protocol: ''),
      join(r.correlationId, leader: ''),
      join(r.correlationId, member: ''),
      join(r.correlationId, entries: [...str(''), ...blob([])]),
    ];
    for (final bytes in malformed) {
      expect(() => r.parseResponse(bytes), throwsFormatException);
    }
    expect(() => r.parseResponse(join(r.correlationId, protocol: 'unoffered')),
        throwsFormatException);
    expect(
        r
            .parseResponse(
                join(r.correlationId, error: 23, protocol: 'unoffered'))
            .errorCode,
        23);
    expect(
        JoinGroupResponse.fromBytes(
                join(r.correlationId, protocol: 'unoffered'))
            .protocolName,
        'unoffered');
  });

  test('successful JoinGroup enforces leader-only lists containing the leader',
      () {
    final r = joining();
    final members = [
      ...str('a'),
      ...blob([]),
      ...str('b'),
      ...blob([1])
    ];
    expect(
        r
            .parseResponse(join(r.correlationId, count: 2, entries: members))
            .members
            .map((m) => m.memberId),
        ['a', 'b']);
    expect(
        r
            .parseResponse(
                join(r.correlationId, member: 'b', count: 0, entries: []))
            .memberId,
        'b');
    for (final bytes in [
      join(r.correlationId, count: 0, entries: []),
      join(r.correlationId, entries: [...str('b'), ...blob([])]),
      join(r.correlationId, member: 'b'),
      join(r.correlationId, member: 'b', count: 2, entries: members),
    ]) {
      expect(() => r.parseResponse(bytes), throwsFormatException);
      expect(() => JoinGroupResponse.fromBytes(bytes), throwsFormatException);
    }
    // Broker errors remain readable even with success-inconsistent IDs/lists.
    for (final bytes in [
      join(r.correlationId, error: 25, count: 0, entries: []),
      join(r.correlationId, error: 25, member: 'b', count: 2, entries: members),
      join(r.correlationId, error: 25, entries: [...str('b'), ...blob([])]),
    ]) {
      expect(r.parseResponse(bytes).errorCode, 25);
    }
  });

  test('successful rejoin preserves requested member ID, including followers',
      () {
    final leader = joining(member: 'a');
    final follower = joining(member: 'b');
    expect(leader.parseResponse(join(leader.correlationId)).memberId, 'a');
    expect(
        follower
            .parseResponse(join(follower.correlationId,
                member: 'b', count: 0, entries: []))
            .memberId,
        'b');
    expect(() => follower.parseResponse(join(follower.correlationId)),
        throwsFormatException);
    expect(
        () => leader.parseResponse(
            join(leader.correlationId, member: 'b', count: 0, entries: [])),
        throwsFormatException);
    final fresh = joining();
    expect(fresh.parseResponse(join(fresh.correlationId)).memberId, 'a');
    for (final member in ['', 'new-id']) {
      expect(
          leader
              .parseResponse(join(leader.correlationId,
                  error: 79,
                  generation: -1,
                  protocol: '',
                  leader: '',
                  member: member,
                  count: 0,
                  entries: []))
              .memberId,
          member);
    }
  });

  test('response byte/list snapshots cannot be mutated', () {
    final raw = join(2);
    final r = JoinGroupResponse.fromBytes(raw);
    raw.fillRange(0, raw.length, 0);
    expect(r.members.single.metadata, [0, 128, 255]);
    expect(() => r.members.clear(), throwsUnsupportedError);
    expect(() => r.members.first.metadata.clear(), throwsUnsupportedError);
    final metadata = [3];
    final members = [JoinedGroupMember('a', metadata)];
    final manual = JoinGroupResponse(2, 0, 0, 1, 'range', 'a', 'a', members);
    metadata[0] = 4;
    members.clear();
    expect(manual.members.single.metadata, [3]);
    final assignment = [5];
    final sync = SyncGroupResponse(2, 0, 0, assignment);
    assignment[0] = 6;
    expect(sync.assignment, [5]);
    expect(() => sync.assignment.clear(), throwsUnsupportedError);
  });

  test('all simple responses preserve error codes and opaque assignments', () {
    final s = SyncGroupRequest('g', generationId: 1, memberId: 'a');
    final h = HeartbeatRequest('g', generationId: 1, memberId: 'a');
    final l = LeaveGroupRequest('g', memberId: 'a');
    for (final error in [0, 16, 22, 25, 27, -1]) {
      final sr = s.parseResponse([
        ...status(s.correlationId, error: error),
        ...blob([255, 0])
      ]);
      final hr = h.parseResponse(status(h.correlationId, error: error));
      final lr = l.parseResponse(status(l.correlationId, error: error));
      expect(sr.errorCode, error);
      expect(hr.errorCode, error);
      expect(lr.errorCode, error);
      expect(sr.assignment, [255, 0]);
      for (final r in [sr, hr, lr]) {
        expect(r.toJson()['throttleTimeMs'], 23);
        expect(r.toJson()['errorCode'], error);
      }
    }
    expect(
        s.parseResponse([...status(s.correlationId), ...blob([])]).assignment,
        isEmpty);
  });

  test(
      'every response rejects every truncated prefix, extra bytes, and nonbytes',
      () {
    final parsers = <List<dynamic>>[
      [JoinGroupResponse.fromBytes, join(1)],
      [
        SyncGroupResponse.fromBytes,
        [
          ...status(1),
          ...blob([1, 2, 3])
        ]
      ],
      [HeartbeatResponse.fromBytes, status(1)],
      [LeaveGroupResponse.fromBytes, status(1)],
    ];
    for (final pair in parsers) {
      final parse = pair[0] as Function;
      final bytes = pair[1] as List<int>;
      for (var i = 0; i < bytes.length; i++) {
        expect(() => parse(bytes.sublist(0, i)), throwsFormatException,
            reason: 'prefix $i');
      }
      expect(() => parse([...bytes, 0]), throwsFormatException);
      for (final invalid in [-1, 256]) {
        expect(() => parse([invalid, ...bytes.skip(1)]), throwsFormatException);
      }
    }
  });

  test('every request rejects a mismatched response correlation ID', () {
    final j = joining();
    final s = SyncGroupRequest('g', generationId: 1, memberId: 'a');
    final h = HeartbeatRequest('g', generationId: 1, memberId: 'a');
    final l = LeaveGroupRequest('g', memberId: 'a');
    expect(() => j.parseResponse(join(j.correlationId + 1)),
        throwsFormatException);
    expect(() => s.parseResponse([...status(s.correlationId + 1), ...blob([])]),
        throwsFormatException);
    expect(() => h.parseResponse(status(h.correlationId + 1)),
        throwsFormatException);
    expect(() => l.parseResponse(status(l.correlationId + 1)),
        throwsFormatException);
  });

  test(
      'JoinGroup rejects nulls, invalid UTF8, duplicate members and bad counts',
      () {
    for (final bytes in [
      join(1, protocol: null),
      join(1, leader: null),
      join(1, member: null),
      join(1, entries: [...str(null), ...blob([])]),
      join(1, entries: [...str('a'), ...blob(null)]),
      join(1, entries: [...i16(1), 255, ...blob([])]),
      join(1, entries: [...i16(-2), ...blob([])]),
      join(1, entries: [...str('a'), ...i32(-2)]),
      join(1, entries: [...str('a'), ...i32(2147483647)]),
      join(1, count: -1),
      join(1, count: -2),
      join(1, count: 2147483647),
      join(1,
          count: 2,
          entries: [...str('a'), ...blob([]), ...str('a'), ...blob([])]),
      join(1,
          error: 25,
          count: 2,
          entries: [...str('a'), ...blob([]), ...str('a'), ...blob([])]),
    ]) {
      expect(() => JoinGroupResponse.fromBytes(bytes), throwsFormatException);
    }
  });

  test('SyncGroup rejects null/invalid/truncated assignment lengths', () {
    for (final length in [-1, -2, 1, 2147483647]) {
      expect(() => SyncGroupResponse.fromBytes([...status(1), ...i32(length)]),
          throwsFormatException);
    }
  });
}
