import 'dart:convert';

import 'api/abstract/kafka_request.dart';
import 'api/abstract/kafka_response.dart';
import 'codec/byte_builder.dart';
import 'codec/byte_reader.dart' hide KafkaType;

// Non-flexible schemas verified against Apache Kafka 4.0.2 message JSON:
// JoinGroup v4, SyncGroup v2, Heartbeat v2, LeaveGroup v2.
// These are dynamic classic-group primitives, not a membership lifecycle.
void _string(String value, String name, {bool allowEmpty = false}) {
  final encoded = utf8.encode(value);
  if ((!allowEmpty && value.isEmpty) ||
      encoded.length > 32767 ||
      utf8.decode(encoded) != value) {
    throw ArgumentError('Invalid $name');
  }
}

void _int32(int value, String name, {int minimum = 0}) {
  if (value < minimum || value > 2147483647) {
    throw ArgumentError('Invalid $name');
  }
}

List<int> _snapshotBytes(List<int> bytes) {
  _int32(bytes.length, 'bytes length');
  if (bytes.any((b) => b < 0 || b > 255)) {
    throw ArgumentError('Bytes must be in 0..255');
  }
  return List<int>.unmodifiable(bytes);
}

KafkaBytesReader _reader(List<int> bytes) {
  if (bytes.any((b) => b < 0 || b > 255)) {
    throw const FormatException('Invalid response bytes');
  }
  return KafkaBytesReader.fromBytes(bytes);
}

String _requiredString(KafkaBytesReader r) {
  final value = r.readString();
  if (value == null) throw const FormatException('Unexpected null string');
  return value;
}

List<int> _requiredBytes(KafkaBytesReader r) {
  final value = r.readBytes();
  if (value == null) throw const FormatException('Unexpected null bytes');
  return value;
}

void _end(KafkaBytesReader r) {
  if (!r.isEOF) throw const FormatException('Trailing response bytes');
}

T _matching<T extends KafkaResponse>(T response, int id) {
  if (response.correlationId != id) {
    throw const FormatException('Mismatched response correlation ID');
  }
  return response;
}

/// One offered consumer protocol with opaque, non-null metadata.
class GroupJoinProtocol {
  final String name;
  final List<int> metadata;

  GroupJoinProtocol(this.name, List<int> metadata)
      : metadata = _snapshotBytes(metadata) {
    _string(name, 'protocol name');
  }

  Map<String, dynamic> toJson() => {'name': name, 'metadata': metadata};
}

/// JoinGroup v4. An empty member ID starts a dynamic membership handshake.
/// Error 79 (MEMBER_ID_REQUIRED) is returned to the caller without retrying.
class JoinGroupRequest extends KafkaRequest {
  final int apiKey = 11;
  final int apiVersion = 4;
  final String protocolType = 'consumer';
  final String groupId;
  final int sessionTimeoutMs;
  final int rebalanceTimeoutMs;
  final String memberId;
  final List<GroupJoinProtocol> protocols;

  JoinGroupRequest(this.groupId,
      {required this.sessionTimeoutMs,
      required this.rebalanceTimeoutMs,
      this.memberId = '',
      required List<GroupJoinProtocol> protocols})
      : protocols = List.unmodifiable(protocols) {
    _string(groupId, 'groupId');
    _string(memberId, 'memberId', allowEmpty: true);
    _int32(sessionTimeoutMs, 'sessionTimeoutMs', minimum: 1);
    _int32(rebalanceTimeoutMs, 'rebalanceTimeoutMs', minimum: 1);
    _int32(this.protocols.length, 'protocol count', minimum: 1);
    final names = <String>{};
    for (final protocol in this.protocols) {
      if (!names.add(protocol.name)) {
        throw ArgumentError('Duplicate protocol name');
      }
    }
  }

  @override
  List<int> toBytes() {
    final b =
        KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
          ..addString(groupId)
          ..addInt32(sessionTimeoutMs)
          ..addInt32(rebalanceTimeoutMs)
          ..addString(memberId)
          ..addString(protocolType)
          ..addInt32(protocols.length);
    for (final protocol in protocols) {
      b
        ..addString(protocol.name)
        ..addBytes(protocol.metadata);
    }
    return KafkaRequest.frame(b.takeBytes());
  }

  @override
  JoinGroupResponse parseResponse(List<int> bytes) {
    final response =
        _matching(JoinGroupResponse.fromBytes(bytes), correlationId);
    if (response.errorCode == 0 &&
        !protocols.any((p) => p.name == response.protocolName)) {
      throw const FormatException('Coordinator selected an unoffered protocol');
    }
    if (response.errorCode == 0 &&
        memberId.isNotEmpty &&
        response.memberId != memberId) {
      throw const FormatException('Coordinator changed an existing member ID');
    }
    return response;
  }
}

/// A member and its opaque metadata returned to the group leader.
class JoinedGroupMember {
  final String memberId;
  final List<int> metadata;

  JoinedGroupMember(this.memberId, List<int> metadata)
      : metadata = _snapshotBytes(metadata) {
    _string(memberId, 'memberId', allowEmpty: true);
  }

  Map<String, dynamic> toJson() => {'memberId': memberId, 'metadata': metadata};
}

class JoinGroupResponse extends KafkaResponse {
  final int throttleTimeMs;
  final int errorCode;
  final int generationId;
  final String protocolName;
  final String leaderId;
  final String memberId;
  final List<JoinedGroupMember> members;

  JoinGroupResponse(
      super.correlationId,
      this.throttleTimeMs,
      this.errorCode,
      this.generationId,
      this.protocolName,
      this.leaderId,
      this.memberId,
      List<JoinedGroupMember> members)
      : members = List.unmodifiable(members);

  factory JoinGroupResponse.fromBytes(List<int> bytes) {
    final r = _reader(bytes);
    final id = r.readInt32();
    final throttle = r.readInt32();
    final error = r.readInt16();
    final generation = r.readInt32();
    final protocol = _requiredString(r);
    final leader = _requiredString(r);
    final member = _requiredString(r);
    final count = r.readInt32();
    if (count < 0 || count > r.remaining ~/ 6) {
      throw const FormatException('Invalid member count');
    }
    final members = <JoinedGroupMember>[];
    final ids = <String>{};
    for (var i = 0; i < count; i++) {
      final memberId = _requiredString(r);
      if (!ids.add(memberId)) {
        throw const FormatException('Duplicate group member');
      }
      members.add(JoinedGroupMember(memberId, _requiredBytes(r)));
    }
    _end(r);
    if (error == 0 &&
        (generation < 0 ||
            protocol.isEmpty ||
            leader.isEmpty ||
            member.isEmpty ||
            members.any((m) => m.memberId.isEmpty))) {
      throw const FormatException('Invalid successful JoinGroup response');
    }
    if (error == 0) {
      if (leader == member && !ids.contains(member)) {
        throw const FormatException('Leader is absent from the member list');
      }
      if (leader != member && members.isNotEmpty) {
        throw const FormatException(
            'Follower received a leader-only member list');
      }
    }
    return JoinGroupResponse(
        id, throttle, error, generation, protocol, leader, member, members);
  }

  @override
  Map<String, dynamic> toJson() => {
        'correlationId': correlationId,
        'throttleTimeMs': throttleTimeMs,
        'errorCode': errorCode,
        'generationId': generationId,
        'protocolName': protocolName,
        'leaderId': leaderId,
        'memberId': memberId,
        'members': members.map((m) => m.toJson()).toList(),
      };
}

/// An opaque assignment for one dynamic member.
class GroupAssignment {
  final String memberId;
  final List<int> assignment;

  GroupAssignment(this.memberId, List<int> assignment)
      : assignment = _snapshotBytes(assignment) {
    _string(memberId, 'memberId');
  }

  Map<String, dynamic> toJson() =>
      {'memberId': memberId, 'assignment': assignment};
}

/// SyncGroup v2. Followers send the default empty assignment list.
class SyncGroupRequest extends KafkaRequest {
  final int apiKey = 14;
  final int apiVersion = 2;
  final String groupId;
  final int generationId;
  final String memberId;
  final List<GroupAssignment> assignments;

  SyncGroupRequest(this.groupId,
      {required this.generationId,
      required this.memberId,
      List<GroupAssignment> assignments = const []})
      : assignments = List.unmodifiable(assignments) {
    _string(groupId, 'groupId');
    _string(memberId, 'memberId');
    _int32(generationId, 'generationId');
    _int32(this.assignments.length, 'assignment count');
    final ids = <String>{};
    for (final assignment in this.assignments) {
      if (!ids.add(assignment.memberId)) {
        throw ArgumentError('Duplicate assignment member');
      }
    }
  }

  @override
  List<int> toBytes() {
    final b =
        KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
          ..addString(groupId)
          ..addInt32(generationId)
          ..addString(memberId)
          ..addInt32(assignments.length);
    for (final a in assignments) {
      b
        ..addString(a.memberId)
        ..addBytes(a.assignment);
    }
    return KafkaRequest.frame(b.takeBytes());
  }

  @override
  SyncGroupResponse parseResponse(List<int> bytes) =>
      _matching(SyncGroupResponse.fromBytes(bytes), correlationId);
}

class SyncGroupResponse extends KafkaResponse {
  final int throttleTimeMs;
  final int errorCode;
  final List<int> assignment;

  SyncGroupResponse(super.correlationId, this.throttleTimeMs, this.errorCode,
      List<int> assignment)
      : assignment = _snapshotBytes(assignment);

  factory SyncGroupResponse.fromBytes(List<int> bytes) {
    final r = _reader(bytes);
    final result = SyncGroupResponse(
        r.readInt32(), r.readInt32(), r.readInt16(), _requiredBytes(r));
    _end(r);
    return result;
  }

  @override
  Map<String, dynamic> toJson() => {
        'correlationId': correlationId,
        'throttleTimeMs': throttleTimeMs,
        'errorCode': errorCode,
        'assignment': assignment
      };
}

/// One Heartbeat v2 exchange; does not schedule subsequent heartbeats.
class HeartbeatRequest extends KafkaRequest {
  final int apiKey = 12;
  final int apiVersion = 2;
  final String groupId;
  final int generationId;
  final String memberId;

  HeartbeatRequest(this.groupId,
      {required this.generationId, required this.memberId}) {
    _string(groupId, 'groupId');
    _string(memberId, 'memberId');
    _int32(generationId, 'generationId');
  }

  @override
  List<int> toBytes() => KafkaRequest.frame(
      (KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
            ..addString(groupId)
            ..addInt32(generationId)
            ..addString(memberId))
          .takeBytes());

  @override
  HeartbeatResponse parseResponse(List<int> bytes) =>
      _matching(HeartbeatResponse.fromBytes(bytes), correlationId);
}

class HeartbeatResponse extends KafkaResponse {
  final int throttleTimeMs;
  final int errorCode;

  HeartbeatResponse(super.correlationId, this.throttleTimeMs, this.errorCode);

  factory HeartbeatResponse.fromBytes(List<int> bytes) {
    final r = _reader(bytes);
    final result =
        HeartbeatResponse(r.readInt32(), r.readInt32(), r.readInt16());
    _end(r);
    return result;
  }

  @override
  Map<String, dynamic> toJson() => {
        'correlationId': correlationId,
        'throttleTimeMs': throttleTimeMs,
        'errorCode': errorCode
      };
}

/// LeaveGroup v2 for a single dynamic member.
class LeaveGroupRequest extends KafkaRequest {
  final int apiKey = 13;
  final int apiVersion = 2;
  final String groupId;
  final String memberId;

  LeaveGroupRequest(this.groupId, {required this.memberId}) {
    _string(groupId, 'groupId');
    _string(memberId, 'memberId');
  }

  @override
  List<int> toBytes() => KafkaRequest.frame(
      (KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
            ..addString(groupId)
            ..addString(memberId))
          .takeBytes());

  @override
  LeaveGroupResponse parseResponse(List<int> bytes) =>
      _matching(LeaveGroupResponse.fromBytes(bytes), correlationId);
}

class LeaveGroupResponse extends KafkaResponse {
  final int throttleTimeMs;
  final int errorCode;

  LeaveGroupResponse(super.correlationId, this.throttleTimeMs, this.errorCode);

  factory LeaveGroupResponse.fromBytes(List<int> bytes) {
    final r = _reader(bytes);
    final result =
        LeaveGroupResponse(r.readInt32(), r.readInt32(), r.readInt16());
    _end(r);
    return result;
  }

  @override
  Map<String, dynamic> toJson() => {
        'correlationId': correlationId,
        'throttleTimeMs': throttleTimeMs,
        'errorCode': errorCode
      };
}
