import 'api/request/api18_api_vesions_request.dart';
import 'api/response/api18_api_versions_response.dart';
import 'broker/broker.dart';
import 'classic_group_driver.dart';
import 'consumer_group_metadata.dart';
import 'group_membership_protocol.dart';
import 'group_offsets_protocol.dart';
import 'kafka_client_base.dart';
import 'range_group_assignor.dart';
import 'reliability.dart';

/// Owns a dedicated client for one dynamic classic-group generation. Only
/// read-safe discovery/metadata can retry; membership requests dispatch once.
/// Quota throttling is not supported in this bounded increment: negative
/// response throttles are malformed and positive throttles fail closed rather
/// than publishing ownership or replaying a request. ApiVersions v0 carries no
/// throttle field.
class NativeClassicGroupDriver implements ClassicGroupDriver {
  final KafkaClient _client;
  final String groupId;
  final Set<String> topics;
  final int sessionTimeoutMs;
  final int rebalanceTimeoutMs;
  Broker? _coordinator;
  bool _supportsCommit = false;

  NativeClassicGroupDriver(
      {required KafkaClient client,
      required this.groupId,
      required Set<String> topics,
      required this.sessionTimeoutMs,
      required this.rebalanceTimeoutMs})
      : _client = client,
        topics = Set.unmodifiable(topics) {
    if (this.topics.isEmpty) throw ArgumentError('Topics must not be empty');
    _joinRequest(''); // Validate all configuration before I/O.
  }

  JoinGroupRequest _joinRequest(String memberId) => JoinGroupRequest(groupId,
          sessionTimeoutMs: sessionTimeoutMs,
          rebalanceTimeoutMs: rebalanceTimeoutMs,
          memberId: memberId,
          protocols: [
            GroupJoinProtocol('range',
                ConsumerGroupSubscription(topics.toList()..sort()).toBytes())
          ]);

  Broker get _broker =>
      _coordinator ?? (throw StateError('Driver is not prepared'));

  @override
  Future<void> prepare() async {
    final broker = await _client.findGroupCoordinator(groupId);
    final response = await _client.send(ApiVersionsRequest(), broker: broker)
        as ApiVersionsResponse;
    _check(response.errorCode, 'api versions');
    response.validateSupportedVersions();
    for (final entry in const {11: 4, 14: 2, 12: 2, 13: 2}.entries) {
      if (!response.apiVersions.any((v) =>
          v.apiKey == entry.key &&
          v.minVersion <= entry.value &&
          v.maxVersion >= entry.value)) {
        throw UnsupportedError('Broker does not support implemented API '
            '${entry.key} version ${entry.value}');
      }
    }
    _supportsCommit = response.apiVersions
        .any((v) => v.apiKey == 8 && v.minVersion <= 6 && v.maxVersion >= 6);
    _coordinator = broker;
  }

  @override
  Future<JoinGroupResponse> join(String memberId) async {
    final response = await _client.send(_joinRequest(memberId), broker: _broker)
        as JoinGroupResponse;
    _checkThrottle(response.throttleTimeMs);
    // Only the lifecycle controller may authorize the bounded ID continuation.
    if (response.errorCode != 79) _check(response.errorCode, 'join group');
    return response;
  }

  Future<Map<String, List<int>>> _partitions(Set<String> requested) async {
    if (requested.isEmpty) return {};
    final metadata = await _client.metadata(requested);
    // Build once: duplicate node IDs cannot prove an unambiguous endpoint,
    // including when a third occurrence follows an already rejected duplicate.
    final seenBrokerIds = <int>{};
    final usableLeaderIds = <int>{};
    for (final broker in metadata.brokers) {
      if (!seenBrokerIds.add(broker.nodeId)) {
        usableLeaderIds.remove(broker.nodeId);
      } else if (broker.host.isNotEmpty &&
          broker.port > 0 &&
          broker.port <= 65535) {
        usableLeaderIds.add(broker.nodeId);
      }
    }
    final result = <String, List<int>>{};
    for (final topic in metadata.topics) {
      if (!requested.contains(topic.name) || result.containsKey(topic.name)) {
        throw const FormatException('Unexpected or duplicate topic metadata');
      }
      _check(topic.errorCode, 'group topic metadata');
      final ids = <int>[];
      for (final partition in topic.partitions) {
        // Metadata v0 may flag an unavailable follower replica while returning
        // a usable leader (REPLICA_NOT_AVAILABLE). Preserve every partition and
        // the raw error; only this assignment-metadata check accepts the warning.
        // This applies to raw sessions too, independently of rejoin policy.
        final usableReplicaWarning = partition.errorCode == 9 &&
            partition.leaderId >= 0 &&
            usableLeaderIds.contains(partition.leaderId);
        if (!usableReplicaWarning) {
          _check(partition.errorCode, 'group partition metadata');
        }
        final id = partition.partitionIndex;
        if (id < 0 || id > 2147483647 || ids.contains(id)) {
          throw const FormatException(
              'Invalid or duplicate partition metadata');
        }
        ids.add(id);
      }
      result[topic.name] = ids;
    }
    if (result.length != requested.length) {
      throw const FormatException('Missing subscribed topic metadata');
    }
    return result;
  }

  @override
  Future<ConsumerGroupAssignment> sync(JoinGroupResponse joined) async {
    _checkThrottle(joined.throttleTimeMs);
    _check(joined.errorCode, 'join group');
    if (joined.protocolName != 'range') {
      throw const FormatException('Expected range protocol');
    }
    final assignments = <GroupAssignment>[];
    Map<String, List<int>>? partitions;
    List<int>? submittedLocal;
    if (joined.leaderId == joined.memberId) {
      final subscriptions = <String, ConsumerGroupSubscription>{};
      for (final member in joined.members) {
        if (member.memberId.isEmpty ||
            subscriptions.containsKey(member.memberId)) {
          throw const FormatException('Invalid or duplicate group member');
        }
        final subscription =
            ConsumerGroupSubscription.fromBytes(member.metadata);
        if (subscription.rackId != null) {
          throw UnsupportedError(
              'Rack-aware range assignment is not supported');
        }
        subscriptions[member.memberId] = subscription;
      }
      final own = subscriptions[joined.memberId];
      if (own == null ||
          own.topics.length != topics.length ||
          !topics.containsAll(own.topics)) {
        throw const FormatException(
            'Leader subscription does not match local topics');
      }
      partitions = await _partitions(
          subscriptions.values.expand((s) => s.topics).toSet());
      final computed = assignEagerRange(subscriptions, partitions);
      assignments.addAll(computed.entries
          .map((e) => GroupAssignment(e.key, e.value.toBytes())));
      submittedLocal = assignments
          .singleWhere((assignment) => assignment.memberId == joined.memberId)
          .assignment;
    } else if (joined.members.isNotEmpty) {
      throw const FormatException('Follower received leader member metadata');
    }
    final response = await _client.send(
        SyncGroupRequest(groupId,
            generationId: joined.generationId,
            memberId: joined.memberId,
            assignments: assignments),
        broker: _broker) as SyncGroupResponse;
    _checkThrottle(response.throttleTimeMs);
    _check(response.errorCode, 'sync group');
    if (submittedLocal != null &&
        !_sameBytes(submittedLocal, response.assignment)) {
      throw const FormatException('Coordinator changed leader self-assignment');
    }
    final local = ConsumerGroupAssignment.fromBytes(response.assignment);
    for (final topic in local.partitions.keys) {
      if (!topics.contains(topic)) {
        throw const FormatException('Assigned topic is not subscribed');
      }
    }
    // Follower has not read metadata yet. Read all local subscribed topics,
    // including those yielding an empty assignment, before publishing ownership.
    partitions ??= await _partitions(topics);
    for (final entry in local.partitions.entries) {
      if (!partitions[entry.key]!.toSet().containsAll(entry.value)) {
        throw const FormatException(
            'Assigned partition is absent from metadata');
      }
    }
    return local;
  }

  @override
  Future<HeartbeatResponse> heartbeat(JoinGroupResponse joined) async {
    final response = await _client.send(
        HeartbeatRequest(groupId,
            generationId: joined.generationId, memberId: joined.memberId),
        broker: _broker) as HeartbeatResponse;
    _checkThrottle(response.throttleTimeMs);
    _check(response.errorCode, 'heartbeat');
    return response;
  }

  @override
  Future<LeaveGroupResponse> leave(String memberId) async {
    final response = await _client.send(
        LeaveGroupRequest(groupId, memberId: memberId),
        broker: _broker) as LeaveGroupResponse;
    _checkThrottle(response.throttleTimeMs);
    _check(response.errorCode, 'leave group');
    return response;
  }

  /// Support is checked from preparation's coordinator ApiVersions snapshot.
  /// Existing membership-only sessions do not require OffsetCommit support.
  void requireMemberCommitSupport() {
    if (!_supportsCommit) {
      throw UnsupportedError('Broker does not support OffsetCommit v6');
    }
  }

  /// One attempt on the prepared coordinator, never discovery/replay/fallback.
  Future<GroupOffsetCommitResponse> commit(String topic, Map<int, int> offsets,
      {required int generationId,
      required String memberId,
      required KafkaCancellationToken cancellationToken}) {
    requireMemberCommitSupport();
    final request = MemberOffsetCommitRequest(groupId, topic, offsets,
        generationId: generationId, memberId: memberId);
    return _client
        .send(request, broker: _broker, cancellationToken: cancellationToken)
        .then((response) => response as GroupOffsetCommitResponse);
  }

  @override
  Future<void> close() => _client.close();
}

void _check(int code, String phase) {
  if (code != 0) throw KafkaBrokerException(code, phase);
}

void _checkThrottle(int milliseconds) {
  if (milliseconds < 0) {
    throw const FormatException('Negative group response throttle');
  }
  if (milliseconds > 0) {
    throw UnsupportedError('Group quota throttling is not supported');
  }
}

bool _sameBytes(List<int> expected, List<int> actual) {
  if (expected.length != actual.length) return false;
  for (var i = 0; i < expected.length; i++) {
    if (expected[i] != actual[i]) return false;
  }
  return true;
}
