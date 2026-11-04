import 'consumer_group_metadata.dart';
import 'group_membership_protocol.dart';

/// Internal control-plane boundary. Every operation is single-dispatch except
/// the existing read-safe discovery/metadata paths. The implementation owns its
/// client. It never sends Fetch or Produce. The native implementation separately
/// provides optional explicit generation commits to the processing layer.
abstract class ClassicGroupDriver {
  Future<void> prepare();
  Future<JoinGroupResponse> join(String memberId);

  /// Computes/receives range assignment and validates every local partition
  /// against fresh topic metadata before returning. Throws on broker errors.
  Future<ConsumerGroupAssignment> sync(JoinGroupResponse joined);
  Future<HeartbeatResponse> heartbeat(JoinGroupResponse joined);
  Future<LeaveGroupResponse> leave(String memberId);
  Future<void> close();
}
