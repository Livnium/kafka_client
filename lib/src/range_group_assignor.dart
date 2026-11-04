import 'consumer_group_metadata.dart';

/// Classic eager range for dynamic members only. No rack-aware or cooperative
/// balancing is attempted. Each topic is independently divided into contiguous
/// ranges in partition-ID order among its lexicographically sorted subscribers.
Map<String, ConsumerGroupAssignment> assignEagerRange(
    Map<String, ConsumerGroupSubscription> subscriptions,
    Map<String, List<int>> partitions) {
  final members = subscriptions.keys.toList()..sort();
  final result = <String, Map<String, List<int>>>{};
  final topics = <String>{};
  for (final member in members) {
    if (member.isEmpty) throw const FormatException('Empty member ID');
    final subscription = subscriptions[member]!;
    if (subscription.rackId != null) {
      throw UnsupportedError('Rack-aware range assignment is not supported');
    }
    topics.addAll(subscription.topics);
    result[member] = {};
  }
  final sortedTopics = topics.toList()..sort();
  for (final topic in sortedTopics) {
    final supplied = partitions[topic];
    if (supplied == null) throw FormatException('Missing metadata for $topic');
    final ids = supplied.toList()..sort();
    if (ids.any((id) => id < 0 || id > 2147483647) ||
        ids.toSet().length != ids.length) {
      throw FormatException('Invalid partition metadata for $topic');
    }
    final eligible = members
        .where((member) => subscriptions[member]!.topics.contains(topic))
        .toList();
    final size = ids.length ~/ eligible.length;
    final remainder = ids.length % eligible.length;
    var offset = 0;
    for (var i = 0; i < eligible.length; i++) {
      final count = size + (i < remainder ? 1 : 0);
      if (count > 0) {
        result[eligible[i]]![topic] = ids.sublist(offset, offset + count);
      }
      offset += count;
    }
  }
  return Map.unmodifiable(result.map(
      (member, owned) => MapEntry(member, ConsumerGroupAssignment(owned))));
}
