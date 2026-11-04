/// A broker-stored next position for an explicitly managed partition.
///
/// This is not partition ownership or an acknowledgement of application work.
/// The application must finish processing before it commits the next offset.
class KafkaCommittedOffset {
  /// The next absolute offset to read, not the last record already processed.
  final int nextOffset;

  /// Stored leader epoch, or -1 when unknown. No truncation handling is implied.
  final int leaderEpoch;

  /// Optional application metadata preserved by Kafka.
  final String? metadata;

  const KafkaCommittedOffset(
      {required this.nextOffset, required this.leaderEpoch, this.metadata});
}

/// A complete OffsetCommit response reported one or more partition errors.
///
/// Zero codes identify successful partitions; they are not rolled back when
/// another partition fails. No automatic replay occurs. Transport/protocol
/// failures instead retain their original exception and can have an unknown
/// commit outcome. This exception is not used to label those outcomes rejected.
class KafkaOffsetCommitException implements Exception {
  /// Every requested partition's broker error code, including successful zeros.
  final Map<int, int> partitionErrors;

  KafkaOffsetCommitException(Map<int, int> partitionErrors)
      : partitionErrors = Map<int, int>.unmodifiable(partitionErrors);

  /// Partitions whose offsets the broker acknowledged successfully.
  Set<int> get successfulPartitions =>
      Set<int>.unmodifiable(partitionErrors.entries
          .where((entry) => entry.value == 0)
          .map((entry) => entry.key));

  @override
  String toString() =>
      'KafkaOffsetCommitException: partition error codes $partitionErrors; '
      'successful partitions are not rolled back';
}
