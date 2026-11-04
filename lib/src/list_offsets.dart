/// Explicit broker position to look up without changing a consumer's position.
enum KafkaOffsetPosition {
  /// The earliest retained log offset, which can be greater than zero.
  earliest,

  /// The exclusive read-uncommitted high watermark, not the last record offset.
  latest,
}
