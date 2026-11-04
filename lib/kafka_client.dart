/// Experimental native Dart Kafka client with explicit partition/offset APIs.
///
/// The verified baseline is Kafka 4.0.2, plaintext or validated TLS, optional
/// SASL PLAIN over TLS, uncompressed records, and read-uncommitted consumption.
/// Manual broker-stored offsets are supported without coordinated membership.
/// Group assignment/rebalancing, SCRAM, compression, idempotence and transactions
/// are absent.
///
/// Start with [KafkaClient], [Broker], and [KafkaRecord]. Always close clients.
/// Advanced callers can import `package:kafka_client/kafka_protocol.dart` for
/// explicit request/response types and low-level sends.
library;

export 'src/kafka_client_base.dart' show KafkaClient;
export 'src/broker/broker.dart' show Broker;
export 'src/codec/record_batch.dart' show KafkaRecord, KafkaRecordHeader;
export 'src/api/request/api0_produce_request.dart' show Record;
export 'src/api/response/api0_produce_response.dart'
    show ProduceResponse, TopicResponse, PartitionResponse;
export 'src/api/response/api1_fetch_response.dart'
    show FetchResponse, FetchTopicResponse, FetchPartitionResponse;
export 'src/api/response/api3_metadata_response.dart'
    show MetadataResponse, BrokerMetadata, TopicMetadata, PartitionMetadata;
export 'src/security/security.dart'
    show KafkaTlsConfig, KafkaSaslPlain, KafkaAuthenticationException;
export 'src/reliability.dart'
    show
        KafkaTlsFetchRecovery,
        KafkaStableOffsetRetry,
        KafkaResourceLimitException,
        KafkaConnectionException,
        KafkaBrokerException,
        KafkaCancelledException,
        KafkaCancellationToken;

export 'src/topic_subscription.dart' show KafkaTopicRecord;
export 'src/group_offsets.dart'
    show KafkaCommittedOffset, KafkaOffsetCommitException;
