/// Advanced, explicitly versioned Kafka protocol requests and responses.
///
/// Requests are single-use. [KafkaClient.send] does not select a partition leader
/// or negotiate a version for caller-constructed requests. Prefer the high-level
/// methods unless implementing protocol tooling. Classic-group requests are
/// low-level primitives, not a managed consumer or live membership lease.
/// Unverified legacy experiments remain unexported.
library;

export 'kafka_client.dart';
export 'src/api/abstract/kafka_request.dart' show KafkaRequest;
export 'src/api/request/api0_produce_request.dart'
    show ProduceRequest, TopicData, PartitionData;
export 'src/api/request/api1_fetch_request.dart'
    show FetchRequest, FetchTopic, FetchPartition;
export 'src/api/request/api2_list_offsets_request.dart' show ListOffsetsRequest;
export 'src/api/request/api3_metadata_request.dart' show MetadataRequest;
export 'src/api/request/api18_api_vesions_request.dart' show ApiVersionsRequest;
export 'src/api/response/api18_api_versions_response.dart'
    show ApiVersionsResponse, ApiVersion;
export 'src/group_offsets_protocol.dart';
export 'src/group_membership_protocol.dart';
export 'src/consumer_group_metadata.dart';
