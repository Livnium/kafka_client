import '../../codec/byte_reader.dart';
import '../abstract/kafka_response.dart';

/// Represents a response to a MetadataRequest from Kafka.
class MetadataResponse extends KafkaResponse {
  /// List of broker metadata information.
  final List<BrokerMetadata> brokers;

  /// List of topic metadata information.
  final List<TopicMetadata> topics;

  MetadataResponse(super.correlationId, this.brokers, this.topics);

  /// Creates a [MetadataResponse] instance from the given bytes.
  factory MetadataResponse.fromBytes(List<int> data) {
    var reader = KafkaBytesReader.fromBytes(data);

    // Read the correlation ID
    final correlationId = reader.readInt32();

    // Read broker metadata
    final brokersCount = reader.readInt32();
    if (brokersCount < 0 || brokersCount > reader.length - reader.offset) {
      throw FormatException("Invalid brokersCount");
    }
    final brokers = <BrokerMetadata>[];
    for (var i = 0; i < brokersCount; i++) {
      final nodeId = reader.readInt32();
      final host = reader.readString();
      final port = reader.readInt32();

      // Handle possible null host
      if (host == null) {
        throw Exception('Broker host cannot be null');
      }

      brokers.add(BrokerMetadata(
        nodeId: nodeId,
        host: host,
        port: port,
      ));
    }

    // Read topic metadata
    final topicsCount = reader.readInt32();
    if (topicsCount < 0 || topicsCount > reader.length - reader.offset) {
      throw FormatException("Invalid topicsCount");
    }
    final topics = <TopicMetadata>[];
    for (var i = 0; i < topicsCount; i++) {
      final errorCode = reader.readInt16();
      final name = reader.readString();

      // Handle possible null topic name
      if (name == null) {
        throw Exception('Topic name cannot be null');
      }

      final partitionsCount = reader.readInt32();
      if (partitionsCount < 0 ||
          partitionsCount > reader.length - reader.offset) {
        throw FormatException("Invalid partitionsCount");
      }
      final partitions = <PartitionMetadata>[];
      for (var j = 0; j < partitionsCount; j++) {
        final partitionErrorCode = reader.readInt16();
        final partitionIndex = reader.readInt32();
        final leaderId = reader.readInt32();

        // Read replica nodes
        final replicaNodesCount = reader.readInt32();
        if (replicaNodesCount < 0 ||
            replicaNodesCount > reader.length - reader.offset) {
          throw FormatException("Invalid replicaNodesCount");
        }
        final replicaNodes = <int>[];
        for (var k = 0; k < replicaNodesCount; k++) {
          replicaNodes.add(reader.readInt32());
        }

        // Read ISR nodes
        final isrNodesCount = reader.readInt32();
        if (isrNodesCount < 0 ||
            isrNodesCount > reader.length - reader.offset) {
          throw FormatException("Invalid isrNodesCount");
        }
        final isrNodes = <int>[];
        for (var k = 0; k < isrNodesCount; k++) {
          isrNodes.add(reader.readInt32());
        }

        partitions.add(PartitionMetadata(
          errorCode: partitionErrorCode,
          partitionIndex: partitionIndex,
          leaderId: leaderId,
          replicaNodes: replicaNodes,
          isrNodes: isrNodes,
        ));
      }

      topics.add(TopicMetadata(
        errorCode: errorCode,
        name: name,
        partitions: partitions,
      ));
    }

    // Create and return a MetadataResponse instance.
    if (!reader.isEOF) throw FormatException("Trailing metadata bytes");
    return MetadataResponse(correlationId, brokers, topics);
  }

  @override
  String toString() {
    return 'MetadataResponse(correlationId: $correlationId, brokers: $brokers, topics: $topics)';
  }

  @override
  Map<String, dynamic> toJson() {
    return {
      'correlationId': correlationId,
      'brokers': brokers.map((broker) => broker.toJson()).toList(),
      'topics': topics.map((topic) => topic.toJson()).toList(),
    };
  }
}

/// Represents metadata information about a Kafka broker.
class BrokerMetadata {
  final int nodeId;
  final String host;
  final int port;

  BrokerMetadata({
    required this.nodeId,
    required this.host,
    required this.port,
  });

  @override
  String toString() {
    return 'BrokerMetadata(nodeId: $nodeId, host: $host, port: $port)';
  }

  Map<String, dynamic> toJson() {
    return {
      'nodeId': nodeId,
      'host': host,
      'port': port,
    };
  }
}

/// Represents metadata information about a partition within a Kafka topic.
class PartitionMetadata {
  final int errorCode;
  final int partitionIndex;
  final int leaderId;
  final List<int> replicaNodes;
  final List<int> isrNodes;

  PartitionMetadata({
    required this.errorCode,
    required this.partitionIndex,
    required this.leaderId,
    required this.replicaNodes,
    required this.isrNodes,
  });

  @override
  String toString() {
    return 'PartitionMetadata(errorCode: $errorCode, partitionIndex: $partitionIndex, leaderId: $leaderId, replicaNodes: $replicaNodes, isrNodes: $isrNodes)';
  }

  Map<String, dynamic> toJson() {
    return {
      'errorCode': errorCode,
      'partitionIndex': partitionIndex,
      'leaderId': leaderId,
      'replicaNodes': replicaNodes,
      'isrNodes': isrNodes,
    };
  }
}

/// Represents metadata information about a Kafka topic.
class TopicMetadata {
  final int errorCode;
  final String name;
  final List<PartitionMetadata> partitions;

  TopicMetadata({
    required this.errorCode,
    required this.name,
    required this.partitions,
  });

  @override
  String toString() {
    return 'TopicMetadata(errorCode: $errorCode, name: $name, partitions: $partitions)';
  }

  Map<String, dynamic> toJson() {
    return {
      'errorCode': errorCode,
      'name': name,
      'partitions': partitions.map((p) => p.toJson()).toList(),
    };
  }
}
