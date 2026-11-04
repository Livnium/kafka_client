/// Experimental single-generation classic membership and bounded record delivery.
///
/// Sessions join once, maintain heartbeats and revoke locally on loss. They can
/// deliver assigned records, with optional explicit processing acknowledgements
/// and generation commits. They never automatically rejoin or commit. Assignment
/// guards do not fence broker operations or asynchronous application effects.
library;

export 'kafka_client.dart';
export 'src/group_session.dart' show KafkaGroupSession;
export 'src/classic_group_session.dart'
    show
        KafkaGroupAssignment,
        KafkaGroupTermination,
        KafkaGroupStableHeartbeatFailure,
        KafkaGroupSessionState,
        KafkaGroupStopReason,
        KafkaGroupLeaveOutcome;

export 'src/group_delivery.dart'
    show
        KafkaGroupDelivery,
        KafkaGroupRecord,
        KafkaGroupDeliveryTermination,
        KafkaGroupDeliveryStopReason;

export 'src/group_processing.dart'
    show
        KafkaGroupProcessing,
        KafkaGroupCommitResult,
        KafkaGroupPartitionCommit,
        KafkaGroupCommitOutcome,
        KafkaGroupProcessingCleanupException;

export 'src/group_consumer.dart'
    show
        KafkaGroupConsumer,
        KafkaGroupConsumerState,
        KafkaGroupConsumerTermination,
        KafkaGroupConsumerStopReason;
