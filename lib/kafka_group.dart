/// Experimental classic-group sessions, delivery and bounded serial consumption.
///
/// [KafkaGroupSession] joins one dynamic classic-group generation, maintains
/// heartbeats and revokes locally on loss. Its delivery and processing handles
/// expose assigned records, explicit acknowledgements and generation commits;
/// the session does not automatically rejoin or commit.
///
/// [KafkaGroupConsumer] adds an owned serial processing callback, an explicit
/// choice of commit-after-processing behavior and a finite rejoin policy.
/// Replacement is limited to eligible stable-heartbeat failures after successful
/// callback, commit and owned cleanup drains. Other failures remain terminal.
/// It reads fresh committed positions for each generation; missing positions
/// require explicit numeric fallbacks. It does not silently reset offsets or
/// provide general transparent group recovery.
///
/// Assignment guards do not fence broker operations or asynchronous application
/// effects. Neither API provides exactly-once application processing.
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
