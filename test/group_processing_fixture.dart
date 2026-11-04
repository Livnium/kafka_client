// Test-only probes for the owned broker processing suites.
import 'dart:async';
import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';
import 'group_delivery_fixture.dart';

class GroupProcessingProbe {
  final records = <KafkaGroupRecord>[];
  final errors = <Object>[];
  final done = Completer<void>();
  late final StreamSubscription<KafkaGroupRecord> subscription;
  Object? _callbackError;
  StackTrace? _callbackStack;

  GroupProcessingProbe(
      KafkaGroupProcessing delivery, void Function(KafkaGroupRecord) inspect) {
    subscription = delivery.records.listen((record) {
      records.add(record);
      if (_callbackError != null) return;
      try {
        inspect(record);
      } catch (error, stack) {
        _callbackError = error;
        _callbackStack = stack;
      }
    }, onError: (Object error, StackTrace _) {
      errors.add(error);
    }, onDone: () {
      done.complete();
    }, cancelOnError: false);
    addTearDown(() async {
      try {
        await delivery.cancel().timeout(deliveryDeadline);
      } finally {
        await subscription.cancel().timeout(deliveryDeadline);
      }
    });
  }

  void checkCallbacks() {
    if (_callbackError != null) {
      Error.throwWithStackTrace(_callbackError!, _callbackStack!);
    }
  }

  void checkHealthy() {
    checkCallbacks();
    expect(errors, isEmpty);
  }

  Future<void> received(int count) async {
    final clock = Stopwatch()..start();
    while (true) {
      checkHealthy();
      if (records.length >= count) return;
      if (done.isCompleted) fail('Delivery ended before $count records');
      if (clock.elapsed >= deliveryDeadline) {
        throw TimeoutException('Delivery did not reach $count records');
      }
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
  }
}

void expectConfirmedProcessingCommit(KafkaGroupCommitResult result,
    KafkaGroupAssignment token, String topic, int nextOffset) {
  expect(result.assignment, same(token));
  expect(result.topic, topic);
  expect(result.locallyCurrent, isTrue);
  expect(result.error, isNull);
  expect(result.partitions.keys, [0]);
  expect(result.partitions[0]!.nextOffset, nextOffset);
  expect(result.partitions[0]!.outcome, KafkaGroupCommitOutcome.confirmed);
  expect(result.partitions[0]!.errorCode, 0);
}
