// Helpers shared only by the opt-in, owned real-broker delivery tests.
import 'dart:async';
import 'dart:convert';

import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';

const deliveryDeadline = Duration(seconds: 15);

// Keep assertions in the original, synchronous listener, without unhandled
// asynchronous test failures. Do not transform the delivery stream: the public
// guarantee covers the original callback, not downstream queues or async work.
class GroupDeliveryProbe {
  final records = <KafkaGroupRecord>[];
  final errors = <Object>[];
  final done = Completer<void>();
  late final StreamSubscription<KafkaGroupRecord> subscription;
  Object? _callbackError;
  StackTrace? _callbackStack;

  GroupDeliveryProbe(
      KafkaGroupDelivery delivery, void Function(KafkaGroupRecord) inspect) {
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

Future<int> produceDeliveryRecords(
    KafkaClient producer, String topic, List<KafkaRecord> records) async {
  // Produce is never replayed, including after an ambiguous failure.
  final response = await producer
      .produce(topic, records, acks: -1)
      .timeout(deliveryDeadline);
  expect(response, isNotNull);
  expect(response!.responses.single.name, topic);
  final partition = response.responses.single.partitionResponses.single;
  expect(partition.index, 0);
  expect(partition.errorCode, 0);
  expect(partition.baseOffset, greaterThanOrEqualTo(0));
  return partition.baseOffset;
}

KafkaRecord deliveryMarker(String value) => KafkaRecord(
    key: utf8.encode('group-delivery/$value'), value: utf8.encode(value));

void expectDeliveryRecord(
    KafkaGroupRecord actual,
    KafkaGroupSession session,
    KafkaGroupAssignment token,
    String topic,
    int offset,
    KafkaRecord expected) {
  // These checks run inside the original callback, before any await.
  expect(actual.assignment, same(token));
  expect(session.isCurrent(actual.assignment), isTrue);
  session.assertCurrent(actual.assignment);
  expect(actual.topic, topic);
  expect(actual.partition, 0);
  expect(actual.offset, offset);
  expect(actual.record.offset, offset);
  expect(actual.record.key, expected.key);
  expect(actual.record.value, expected.value);
  expect(actual.record.timestamp, expected.timestamp);
  expect(actual.record.headers.map((header) => header.key),
      expected.headers.map((header) => header.key));
  expect(actual.record.headers.map((header) => header.value),
      expected.headers.map((header) => header.value));
}

Future<void> expectNoDeliveryCommit(
    KafkaClient reader, String group, String topic) async {
  expect(
      (await reader.committedOffsets(group, topic, partitions: {0}))[0], isNull,
      reason: 'Delivery, pause, cancellation and revocation never commit');
}

void expectCleanDeliveryStop(KafkaGroupDeliveryTermination stopped,
    KafkaGroupDeliveryStopReason reason) {
  expect(stopped.reason, reason);
  expect(stopped.error, isNull);
  expect(stopped.cleanupError, isNull);
}
