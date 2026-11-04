// Opt-in owned TLS/SASL_SSL fixture tests. Credentials are disposable inputs.
import 'dart:io';
import 'dart:typed_data';

import 'package:kafka_client/kafka_group.dart';
import 'package:test/test.dart';

import 'group_consumer_integration_test.dart' show expectCleanConsumerStop;
import 'group_delivery_fixture.dart';
import 'group_membership_integration_test.dart' as membership;
import 'operational_integration_test.dart' as fixture;

void main() {
  final env = Platform.environment;
  final tlsPort = int.tryParse(env['KAFKA_SECURE_TLS_PORT'] ?? '');
  final saslPort = int.tryParse(env['KAFKA_SECURE_SASL_PORT'] ?? '');
  final ca = env['KAFKA_SECURE_CA'], wrongCa = env['KAFKA_SECURE_WRONG_CA'];
  final topic = env['KAFKA_SECURE_TOPIC'];
  final username = env['KAFKA_SECURE_USERNAME'];
  final password = env['KAFKA_SECURE_PASSWORD'];
  final configured = [tlsPort, saslPort, ca, wrongCa, topic, username, password]
      .every((value) => value != null && value != '');

  KafkaTlsConfig trust(String path) => KafkaTlsConfig(
      trustedCertificates: File(path).readAsBytesSync(),
      withTrustedRoots: false);
  KafkaSaslPlain credentials() =>
      KafkaSaslPlain(username: username!, password: password!);

  KafkaClient reader(bool sasl) {
    final client = KafkaClient(
        brokers: [Broker('localhost', sasl ? saslPort! : tlsPort!)],
        tls: trust(ca!),
        sasl: sasl ? credentials() : null,
        allowTlsBootstrapFallback: false,
        maxReadRetries: 10,
        retryBaseDelay: const Duration(milliseconds: 100));
    addTearDown(() async {
      await client.close().timeout(deliveryDeadline);
      await fixture.idle(client);
      expect(client.connectionCount, 0);
      expect(client.bootstrapTlsFallbackCount, 0);
      expect(client.tlsFetchRecovery, isNull);
    });
    return client;
  }

  KafkaGroupConsumer consumer(String group, bool sasl,
      {int? fallback,
      bool badHost = false,
      bool badTrust = false,
      KafkaSaslPlain? observed}) {
    final owner = KafkaGroupConsumer(
        brokers: [
          Broker(
              badHost ? '127.0.0.1' : 'localhost', sasl ? saslPort! : tlsPort!)
        ],
        groupId: group,
        topics: {topic!},
        commitAfterProcessing: true,
        missingOffsets: fallback == null
            ? {}
            : {
                topic: {0: fallback}
              },
        tls: trust(badTrust ? wrongCa! : ca!),
        sasl: sasl ? (observed ?? credentials()) : null,
        allowTlsBootstrapFallback: false,
        sessionTimeout: const Duration(seconds: 6),
        heartbeatInterval: const Duration(seconds: 1),
        localExpiry: const Duration(seconds: 4),
        pollInterval: const Duration(milliseconds: 20));
    addTearDown(() async {
      owner.stop();
      await owner.closed.timeout(deliveryDeadline);
    });
    return owner;
  }

  group('real Kafka strict secure consumer runner', () {
    for (final sasl in [false, true]) {
      final mode = sasl ? 'SASL_SSL PLAIN' : 'TLS';
      test(
          '$mode loads stored positions, processes, commits and reads back on fresh transport',
          () async {
        final client = reader(sasl);
        final group = membership.uniqueGroup('consumer-secure');
        await membership.checkedCoordinator(client, group, topic!);
        final markers = [
          deliveryMarker('secure/skipped'),
          KafkaRecord(key: [
            0,
            255
          ], value: [
            255,
            0,
            1
          ], headers: [
            KafkaRecordHeader('duplicate', [1, 0, 255]),
            KafkaRecordHeader('duplicate', null),
            KafkaRecordHeader('empty', []),
          ]),
          KafkaRecord(key: [1], value: null),
          KafkaRecord(key: null, value: []),
        ];
        final barrier = deliveryMarker('secure/commit-response-barrier');
        final base =
            await produceDeliveryRecords(client, topic, [...markers, barrier]);
        await client.commitOffsets(group, topic, nextOffsets: {0: base + 1});
        final observed = _ObservedCredentials(credentials());
        final owner = consumer(group, sasl, fallback: base, observed: observed);
        final records = <KafkaGroupRecord>[];
        var commitResponseBarrierReached = false;
        final running = owner.run((record) {
          if (record.offset == base + markers.length) {
            // A new callback proves the prior commit response settled, unlike
            // an independent OffsetFetch which can observe storage earlier.
            expect(owner.isCurrent(record.assignment), isTrue);
            expect(record.record.value, barrier.value);
            commitResponseBarrierReached = true;
            owner.stop();
            return;
          }
          final index = records.length + 1;
          expect(index, lessThan(markers.length));
          expect(owner.isCurrent(record.assignment), isTrue);
          expect(record.topic, topic);
          expect(record.partition, 0);
          expect(record.offset, base + index);
          expect(record.record.key, markers[index].key);
          expect(record.record.value, markers[index].value);
          expect(record.record.headers.map((h) => h.key),
              markers[index].headers.map((h) => h.key));
          expect(record.record.headers.map((h) => h.value),
              markers[index].headers.map((h) => h.value));
          records.add(record);
        });
        final result = await running.timeout(deliveryDeadline);
        expectCleanConsumerStop(
            result, KafkaGroupConsumerStopReason.cancelled, 1);
        expect(commitResponseBarrierReached, isTrue);
        expect(records, hasLength(3));
        expect(result.lastCommit!.partitions[0]!.outcome,
            KafkaGroupCommitOutcome.confirmed);
        if (sasl) {
          expect(observed.payloadReads, greaterThanOrEqualTo(3),
              reason:
                  'Control, position lookup and delivery authenticate their own sockets');
        }
        expect(
            (await reader(sasl)
                    .committedOffsets(group, topic, partitions: {0}))[0]!
                .nextOffset,
            base + markers.length);
      });

      for (final badHost in [false, true]) {
        test(
            '$mode rejects ${badHost ? 'hostname' : 'wrong CA'} without processing or rejoin',
            () async {
          final observed = _ObservedCredentials(credentials());
          final owner = consumer(
              membership.uniqueGroup('consumer-untrusted'), sasl,
              badHost: badHost, badTrust: !badHost, observed: observed);
          var calls = 0;
          final result = await owner.run((_) {
            calls++;
          }).timeout(deliveryDeadline);
          expect(result.reason, KafkaGroupConsumerStopReason.failure);
          expect(result.error, isA<HandshakeException>());
          expect(result.cleanupErrors, isEmpty);
          expect(result.generationsStarted, 1);
          expect(result.lastSession!.leaveOutcome,
              KafkaGroupLeaveOutcome.notAttempted);
          expect(observed.payloadReads, 0,
              reason: 'TLS trust must be validated before SASL payload access');
          expect(calls, 0);
          expect(owner.assignment, isNull);
          expect(owner.state, KafkaGroupConsumerState.closed);
          await expectLater(owner.run((_) {}), throwsStateError);
        });
      }
    }

    test('rejected SASL credentials are terminal, not replayed, and sanitized',
        () async {
      final wrongPassword = '${password!}-wrong';
      final observed = _ObservedCredentials(
          KafkaSaslPlain(username: username!, password: wrongPassword));
      final owner = consumer(membership.uniqueGroup('consumer-bad-auth'), true,
          observed: observed);
      var calls = 0;
      final result = await owner.run((_) {
        calls++;
      }).timeout(deliveryDeadline);
      expect(result.reason, KafkaGroupConsumerStopReason.failure);
      expect(result.error, isA<KafkaAuthenticationException>());
      expect(result.cleanupErrors, isEmpty);
      expect(result.generationsStarted, 1);
      expect(result.lastSession!.leaveOutcome,
          KafkaGroupLeaveOutcome.notAttempted);
      expect(observed.payloadReads, 1);
      expect(calls, 0);
      expect(owner.assignment, isNull);
      for (final secret in [username, password, wrongPassword]) {
        expect(result.error.toString(), isNot(contains(secret)));
      }
    });
  },
      skip:
          configured ? false : 'Run owned secure fixture with --concurrency=1',
      timeout: const Timeout(Duration(minutes: 2)));
}

class _ObservedCredentials implements KafkaSaslPlain {
  final KafkaSaslPlain delegate;
  int payloadReads = 0;
  _ObservedCredentials(this.delegate);
  @override
  Uint8List get authBytes {
    payloadReads++;
    return delegate.authBytes;
  }

  @override
  String toString() => 'ObservedCredentials(<redacted>)';
}
