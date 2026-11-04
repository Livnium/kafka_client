import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:kafka_client/kafka_client.dart';
import 'package:test/test.dart';

const _deadline = Duration(seconds: 10);

Future<void> _untilSettled(KafkaClient client) async {
  final clock = Stopwatch()..start();
  while (client.activeSubscriptionCount != 0 ||
      client.pendingRequestCount != 0 ||
      client.admittedRequestCount != 0 ||
      client.queuedRequestBytes != 0 ||
      client.bufferedResponseBytes != 0) {
    if (clock.elapsed >= _deadline) {
      fail('Subscription work did not settle within $_deadline');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

void _expectClosed(KafkaClient client) {
  expect(client.activeSubscriptionCount, 0);
  expect(client.pendingRequestCount, 0);
  expect(client.admittedRequestCount, 0);
  expect(client.queuedRequestBytes, 0);
  expect(client.bufferedResponseBytes, 0);
  expect(client.connectionCount, 0);
}

void main() {
  final env = Platform.environment;
  final tlsPort = int.tryParse(env['KAFKA_SECURE_TLS_PORT'] ?? '');
  final saslPort = int.tryParse(env['KAFKA_SECURE_SASL_PORT'] ?? '');
  final scramPort = int.tryParse(env['KAFKA_SECURE_SCRAM_PORT'] ?? '');
  final caPath = env['KAFKA_SECURE_CA'];
  final wrongCaPath = env['KAFKA_SECURE_WRONG_CA'];
  final topic = env['KAFKA_SECURE_TOPIC'];
  final username = env['KAFKA_SECURE_USERNAME'];
  final password = env['KAFKA_SECURE_PASSWORD'];
  final configured = [
    tlsPort,
    saslPort,
    caPath,
    wrongCaPath,
    topic,
    username,
    password,
  ].every((value) => value != null && value != '');

  group('real Kafka 4.0.2 secure topic subscriptions', () {
    KafkaTlsConfig trustedTls() => KafkaTlsConfig(
          trustedCertificates: File(caPath!).readAsBytesSync(),
          withTrustedRoots: false,
        );

    KafkaSaslPlain credentials() =>
        KafkaSaslPlain(username: username!, password: password!);

    KafkaClient connect({
      required int port,
      String host = 'localhost',
      KafkaTlsConfig? tls,
      KafkaSaslPlain? sasl,
    }) {
      final client = KafkaClient(
        brokers: [Broker(host, port)],
        tls: tls ?? trustedTls(),
        sasl: sasl,
        requestTimeout: const Duration(seconds: 5),
        connectionTimeout: const Duration(seconds: 5),
      );
      addTearDown(() async {
        await client.close().timeout(_deadline);
        _expectClosed(client);
      });
      // Exercise the default strict TLS policy, without recovery opt-ins.
      expect(client.allowTlsBootstrapFallback, isFalse);
      expect(client.tlsFetchRecovery, isNull);
      return client;
    }

    Future<void> roundTrip(
        {required int port, required bool authenticated}) async {
      final observed =
          authenticated ? _ObservedCredentials(credentials()) : null;
      final producer =
          connect(port: port, sasl: authenticated ? credentials() : null);
      // Subscription must establish its own verified/authenticated connection.
      final consumer = connect(port: port, sasl: observed);
      final timestamp = DateTime.now().millisecondsSinceEpoch;
      final records = [
        KafkaRecord(
          key: [0, 255],
          value: utf8.encode('secure-subscription-$authenticated-नमस्ते'),
          timestamp: timestamp,
          headers: [
            KafkaRecordHeader('secure-subscription', [1, 0, 255])
          ],
        ),
        KafkaRecord(
            key: utf8.encode('tombstone'), value: null, timestamp: timestamp),
        KafkaRecord(key: [], value: [], timestamp: timestamp),
      ];
      final produced = await producer.produce(topic!, records, acks: -1);
      final offset =
          produced!.responses.single.partitionResponses.single.baseOffset;
      final stream = consumer.subscribe(
        topic,
        startOffsets: {0: offset},
        pollInterval: const Duration(milliseconds: 20),
        retryDelay: const Duration(milliseconds: 20),
      );
      expect(consumer.activeSubscriptionCount, 0);
      expect(consumer.connectionCount, 0);
      final received = await stream
          .map((event) {
            expect(consumer.activeSubscriptionCount, 1);
            return event;
          })
          .take(records.length)
          .toList()
          .timeout(_deadline);
      expect(received, hasLength(records.length));
      for (var index = 0; index < records.length; index++) {
        final event = received[index];
        expect(event.topic, topic);
        expect(event.partition, 0);
        expect(event.offset, offset + index);
        expect(event.record.offset, offset + index);
        expect(event.record.key, records[index].key);
        expect(event.record.value, records[index].value);
        expect(event.record.timestamp, timestamp);
      }
      expect(received.first.record.headers.single.key, 'secure-subscription');
      expect(received.first.record.headers.single.value, [1, 0, 255]);
      if (observed != null) expect(observed.payloadReads, greaterThan(0));
      await _untilSettled(consumer);
      expect(consumer.bootstrapTlsFallbackCount, 0);
      // take() cancels the stream, not the reusable shared client.
      await consumer.metadata({topic}).timeout(_deadline);
      await consumer.close().timeout(_deadline);
      _expectClosed(consumer);
    }

    Future<Object> terminalFailure(
        KafkaClient client, Matcher expectedFailure) async {
      final events = <KafkaTopicRecord>[];
      final failures = <Object>[];
      final done = Completer<void>();
      final stream = client.subscribe(
        topic!,
        startOffsets: {0: 0},
        pollInterval: const Duration(milliseconds: 20),
        retryDelay: const Duration(milliseconds: 20),
      );
      expect(client.activeSubscriptionCount, 0);
      expect(client.connectionCount, 0);
      final subscription = stream.listen(
        events.add,
        onError: (Object error, StackTrace _) => failures.add(error),
        onDone: () => done.complete(),
        // Do not let the listener manufacture termination on the first error.
        cancelOnError: false,
      );
      addTearDown(() => subscription.cancel().timeout(_deadline));
      // This must finish before cancel/close: security failures must not enter
      // the subscription's otherwise indefinite transient-retry loop.
      await done.future.timeout(_deadline);
      expect(events, isEmpty);
      expect(failures, hasLength(1));
      expect(failures.single, expectedFailure);
      expect(client.activeSubscriptionCount, 0);
      await _untilSettled(client);
      expect(client.connectionCount, 0);
      expect(client.bootstrapTlsFallbackCount, 0);
      return failures.single;
    }

    test('trusted TLS streams produced records and releases its subscription',
        () async {
      await roundTrip(port: tlsPort!, authenticated: false);
    });

    test('trusted SASL_SSL PLAIN streams produced records after authentication',
        () async {
      await roundTrip(port: saslPort!, authenticated: true);
    });

    for (final authenticated in [false, true]) {
      final mode = authenticated ? 'SASL_SSL PLAIN' : 'TLS';
      test('$mode wrong CA is terminal and never reads credentials', () async {
        final observed =
            authenticated ? _ObservedCredentials(credentials()) : null;
        final client = connect(
          port: authenticated ? saslPort! : tlsPort!,
          tls: KafkaTlsConfig(
            trustedCertificates: File(wrongCaPath!).readAsBytesSync(),
            withTrustedRoots: false,
          ),
          sasl: observed,
        );
        await terminalFailure(client, isA<HandshakeException>());
        if (observed != null) expect(observed.payloadReads, 0);
      });

      test('$mode hostname rejection is terminal and never reads credentials',
          () async {
        final observed =
            authenticated ? _ObservedCredentials(credentials()) : null;
        // The owned fixture certificate covers DNS localhost, not this IP.
        final client = connect(
          port: authenticated ? saslPort! : tlsPort!,
          host: '127.0.0.1',
          sasl: observed,
        );
        await terminalFailure(client, isA<HandshakeException>());
        if (observed != null) expect(observed.payloadReads, 0);
      });
    }

    test('rejected SASL password emits one sanitized error and stream done',
        () async {
      final wrongPassword = '${password!}-wrong';
      final observed = _ObservedCredentials(
          KafkaSaslPlain(username: username!, password: wrongPassword));
      final client = connect(port: saslPort!, sasl: observed);
      final failure =
          await terminalFailure(client, isA<KafkaAuthenticationException>());
      expect(observed.payloadReads, 1,
          reason: 'A rejected authentication must not be replayed');
      expect(failure.toString(), isNot(contains(username)));
      expect(failure.toString(), isNot(contains(password)));
      expect(failure.toString(), isNot(contains(wrongPassword)));
    });

    test('rejected PLAIN mechanism ends the stream before reading credentials',
        () async {
      final observed = _ObservedCredentials(credentials());
      final client = connect(port: scramPort!, sasl: observed);
      final failure =
          await terminalFailure(client, isA<KafkaAuthenticationException>());
      expect((failure as KafkaAuthenticationException).errorCode, 33);
      expect(observed.payloadReads, 0);
      expect(failure.toString(), isNot(contains(username)));
      expect(failure.toString(), isNot(contains(password)));
    },
        skip: scramPort == null
            ? 'Secure SCRAM-only fixture unavailable'
            : false);
  },
      skip: configured
          ? false
          : 'Run tool/run_secure_broker_integration.sh dart test '
              'test/topic_subscription_security_integration_test.dart',
      timeout: const Timeout(Duration(seconds: 30)));
}

// Observe token access without retaining or logging payloads beyond the source
// credentials. All values come from the runner's disposable synthetic fixture.
class _ObservedCredentials implements KafkaSaslPlain {
  final KafkaSaslPlain _delegate;
  int payloadReads = 0;

  _ObservedCredentials(this._delegate);

  @override
  Uint8List get authBytes {
    payloadReads++;
    return _delegate.authBytes;
  }

  @override
  String toString() => 'ObservedCredentials(<redacted>)';
}
