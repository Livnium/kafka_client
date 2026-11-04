import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:kafka_client/kafka_client.dart';

/// Appends one record to partition 0 of a pre-created, dedicated topic, then
/// verifies that exact record over TLS. Do not run against a topic you cannot
/// write to. Every advertised broker hostname must match its TLS certificate.
///
/// Required: KAFKA_HOST, KAFKA_TOPIC. Optional: KAFKA_PORT (default 9093),
/// KAFKA_CA_FILE (otherwise platform roots), and paired KAFKA_USERNAME and
/// KAFKA_PASSWORD for SASL PLAIN. A custom CA replaces platform trust roots.
///
/// Environment variables are a demonstration, not a secret manager. Inject
/// credentials from a trusted runtime provider; never commit, print, or persist
/// them. This example does not save configuration or credentials.
Future<void> main() async {
  try {
    final config = SecureExampleConfig.fromEnvironment(Platform.environment);
    final offset = await runSecureExample(config);
    final mode = config.sasl == null ? 'tls' : 'sasl_tls';
    stdout.writeln('SECURE_EXAMPLE_OK offset=$offset mode=$mode');
  } catch (error) {
    // Do not echo raw exceptions, stack traces, paths, endpoints, or credentials.
    stderr
        .writeln('SECURE_EXAMPLE_ERROR ${secureExampleFailureCategory(error)}');
    stderr.writeln('A write may already be stored; do not blindly rerun it.');
    exitCode = 1;
  }
}

/// Validated example settings; parsing does no I/O and opens no connection.
class SecureExampleConfig {
  final String host;
  final int port;
  final String topic;
  final String? caFile;
  final KafkaSaslPlain? sasl;

  SecureExampleConfig._(
      this.host, this.port, this.topic, this.caFile, this.sasl);

  factory SecureExampleConfig.fromEnvironment(Map<String, String> environment) {
    final host = environment['KAFKA_HOST'];
    if (host == null ||
        host.isEmpty ||
        RegExp(r'[\s/\x00]').hasMatch(host) ||
        (host.contains(':') && InternetAddress.tryParse(host) == null)) {
      throw const FormatException(
          'KAFKA_HOST must be a hostname or IP address');
    }
    final portText = environment['KAFKA_PORT'] ?? '9093';
    final port =
        RegExp(r'^[0-9]+$').hasMatch(portText) ? int.tryParse(portText) : null;
    if (port == null || port < 1 || port > 65535) {
      throw const FormatException(
          'KAFKA_PORT must be an integer from 1 to 65535');
    }
    final topic = environment['KAFKA_TOPIC'];
    if (topic == null ||
        topic.isEmpty ||
        topic.length > 249 ||
        topic == '.' ||
        topic == '..' ||
        !RegExp(r'^[a-zA-Z0-9._-]+$').hasMatch(topic)) {
      throw const FormatException('KAFKA_TOPIC must be a valid topic name');
    }
    final caFile = environment['KAFKA_CA_FILE'];
    if (caFile != null &&
        (caFile.trim().isEmpty || caFile.contains('\u0000'))) {
      throw const FormatException('KAFKA_CA_FILE must name a certificate file');
    }
    final username = environment['KAFKA_USERNAME'];
    final password = environment['KAFKA_PASSWORD'];
    if ((username == null) != (password == null)) {
      throw const FormatException(
          'Set both SASL credential variables or neither');
    }
    // The credential constructor rejects empty/NUL/malformed/oversized values.
    // Preserve the values exactly: trimming a password would change its meaning.
    final sasl = username == null
        ? null
        : KafkaSaslPlain(username: username, password: password!);
    return SecureExampleConfig._(host, port, topic, caFile, sasl);
  }
}

/// Uses only the public client API. TLS is mandatory, including without SASL.
Future<int> runSecureExample(SecureExampleConfig config) async {
  final caFile = config.caFile;
  final tls = caFile == null
      ? KafkaTlsConfig()
      : KafkaTlsConfig(
          trustedCertificates: await File(caFile).readAsBytes(),
          withTrustedRoots: false,
        );
  final client = KafkaClient(
    brokers: [Broker(config.host, config.port)],
    tls: tls,
    sasl: config.sasl,
    requestTimeout: const Duration(seconds: 5),
    connectionTimeout: const Duration(seconds: 5),
  );
  try {
    final random = Random.secure();
    final expected = KafkaRecord(
      key: List<int>.generate(16, (_) => random.nextInt(256)),
      value: utf8.encode('Hello from secure Dart'),
    );
    // Exactly one Produce call. Failure can be ambiguous, even before this
    // example prints success, so it must never trigger automatic write replay.
    final produced = await client.produce(config.topic, [expected], acks: -1);
    if (produced == null) throw StateError('Missing Produce acknowledgement');
    final offset =
        produced.responses.single.partitionResponses.single.baseOffset;
    if (offset < 0) throw StateError('Invalid acknowledged offset');

    // Fetch can briefly be empty. Only reads are polled, at the same inclusive
    // offset. Five attempts are finite, not an overall-operation deadline:
    // each high-level Fetch can perform several bounded low-level requests.
    for (var attempt = 0; attempt < 5; attempt++) {
      final fetched = await client.fetch(config.topic, offset: offset);
      final records = fetched.topics.single.partitions.single.records;
      if (verifyAcknowledgedRecord(records,
          offset: offset, expected: expected)) {
        return offset;
      }
      if (attempt < 4) {
        await Future<void>.delayed(const Duration(milliseconds: 100));
      }
    }
    throw StateError('Acknowledged record not visible within read attempts');
  } finally {
    // Closing is terminal and does not gracefully drain pending Produce acks.
    // All intended operations above are awaited before teardown begins.
    await client.close();
  }
}

/// Returns false only when the acknowledged offset is absent. Other records
/// are ignored, never decoded or printed. A conflicting record fails closed.
bool verifyAcknowledgedRecord(Iterable<KafkaRecord> records,
    {required int offset, required KafkaRecord expected}) {
  if (offset < 0) throw ArgumentError('Expected offset must be nonnegative');
  final matches = records.where((record) => record.offset == offset).toList();
  if (matches.isEmpty) return false;
  if (matches.length != 1) throw StateError('Duplicate acknowledged offset');
  final actual = matches.single;
  if (!_sameBytes(actual.key, expected.key) ||
      !_sameBytes(actual.value, expected.value) ||
      actual.headers.length != expected.headers.length) {
    throw StateError('Acknowledged record did not match');
  }
  for (var i = 0; i < actual.headers.length; i++) {
    if (actual.headers[i].key != expected.headers[i].key ||
        !_sameBytes(actual.headers[i].value, expected.headers[i].value)) {
      throw StateError('Acknowledged record headers did not match');
    }
  }
  // Brokers configured for LogAppendTime can legitimately change timestamps.
  return true;
}

bool _sameBytes(List<int>? left, List<int>? right) {
  if (left == null || right == null) return left == null && right == null;
  if (left.length != right.length) return false;
  for (var i = 0; i < left.length; i++) {
    if (left[i] != right[i]) return false;
  }
  return true;
}

/// Fixed labels only; even an untrusted exception's toString is never called.
/// The retained `verification` label covers every StateError, including client
/// state, ApiVersions and leader discovery failures, not only record mismatches.
String secureExampleFailureCategory(Object error) {
  if (error is KafkaAuthenticationException) return 'authentication';
  if (error is TlsException) return 'tls';
  if (error is TimeoutException) return 'timeout';
  if (error is KafkaBrokerException) return 'broker';
  if (error is KafkaResourceLimitException) return 'resource';
  if (error is KafkaConnectionException || error is SocketException) {
    return 'connection';
  }
  // Format errors can come from local settings or malformed broker data.
  if (error is FormatException) return 'format';
  if (error is FileSystemException || error is ArgumentError) {
    return 'configuration';
  }
  if (error is StateError) return 'verification';
  return 'unexpected';
}
