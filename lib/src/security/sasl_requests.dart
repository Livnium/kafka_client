import 'dart:typed_data';

import '../api/abstract/kafka_request.dart';
import '../codec/byte_builder.dart';
import '../codec/byte_reader.dart' hide KafkaType;
import 'security.dart';

/// SASL handshake v1 selects PLAIN and enables the framed Authenticate API.
/// This internal request must only be sent on a verified TLS connection.
class SaslHandshakeRequest extends KafkaRequest {
  /// Kafka SASL handshake API key.
  final int apiKey = 17;

  /// Non-flexible handshake version supporting SaslAuthenticate.
  final int apiVersion = 1;

  @override
  List<int> toBytes() => KafkaRequest.frame(
      (KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
            ..addString('PLAIN'))
          .takeBytes());

  @override
  SaslHandshakeResponse parseResponse(List<int> bytes) {
    try {
      final r = KafkaBytesReader.fromBytes(bytes);
      final id = r.readInt32();
      if (id != correlationId) throw const FormatException();
      final error = r.readInt16();
      final count = r.readInt32();
      if (count < 0 || count > r.remaining ~/ 2) {
        throw const FormatException();
      }
      final mechanisms = <String>[];
      for (var i = 0; i < count; i++) {
        final mechanism = r.readString();
        if (mechanism == null || mechanism.isEmpty) {
          throw const FormatException();
        }
        mechanisms.add(mechanism);
      }
      if (!r.isEOF) throw const FormatException();
      return SaslHandshakeResponse._(id, error, mechanisms);
    } catch (_) {
      // Never propagate errors that might include a malicious server payload.
      throw const FormatException('Malformed SASL handshake response');
    }
  }

  @override
  String toString() => 'SaslHandshakeRequest(PLAIN)';
}

/// Parsed SASL handshake response; mechanism strings are not logged.
class SaslHandshakeResponse {
  /// Correlation identifier matching the handshake request.
  final int correlationId;

  /// Kafka error code, zero for success.
  final int errorCode;

  /// Mechanisms advertised by the server, retained as immutable data.
  final List<String> mechanisms;

  SaslHandshakeResponse._(
      this.correlationId, this.errorCode, List<String> mechanisms)
      : mechanisms = List.unmodifiable(mechanisms);

  /// Requires a successful handshake explicitly advertising PLAIN.
  void ensureSuccess() {
    if (errorCode != 0 || !mechanisms.contains('PLAIN')) {
      throw KafkaAuthenticationException(errorCode: errorCode);
    }
  }

  @override
  String toString() => 'SaslHandshakeResponse(errorCode: $errorCode)';
}

/// Framed SASL Authenticate v0 carrying a PLAIN token over verified TLS only.
class SaslAuthenticateRequest extends KafkaRequest {
  final Uint8List _authBytes;

  /// Kafka SASL Authenticate API key.
  final int apiKey = 36;

  /// Non-flexible version without broker session-lifetime negotiation.
  final int apiVersion = 0;

  /// Copies sensitive authentication bytes without exposing them in diagnostics.
  SaslAuthenticateRequest(List<int> authBytes)
      : _authBytes = Uint8List.fromList(authBytes) {
    if (authBytes.isEmpty ||
        authBytes.length > 2 * KafkaSaslPlain.maxCredentialBytes + 2 ||
        authBytes.any((b) => b < 0 || b > 255)) {
      throw ArgumentError('Invalid SASL authentication payload');
    }
  }

  @override
  List<int> toBytes() => KafkaRequest.frame(
      (KafkaBytesBuilder.withRequestHeader(apiKey, apiVersion, correlationId)
            ..addBytes(_authBytes))
          .takeBytes());

  @override
  SaslAuthenticateResponse parseResponse(List<int> bytes) {
    try {
      final r = KafkaBytesReader.fromBytes(bytes);
      final id = r.readInt32();
      if (id != correlationId) throw const FormatException();
      final error = r.readInt16();
      // Validate but discard untrusted error text; it may echo credentials.
      r.readString();
      final auth = r.readBytes();
      if (auth == null || !r.isEOF) throw const FormatException();
      return SaslAuthenticateResponse._(id, error, auth);
    } catch (_) {
      throw const FormatException('Malformed SASL authenticate response');
    }
  }

  @override
  String toString() => 'SaslAuthenticateRequest(<redacted>)';
}

/// A sanitized Authenticate result; the broker error message is never retained.
class SaslAuthenticateResponse {
  /// Correlation identifier matching the authentication request.
  final int correlationId;

  /// Kafka error code, zero for success.
  final int errorCode;
  final Uint8List _authBytes;

  SaslAuthenticateResponse._(
      this.correlationId, this.errorCode, List<int> bytes)
      : _authBytes = Uint8List.fromList(bytes);

  /// A defensive copy of server authentication bytes; never log this data.
  Uint8List get authBytes => Uint8List.fromList(_authBytes);

  /// Requires successful PLAIN authentication with no additional challenge.
  void ensureSuccess() {
    if (errorCode != 0 || _authBytes.isNotEmpty) {
      throw KafkaAuthenticationException(errorCode: errorCode);
    }
  }

  @override
  String toString() => 'SaslAuthenticateResponse(errorCode: $errorCode)';
}
