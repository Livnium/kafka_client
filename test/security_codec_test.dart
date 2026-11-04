import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart' hide KafkaType;
import 'package:kafka_client/src/security/sasl_requests.dart';
import 'package:kafka_client/src/security/security.dart';
import 'package:test/test.dart';

String temporaryCredential() => base64Url
    .encode(List<int>.generate(24, (_) => Random.secure().nextInt(256)));

List<int> handshakeResponse(SaslHandshakeRequest request,
        {int error = 0, List<String>? mechanisms}) =>
    (KafkaBytesBuilder()
          ..addInt32(request.correlationId)
          ..addInt16(error)
          ..addArray(mechanisms ?? ['PLAIN'], KafkaType.string))
        .takeBytes();

List<int> authenticateResponse(SaslAuthenticateRequest request,
        {int error = 0, String? message, List<int>? bytes = const []}) =>
    (KafkaBytesBuilder()
          ..addInt32(request.correlationId)
          ..addInt16(error)
          ..addString(message)
          ..addBytes(bytes))
        .takeBytes();

void main() {
  test(
      'PLAIN UTF8 token has empty authorization identity and immutable storage',
      () {
    final username = 'π-${temporaryCredential()}';
    final password = temporaryCredential();
    final config = KafkaSaslPlain(username: username, password: password);
    final expected = [0, ...utf8.encode(username), 0, ...utf8.encode(password)];
    expect(config.authBytes, expected);
    config.authBytes.fillRange(0, expected.length, 255);
    expect(config.authBytes, expected);
    expect(config.toString(), isNot(contains(username)));
    expect(config.toString(), isNot(contains(password)));
  });

  test('invalid credentials reject without echoing either credential', () {
    final secret = temporaryCredential();
    final invalid = [
      '',
      '$secret\u0000',
      List.filled(16385, 'x').join(),
      List.filled(8193, 'π').join(),
      String.fromCharCode(0xd800)
    ];
    for (final value in invalid) {
      for (final usernameInvalid in [true, false]) {
        expect(
            () => KafkaSaslPlain(
                username: usernameInvalid ? value : secret,
                password: usernameInvalid ? secret : value),
            throwsA(isA<ArgumentError>().having(
                (e) => e.toString(), 'redacted', isNot(contains(secret)))));
      }
    }
  });

  test('credential maximum is encoded-byte based and accepted at boundary', () {
    final config = KafkaSaslPlain(
        username: List.filled(8192, 'π').join(),
        password: temporaryCredential());
    expect(config.authBytes.length, greaterThan(16384));
  });

  test('TLS uses fresh validating contexts without global mutation', () {
    final tls = KafkaTlsConfig();
    expect(tls.withTrustedRoots, isTrue);
    final first = tls.createSecurityContext();
    final second = tls.createSecurityContext();
    expect(first, isA<SecurityContext>());
    expect(identical(first, second), isFalse);
    expect(identical(first, SecurityContext.defaultContext), isFalse);
  });

  test('TLS rejects absent trust and invalid CA bytes', () {
    expect(() => KafkaTlsConfig(withTrustedRoots: false), throwsArgumentError);
    for (final bytes in [
      <int>[],
      [-1],
      [256]
    ]) {
      expect(() => KafkaTlsConfig(trustedCertificates: bytes),
          throwsArgumentError);
    }
    final config =
        KafkaTlsConfig(trustedCertificates: utf8.encode('not a certificate'));
    expect(() => config.createSecurityContext(), throwsA(isA<TlsException>()));
    expect(config.toString(), isNot(contains('not a certificate')));
  });

  test('handshake request golden v1 header and PLAIN bytes', () {
    final request = SaslHandshakeRequest();
    final frame = request.toBytes();
    final reader = KafkaBytesReader.fromBytes(frame);
    expect(reader.readInt32(), frame.length - 4);
    expect(reader.readInt16(), 17);
    expect(reader.readInt16(), 1);
    expect(reader.readInt32(), request.correlationId);
    expect(reader.readString(), 'dart_kafka');
    expect(reader.readRaw(reader.remaining), [0, 5, 80, 76, 65, 73, 78]);
    expect(request.parseResponse(handshakeResponse(request)).ensureSuccess,
        returnsNormally);
  });

  test('authenticate request v0 token length and redacted diagnostics', () {
    final config = KafkaSaslPlain(
        username: temporaryCredential(), password: temporaryCredential());
    final bytes = config.authBytes;
    final request = SaslAuthenticateRequest(bytes);
    bytes.fillRange(0, bytes.length, 255);
    final frame = request.toBytes();
    final reader = KafkaBytesReader.fromBytes(frame);
    expect(reader.readInt32(), frame.length - 4);
    expect(reader.readInt16(), 36);
    expect(reader.readInt16(), 0);
    expect(reader.readInt32(), request.correlationId);
    expect(reader.readString(), 'dart_kafka');
    expect(reader.readBytes(), config.authBytes);
    expect(reader.isEOF, isTrue);
    expect(request.toString(), 'SaslAuthenticateRequest(<redacted>)');
    expect(request.parseResponse(authenticateResponse(request)).ensureSuccess,
        returnsNormally);
  });

  test('handshake errors and missing PLAIN fail with numeric code only', () {
    final request = SaslHandshakeRequest();
    for (final response in [
      request.parseResponse(handshakeResponse(request, error: 33)),
      request.parseResponse(
          handshakeResponse(request, mechanisms: ['SCRAM-SHA-256'])),
    ]) {
      expect(
          response.ensureSuccess, throwsA(isA<KafkaAuthenticationException>()));
    }
  });

  test('authentication broker message cannot leak into error or response text',
      () {
    final secret = temporaryCredential();
    final request = SaslAuthenticateRequest([0, 1, 0, 2]);
    final response = request.parseResponse(authenticateResponse(request,
        error: 58, message: secret, bytes: utf8.encode(secret)));
    expect(response.toString(), isNot(contains(secret)));
    expect(
        response.ensureSuccess,
        throwsA(isA<KafkaAuthenticationException>()
            .having((e) => e.toString(), 'no secret', isNot(contains(secret)))
            .having((e) => e.errorCode, 'numeric code', 58)));
    expect(const KafkaAuthenticationException().toString(),
        'KafkaAuthenticationException: authentication failed');
  });

  test('unexpected PLAIN challenge is rejected and response bytes are copied',
      () {
    final request = SaslAuthenticateRequest([0, 1, 0, 2]);
    final response =
        request.parseResponse(authenticateResponse(request, bytes: [1]));
    response.authBytes[0] = 2;
    expect(response.authBytes, [1]);
    expect(
        response.ensureSuccess, throwsA(isA<KafkaAuthenticationException>()));
  });

  test('both response codecs reject every truncation and trailing bytes', () {
    final handshake = SaslHandshakeRequest();
    final auth = SaslAuthenticateRequest([0, 1, 0, 2]);
    for (final entry in <(List<int>, dynamic Function(List<int>))>[
      (handshakeResponse(handshake), handshake.parseResponse),
      (authenticateResponse(auth), auth.parseResponse)
    ]) {
      for (var i = 0; i < entry.$1.length; i++) {
        expect(() => entry.$2(entry.$1.sublist(0, i)), throwsFormatException);
      }
      expect(() => entry.$2([...entry.$1, 0]), throwsFormatException);
    }
  });

  test('invalid SASL counts null strings bytes and correlation fail sanitized',
      () {
    final handshake = SaslHandshakeRequest();
    for (final count in [-1, 2147483647]) {
      final bytes = (KafkaBytesBuilder()
            ..addInt32(handshake.correlationId)
            ..addInt16(0)
            ..addInt32(count))
          .takeBytes();
      expect(() => handshake.parseResponse(bytes), throwsFormatException);
    }
    final nullMechanism = (KafkaBytesBuilder()
          ..addInt32(handshake.correlationId)
          ..addInt16(0)
          ..addInt32(1)
          ..addString(null))
        .takeBytes();
    expect(() => handshake.parseResponse(nullMechanism), throwsFormatException);
    final auth = SaslAuthenticateRequest([0, 1, 0, 2]);
    expect(() => auth.parseResponse(authenticateResponse(auth, bytes: null)),
        throwsFormatException);
    final wrongId = authenticateResponse(auth)..[0] = 127;
    expect(() => auth.parseResponse(wrongId), throwsFormatException);
    final secret = temporaryCredential();
    final malformed = (KafkaBytesBuilder()
          ..addInt32(auth.correlationId)
          ..addInt16(58)
          ..addInt16(1)
          ..addRaw([255])
          ..addRaw(utf8.encode(secret)))
        .takeBytes();
    expect(
        () => auth.parseResponse(malformed),
        throwsA(isA<FormatException>()
            .having((e) => e.toString(), 'no payload', isNot(contains(secret)))
            .having((e) => e.source, 'no source', isNull)));
  });

  test('authenticate payload bounds reject without exposing content', () {
    for (final bytes in [
      <int>[],
      [-1],
      [256],
      List.filled(32771, 1)
    ]) {
      expect(() => SaslAuthenticateRequest(bytes), throwsArgumentError);
    }
  });
}
