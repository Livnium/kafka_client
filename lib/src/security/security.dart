import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

/// Server-authenticated TLS settings for native Kafka connections.
///
/// Certificate and hostname validation remain enabled. This configuration does
/// not provide an insecure bypass, TLS key logging, or client certificates.
/// Custom certificates must use a format supported by [SecurityContext]
/// (normally PEM); platform-specific certificate-loading restrictions apply.
class KafkaTlsConfig {
  final List<int>? _trustedCertificates;

  /// Whether to include the platform's built-in trusted certificate roots.
  final bool withTrustedRoots;

  /// Creates TLS settings, defensively copying optional custom CA bytes.
  ///
  /// With [withTrustedRoots] false, [trustedCertificates] must be supplied and
  /// becomes the only source of trust. Certificate input is limited to 4 MiB.
  KafkaTlsConfig({List<int>? trustedCertificates, this.withTrustedRoots = true})
      : _trustedCertificates = trustedCertificates == null
            ? null
            : List<int>.unmodifiable(trustedCertificates) {
    if (!withTrustedRoots && trustedCertificates == null) {
      throw ArgumentError(
          'TLS requires trusted roots or custom CA certificates');
    }
    if (trustedCertificates != null &&
        (trustedCertificates.isEmpty ||
            trustedCertificates.length > 4 * 1024 * 1024 ||
            trustedCertificates.any((b) => b < 0 || b > 255))) {
      throw ArgumentError('Invalid TLS certificate byte input');
    }
  }

  /// Creates an independent context without changing global TLS defaults.
  ///
  /// Invalid or unsupported certificate containers throw the platform's
  /// certificate-loading exception. No certificate validation is disabled.
  SecurityContext createSecurityContext() {
    final context = SecurityContext(withTrustedRoots: withTrustedRoots);
    final certificates = _trustedCertificates;
    if (certificates != null) {
      context.setTrustedCertificatesBytes(certificates);
    }
    return context;
  }

  @override
  String toString() =>
      'KafkaTlsConfig(withTrustedRoots: $withTrustedRoots, customCA: ${_trustedCertificates != null})';
}

/// SASL/PLAIN credentials, to be sent only over server-authenticated TLS.
///
/// Load credentials at runtime from a suitable secret provider. Do not commit
/// them or log [authBytes]. Dart memory management does not guarantee erasure
/// of credential strings or byte buffers. SCRAM and reauthentication are not
/// implemented by this configuration.
class KafkaSaslPlain {
  final Uint8List _authBytes;

  /// Maximum UTF-8 encoded size of either credential, as a defensive limit.
  static const int maxCredentialBytes = 16 * 1024;

  /// Validates and stores a PLAIN payload without exposing credential fields.
  ///
  /// Empty, NUL-containing, malformed-Unicode, or oversized credentials are
  /// rejected with errors that never include the supplied credential.
  factory KafkaSaslPlain({required String username, required String password}) {
    final user = _credentialBytes(username);
    final pass = _credentialBytes(password);
    return KafkaSaslPlain._(Uint8List.fromList([0, ...user, 0, ...pass]));
  }

  KafkaSaslPlain._(this._authBytes);

  static List<int> _credentialBytes(String value) {
    if (value.isEmpty ||
        value.contains('\u0000') ||
        value.length > maxCredentialBytes) {
      throw ArgumentError('Invalid SASL PLAIN credential');
    }
    final bytes = utf8.encode(value);
    if (bytes.length > maxCredentialBytes || utf8.decode(bytes) != value) {
      throw ArgumentError('Invalid SASL PLAIN credential');
    }
    return bytes;
  }

  /// A fresh copy of the sensitive PLAIN payload; never log or persist it.
  Uint8List get authBytes => Uint8List.fromList(_authBytes);

  @override
  String toString() => 'KafkaSaslPlain(<redacted>)';
}

/// A sanitized authentication failure that never retains broker error text.
class KafkaAuthenticationException implements Exception {
  /// Optional Kafka numeric error code; no untrusted broker message is stored.
  final int? errorCode;

  /// Creates a failure safe for diagnostics without credential disclosure.
  const KafkaAuthenticationException({this.errorCode});

  @override
  String toString() => errorCode == null
      ? 'KafkaAuthenticationException: authentication failed'
      : 'KafkaAuthenticationException: authentication failed (code $errorCode)';
}
