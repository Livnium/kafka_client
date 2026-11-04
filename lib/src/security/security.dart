import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

/// An optional client certificate identity for native mutual TLS (mTLS).
///
/// Load identities from a runtime secret provider, never from committed files.
/// Material is copied and diagnostics are redacted. Dart cannot guarantee
/// erasure of keys, passwords or their copies from process memory.
///
/// Loading uses [SecurityContext] and its platform/container restrictions. In
/// particular, iOS requires a PKCS12 identity containing both the private key
/// and certificate chain; separate PEM identity loading is not portable to iOS.
/// This API does not claim packaged Android/iOS qualification.
class KafkaTlsClientIdentity {
  final List<int> _certificateChain;
  final List<int> _privateKey;
  final String? _password;

  /// Maximum size of each certificate chain, private key or PKCS12 container.
  static const int maxMaterialBytes = 4 * 1024 * 1024;

  /// Maximum UTF-8 encoded size of an optional container/key password.
  static const int maxPasswordBytes = 16 * 1024;

  /// Copies a PEM certificate chain and its matching PEM private key.
  ///
  /// [privateKeyPassword] decrypts an encrypted key. Leave it null for an
  /// unencrypted key. On iOS use [KafkaTlsClientIdentity.pkcs12] instead.
  factory KafkaTlsClientIdentity.pem({
    required List<int> certificateChain,
    required List<int> privateKey,
    String? privateKeyPassword,
  }) {
    _validatePassword(privateKeyPassword);
    return KafkaTlsClientIdentity._(_copyMaterial(certificateChain),
        _copyMaterial(privateKey), privateKeyPassword);
  }

  /// Copies a PKCS12 container containing the certificate chain and private key.
  ///
  /// [password] decrypts that container. Supported PKCS12 encryption algorithms
  /// depend on the running Dart SDK and platform. No format conversion or
  /// insecure fallback is attempted.
  factory KafkaTlsClientIdentity.pkcs12({
    required List<int> bytes,
    String? password,
  }) {
    _validatePassword(password);
    final material = _copyMaterial(bytes);
    return KafkaTlsClientIdentity._(material, material, password);
  }

  KafkaTlsClientIdentity._(
      this._certificateChain, this._privateKey, this._password);

  static List<int> _copyMaterial(List<int> bytes) {
    if (bytes.isEmpty ||
        bytes.length > maxMaterialBytes ||
        bytes.any((b) => b < 0 || b > 255)) {
      throw ArgumentError('Invalid TLS client identity byte input');
    }
    return List<int>.unmodifiable(bytes);
  }

  static void _validatePassword(String? password) {
    if (password == null) return;
    if (password.length > maxPasswordBytes || password.contains('\u0000')) {
      throw ArgumentError('Invalid TLS client identity password');
    }
    final bytes = utf8.encode(password);
    if (bytes.length > maxPasswordBytes || utf8.decode(bytes) != password) {
      throw ArgumentError('Invalid TLS client identity password');
    }
  }

  void _loadInto(SecurityContext context) {
    try {
      context.useCertificateChainBytes(_certificateChain, password: _password);
      context.usePrivateKeyBytes(_privateKey, password: _password);
    } catch (_) {
      // Native errors can contain input or password details. Do not retain a
      // cause or echo native text, including for unsupported platform formats.
      throw TlsException('Unable to load TLS client identity');
    }
  }

  @override
  String toString() => 'KafkaTlsClientIdentity(<redacted>)';
}

/// Server-authenticated TLS settings with optional client authentication.
///
/// Certificate and hostname validation remain enabled. This configuration does
/// not provide an insecure bypass or TLS key logging. Custom certificates and
/// identities must use formats supported by [SecurityContext]; platform-specific
/// certificate-loading restrictions apply.
class KafkaTlsConfig {
  final List<int>? _trustedCertificates;
  final KafkaTlsClientIdentity? _clientIdentity;

  /// Whether to include the platform's built-in trusted certificate roots.
  final bool withTrustedRoots;

  /// Creates TLS settings, defensively copying optional custom CA bytes.
  ///
  /// With [withTrustedRoots] false, [trustedCertificates] must be supplied and
  /// becomes the only source of trust. Certificate input is limited to 4 MiB.
  /// An optional [clientIdentity] authenticates the client to a requesting
  /// broker, but never makes that identity's issuer a trusted server CA.
  KafkaTlsConfig({
    List<int>? trustedCertificates,
    this.withTrustedRoots = true,
    KafkaTlsClientIdentity? clientIdentity,
  })  : _clientIdentity = clientIdentity,
        _trustedCertificates = trustedCertificates == null
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
  /// Invalid or unsupported CA containers throw the platform's loading error.
  /// Client identity loading failures throw a redacted [TlsException]. No
  /// certificate validation is disabled, and no connection has yet been opened.
  SecurityContext createSecurityContext() {
    final context = SecurityContext(withTrustedRoots: withTrustedRoots);
    final certificates = _trustedCertificates;
    if (certificates != null) {
      context.setTrustedCertificatesBytes(certificates);
    }
    _clientIdentity?._loadInto(context);
    return context;
  }

  @override
  String toString() =>
      'KafkaTlsConfig(withTrustedRoots: $withTrustedRoots, customCA: ${_trustedCertificates != null}, clientIdentity: ${_clientIdentity != null})';
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
