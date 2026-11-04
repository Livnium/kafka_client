import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';

// Loopback peers test client identity and TLS ownership, not Kafka persistence
// or packaged mobile behavior. All certificates/keys are synthetic and ephemeral.
class _Material extends ListBase<int> {
  @override
  final int length;
  _Material(this.length);
  @override
  set length(int value) => throw UnsupportedError('immutable');
  @override
  int operator [](int index) => 1;
  @override
  void operator []=(int index, int value) =>
      throw UnsupportedError('immutable');
}

class _Peer {
  final ServerSocket server;
  final SecurityContext context;
  final bool requireIdentity;
  final sockets = <Socket>[];
  final tasks = <Future<void>>[];
  final requests = <int>[];
  final clientSubjects = <String>[];
  final applicationReceived = Completer<void>();
  bool stall = false;
  int accepted = 0;
  _Peer._(this.server, this.context, this.requireIdentity);

  static Future<_Peer> start(SecurityContext context,
      {bool requireIdentity = true}) async {
    final peer = _Peer._(
        await ServerSocket.bind(InternetAddress.loopbackIPv4, 0),
        context,
        requireIdentity);
    peer.server.listen((socket) {
      peer.accepted++;
      peer.sockets.add(socket);
      peer.tasks.add(peer._serve(socket));
    });
    return peer;
  }

  Future<void> _serve(Socket raw) async {
    try {
      final socket = await SecureSocket.secureServer(raw, context,
          requestClientCertificate: true,
          requireClientCertificate: requireIdentity);
      sockets.add(socket);
      final certificate = socket.peerCertificate;
      if (certificate != null) clientSubjects.add(certificate.subject);
      final buffer = <int>[];
      await for (final chunk in socket) {
        buffer.addAll(chunk);
        while (buffer.length >= 4) {
          final size = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (buffer.length < size + 4) break;
          final body = KafkaBytesReader.fromBytes(buffer.sublist(4, size + 4));
          buffer.removeRange(0, size + 4);
          final key = body.readInt16();
          body.readInt16();
          final id = body.readInt32();
          body.readString();
          requests.add(key);
          if (!applicationReceived.isCompleted) applicationReceived.complete();
          if (stall) continue;
          final response = KafkaBytesBuilder()..addInt32(id);
          if (key == 18) {
            response
              ..addInt16(0)
              ..addInt32(3)
              ..addInt16(3)
              ..addInt16(0)
              ..addInt16(0)
              ..addInt16(10)
              ..addInt16(2)
              ..addInt16(2)
              ..addInt16(9)
              ..addInt16(7)
              ..addInt16(7);
          } else if (key == 3) {
            response
              ..addInt32(0)
              ..addInt32(0);
          } else if (key == 10) {
            response
              ..addInt32(0)
              ..addInt16(0)
              ..addString(null)
              ..addInt32(1)
              ..addString('localhost')
              ..addInt32(server.port);
          } else if (key == 9) {
            response
              ..addInt8(0)
              ..addInt32(0)
              ..addInt8(2)
              ..addInt8(6)
              ..addRaw(utf8.encode('topic'))
              ..addInt8(2)
              ..addInt32(0)
              ..addInt64(-1)
              ..addInt32(-1)
              ..addInt8(0)
              ..addInt16(0)
              ..addInt8(0)
              ..addInt8(0)
              ..addInt16(0)
              ..addInt8(0);
          } else if (key == 17) {
            response
              ..addInt16(0)
              ..addInt32(1)
              ..addString('PLAIN');
          } else if (key == 36) {
            response
              ..addInt16(0)
              ..addString(null)
              ..addBytes([]);
          } else {
            throw StateError('Unexpected loopback API');
          }
          socket.add(KafkaRequest.frame(response.takeBytes()));
        }
      }
    } on SocketException {
      // Expected peer rejections/close. Never retain native diagnostic material.
    } on TlsException {
      // A missing/untrusted identity is expected to fail at the TLS boundary.
    } finally {
      raw.destroy();
    }
  }

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
    await Future.wait(tasks).timeout(const Duration(seconds: 3));
  }
}

void main() {
  test('identity byte bounds reject without retaining supplied values', () {
    for (final invalid in <List<int>>[
      [],
      [-1],
      [256],
      _Material(KafkaTlsClientIdentity.maxMaterialBytes + 1),
    ]) {
      for (final create in <void Function()>[
        () => KafkaTlsClientIdentity.pem(
            certificateChain: invalid, privateKey: [1]),
        () => KafkaTlsClientIdentity.pem(
            certificateChain: [1], privateKey: invalid),
        () => KafkaTlsClientIdentity.pkcs12(bytes: invalid),
      ]) {
        expect(create, throwsArgumentError);
      }
    }
    // Constructor size validation is distinct from platform format loading.
    final maximum = _Material(KafkaTlsClientIdentity.maxMaterialBytes);
    expect(
        () => KafkaTlsClientIdentity.pkcs12(bytes: maximum), returnsNormally);
    expect(
        () => KafkaTlsClientIdentity.pem(
            certificateChain: maximum, privateKey: maximum),
        returnsNormally);
  });

  test('identity passwords are byte bounded and diagnostic text is redacted',
      () {
    for (final invalid in [
      'private-marker\u0000',
      'x' * (KafkaTlsClientIdentity.maxPasswordBytes + 1),
      'π' * (KafkaTlsClientIdentity.maxPasswordBytes ~/ 2 + 1),
      String.fromCharCode(0xd800),
    ]) {
      for (final create in <void Function()>[
        () => KafkaTlsClientIdentity.pem(
            certificateChain: [1],
            privateKey: [2],
            privateKeyPassword: invalid),
        () => KafkaTlsClientIdentity.pkcs12(bytes: [1], password: invalid),
      ]) {
        expect(
            create,
            throwsA(isA<ArgumentError>().having((e) => e.toString(), 'redacted',
                isNot(contains('private-marker')))));
      }
    }
    for (final password in <String?>[
      null,
      '',
      'π' * (KafkaTlsClientIdentity.maxPasswordBytes ~/ 2),
    ]) {
      final identity =
          KafkaTlsClientIdentity.pkcs12(bytes: [1], password: password);
      expect(identity.toString(), 'KafkaTlsClientIdentity(<redacted>)');
      expect(
          KafkaTlsConfig(clientIdentity: identity).toString(),
          'KafkaTlsConfig(withTrustedRoots: true, customCA: false, '
          'clientIdentity: true)');
    }
    expect(
        () => KafkaTlsConfig(
            withTrustedRoots: false,
            clientIdentity: KafkaTlsClientIdentity.pkcs12(bytes: [1])),
        throwsArgumentError,
        reason: 'Client identity never supplies server trust');
  });

  group('synthetic local mutual TLS', () {
    late Directory directory;
    late List<int> ca, clientChain, clientKey, pkcs12, encryptedKey;
    late SecurityContext serverContext;
    const password = 'ephemeral-fixture-password';
    final peers = <_Peer>[];
    final clients = <KafkaClient>[];

    Future<void> openssl(List<String> args) async {
      final result = await Process.run('openssl', args,
          workingDirectory: directory.path,
          environment: {'MTLS_FIXTURE_PASSWORD': password});
      if (result.exitCode != 0) {
        // Do not include subprocess output, material or passwords in errors.
        throw StateError('Synthetic mTLS fixture preparation failed');
      }
    }

    setUpAll(() async {
      directory = await Directory.systemTemp.createTemp('kafka-mtls-');
      addTearDown(() => directory.delete(recursive: true));
      await openssl([
        'req',
        '-x509',
        '-newkey',
        'rsa:2048',
        '-nodes',
        '-days',
        '1',
        '-subj',
        '/CN=Owned ephemeral CA',
        '-keyout',
        'ca.key',
        '-out',
        'ca.pem',
        '-addext',
        'basicConstraints=critical,CA:TRUE',
      ]);
      for (final name in ['server', 'client', 'untrusted']) {
        await openssl([
          'req',
          '-newkey',
          'rsa:2048',
          '-nodes',
          '-subj',
          '/CN=${name == 'server' ? 'localhost' : 'owned-$name'}',
          '-keyout',
          '$name.key',
          '-out',
          '$name.csr',
        ]);
        await File('${directory.path}/$name.ext').writeAsString(
            'basicConstraints=critical,CA:FALSE\n'
            'keyUsage=critical,digitalSignature,keyEncipherment\n'
            'extendedKeyUsage=${name == 'server' ? 'serverAuth' : 'clientAuth'}\n'
            '${name == 'server' ? 'subjectAltName=DNS:localhost\n' : ''}');
        if (name == 'untrusted') {
          await openssl([
            'x509',
            '-req',
            '-in',
            '$name.csr',
            '-signkey',
            '$name.key',
            '-days',
            '1',
            '-extfile',
            '$name.ext',
            '-out',
            '$name.pem',
          ]);
        } else {
          await openssl([
            'x509',
            '-req',
            '-in',
            '$name.csr',
            '-CA',
            'ca.pem',
            '-CAkey',
            'ca.key',
            '-CAcreateserial',
            '-days',
            '1',
            '-extfile',
            '$name.ext',
            '-out',
            '$name.pem',
          ]);
        }
      }
      await openssl([
        'pkcs8',
        '-topk8',
        '-in',
        'client.key',
        '-out',
        'encrypted.key',
        '-v2',
        'aes-256-cbc',
        '-passout',
        'env:MTLS_FIXTURE_PASSWORD',
      ]);
      await openssl([
        'pkcs12',
        '-export',
        '-in',
        'client.pem',
        '-inkey',
        'client.key',
        '-certfile',
        'ca.pem',
        '-out',
        'client.p12',
        '-passout',
        'env:MTLS_FIXTURE_PASSWORD',
      ]);
      ca = File('${directory.path}/ca.pem').readAsBytesSync();
      clientChain = File('${directory.path}/client.pem').readAsBytesSync();
      clientKey = File('${directory.path}/client.key').readAsBytesSync();
      encryptedKey = File('${directory.path}/encrypted.key').readAsBytesSync();
      pkcs12 = File('${directory.path}/client.p12').readAsBytesSync();
      serverContext = SecurityContext(withTrustedRoots: false)
        ..useCertificateChain('${directory.path}/server.pem')
        ..usePrivateKey('${directory.path}/server.key')
        ..setTrustedCertificatesBytes(ca);
    });
    tearDown(() async {
      for (final client in clients) {
        await client.close().timeout(const Duration(seconds: 3));
        expect(client.connectionCount, 0);
        expect(client.pendingRequestCount, 0);
        expect(client.admittedRequestCount, 0);
        expect(client.queuedRequestBytes, 0);
        expect(client.bufferedResponseBytes, 0);
      }
      clients.clear();
      for (final peer in peers) {
        await peer.close();
      }
      peers.clear();
    });

    KafkaTlsClientIdentity pem() => KafkaTlsClientIdentity.pem(
        certificateChain: clientChain, privateKey: clientKey);
    KafkaTlsConfig tls(KafkaTlsClientIdentity? identity) => KafkaTlsConfig(
        trustedCertificates: ca,
        withTrustedRoots: false,
        clientIdentity: identity);
    Future<_Peer> peer({bool requireIdentity = true}) async {
      final value =
          await _Peer.start(serverContext, requireIdentity: requireIdentity);
      peers.add(value);
      return value;
    }

    KafkaClient connect(_Peer peer,
        {KafkaTlsConfig? config,
        String host = 'localhost',
        KafkaSaslPlain? sasl,
        bool fallback = false}) {
      final client = KafkaClient(
          brokers: [Broker(host, peer.server.port)],
          tls: config ?? tls(pem()),
          sasl: sasl,
          allowTlsBootstrapFallback: fallback,
          requestTimeout: const Duration(seconds: 3),
          connectionTimeout: const Duration(seconds: 3));
      clients.add(client);
      return client;
    }

    for (final format in ['PEM', 'encrypted PEM', 'PKCS12']) {
      test('$format identity is presented to an authenticated loopback peer',
          () async {
        final identity = format == 'PEM'
            ? pem()
            : format == 'encrypted PEM'
                ? KafkaTlsClientIdentity.pem(
                    certificateChain: clientChain,
                    privateKey: encryptedKey,
                    privateKeyPassword: password)
                : KafkaTlsClientIdentity.pkcs12(
                    bytes: pkcs12, password: password);
        final remote = await peer();
        final client = connect(remote, config: tls(identity));
        expect((await client.metadata()).topics, isEmpty);
        expect(remote.clientSubjects.single, contains('owned-client'));
        expect(remote.requests, [18, 3]);
        final first = tls(identity).createSecurityContext();
        expect(
            identical(first, tls(identity).createSecurityContext()), isFalse);
        expect(identical(first, SecurityContext.defaultContext), isFalse);
      });
    }

    test('input mutation cannot change PEM, PKCS12 or custom server trust',
        () async {
      for (final format in ['PEM', 'PKCS12']) {
        final chain = List<int>.of(clientChain);
        final key = List<int>.of(clientKey);
        final bundle = List<int>.of(pkcs12);
        final trust = List<int>.of(ca);
        final config = KafkaTlsConfig(
            trustedCertificates: trust,
            withTrustedRoots: false,
            clientIdentity: format == 'PEM'
                ? KafkaTlsClientIdentity.pem(
                    certificateChain: chain, privateKey: key)
                : KafkaTlsClientIdentity.pkcs12(
                    bytes: bundle, password: password));
        for (final bytes in [chain, key, bundle, trust]) {
          bytes.fillRange(0, bytes.length, 0);
        }
        final remote = await peer();
        expect(
            (await connect(remote, config: config).metadata()).topics, isEmpty);
        expect(remote.clientSubjects.single, contains('owned-client'));
      }
    });

    test('optional identity leaves server-only TLS usable', () async {
      final remote = await peer(requireIdentity: false);
      expect((await connect(remote, config: tls(null)).metadata()).topics,
          isEmpty);
      expect(remote.clientSubjects, isEmpty);
    });

    test('PLAIN still follows the verified mutual TLS handshake', () async {
      final remote = await peer();
      final client = connect(remote,
          sasl: KafkaSaslPlain(
              username: 'synthetic-user', password: 'synthetic'));
      await client.metadata();
      expect(remote.clientSubjects.single, contains('owned-client'));
      expect(remote.requests, [17, 36, 18, 3]);
    });

    for (final mode in ['missing identity', 'untrusted identity']) {
      test('$mode sends no Kafka or PLAIN to a requiring peer', () async {
        final remote = await peer();
        final identity = mode == 'missing identity'
            ? null
            : KafkaTlsClientIdentity.pem(
                certificateChain:
                    File('${directory.path}/untrusted.pem').readAsBytesSync(),
                privateKey:
                    File('${directory.path}/untrusted.key').readAsBytesSync());
        final client = connect(remote,
            config: tls(identity),
            sasl: KafkaSaslPlain(username: 'synthetic', password: 'synthetic'));
        await expectLater(
            client.metadata(), throwsA(isNot(isA<TimeoutException>())));
        expect(remote.requests, isEmpty);
        expect(remote.clientSubjects, isEmpty);
      });
    }

    for (final mode in [
      'wrong server CA',
      'wrong server CA with PKCS12',
      'wrong server hostname'
    ]) {
      test('client identity cannot bypass $mode', () async {
        final remote = await peer();
        final client = connect(remote,
            config: mode.startsWith('wrong server CA')
                ? KafkaTlsConfig(
                    clientIdentity: mode.endsWith('PKCS12')
                        ? KafkaTlsClientIdentity.pkcs12(
                            bytes: pkcs12, password: password)
                        : pem())
                : tls(pem()),
            host: mode == 'wrong server hostname' ? '127.0.0.1' : 'localhost',
            sasl: KafkaSaslPlain(username: 'synthetic', password: 'synthetic'));
        await expectLater(client.metadata(), throwsA(isA<TlsException>()));
        expect(remote.requests, isEmpty);
      });
    }

    test('same identity is applied to later independently connected endpoints',
        () async {
      final first = await peer();
      final second = await peer();
      final client = connect(first);
      await client.metadata();
      await client.send(MetadataRequest(),
          broker: Broker('localhost', second.server.port));
      expect(first.clientSubjects.single, contains('owned-client'));
      expect(second.clientSubjects.single, contains('owned-client'));
      expect(client.connectionCount, 2);
    });

    test(
        'operation-scoped stable reads present the identity and release resources',
        () async {
      final remote = await peer();
      final client = connect(remote);
      final offsets = await client.committedOffsets('group', 'topic',
          partitions: {0},
          requireStable: true,
          stableOffsetRetry: KafkaStableOffsetRetry());
      expect(offsets[0], isNull);
      expect(remote.requests, [18, 10, 9]);
      expect(remote.clientSubjects.single, contains('owned-client'));
      expect(client.connectionCount, 0);
      expect(client.admittedRequestCount, 0);
    });

    test('close owns a mutual TLS connection with an in-flight request',
        () async {
      final remote = await peer()
        ..stall = true;
      final client = connect(remote);
      final pending = expectLater(client.metadata(), throwsStateError);
      await remote.applicationReceived.future;
      await client.close();
      await pending;
    });

    for (final invalid in [
      'chain',
      'key',
      'mismatch',
      'password',
      'PKCS12',
      'PKCS12 bytes'
    ]) {
      test('invalid $invalid is redacted and fails before opening TCP',
          () async {
        final remote = await peer();
        final identity = invalid.startsWith('PKCS12')
            ? KafkaTlsClientIdentity.pkcs12(
                bytes: invalid == 'PKCS12'
                    ? pkcs12
                    : utf8.encode('private-marker'),
                password: 'incorrect-$password')
            : KafkaTlsClientIdentity.pem(
                certificateChain: invalid == 'chain'
                    ? utf8.encode('private-marker')
                    : clientChain,
                privateKey: invalid == 'key'
                    ? utf8.encode('private-marker')
                    : invalid == 'mismatch'
                        ? File('${directory.path}/untrusted.key')
                            .readAsBytesSync()
                        : invalid == 'password'
                            ? encryptedKey
                            : clientKey,
                privateKeyPassword:
                    invalid == 'password' ? 'incorrect-$password' : null);
        final client = connect(remote, config: tls(identity), fallback: true);
        await expectLater(
            client.metadata(),
            throwsA(isA<TlsException>()
                .having((e) => e.toString(), 'generic',
                    'TlsException: Unable to load TLS client identity')
                .having((e) => e.osError, 'no native cause', isNull)));
        expect(remote.accepted, 0);
        expect(client.bootstrapTlsFallbackCount, 0);
        expect(identity.toString(), isNot(contains(password)));
      });
    }
  });
}
