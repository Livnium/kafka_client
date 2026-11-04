// Genuine broker88 evidence; only synthetic offset metadata, not no-gap claims.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:test/test.dart';
import 'support/transaction_relay_lifecycle.dart';

void _receipt(Map<String, dynamic> value) =>
    print('TRANSACTION_EVIDENCE ${jsonEncode(value)}');
String _hex(List<int> bytes) =>
    bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();

class _Frames {
  final List<int> bytes = [];
  void add(List<int> chunk, void Function(List<int>) accept) {
    bytes.addAll(chunk);
    while (bytes.length >= 4) {
      final length = KafkaBytesReader.fromBytes(bytes).readInt32();
      if (length < 4 || length > 16 * 1024 * 1024) {
        throw const FormatException('Invalid relay frame');
      }
      if (bytes.length < length + 4) return;
      final frame = bytes.sublist(0, length + 4);
      bytes.removeRange(0, length + 4);
      accept(frame);
    }
  }
}

/// The relay uses this owned fixture's CA-signed certificate and verifies the
/// upstream localhost certificate. Only FindCoordinator v2 endpoint routing is
/// rewritten. OffsetFetch bytes are unchanged; auth frames are never retained.
class _ObservedRelay {
  final SecureServerSocket server;
  final int upstreamPort;
  final SecurityContext trust;
  final sockets = <Socket>[];
  final failures = <Object>[];
  final second88 = Completer<void>();
  int fetchRequests = 0, responses88 = 0;
  bool closed = false;
  int _connectionSequence = 0;
  TransactionRelayLifecycle? _second88Connection;
  _ObservedRelay(this.server, this.upstreamPort, this.trust);
  static Future<_ObservedRelay> start(int port, String ca) async {
    final root = File(ca).parent.path;
    final identity = SecurityContext(withTrustedRoots: false)
      ..useCertificateChain('$root/server.pem')
      ..usePrivateKey('$root/server.key');
    final trust = SecurityContext(withTrustedRoots: false)
      ..setTrustedCertificates(ca);
    final server = await SecureServerSocket.bind(
        InternetAddress.loopbackIPv4, 0, identity);
    final relay = _ObservedRelay(server, port, trust);
    server.listen(relay._accept, onError: relay.failures.add);
    return relay;
  }

  Future<void> _accept(SecureSocket downstream) async {
    sockets.add(downstream);
    final lifecycle = TransactionRelayLifecycle(++_connectionSequence);
    void recordError(Object error, RelayLeg leg, String callback) {
      final event = lifecycle.observeError(error, leg, callback: callback);
      _receipt(event);
      if (event['event'] != 'expected-client-close-transport-event') {
        failures.add(error);
      }
    }

    try {
      final upstream = await SecureSocket.connect('localhost', upstreamPort,
          context: trust, timeout: const Duration(seconds: 5));
      sockets.add(upstream);
      if (closed) {
        downstream.destroy();
        upstream.destroy();
        return;
      }
      final requests = <int, List<int>>{};
      final requestFrames = <int, List<int>>{};
      final incoming = _Frames(), outgoing = _Frames();
      downstream.listen(
          (chunk) {
            incoming.add(chunk, (frame) {
              final r = KafkaBytesReader.fromBytes(frame.sublist(4));
              final key = r.readInt16(),
                  version = r.readInt16(),
                  id = r.readInt32();
              requests[id] = [key, version];
              if (key == 9) {
                fetchRequests++;
                expect(version, 7);
                expect(frame.sublist(frame.length - 2), [1, 0]);
                requestFrames[id] = frame;
              }
              lifecycle.requestReceived(id);
              upstream.add(frame);
            });
          },
          onDone: upstream.destroy,
          onError: (Object error) {
            if (!closed) {
              recordError(
                  error, RelayLeg.downstream, 'downstream-stream-onError');
            }
            upstream.destroy();
          });
      upstream.listen(
          (chunk) {
            outgoing.add(chunk, (frame) {
              final r = KafkaBytesReader.fromBytes(frame.sublist(4));
              final id = r.readInt32();
              final request = requests.remove(id)!;
              if (request[0] == 10) {
                expect(request[1], 2);
                final throttle = r.readInt32(), error = r.readInt16();
                final message = r.readString();
                final node = r.readInt32();
                r.readString();
                r.readInt32();
                final b = KafkaBytesBuilder()
                  ..addInt32(id)
                  ..addInt32(throttle)
                  ..addInt16(error)
                  ..addString(message)
                  ..addInt32(node)
                  ..addString('localhost')
                  ..addInt32(server.port);
                downstream.add(KafkaRequest.frame(b.takeBytes()));
                lifecycle.responseForwarded(id);
              } else {
                downstream.add(frame);
                lifecycle.responseForwarded(id);
                if (request[0] == 9) {
                  final decoded = GroupOffsetFetchResponse.fromBytes(
                      frame.sublist(4),
                      apiVersion: request[1]);
                  final errors = decoded.topics
                      .expand((t) => t.partitions)
                      .map((p) => p.errorCode)
                      .toList();
                  _receipt({
                    'event': 'dart-operation-wire-response',
                    'version': request[1],
                    'correlation': id,
                    'requestHex': _hex(requestFrames[id]!),
                    'requireStable': request[1] == 7 &&
                        requestFrames[id]![requestFrames[id]!.length - 2] == 1,
                    'responseHex': _hex(frame),
                    'errors': errors,
                    'offsets': decoded.topics
                        .expand((t) => t.partitions)
                        .map((p) => p.committedOffset)
                        .toList()
                  });
                  if (errors.contains(88)) {
                    responses88++;
                    // A second fetch proves the policy consumed the first real88
                    // and admitted a retry. Release never depends on elapsed time.
                    if (responses88 >= 2 && !second88.isCompleted) {
                      downstream.flush().then((_) {
                        if (!second88.isCompleted) {
                          lifecycle.markSecond88Flushed();
                          _second88Connection = lifecycle;
                          second88.complete();
                        }
                      }, onError: (Object error) {
                        if (!second88.isCompleted) {
                          second88.completeError(error);
                        }
                      });
                    }
                  }
                }
              }
            });
          },
          onDone: downstream.destroy,
          onError: (Object error) {
            if (!closed) {
              recordError(error, RelayLeg.upstream, 'upstream-stream-onError');
            }
            downstream.destroy();
          });
    } catch (error) {
      if (!closed) failures.add(error);
      downstream.destroy();
    }
  }

  void armIntentionalClientClose() {
    final lifecycle = _second88Connection;
    if (!second88.isCompleted || lifecycle == null) {
      throw StateError('No flushed second88 connection to arm');
    }
    lifecycle.armIntentionalClientClose();
    _receipt({
      'event': 'intentional-client-close-armed',
      'connectionId': lifecycle.connectionId,
      'pendingResponses': lifecycle.pendingResponses,
      'requests': lifecycle.requests,
      'responses': lifecycle.responses
    });
  }

  Future<void> close() async {
    closed = true;
    for (final s in sockets) {
      s.destroy();
    }
    await server.close();
  }
}

class _PendingTransaction {
  final Process process;
  final String group;
  bool resolved = false;
  _PendingTransaction(this.process, this.group);
  static Future<_PendingTransaction> start(
      String mode, String resolution) async {
    final process = await Process.start('bash',
        ['tool/transaction_offset_probe/run.sh', '--hold', mode, resolution]);
    final ready = Completer<String>();
    process.stderr
        .drain<void>(); // Java configs/auth material are not evidence.
    process.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen((line) {
      final event = jsonDecode(line) as Map<String, dynamic>;
      _receipt({'event': 'official-java-helper', 'receipt': event});
      if (event['event'] == 'awaiting-resolution-command' &&
          !ready.isCompleted) {
        ready.complete(event['group'] as String);
      }
    }, onError: (Object e) {
      if (!ready.isCompleted) ready.completeError(e);
    });
    process.exitCode.then((code) {
      if (!ready.isCompleted) {
        ready.completeError(
            StateError('Helper exited before pending gate: $code'));
      }
    });
    try {
      return _PendingTransaction(
          process, await ready.future.timeout(const Duration(seconds: 30)));
    } catch (_) {
      process.kill();
      await process.exitCode;
      rethrow;
    }
  }

  Future<void> resolve(String resolution) async {
    process.stdin.writeln(resolution);
    await process.stdin.flush();
    await process.stdin.close();
    final code = await process.exitCode.timeout(const Duration(seconds: 15));
    resolved = true;
    expect(code, 0);
  }

  Future<void> close() async {
    if (!resolved) {
      process.kill();
      await process.exitCode.timeout(const Duration(seconds: 10));
    }
  }
}

void main() {
  final env = Platform.environment;
  final configured = env['KAFKA_TRANSACTION_FIXTURE'] == '1';
  for (final mode in ['SSL', 'SASL_SSL']) {
    for (final scenario in [
      'commit',
      'abort',
      'attempts',
      'deadline',
      'cancel',
      'close'
    ]) {
      test('$mode genuine transaction88 $scenario', () async {
        final resolution = scenario == 'commit' ? 'commit' : 'abort';
        final helper = await _PendingTransaction.start(mode, resolution);
        addTearDown(helper.close);
        final port = int.parse(env[mode == 'SSL'
            ? 'KAFKA_SECURE_TLS_PORT'
            : 'KAFKA_SECURE_SASL_PORT']!);
        final relay = await _ObservedRelay.start(port, env['KAFKA_SECURE_CA']!);
        addTearDown(relay.close);
        final client = KafkaClient(
            brokers: [Broker('localhost', relay.server.port)],
            tls: KafkaTlsConfig(
                trustedCertificates:
                    File(env['KAFKA_SECURE_CA']!).readAsBytesSync(),
                withTrustedRoots: false),
            sasl: mode == 'SASL_SSL'
                ? KafkaSaslPlain(
                    username: env['KAFKA_SECURE_USERNAME']!,
                    password: env['KAFKA_SECURE_PASSWORD']!)
                : null,
            requestTimeout: const Duration(seconds: 5),
            maxReadRetries: 0);
        addTearDown(client.close);
        final token = KafkaCancellationToken();
        final policy = KafkaStableOffsetRetry(
            maxAttempts: scenario == 'attempts' ? 3 : 20,
            timeout:
                Duration(milliseconds: scenario == 'deadline' ? 1700 : 15000),
            retryDelay: const Duration(milliseconds: 500));
        Future<void> resolveTransaction() async {
          _receipt({
            'event': 'transaction-resolution-command',
            'group': helper.group,
            'mode': mode,
            'scenario': scenario,
            'resolution': resolution,
            'operation88Responses': relay.responses88
          });
          await helper.resolve(resolution);
        }

        final started = Stopwatch()..start();
        _receipt({
          'event': 'dart-transaction-case-start',
          'topic': env['KAFKA_SECURE_TOPIC'],
          'mode': mode,
          'scenario': scenario,
          'group': helper.group
        });
        final operation = client.committedOffsets(
            helper.group, env['KAFKA_SECURE_TOPIC']!,
            partitions: {0},
            requireStable: true,
            stableOffsetRetry: policy,
            cancellationToken: token);
        // Attach an error handler immediately, before waiting for wire gates.
        final outcome =
            operation.then<Object>((v) => v, onError: (Object e) => e);
        if (scenario == 'commit' || scenario == 'abort') {
          await relay.second88.future.timeout(const Duration(seconds: 8));
          await resolveTransaction();
          final value = await outcome;
          expect(value, isA<Map<int, KafkaCommittedOffset?>>());
          expect((value as Map<int, KafkaCommittedOffset?>)[0]!.nextOffset,
              scenario == 'commit' ? 20 : 10);
        } else if (scenario == 'attempts' || scenario == 'deadline') {
          final error = await outcome;
          if (scenario == 'attempts') {
            expect(
                error,
                isA<KafkaBrokerException>()
                    .having((e) => e.errorCode, 'broker error', 88));
            expect(relay.fetchRequests, 3);
            expect(relay.responses88, 3);
          } else {
            expect(error, isA<TimeoutException>());
            expect(relay.responses88, greaterThanOrEqualTo(2));
            expect(started.elapsed, lessThan(const Duration(seconds: 5)));
          }
          await resolveTransaction();
        } else {
          await relay.second88.future.timeout(const Duration(seconds: 8));
          if (scenario == 'cancel') {
            token.cancel();
          } else {
            relay.armIntentionalClientClose();
            await client.close();
          }
          final error = await outcome;
          expect(
              error,
              scenario == 'cancel'
                  ? isA<KafkaCancelledException>()
                  : isA<StateError>());
          final count = relay.fetchRequests;
          await Future<void>.delayed(const Duration(milliseconds: 1100));
          expect(relay.fetchRequests, count,
              reason: 'No late reads after cancellation/close');
          await resolveTransaction();
        }
        await client.close();
        expect(client.pendingRequestCount, 0);
        expect(client.admittedRequestCount, 0);
        expect(client.connectionCount, 0);
        expect(client.queuedRequestBytes, 0);
        expect(client.bufferedResponseBytes, 0);
        expect(relay.failures, isEmpty);
        _receipt({
          'event': 'dart-transaction-case-pass',
          'mode': mode,
          'scenario': scenario,
          'operation88Responses': relay.responses88,
          'operationFetchRequests': relay.fetchRequests,
          'elapsedMs': started.elapsedMilliseconds,
          'noGapClaim': false
        });
      },
          skip: configured
              ? false
              : 'Run only through guarded owned transaction fixture',
          timeout: const Timeout(Duration(seconds: 60)));
    }
  }
}
