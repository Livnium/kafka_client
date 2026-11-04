import 'dart:io';

import 'package:kafka_client/kafka_protocol.dart';
import 'package:kafka_client/src/codec/byte_builder.dart';
import 'package:kafka_client/src/codec/byte_reader.dart';
import 'package:kafka_client/src/native_classic_group_driver.dart';
import 'package:test/test.dart';

const _fixedVersions = {
  0: 3,
  1: 4,
  3: 0,
  8: 6,
  10: 2,
  11: 4,
  12: 2,
  13: 2,
  14: 2
};

class _VersionPeer {
  final ServerSocket server;
  final sockets = <Socket>[];
  final keys = <int>[];
  late List<ApiVersion> versions;
  int error = 0;
  late int leaderPort;

  _VersionPeer(this.server) {
    versions = [
      for (final entry in _fixedVersions.entries)
        ApiVersion(entry.key, entry.value, entry.value)
    ];
    server.listen((socket) {
      sockets.add(socket);
      final buffer = <int>[];
      socket.listen((data) {
        buffer.addAll(data);
        while (buffer.length >= 4) {
          final size = KafkaBytesReader.fromBytes(buffer).readInt32();
          if (buffer.length < size + 4) break;
          final r = KafkaBytesReader.fromBytes(buffer.sublist(4, size + 4));
          buffer.removeRange(0, size + 4);
          final key = r.readInt16();
          r.readInt16();
          final id = r.readInt32();
          keys.add(key);
          final b = KafkaBytesBuilder()..addInt32(id);
          if (key == 18) {
            b
              ..addInt16(error)
              ..addInt32(versions.length);
            for (final v in versions) {
              b
                ..addInt16(v.apiKey)
                ..addInt16(v.minVersion)
                ..addInt16(v.maxVersion);
            }
          } else if (key == 3) {
            b
              ..addInt32(1)
              ..addInt32(7)
              ..addString('127.0.0.1')
              ..addInt32(leaderPort)
              ..addInt32(1)
              ..addInt16(0)
              ..addString('topic')
              ..addInt32(1)
              ..addInt16(0)
              ..addInt32(0)
              ..addInt32(7)
              ..addInt32(1)
              ..addInt32(7)
              ..addInt32(1)
              ..addInt32(7);
          } else if (key == 10) {
            b
              ..addInt32(0)
              ..addInt16(0)
              ..addString(null)
              ..addInt32(7)
              ..addString('127.0.0.1')
              ..addInt32(leaderPort);
          } else if (key == 0) {
            b
              ..addInt32(1)
              ..addString('topic')
              ..addInt32(1)
              ..addInt32(0)
              ..addInt16(0)
              ..addInt64(42)
              ..addInt64(-1)
              ..addInt32(0);
          } else if (key == 1) {
            b
              ..addInt32(0)
              ..addInt32(1)
              ..addString('topic')
              ..addInt32(1)
              ..addInt32(0)
              ..addInt16(0)
              ..addInt64(0)
              ..addInt64(0)
              ..addInt32(-1)
              ..addBytes([]);
          } else if (key == 11) {
            // A valid first-Join ID continuation response. Merely sending Join
            // is a membership mutation, so malformed capabilities must stop it.
            b
              ..addInt32(0)
              ..addInt16(79)
              ..addInt32(-1)
              ..addString('')
              ..addString('')
              ..addString('allocated-member')
              ..addInt32(0);
          } else {
            fail('Unexpected API $key');
          }
          socket.add(KafkaRequest.frame(b.takeBytes()));
        }
      }, onError: (Object _) {});
    });
  }

  static Future<_VersionPeer> start() async =>
      _VersionPeer(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0));

  Future<void> close() async {
    for (final socket in sockets) {
      socket.destroy();
    }
    await server.close();
  }
}

class _Fixture {
  final _VersionPeer bootstrap, leader;
  final KafkaClient client;

  _Fixture(this.bootstrap, this.leader)
      : client = KafkaClient(
            brokers: [Broker('127.0.0.1', bootstrap.server.port)],
            maxReadRetries: 3,
            retryBaseDelay: Duration.zero) {
    bootstrap.leaderPort = leader.leaderPort = leader.server.port;
  }

  static Future<_Fixture> start() async =>
      _Fixture(await _VersionPeer.start(), await _VersionPeer.start());

  _VersionPeer target(String operation) =>
      operation == 'Metadata' ? bootstrap : leader;
  int requiredKey(String operation) =>
      {'Metadata': 3, 'Produce': 0, 'Fetch': 1, 'Group': 11}[operation]!;

  Future<Object?> run(String operation) async {
    switch (operation) {
      case 'Metadata':
        return client.metadata({'topic'});
      case 'Produce':
        return client.produce('topic', [
          KafkaRecord(value: [1])
        ]);
      case 'Fetch':
        return client.fetch('topic');
      case 'Group':
        final driver = NativeClassicGroupDriver(
            client: client,
            groupId: 'g',
            topics: {'topic'},
            sessionTimeoutMs: 10000,
            rebalanceTimeoutMs: 10000);
        await driver.prepare();
        return driver.join('');
    }
    throw StateError('Unknown operation');
  }

  void expectBlocked(String operation) {
    expect(bootstrap.keys,
        operation == 'Metadata' ? [18] : [18, operation == 'Group' ? 10 : 3]);
    expect(leader.keys, operation == 'Metadata' ? isEmpty : [18]);
    if (operation == 'Metadata') expect(leader.sockets, isEmpty);
  }

  void expectDispatchedOnce(String operation) {
    expect(
        bootstrap.keys,
        operation == 'Metadata'
            ? [18, 3]
            : [18, operation == 'Group' ? 10 : 3]);
    expect(leader.keys,
        operation == 'Metadata' ? isEmpty : [18, requiredKey(operation)]);
  }

  Future<void> close() async {
    await client.close();
    expect(client.pendingRequestCount, 0);
    expect(client.admittedRequestCount, 0);
    expect(client.queuedRequestBytes, 0);
    expect(client.bufferedResponseBytes, 0);
    await bootstrap.close();
    await leader.close();
  }
}

void main() {
  final malformed = <String, void Function(List<ApiVersion>, int)>{
    'duplicate identical required key': (table, key) =>
        table.add(ApiVersion(key, _fixedVersions[key]!, _fixedVersions[key]!)),
    'duplicate contradictory required key': (table, key) => table.add(
        ApiVersion(key, _fixedVersions[key]! + 1, _fixedVersions[key]! + 1)),
    'contradictory required key first': (table, key) => table.insert(
        0, ApiVersion(key, _fixedVersions[key]! + 1, _fixedVersions[key]! + 1)),
    'third occurrence of required key': (table, key) =>
        table.addAll([ApiVersion(key, 0, 100), ApiVersion(key, 0, 100)]),
    'duplicate unknown key': (table, key) =>
        table.addAll([ApiVersion(32767, 0, 0), ApiVersion(32767, 1, 1)]),
    'negative API key': (table, key) => table.add(ApiVersion(-1, 0, 0)),
    'negative minimum for required key': (table, key) {
      table.removeWhere((v) => v.apiKey == key);
      table.add(ApiVersion(key, -1, _fixedVersions[key]!));
    },
    'negative maximum': (table, key) => table.add(ApiVersion(32767, 0, -1)),
    'negative minimum for unknown key': (table, key) =>
        table.add(ApiVersion(32767, -1, 0)),
    'inverted unknown range': (table, key) =>
        table.add(ApiVersion(32767, 2, 1)),
  };
  for (final operation in ['Metadata', 'Produce', 'Fetch', 'Group']) {
    for (final entry in malformed.entries) {
      test('$operation rejects ${entry.key} before application dispatch',
          () async {
        final f = await _Fixture.start();
        try {
          entry.value(f.target(operation).versions, f.requiredKey(operation));
          await expectLater(f.run(operation), throwsFormatException);
          f.expectBlocked(operation);
        } finally {
          await f.close();
        }
      });
    }
    test('$operation accepts unknown keys and supported range boundaries',
        () async {
      final f = await _Fixture.start();
      try {
        final table = f.target(operation).versions;
        table.addAll(
            [ApiVersion(12345, 0, 32767), ApiVersion(32767, 32767, 32767)]);
        final key = f.requiredKey(operation);
        table.removeWhere((v) => v.apiKey == key);
        table.add(ApiVersion(key, 0, 32767));
        expect(await f.run(operation), isNotNull);
        f.expectDispatchedOnce(operation);
      } finally {
        await f.close();
      }
    });
    test('$operation accepts exact fixed versions', () async {
      final f = await _Fixture.start();
      try {
        expect(await f.run(operation), isNotNull);
        f.expectDispatchedOnce(operation);
      } finally {
        await f.close();
      }
    });
    for (final unsupported in ['missing', 'too old', 'too new']) {
      test(
          '$operation fails without downgrade when required API is $unsupported',
          () async {
        final f = await _Fixture.start();
        try {
          final key = f.requiredKey(operation),
              version = _fixedVersions[f.requiredKey(operation)]!;
          final table = f.target(operation).versions
            ..removeWhere((v) => v.apiKey == key);
          if (unsupported == 'too old') {
            // Metadata's implemented version is zero, so no older nonnegative
            // version exists; a missing key is its equivalent boundary case.
            if (version > 0) table.add(ApiVersion(key, 0, version - 1));
          } else if (unsupported == 'too new') {
            table.add(ApiVersion(key, version + 1, version + 1));
          }
          await expectLater(f.run(operation), throwsUnsupportedError);
          f.expectBlocked(operation);
        } finally {
          await f.close();
        }
      });
    }
    for (final code in [35, 12345]) {
      test(
          '$operation preserves ApiVersions error $code before table validation',
          () async {
        final f = await _Fixture.start();
        try {
          final target = f.target(operation)..error = code;
          malformed['negative API key']!(
              target.versions, f.requiredKey(operation));
          await expectLater(
              f.run(operation),
              throwsA(operation == 'Group'
                  ? isA<KafkaBrokerException>()
                      .having((e) => e.errorCode, 'code', code)
                  : isA<StateError>().having(
                      (e) => e.message, 'message', 'ApiVersions error $code')));
          f.expectBlocked(operation);
        } finally {
          await f.close();
        }
      });
    }
  }

  test('raw ApiVersions retains error diagnostics and future keys', () {
    for (final code in [0, 35, 12345]) {
      final b = KafkaBytesBuilder()
        ..addInt32(7)
        ..addInt16(code)
        ..addInt32(3)
        ..addInt16(32767)
        ..addInt16(0)
        ..addInt16(32767)
        ..addInt16(0)
        ..addInt16(-1)
        ..addInt16(3)
        ..addInt16(0)
        ..addInt16(4)
        ..addInt16(4);
      final response = ApiVersionsResponse.fromBytes(b.takeBytes());
      expect(response.correlationId, 7);
      expect(response.errorCode, code);
      expect(
          response.apiVersions
              .map((v) => [v.apiKey, v.minVersion, v.maxVersion]),
          [
            [32767, 0, 32767],
            [0, -1, 3],
            [0, 4, 4]
          ]);
    }
  });

  test('constructed capability tables cannot exceed signed INT16 bounds', () {
    for (final version in [
      ApiVersion(32768, 0, 0),
      ApiVersion(0, 32768, 32768),
      ApiVersion(0, 0, 32768),
    ]) {
      final response = ApiVersionsResponse(0, [version], 7);
      expect(response.validateSupportedVersions, throwsFormatException);
    }
  });
}
