import 'package:kafka_client/src/consumer_group_metadata.dart';
import 'package:kafka_client/src/range_group_assignor.dart';
import 'package:test/test.dart';

void main() {
  test('uneven partitions are contiguous, with sorted IDs and members', () {
    final result = assignEagerRange({
      'c': ConsumerGroupSubscription(['t']),
      'a': ConsumerGroupSubscription(['t']),
      'b': ConsumerGroupSubscription(['t']),
    }, {
      't': [6, 2, 4, 0, 5, 1, 3]
    });
    expect(result['a']!.partitions, {
      't': [0, 1, 2]
    });
    expect(result['b']!.partitions, {
      't': [3, 4]
    });
    expect(result['c']!.partitions, {
      't': [5, 6]
    });
  });
  test('each topic uses only eligible members, including peer-only topics', () {
    final result = assignEagerRange({
      'a': ConsumerGroupSubscription(['x']),
      'b': ConsumerGroupSubscription(['x', 'y']),
      'c': ConsumerGroupSubscription(['y']),
      'd': ConsumerGroupSubscription([]),
    }, {
      'x': [0, 1, 2],
      'y': [0, 1, 2, 3, 4]
    });
    expect(result['a']!.partitions, {
      'x': [0, 1]
    });
    expect(result['b']!.partitions, {
      'x': [2],
      'y': [0, 1, 2]
    });
    expect(result['c']!.partitions, {
      'y': [3, 4]
    });
    expect(result['d']!.partitions, isEmpty);
    expect(() => result.clear(), throwsUnsupportedError);
  });
  test('empty assignment and more members than partitions are valid', () {
    final result = assignEagerRange({
      'a': ConsumerGroupSubscription(['x', 'empty']),
      'b': ConsumerGroupSubscription(['x']),
    }, {
      'x': [0],
      'empty': []
    });
    expect(result['a']!.partitions, {
      'x': [0]
    });
    expect(result['b']!.partitions, isEmpty);
    expect(assignEagerRange({}, {}), isEmpty);
  });
  test('missing, negative and duplicate partition metadata fail closed', () {
    final members = {
      'a': ConsumerGroupSubscription(['t'])
    };
    for (final metadata in <Map<String, List<int>>>[
      {},
      {
        't': [-1]
      },
      {
        't': [0, 0]
      }
    ]) {
      expect(() => assignEagerRange(members, metadata), throwsFormatException);
    }
  });
  test('rack metadata is explicitly unsupported', () {
    expect(
        () => assignEagerRange({
              'a':
                  ConsumerGroupSubscription(['t'], version: 3, rackId: 'rack-a')
            }, {
              't': [0]
            }),
        throwsUnsupportedError);
  });
}
