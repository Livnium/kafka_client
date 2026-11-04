"""No-broker regressions for bounded, read-only operational startup readiness."""
import contextlib
import io
import json
import os
import pathlib
import struct
import tempfile
import unittest
from unittest import mock

import run_operational_integration as runner


TOPIC = b'operational-owned'


def integer(value, kind='i'):
    return struct.pack('>' + kind, value)


def selection(topic=TOPIC, topics=1, partitions=1, partition=0):
    return (integer(topics) + integer(len(topic), 'h') + topic +
            integer(partitions) + integer(partition))


def frame(body):
    return integer(len(body)) + body


def offsets_response(error=0, offset=42, correlation=1, **selected):
    return (integer(correlation) + selection(**selected) + integer(error, 'h') +
            integer(-1, 'q') + integer(offset, 'q'))


def fetch_response(error=0, correlation=2, watermark=42, stable=-1,
                   throttle=0, aborted=-1, transactions=b'', records=b'',
                   records_size=None, **selected):
    if records_size is None:
        records_size = len(records)
    return (integer(correlation) + integer(throttle) + selection(**selected) +
            integer(error, 'h') + integer(watermark, 'q') + integer(stable, 'q') +
            integer(aborted) + transactions + integer(records_size) + records)


class Clock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        self.sleeps.append(duration)
        self.now += duration


class Socket:
    def __init__(self, responses, *, fragment=2, clock=None, read_delay=0):
        self.data = bytearray(responses)
        self.fragment, self.clock, self.read_delay = fragment, clock, read_delay
        self.timeouts, self.sent = [], []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def settimeout(self, value):
        if value <= 0:
            raise AssertionError('Socket must never have an unbounded timeout')
        self.timeouts.append(value)

    def sendall(self, data):
        self.sent.append(data)

    def recv(self, size):
        if self.clock:
            self.clock.now += self.read_delay
        count = min(size, self.fragment, len(self.data))
        result = bytes(self.data[:count])
        del self.data[:count]
        return result


class OperationalReadinessTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.run = pathlib.Path(self.temporary.name)
        self.fixture = runner.BrokerFixture(self.run / 'kafka', self.run, 19292, 19293)
        self.fixture.readiness_topic = TOPIC.decode()
        self.clock = Clock()
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.object(runner.time, 'monotonic', self.clock.monotonic))
        self.stack.enter_context(mock.patch.object(runner.time, 'sleep', self.clock.sleep))
        self.process = mock.Mock()
        self.process.poll.return_value = None

        def launch(*args, **kwargs):
            kwargs['stdout'].write('Kafka Server started\n')
            kwargs['stdout'].flush()
            return self.process

        self.launch = self.stack.enter_context(mock.patch.object(
            runner.subprocess, 'Popen', side_effect=launch))
        self.connect = self.stack.enter_context(mock.patch.object(runner.socket, 'create_connection'))
        self.addCleanup(self.close_log)

    def close_log(self):
        if self.fixture.log_handle is not None:
            self.fixture.log_handle.close()

    def socket_for(self, offsets=None, fetched=None, **kwargs):
        connection = Socket(frame(offsets if offsets is not None else offsets_response()) +
                            frame(fetched if fetched is not None else fetch_response()), **kwargs)
        self.connect.return_value = connection
        return connection

    def receipt(self):
        return json.loads((self.run / f'readiness-{self.fixture.generation}.json').read_text())

    def test_success_requires_latest_then_fetch_on_owned_endpoint(self):
        connection = self.socket_for()
        self.fixture.start()
        self.connect.assert_called_once_with(('127.0.0.1', 19292), 2)
        self.assertTrue(connection.closed)
        self.assertFalse(connection.data)
        # Exactly two read-only APIs; Fetch offset is the ListOffsets result.
        expected_offsets = (integer(2, 'h') + integer(1, 'h') + integer(1) +
                            integer(-1, 'h') + integer(-1) + selection() + integer(-1, 'q'))
        expected_fetch = (integer(1, 'h') + integer(4, 'h') + integer(2) +
                          integer(-1, 'h') + integer(-1) + integer(0) + integer(1) +
                          integer(1) + b'\0' + selection() + integer(42, 'q') + integer(1))
        self.assertEqual(connection.sent, [frame(expected_offsets), frame(expected_fetch)])
        self.assertTrue(all(0 < timeout <= 2 for timeout in connection.timeouts))
        self.assertEqual(self.clock.sleeps, [])
        self.assertEqual(self.launch.call_count, 1)  # No readiness subprocess.
        self.assertEqual(self.receipt(), dict(generation=1, topic_probe_required=True,
                                             attempts=1, transient_error_codes=[],
                                             verified_offset=42, ready=True, elapsed_seconds=0))

    def test_empty_topic_offset_zero_and_null_records_are_valid(self):
        connection = self.socket_for(offsets_response(offset=0),
                                     fetch_response(watermark=0, records_size=-1))
        self.fixture._partition_ready(160)
        self.assertTrue(connection.closed)

    def test_transient_fetch_error_never_counts_as_ready(self):
        pending = Socket(frame(offsets_response()) + frame(fetch_response(error=6, watermark=-1)))
        ready = self.socket_for()
        self.connect.side_effect = [pending, ready]
        self.fixture.start()
        self.assertEqual(self.connect.call_count, 2)
        self.assertEqual(self.clock.sleeps, [.1])
        self.assertTrue(pending.closed and ready.closed)
        self.assertEqual(self.receipt()['attempts'], 2)
        self.assertEqual(self.receipt()['transient_error_codes'], [6])
        self.assertEqual(self.receipt()['verified_offset'], 42)
        self.assertAlmostEqual(self.receipt()['elapsed_seconds'], .1)

    def test_transient_offsets_error_does_not_send_fetch(self):
        pending = Socket(frame(offsets_response(error=5, offset=-1)))
        ready = self.socket_for()
        self.connect.side_effect = [pending, ready]
        self.fixture.start()
        self.assertEqual(len(pending.sent), 1)
        self.assertTrue(pending.closed and ready.closed)

    def test_connection_failure_is_retryable_not_success(self):
        ready = self.socket_for()
        self.connect.side_effect = [ConnectionRefusedError('not listening'), ready]
        self.fixture.start()
        self.assertEqual(self.connect.call_count, 2)

    def test_nontransient_partition_errors_fail_immediately(self):
        for offsets, fetched, api in (
                (offsets_response(error=29), fetch_response(), 'ListOffsets'),
                (offsets_response(), fetch_response(error=29), 'Fetch')):
            with self.subTest(api=api):
                connection = self.socket_for(offsets, fetched)
                with self.assertRaisesRegex(RuntimeError, api + ' readiness partition error 29'):
                    self.fixture._partition_ready(160)
                self.assertTrue(connection.closed)

    def test_repeated_not_ready_reaches_original_deadline(self):
        with mock.patch.object(self.fixture, '_partition_ready',
                               side_effect=runner._PartitionNotReady('Fetch error 6')) as ready:
            with self.assertRaisesRegex(RuntimeError, 'exceeded 60 seconds: Fetch error 6'):
                self.fixture.start()
        self.assertAlmostEqual(self.clock.now, 160)
        self.assertTrue(ready.call_count > 1)
        self.assertTrue(all(call.args == (160,) for call in ready.call_args_list))
        self.assertTrue(all(0 < pause <= .1 for pause in self.clock.sleeps))
        self.assertFalse(self.receipt()['ready'])
        self.assertIsNone(self.receipt()['verified_offset'])
        self.assertEqual(self.receipt()['elapsed_seconds'], 60)

    def test_slow_fragmented_response_cannot_reset_attempt_deadline(self):
        connection = self.socket_for(clock=self.clock, read_delay=.4, fragment=1)
        with self.assertRaises(TimeoutError):
            self.fixture._partition_ready(160)
        self.assertTrue(connection.closed)
        self.assertLessEqual(self.clock.now, 102.4)
        self.assertGreater(connection.timeouts[0], connection.timeouts[-1])

    def test_attempt_is_clipped_to_remaining_start_deadline(self):
        connection = self.socket_for(clock=self.clock, read_delay=.1, fragment=1)
        with self.assertRaises(TimeoutError):
            self.fixture._partition_ready(100.25)
        self.connect.assert_called_once_with(('127.0.0.1', 19292), .25)
        self.assertTrue(all(0 < timeout <= .25 for timeout in connection.timeouts))
        self.assertTrue(connection.closed)

    def test_expired_deadline_does_not_connect(self):
        with self.assertRaises(TimeoutError):
            self.fixture._partition_ready(100)
        self.connect.assert_not_called()

    def test_success_after_start_deadline_is_not_accepted(self):
        def late(deadline):
            self.clock.now = deadline
        with mock.patch.object(self.fixture, '_partition_ready', side_effect=late):
            with self.assertRaisesRegex(RuntimeError, 'exceeded 60 seconds'):
                self.fixture.start()

    def test_initial_start_retains_marker_before_topic_exists(self):
        self.fixture.readiness_topic = None
        with mock.patch.object(self.fixture, '_partition_ready') as ready:
            self.fixture.start()
        ready.assert_not_called()
        self.connect.assert_not_called()
        self.assertEqual(self.receipt(), dict(generation=1, topic_probe_required=False,
                                             attempts=0, transient_error_codes=[],
                                             verified_offset=None, ready=True, elapsed_seconds=0))

    def test_startup_marker_is_still_required(self):
        self.launch.side_effect = lambda *args, **kwargs: self.process
        with mock.patch.object(self.fixture, '_partition_ready') as ready:
            with self.assertRaisesRegex(RuntimeError, 'startup marker not observed'):
                self.fixture.start()
        ready.assert_not_called()
        self.assertAlmostEqual(self.clock.now, 160)

    def test_startup_time_is_not_reset_when_marker_appears(self):
        def late_marker(*args, **kwargs):
            self.clock.now = 159.75
            kwargs['stdout'].write('Kafka Server started\n')
            kwargs['stdout'].flush()
            return self.process
        self.launch.side_effect = late_marker
        self.connect.side_effect = ConnectionRefusedError('not listening')
        with self.assertRaisesRegex(RuntimeError, 'exceeded 60 seconds'):
            self.fixture.start()
        self.assertAlmostEqual(self.clock.now, 160)
        self.assertLessEqual(self.connect.call_count, 3)
        self.assertTrue(all(call.args[1] <= .25 for call in self.connect.call_args_list))

    def test_owned_broker_exit_fails_before_probe(self):
        self.process.poll.return_value = 1
        with self.assertRaisesRegex(RuntimeError, 'Broker exited during startup'):
            self.fixture.start()
        self.connect.assert_not_called()

    def test_owned_broker_exit_after_probe_does_not_report_ready(self):
        self.socket_for()
        self.process.poll.side_effect = [None, 1]
        with self.assertRaisesRegex(RuntimeError, 'Broker exited during startup'):
            self.fixture.start()

    def test_already_running_process_is_not_replaced(self):
        self.fixture.process = self.process
        with self.assertRaisesRegex(RuntimeError, 'already running'):
            self.fixture.start()
        self.launch.assert_not_called()

    def test_response_identity_and_structure_fail_closed(self):
        bad_offsets = [offsets_response(correlation=99), offsets_response(topic=b'other'),
                       offsets_response(topics=0), offsets_response(topics=2),
                       offsets_response(partitions=0), offsets_response(partitions=2),
                       offsets_response(partition=1), offsets_response(offset=-1),
                       offsets_response()[:-1], offsets_response() + b'x']
        bad_fetches = [fetch_response(correlation=99), fetch_response(topic=b'other'),
                       fetch_response(topics=0), fetch_response(topics=2),
                       fetch_response(partitions=0), fetch_response(partitions=2),
                       fetch_response(partition=1), fetch_response(watermark=-1),
                       fetch_response(watermark=41), fetch_response(stable=-2),
                       fetch_response(throttle=-1), fetch_response(aborted=-2),
                       fetch_response(aborted=2**31 - 1), fetch_response(records_size=-2),
                       fetch_response(records_size=2**31 - 1), fetch_response()[:-1],
                       fetch_response() + b'x']
        for index, malformed in enumerate(bad_offsets):
            with self.subTest(api='offsets', case=index):
                connection = self.socket_for(offsets=malformed)
                with self.assertRaises(RuntimeError):
                    self.fixture._partition_ready(160)
                self.assertTrue(connection.closed)
                self.assertEqual(len(connection.sent), 1)
        for index, malformed in enumerate(bad_fetches):
            with self.subTest(api='fetch', case=index):
                connection = self.socket_for(fetched=malformed)
                with self.assertRaises(RuntimeError):
                    self.fixture._partition_ready(160)
                self.assertTrue(connection.closed)

    def test_bad_frame_lengths_fail_without_reading_body(self):
        for size in (-1, 0, 3, 1024 * 1024 + 1, 2**31 - 1):
            with self.subTest(size=size):
                connection = Socket(integer(size) + b'body-not-read')
                self.connect.return_value = connection
                with self.assertRaisesRegex(RuntimeError, 'frame size'):
                    self.fixture._partition_ready(160)
                self.assertEqual(connection.data, b'body-not-read')
                self.assertTrue(connection.closed)

    def test_connection_closed_mid_frame_is_never_success(self):
        for payload in (b'', b'\0', integer(200) + b'incomplete'):
            with self.subTest(payload=payload):
                connection = Socket(payload)
                self.connect.return_value = connection
                with self.assertRaises(ConnectionError):
                    self.fixture._partition_ready(160)
                self.assertTrue(connection.closed)

    def test_invalid_topic_fails_before_connect(self):
        for topic in ('', 'x' * 250):
            with self.subTest(topic_length=len(topic)):
                self.fixture.readiness_topic = topic
                with self.assertRaisesRegex(RuntimeError, 'owned readiness topic'):
                    self.fixture._partition_ready(160)
        self.connect.assert_not_called()


class TopicRegistrationTest(unittest.TestCase):
    def test_topic_is_registered_only_after_successful_creation(self):
        for fail_creation in (False, True):
            with self.subTest(fail_creation=fail_creation), tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary)
                home = root / 'kafka'
                (home / 'libs').mkdir(parents=True)
                (home / 'libs/kafka-clients-4.0.2.jar').touch()
                fixtures = []
                real_fixture = runner.BrokerFixture

                def construct(*args):
                    fixture = real_fixture(*args)
                    fixtures.append(fixture)
                    fixture.format = mock.Mock()
                    fixture.start = mock.Mock(side_effect=lambda: self.assertIsNone(fixture.readiness_topic))

                    def create(*args):
                        self.assertIsNone(fixture.readiness_topic)
                        if fail_creation:
                            raise RuntimeError('create failed')
                        return 'created\n'
                    fixture.command = mock.Mock(side_effect=create)
                    return fixture

                def control(directory, fixture):
                    self.assertTrue(fixture.readiness_topic.startswith('operational-run-'))
                    return mock.Mock()

                child = mock.Mock()
                child.stdout = []
                child.wait.return_value = 0
                child.poll.return_value = 0
                with contextlib.ExitStack() as stack:
                    stack.enter_context(mock.patch.dict(os.environ, KAFKA_HOME=str(home), KAFKA_VERSION='4.0.2',
                                                        KAFKA_OPERATIONAL_RUN_ROOT=str(root / 'runs')))
                    stack.enter_context(mock.patch.object(runner, 'BrokerFixture', side_effect=construct))
                    controls = stack.enter_context(mock.patch.object(runner, 'FileControl', side_effect=control))
                    stack.enter_context(mock.patch.object(runner.subprocess, 'Popen', return_value=child))
                    stack.enter_context(mock.patch.object(runner.socket, 'socket'))
                    stack.enter_context(mock.patch.object(runner.signal, 'signal'))
                    stack.enter_context(mock.patch.object(runner.os, 'umask'))
                    stack.enter_context(mock.patch.object(runner.sys, 'argv', ['runner', '--', 'test']))
                    stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                    if fail_creation:
                        with self.assertRaisesRegex(RuntimeError, 'create failed'):
                            runner.main()
                        self.assertIsNone(fixtures[0].readiness_topic)
                        controls.assert_not_called()
                    else:
                        self.assertEqual(runner.main(), 0)
                        controls.assert_called_once()


if __name__ == '__main__':
    unittest.main()
