import copy
import unittest

from summarize_ci_tests import summarize


def event(kind, **fields):
    return {"type": kind, "time": 0, **fields}


def test_start(test_id, name, group_ids=None):
    return event("testStart", test={
        "id": test_id, "suiteID": 0, "name": name,
        "groupIDs": [2] if group_ids is None else group_ids,
        "url": "file:///test/fixture_test.dart" if group_ids != [] else None,
    })


def test_done(test_id, *, hidden=False, skipped=False, result="success"):
    return event("testDone", testID=test_id, hidden=hidden, skipped=skipped, result=result)


def test_error(test_id):
    return event("error", testID=test_id, error="late error", stackTrace="", isFailure=False)


class CiTestSummaryTest(unittest.TestCase):
    def events(self):
        return [
            event("start", protocolVersion="0.1.1", runnerVersion="1.25.2", pid=1),
            event("suite", suite={"id": 0, "path": "test/fixture_test.dart", "platform": "vm"}),
            event("allSuites", count=1),
            test_start(1, "loading test/fixture_test.dart", []),
            test_done(1, hidden=True),
            event("group", group={"id": 2, "suiteID": 0, "parentID": None,
                                  "name": "", "testCount": 2, "url": None}),
            test_start(3, "one"),
            test_done(3),
            test_start(4, "two"),
            test_done(4, skipped=True),
            event("done", success=True),
        ]

    def find(self, events, kind, test_id=None):
        return next(item for item in events if item["type"] == kind and (
            test_id is None or item.get("testID", item.get("test", {}).get("id")) == test_id))

    def assert_rejected(self, events):
        with self.assertRaises(ValueError):
            summarize(events)

    def test_skips_and_hidden_loads_are_not_passes(self):
        report, success = summarize(self.events())
        self.assertTrue(success)
        self.assertIn("Executed passes: **1**", report)
        self.assertIn("skipped tests: **1**", report)
        self.assertIn("test/fixture_test.dart: 1", report)
        self.assertIn("not broker qualification", report)

    def test_missing_terminal_event_fails(self):
        report, success = summarize(self.events()[:-1])
        self.assertFalse(success)
        self.assertIn("No terminal reporter event", report)

    def test_failed_suite_load_fails(self):
        events = self.events()
        result = self.find(events, "testDone", 1)
        result.update(result="error", hidden=False)
        events[-1]["success"] = False
        report, success = summarize(events)
        self.assertFalse(success)
        self.assertIn("failed suite loads: **1**", report)
        self.assertIn("failed tests: **0**", report)

    def test_failed_test_fails(self):
        events = self.events()
        self.find(events, "testDone", 3)["result"] = "failure"
        events[-1]["success"] = False
        report, success = summarize(events)
        self.assertFalse(success)
        self.assertIn("failed tests: **1**", report)

    def test_all_skips_fail(self):
        events = self.events()
        self.find(events, "testDone", 3)["skipped"] = True
        self.assertFalse(summarize(events)[1])

    def test_unsuccessful_or_incomplete_terminal_event_fails(self):
        for outcome in (False, None):
            with self.subTest(outcome=outcome):
                events = self.events()
                events[-1]["success"] = outcome
                self.assertFalse(summarize(events)[1])

    def test_orphan_completion_is_rejected(self):
        events = self.events()
        events.insert(-1, test_done(999))
        self.assert_rejected(events)

    def test_started_but_uncompleted_test_is_rejected(self):
        events = self.events()
        events.remove(self.find(events, "testDone", 3))
        self.assert_rejected(events)

    def test_duplicate_completion_cannot_replace_failure(self):
        events = self.events()
        original = self.find(events, "testDone", 3)
        original["result"] = "failure"
        events.insert(-1, test_done(3))
        self.assert_rejected(events)

    def test_duplicate_test_start_is_rejected(self):
        events = self.events()
        events.insert(-1, copy.deepcopy(self.find(events, "testStart", 3)))
        self.assert_rejected(events)

    def test_missing_or_repeated_runner_start_is_rejected(self):
        events = self.events()
        self.assert_rejected(events[1:])
        events.insert(1, copy.deepcopy(events[0]))
        self.assert_rejected(events)

    def test_duplicate_terminal_event_is_rejected(self):
        for outcome in (True, False, None):
            with self.subTest(outcome=outcome):
                self.assert_rejected(self.events() + [event("done", success=outcome)])

    def test_event_after_terminal_is_rejected(self):
        self.assert_rejected(self.events() + [event("print", testID=3,
                                                    messageType="print", message="late")])

    def test_boolean_flags_do_not_accept_strings_or_integers(self):
        for field in ("hidden", "skipped"):
            for value in ("false", 0, 1, None):
                with self.subTest(field=field, value=value):
                    events = self.events()
                    self.find(events, "testDone", 3)[field] = value
                    self.assert_rejected(events)

    def test_terminal_success_type_is_checked(self):
        for value in ("true", 1, 0, [], {}):
            with self.subTest(value=value):
                events = self.events()
                events[-1]["success"] = value
                self.assert_rejected(events)
        events = self.events()
        del events[-1]["success"]
        self.assert_rejected(events)

    def test_boolean_and_string_ids_are_rejected(self):
        for value in (True, False, "3", -1):
            with self.subTest(value=value):
                events = self.events()
                self.find(events, "testDone", 3)["testID"] = value
                self.assert_rejected(events)

    def test_unknown_suite_reference_is_rejected(self):
        events = self.events()
        self.find(events, "testStart", 3)["test"]["suiteID"] = 999
        self.assert_rejected(events)

    def test_invalid_group_references_are_rejected(self):
        for value in ([999], [2, 2], [True], "2"):
            with self.subTest(value=value):
                events = self.events()
                self.find(events, "testStart", 3)["test"]["groupIDs"] = value
                self.assert_rejected(events)

    def test_duplicate_suite_or_group_metadata_is_rejected(self):
        for kind in ("suite", "group"):
            with self.subTest(kind=kind):
                events = self.events()
                events.insert(-1, copy.deepcopy(self.find(events, kind)))
                self.assert_rejected(events)

    def test_missing_duplicate_or_inconsistent_suite_count_is_rejected(self):
        events = self.events()
        events.remove(self.find(events, "allSuites"))
        self.assert_rejected(events)
        events = self.events()
        events.insert(-1, event("allSuites", count=1))
        self.assert_rejected(events)
        for value in (2, True, "1"):
            with self.subTest(value=value):
                events = self.events()
                self.find(events, "allSuites")["count"] = value
                self.assert_rejected(events)

    def test_suite_count_can_arrive_after_tests(self):
        events = self.events()
        count = self.find(events, "allSuites")
        events.remove(count)
        events.insert(-1, count)
        self.assertTrue(summarize(events)[1])

    def test_hidden_failed_completion_is_rejected(self):
        events = self.events()
        self.find(events, "testDone", 3).update(result="failure", hidden=True)
        self.assert_rejected(events)

    def test_error_after_completion_is_a_failure(self):
        events = self.events()
        events.insert(-1, test_error(3))
        events[-1]["success"] = False
        report, success = summarize(events)
        self.assertFalse(success)
        self.assertIn("failed tests: **1**", report)
        self.assertIn("Executed passes: **0**", report)

    def test_late_error_makes_hidden_virtual_test_visible(self):
        events = self.events()
        events[-1:-1] = [test_start(5, "setUpAll"), test_done(5, hidden=True), test_error(5)]
        events[-1]["success"] = False
        report, success = summarize(events)
        self.assertFalse(success)
        self.assertIn("failed tests: **1**", report)
        self.assertIn("Executed passes: **1**", report)

    def test_multiple_error_events_count_one_failed_test(self):
        events = self.events()
        events[-1:-1] = [test_error(3), test_error(3)]
        events[-1]["success"] = False
        report, success = summarize(events)
        self.assertFalse(success)
        self.assertIn("failed tests: **1**", report)

    def test_late_error_in_hidden_loader_is_a_failed_load(self):
        events = self.events()
        events.insert(-1, test_error(1))
        events[-1]["success"] = False
        report, success = summarize(events)
        self.assertFalse(success)
        self.assertIn("failed suite loads: **1**", report)

    def test_orphan_diagnostic_and_invalid_error_flag_are_rejected(self):
        events = self.events()
        events.insert(-1, test_error(999))
        self.assert_rejected(events)
        events = self.events()
        invalid = test_error(3)
        invalid["isFailure"] = "false"
        events.insert(-1, invalid)
        self.assert_rejected(events)

    def test_non_object_unknown_type_and_invalid_time_are_rejected(self):
        for value in ([], "event", None, event("futureType"), {"type": "print", "time": True}):
            with self.subTest(value=value):
                events = self.events()
                events.insert(-1, value)
                self.assert_rejected(events)

    def test_unknown_suite_path_does_not_break_skip_summary(self):
        events = self.events()
        self.find(events, "suite")["suite"]["path"] = None
        report, success = summarize(events)
        self.assertTrue(success)
        self.assertIn("<unknown suite>: 1", report)


if __name__ == "__main__":
    unittest.main()
