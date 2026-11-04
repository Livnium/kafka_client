#!/usr/bin/env python3
"""Summarize Dart's JSON test reporter without calling skipped tests passes."""

import argparse
from collections import Counter
import json
from pathlib import Path


def require(condition, message):
    if not condition:
        raise ValueError(message)


def is_id(value):
    return type(value) is int and value >= 0


def nullable_text(value):
    return value is None or isinstance(value, str)


def validate_entry(entry, suites):
    require(isinstance(entry, dict), "Expected test/group metadata object")
    require(is_id(entry.get("id")), "Invalid test/group ID")
    require(is_id(entry.get("suiteID")) and entry["suiteID"] in suites,
            "Test/group references an unknown suite")
    require(isinstance(entry.get("name"), str), "Invalid test/group name")
    require("url" in entry and nullable_text(entry["url"]), "Invalid test/group URL")


def read_results(events):
    """Validate lifecycle and result fields before accepting reporter counts.

    Events from different suites may interleave, and error events may follow
    testDone. TestDone itself is emitted only once for each started test.
    """
    suites = {}
    groups = {}
    tests = {}
    results = {}
    errored = set()
    started = False
    suite_count = None
    terminal = None
    for event in events:
        require(isinstance(event, dict), "Expected a reporter event object")
        require(terminal is None, "Reporter event received after terminal done")
        require(is_id(event.get("time")), "Invalid reporter event time")
        kind = event.get("type")
        if kind == "start":
            require(not started, "Duplicate reporter start event")
            require(event.get("protocolVersion") == "0.1.1", "Unsupported reporter protocol")
            require("runnerVersion" in event and nullable_text(event["runnerVersion"]),
                    "Invalid reporter runner version")
            require(is_id(event.get("pid")), "Invalid reporter process ID")
            started = True
            continue
        require(started, "Reporter start event must be first")
        if kind == "suite":
            suite = event.get("suite")
            require(isinstance(suite, dict) and is_id(suite.get("id")), "Invalid suite metadata")
            require(suite["id"] not in suites, "Duplicate suite ID")
            require("path" in suite and nullable_text(suite["path"]), "Invalid suite path")
            require("platform" in suite and nullable_text(suite["platform"]), "Invalid suite platform")
            suites[suite["id"]] = suite["path"]
        elif kind == "allSuites":
            require(suite_count is None, "Duplicate allSuites event")
            require(is_id(event.get("count")), "Invalid suite count")
            suite_count = event["count"]
        elif kind == "group":
            group = event.get("group")
            validate_entry(group, suites)
            require(group["id"] not in groups, "Duplicate group ID")
            parent = group.get("parentID")
            require("parentID" in group and (parent is None or (
                is_id(parent) and parent in groups
                and groups[parent]["suiteID"] == group["suiteID"])), "Invalid group parent")
            require(is_id(group.get("testCount")), "Invalid group test count")
            groups[group["id"]] = group
        elif kind == "testStart":
            test = event.get("test")
            validate_entry(test, suites)
            require(test["id"] not in tests, "Duplicate testStart ID")
            group_ids = test.get("groupIDs")
            require(isinstance(group_ids, list) and all(
                is_id(group_id) and group_id in groups
                and groups[group_id]["suiteID"] == test["suiteID"]
                for group_id in group_ids), "Invalid test group references")
            require(len(set(group_ids)) == len(group_ids), "Duplicate test group references")
            tests[test["id"]] = test
        elif kind == "testDone":
            require(is_id(event.get("testID")) and event["testID"] in tests,
                    "testDone references a test that never started")
            require(event["testID"] not in results, "Duplicate testDone ID")
            require(event.get("result") in ("success", "failure", "error"), "Invalid test result")
            require(type(event.get("hidden")) is bool and type(event.get("skipped")) is bool,
                    "Test hidden/skipped flags must be booleans")
            require(not (event["hidden"] or event["skipped"]) or event["result"] == "success",
                    "Hidden/skipped test completion cannot contain a failed result")
            results[event["testID"]] = event
        elif kind in ("error", "print"):
            require(is_id(event.get("testID")) and event["testID"] in tests,
                    "Diagnostic references a test that never started")
            if kind == "error":
                require(type(event.get("isFailure")) is bool
                        and isinstance(event.get("error"), str)
                        and isinstance(event.get("stackTrace"), str), "Invalid error event")
                errored.add(event["testID"])
            else:
                require(isinstance(event.get("messageType"), str)
                        and isinstance(event.get("message"), str), "Invalid print event")
        elif kind == "debug":
            require(is_id(event.get("suiteID")) and event["suiteID"] in suites,
                    "Debug event references an unknown suite")
        elif kind == "done":
            require("success" in event and (event["success"] is None
                    or type(event["success"]) is bool), "Invalid terminal success flag")
            terminal = event
        else:
            raise ValueError(f"Unsupported reporter event type: {kind!r}")

    require(started, "Missing reporter start event")
    require(suite_count is not None and suite_count == len(suites),
            "Missing allSuites event or inconsistent suite count")
    require(tests.keys() == results.keys(), "Started test has no completion")
    # Dart allows errors after completion, without emitting another testDone.
    # Such errors also make a previously hidden virtual test visible.
    for test_id in errored:
        results[test_id] = {**results[test_id], "result": "error", "hidden": False, "skipped": False}
    return suites, tests, results, terminal


def summarize(events):
    suites, tests, results, terminal = read_results(events)

    # Failed suite loads become visible in Dart's reporter; "hidden" alone
    # cannot distinguish them from real test cases.
    loaders = {test_id for test_id, test in tests.items()
               if test.get("groupIDs") == [] and test.get("url") is None
               and test.get("name") == f"loading {suites[test['suiteID']]}"}
    visible = [event for event in results.values()
               if not event["hidden"] and event["testID"] not in loaders]
    skipped = [event for event in visible if event["skipped"]]
    passed = sum(not event["skipped"] and event["result"] == "success"
                 for event in visible)
    failed = sum(not event["skipped"] and event["result"] != "success"
                 for event in visible)
    load_failures = sum(event["testID"] in loaders and event["result"] != "success"
                        for event in results.values())
    success = (terminal is not None and terminal.get("success") is True
               and passed > 0 and failed == 0 and load_failures == 0)
    status = "passed" if success else "failed or incomplete"
    lines = [
        "## Ordinary native Dart regressions",
        "",
        f"Result: **{status}**. Executed passes: **{passed}**; "
        f"failed tests: **{failed}**; skipped tests: **{len(skipped)}**; "
        f"failed suite loads: **{load_failures}**.",
        "",
        "External Kafka/Java fixtures were not started by this workflow. "
        "Skipped tests are not broker qualification. Python runner regressions "
        "also do not execute the real-broker fixtures.",
        "",
        "The uploaded JSON report and expanded log retain individual test "
        "names, skip reasons and failures. See doc/NATIVE_CI.md for limits.",
    ]
    if skipped:
        counts = Counter(suites[tests[event["testID"]]["suiteID"]] or "<unknown suite>"
                         for event in skipped)
        lines.extend(["", "### Skipped cases by suite", ""])
        lines.extend(f"- {path}: {count}" for path, count in sorted(counts.items()))
    if terminal is None:
        lines.extend(["", "No terminal reporter event was received."])
    return "\n".join(lines) + "\n", success


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        with args.report.open(encoding="utf-8") as stream:
            report, success = summarize(json.loads(line) for line in stream if line.strip())
    except (OSError, ValueError, KeyError, TypeError) as error:
        report = f"## Native test report unavailable\n\n{error}\n"
        success = False
    print(report, end="")
    if args.output:
        with args.output.open("a", encoding="utf-8") as stream:
            stream.write(report)
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
