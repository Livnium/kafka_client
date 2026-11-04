#!/usr/bin/env python3
# Copyright Livnium.
# SPDX-License-Identifier: Apache-2.0
"""Regenerate synthetic Kafka 4.0.2 codec goldens offline, without a broker."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "tool" / "RecordBatchOracle.java"
FIXTURE = ROOT / "test" / "fixtures" / "record_batch_java_4_0_2.json"
JARS = {
    "kafka-clients-4.0.2.jar":
        "90ef3a38696ee29e4f8dec2b2910bd31d8da0e69cb53a62a231a7d05fea32ad9",
    "slf4j-api-1.7.36.jar":
        "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0",
}
CASES = (
    "create-time-binary-headers-negative-delta",
    "log-append-time",
    "compacted-sparse-prefix-tail",
    "delete-horizon-sparse",
    "compacted-empty",
    "commit-control",
    "abort-control",
    "transactional-visible-read-uncommitted",
    "above-js-safe-integer",
    "no-timestamp-sentinel",
)


def verified_jars(kafka_home):
    paths = []
    for name, expected in JARS.items():
        path = kafka_home / "libs" / name
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected:
            raise RuntimeError(f"Unexpected SHA256 for {name}; no Java command ran")
        paths.append(str(path.resolve()))
    return paths


def generate(kafka_home, java="java", javac="javac"):
    jars = verified_jars(kafka_home)
    with tempfile.TemporaryDirectory(prefix="record-batch-java-") as temporary:
        subprocess.run(
            [javac, "-J-Xmx128m", "-encoding", "UTF-8", "-cp", os.pathsep.join(jars),
             "-d", temporary, str(SOURCE)],
            check=True, capture_output=True, text=True, timeout=60,
        )
        result = subprocess.run(
            [java, "-Xmx128m", "-cp", os.pathsep.join([temporary, *jars]),
             "RecordBatchOracle"],
            check=True, capture_output=True, text=True, timeout=60,
        )
    cases = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    if [case["name"] for case in cases] != list(CASES):
        raise RuntimeError("Java oracle returned an unexpected case list")
    # Normalize source newlines so checkout line-ending policy cannot change provenance.
    source = SOURCE.read_text(encoding="utf-8").replace("\r\n", "\n")
    artifact = {
        "formatVersion": 1,
        "oracle": {
            "kafkaVersion": "4.0.2",
            "generator": "tool/RecordBatchOracle.java",
            "generatorSha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
            "jarsSha256": JARS,
        },
        "cases": cases,
    }
    return json.dumps(artifact, indent=2, sort_keys=True) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-home", type=Path,
                        default=os.environ.get("KAFKA_HOME"))
    parser.add_argument("--java", default="java")
    parser.add_argument("--javac", default="javac")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true",
                      help="Compare with the checked-in fixture (default)")
    mode.add_argument("--write", action="store_true",
                      help="Replace the fixture after reviewing the generator")
    args = parser.parse_args(argv)
    if args.kafka_home is None:
        parser.error("Set KAFKA_HOME or --kafka-home to the verified Kafka 4.0.2 distribution")
    try:
        text = generate(args.kafka_home, java=args.java, javac=args.javac)
        if args.write:
            with FIXTURE.open("w", encoding="utf-8", newline="\n") as output:
                output.write(text)
            print("Wrote 10 offline Java record-batch fixtures")
        elif FIXTURE.read_text(encoding="utf-8") != text:
            print("Fixture differs; review the generator before using --write", file=sys.stderr)
            return 1
        else:
            print("Verified 10 deterministic Java record-batch fixtures")
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"Record-batch fixture generation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
