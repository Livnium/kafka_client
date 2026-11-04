#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd -- "$(dirname -- "$0")" && pwd)"
: "${KAFKA_HOME:?Verified Kafka 4.0.2 required}"
: "${JAVA_HOME:?JDK required}"
BUILD="$(mktemp -d)"
PID=''
cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  rm -rf -- "$BUILD"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"$JAVA_HOME/bin/javac" -proc:none -cp "$KAFKA_HOME/libs/*" -d "$BUILD" "$HERE/OffsetFetchWireOracle.java" "$HERE/PendingTransactionalOffsetProbe.java"
export KAFKA_COMBINED_USERNAME="$KAFKA_SECURE_USERNAME" KAFKA_COMBINED_PASSWORD="$KAFKA_SECURE_PASSWORD"
# Preserve stdin explicitly: background jobs otherwise get /dev/null in shells.
"$JAVA_HOME/bin/java" -cp "$BUILD:$KAFKA_HOME/libs/*" PendingTransactionalOffsetProbe "$@" <&0 &
PID=$!
wait "$PID"
