#!/usr/bin/env bash
# Separate opt-in mTLS qualification. Only the owned loopback broker is started.
# Synthetic one-day identities are deleted by the underlying fixture's trap.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export KAFKA_SECURE_MTLS=1
export KAFKA_SECURE_RUN_ROOT="${KAFKA_SECURE_RUN_ROOT:-$ROOT/.dart_tool/kafka-mtls}"
DART="${DART:-dart}"
exec bash "$ROOT/tool/run_secure_broker_integration.sh" \
  "$DART" test test/mtls_broker_integration_test.dart --reporter expanded
