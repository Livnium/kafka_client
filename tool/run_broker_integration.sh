#!/usr/bin/env bash
# Disposable loopback-only Kafka qualification fixture. Requires Java 17+, Kafka, Dart.
# Official setup: https://kafka.apache.org/40/getting-started/quickstart/
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Default 4.0.2; other stable 4.x releases require explicit KAFKA_VERSION.
KAFKA_VERSION="$(python3 "$ROOT/tool/kafka_distribution.py")"
export KAFKA_VERSION
DART="${DART:-dart}"
RUN_ROOT="${KAFKA_TEST_RUN_ROOT:-$ROOT/.dart_tool/kafka-integration}"
mkdir -p "$RUN_ROOT"
RUN_ROOT="$(cd "$RUN_ROOT" && pwd)"
RUN_DIR="$(mktemp -d "$RUN_ROOT/run.XXXXXXXX")"
printf '%s\n' "$KAFKA_VERSION" > "$RUN_DIR/kafka-version.txt"
export LOG_DIR="$RUN_DIR/logs"
export KAFKA_HEAP_OPTS='-Xms256m -Xmx512m'
export KAFKA_OPTS="${KAFKA_OPTS:-} -Djava.io.tmpdir=$RUN_DIR"
export KAFKA_TEST_HOST=127.0.0.1
export KAFKA_TEST_PORT="${KAFKA_TEST_PORT:-19092}"
CONTROLLER_PORT="${KAFKA_TEST_CONTROLLER_PORT:-19093}"
export KAFKA_TEST_TOPIC="dart-roundtrip-$(basename "$RUN_DIR" | tr . -)"
export KAFKA_TEST_ORACLE_TOPIC="$KAFKA_TEST_TOPIC-oracle"
PID=''
cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [[ -n "$PID" ]]; then
    kill "$PID" 2>/dev/null || true
    for _ in {1..100}; do
      kill -0 "$PID" 2>/dev/null || break
      sleep 0.1
    done
    kill -9 "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  if [[ "$status" != 0 ]]; then tail -n 80 "$RUN_DIR/server.log" 2>/dev/null || true; fi
  # Retain bounded logs/config as diagnostics; remove only this run's broker data.
  rm -rf "$RUN_DIR/data"
  echo "Kafka fixture stopped; diagnostics: $RUN_DIR"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# Fail before starting if another process owns either selected port.
python3 - "$KAFKA_TEST_PORT" "$CONTROLLER_PORT" <<'PY'
import socket,sys
sockets=[]
for port in sys.argv[1:]:
 s=socket.socket(); s.bind(('127.0.0.1',int(port))); sockets.append(s)
PY
cat > "$RUN_DIR/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:$CONTROLLER_PORT
listeners=PLAINTEXT://127.0.0.1:$KAFKA_TEST_PORT,CONTROLLER://127.0.0.1:$CONTROLLER_PORT
advertised.listeners=PLAINTEXT://127.0.0.1:$KAFKA_TEST_PORT,CONTROLLER://127.0.0.1:$CONTROLLER_PORT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$RUN_DIR/data
num.partitions=1
num.network.threads=2
num.io.threads=2
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.segment.bytes=10485760
log.retention.bytes=52428800
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
EOF
CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
"$KAFKA_HOME/bin/kafka-storage.sh" format --standalone -t "$CLUSTER_ID" -c "$RUN_DIR/server.properties" > "$RUN_DIR/format.log" 2>&1
"$KAFKA_HOME/bin/kafka-server-start.sh" "$RUN_DIR/server.properties" > "$RUN_DIR/server.log" 2>&1 &
PID=$!
ready=false
for _ in {1..60}; do
  kill -0 "$PID" 2>/dev/null || { echo 'Kafka exited before readiness' >&2; exit 1; }
  if grep -q 'Kafka Server started' "$RUN_DIR/server.log"; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { echo 'Kafka startup timed out' >&2; exit 1; }
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "127.0.0.1:$KAFKA_TEST_PORT" --create --topic "$KAFKA_TEST_TOPIC" --partitions 1 --replication-factor 1 > "$RUN_DIR/topic.log" 2>&1
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "127.0.0.1:$KAFKA_TEST_PORT" --describe --topic "$KAFKA_TEST_TOPIC" >> "$RUN_DIR/topic.log" 2>&1
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "127.0.0.1:$KAFKA_TEST_PORT" --create --topic "$KAFKA_TEST_ORACLE_TOPIC" --partitions 1 --replication-factor 1 >> "$RUN_DIR/topic.log" 2>&1
printf 'java-to-dart-नमस्ते\n' | "$KAFKA_HOME/bin/kafka-console-producer.sh" --bootstrap-server "127.0.0.1:$KAFKA_TEST_PORT" --topic "$KAFKA_TEST_ORACLE_TOPIC" > "$RUN_DIR/java-producer.log" 2>&1
cat "$RUN_DIR/topic.log"
echo "Kafka $KAFKA_VERSION ready on loopback $KAFKA_TEST_PORT; broker PID $PID"
cd "$ROOT"
if [[ $# -gt 0 ]]; then
  "$@"
else
  "$DART" test test/integration_test.dart --reporter expanded
  "$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "127.0.0.1:$KAFKA_TEST_PORT" --topic "$KAFKA_TEST_ORACLE_TOPIC" --from-beginning --max-messages 2 --timeout-ms 10000 > "$RUN_DIR/java-consumer.txt" 2> "$RUN_DIR/java-consumer.log"
  printf 'java-to-dart-नमस्ते\ndart-to-java-नमस्ते\n' > "$RUN_DIR/expected-oracle.txt"
  diff -u "$RUN_DIR/expected-oracle.txt" "$RUN_DIR/java-consumer.txt"
  echo 'Official Java consumer verified Dart-produced record'
fi
