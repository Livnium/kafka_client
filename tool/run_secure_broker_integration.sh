#!/usr/bin/env bash
# Local-only TLS/SASL-PLAIN fixture. Requires Java 17+, OpenSSL, Python, selected Kafka 4.x, Dart.
# https://kafka.apache.org/40/security/authentication-using-sasl/
# https://kafka.apache.org/40/security/encryption-and-authentication-using-ssl/
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Default 4.0.2; other stable 4.x releases require explicit KAFKA_VERSION.
KAFKA_VERSION="$(python3 "$ROOT/tool/kafka_distribution.py")"
export KAFKA_VERSION
DART="${DART:-dart}"
# A separate wrapper opts into client-certificate qualification. Never weaken
# ordinary fixture defaults or reinterpret arbitrary values as enabled.
MTLS="${KAFKA_SECURE_MTLS:-0}"
[[ "$MTLS" == 0 || "$MTLS" == 1 ]] || { echo 'KAFKA_SECURE_MTLS must be 0 or 1' >&2; exit 1; }
CLIENT_AUTH=none
if [[ "$MTLS" == 1 ]]; then CLIENT_AUTH=required; fi
RUN_ROOT="${KAFKA_SECURE_RUN_ROOT:-$ROOT/.dart_tool/kafka-security}"
mkdir -p "$RUN_ROOT"
RUN_ROOT="$(cd "$RUN_ROOT" && pwd)"
RUN_DIR="$(mktemp -d "$RUN_ROOT/private.XXXXXXXX")"
chmod 700 "$RUN_DIR"
RESULT="$RUN_ROOT/result-$(basename "$RUN_DIR").txt"
export KAFKA_SECURE_CA="$RUN_DIR/ca.pem"
export KAFKA_SECURE_WRONG_CA="$RUN_DIR/wrong-ca.pem"
export KAFKA_SECURE_TLS_PORT="${KAFKA_SECURE_TLS_PORT:-19192}"
export KAFKA_SECURE_SASL_PORT="${KAFKA_SECURE_SASL_PORT:-19194}"
export KAFKA_SECURE_SCRAM_PORT="${KAFKA_SECURE_SCRAM_PORT:-19196}"
CONTROLLER_PORT="${KAFKA_SECURE_CONTROLLER_PORT:-19193}"
ADMIN_PORT="${KAFKA_SECURE_ADMIN_PORT:-19195}"
export KAFKA_SECURE_USERNAME="u$(openssl rand -hex 8)"
export KAFKA_SECURE_PASSWORD="$(openssl rand -hex 24)"
export KAFKA_SECURE_STORE_PASSWORD="$(openssl rand -hex 24)"
export KAFKA_SECURE_TOPIC="secure-$(basename "$RUN_DIR" | tr . -)"
export KAFKA_SECURE_ORACLE_TOPIC="$KAFKA_SECURE_TOPIC-oracle"
export LOG_DIR="$RUN_DIR/logs"
export KAFKA_HEAP_OPTS='-Xms256m -Xmx512m'
export KAFKA_OPTS="${KAFKA_OPTS:-} -Djava.io.tmpdir=$RUN_DIR"
PID=''
STAGE=initializing
cleanup() {
  status=$?
  trap - EXIT INT TERM
  set +e # Cleanup must still remove secrets if diagnostics fail.
  if [[ -n "$PID" ]]; then
    kill "$PID" 2>/dev/null || true
    for _ in {1..100}; do kill -0 "$PID" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  # Never retain broker configuration/logs, CA keys, certificates or credentials.
  # Only explicitly selected test output is retained, after secret redaction.
  python3 - "$RUN_DIR" "$RESULT" "$status" "$STAGE" "$ROOT/tool" <<'PY'
import os,pathlib,sys
root,result,status,stage,tool=sys.argv[1:]
sys.path.insert(0,tool)
from secure_fixture_result import mtls_summary
version=os.environ['KAFKA_VERSION']
parts=[f'Kafka {version} security fixture: stage={stage}, exit={status}, mtls={os.environ.get("KAFKA_SECURE_MTLS", "0")}\n']
test=pathlib.Path(root)/'tests.log'
if os.environ.get('KAFKA_SECURE_MTLS') == '1':
 text,status=mtls_summary(test.read_text(errors='replace') if test.exists() else '',int(status),stage,version)
 parts=[text]
elif test.exists(): parts.append(test.read_text(errors='replace'))
text=''.join(parts)
for name in ['KAFKA_SECURE_USERNAME','KAFKA_SECURE_PASSWORD','KAFKA_SECURE_STORE_PASSWORD']:
 value=os.environ.get(name)
 if value: text=text.replace(value,'[REDACTED]')
text=text.replace(root,'[ephemeral-fixture]')
pathlib.Path(result).write_text(text)
print(text,end='')
sys.exit(int(status))
PY
  report_status=$?
  if [[ "$report_status" != 0 && "$status" == 0 ]]; then status=$report_status; fi
  cleanup_status=0
  rm -rf -- "$RUN_DIR" || cleanup_status=$?
  if [[ "$cleanup_status" != 0 || -e "$RUN_DIR" ]]; then
    status=1
    echo 'Secure fixture cleanup failed; private material may remain.' >&2
    printf '\nCleanup failed; private material may remain.\n' >> "$RESULT"
  else
    echo "Secure Kafka stopped; ephemeral keys/credentials removed; sanitized result: $RESULT"
  fi
  unset KAFKA_SECURE_PASSWORD KAFKA_SECURE_USERNAME KAFKA_SECURE_STORE_PASSWORD
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
python3 - "$KAFKA_SECURE_TLS_PORT" "$KAFKA_SECURE_SASL_PORT" "$CONTROLLER_PORT" "$ADMIN_PORT" "$KAFKA_SECURE_SCRAM_PORT" <<'PY'
import socket,sys
sockets=[]
for port in sys.argv[1:]:
 s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
 s.bind(('127.0.0.1',int(port)));sockets.append(s)
PY
STAGE=certificates
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 -subj '/CN=Kafka Dart ephemeral test CA' -keyout "$RUN_DIR/ca.key" -out "$RUN_DIR/ca.pem" > "$RUN_DIR/openssl.log" 2>&1
openssl req -newkey rsa:2048 -nodes -sha256 -subj '/CN=localhost' -keyout "$RUN_DIR/server.key" -out "$RUN_DIR/server.csr" >> "$RUN_DIR/openssl.log" 2>&1
cat > "$RUN_DIR/server.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost
EOF
openssl x509 -req -in "$RUN_DIR/server.csr" -CA "$RUN_DIR/ca.pem" -CAkey "$RUN_DIR/ca.key" -CAcreateserial -days 1 -sha256 -extfile "$RUN_DIR/server.ext" -out "$RUN_DIR/server.pem" >> "$RUN_DIR/openssl.log" 2>&1
# Prove the fixture certificate is valid for DNS localhost and not the IP literal.
openssl verify -CAfile "$RUN_DIR/ca.pem" -verify_hostname localhost "$RUN_DIR/server.pem" >> "$RUN_DIR/openssl.log" 2>&1
if openssl verify -CAfile "$RUN_DIR/ca.pem" -verify_ip 127.0.0.1 "$RUN_DIR/server.pem" >> "$RUN_DIR/openssl.log" 2>&1; then
  echo 'Fixture certificate unexpectedly covers IP address' >&2; exit 1
fi
openssl pkcs12 -export -in "$RUN_DIR/server.pem" -inkey "$RUN_DIR/server.key" -certfile "$RUN_DIR/ca.pem" -name kafka -out "$RUN_DIR/server.p12" -passout env:KAFKA_SECURE_STORE_PASSWORD >> "$RUN_DIR/openssl.log" 2>&1
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 -subj '/CN=Untrusted ephemeral test CA' -keyout "$RUN_DIR/wrong-ca.key" -out "$RUN_DIR/wrong-ca.pem" >> "$RUN_DIR/openssl.log" 2>&1
if [[ "$MTLS" == 1 ]]; then
  export KAFKA_SECURE_CLIENT_CERT="$RUN_DIR/client.pem"
  export KAFKA_SECURE_CLIENT_KEY="$RUN_DIR/client.key"
  export KAFKA_SECURE_CLIENT_PKCS12="$RUN_DIR/client.p12"
  export KAFKA_SECURE_UNTRUSTED_CLIENT_CERT="$RUN_DIR/untrusted-client.pem"
  export KAFKA_SECURE_UNTRUSTED_CLIENT_KEY="$RUN_DIR/untrusted-client.key"
  cat > "$RUN_DIR/client.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth
EOF
  for name in client untrusted-client; do
    signer=ca
    if [[ "$name" == untrusted-client ]]; then signer=wrong-ca; fi
    openssl req -newkey rsa:2048 -nodes -sha256 -subj '/CN=Owned ephemeral Kafka client' -keyout "$RUN_DIR/$name.key" -out "$RUN_DIR/$name.csr" >> "$RUN_DIR/openssl.log" 2>&1
    openssl x509 -req -in "$RUN_DIR/$name.csr" -CA "$RUN_DIR/$signer.pem" -CAkey "$RUN_DIR/$signer.key" -CAcreateserial -days 1 -sha256 -extfile "$RUN_DIR/client.ext" -out "$RUN_DIR/$name.pem" >> "$RUN_DIR/openssl.log" 2>&1
  done
  openssl pkcs12 -export -in "$RUN_DIR/client.pem" -inkey "$RUN_DIR/client.key" -certfile "$RUN_DIR/ca.pem" -name kafka-client -out "$RUN_DIR/client.p12" -passout env:KAFKA_SECURE_STORE_PASSWORD >> "$RUN_DIR/openssl.log" 2>&1
fi
cat > "$RUN_DIR/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:$CONTROLLER_PORT
listeners=SSL://127.0.0.1:$KAFKA_SECURE_TLS_PORT,SASL_SSL://127.0.0.1:$KAFKA_SECURE_SASL_PORT,SCRAM://127.0.0.1:$KAFKA_SECURE_SCRAM_PORT,CONTROLLER://127.0.0.1:$CONTROLLER_PORT,PLAINTEXT://127.0.0.1:$ADMIN_PORT
advertised.listeners=SSL://localhost:$KAFKA_SECURE_TLS_PORT,SASL_SSL://localhost:$KAFKA_SECURE_SASL_PORT,SCRAM://localhost:$KAFKA_SECURE_SCRAM_PORT,CONTROLLER://127.0.0.1:$CONTROLLER_PORT,PLAINTEXT://127.0.0.1:$ADMIN_PORT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,SASL_SSL:SASL_SSL,SCRAM:SASL_SSL,CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
ssl.keystore.type=PKCS12
ssl.keystore.location=$RUN_DIR/server.p12
ssl.keystore.password=$KAFKA_SECURE_STORE_PASSWORD
ssl.key.password=$KAFKA_SECURE_STORE_PASSWORD
ssl.client.auth=$CLIENT_AUTH
# SASL_SSL intentionally ignores the unprefixed client-auth property. Require
# it explicitly per listener in mTLS mode, and preserve none otherwise.
listener.name.sasl_ssl.ssl.client.auth=$CLIENT_AUTH
listener.name.scram.ssl.client.auth=$CLIENT_AUTH
ssl.truststore.type=PEM
ssl.truststore.location=$RUN_DIR/ca.pem
sasl.enabled.mechanisms=PLAIN
listener.name.sasl_ssl.sasl.enabled.mechanisms=PLAIN
listener.name.scram.sasl.enabled.mechanisms=SCRAM-SHA-256
listener.name.scram.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_$KAFKA_SECURE_USERNAME="$KAFKA_SECURE_PASSWORD";
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
EOF
# Trust PEM is public material; the configs containing passwords remain private.
cat > "$RUN_DIR/ssl-client.properties" <<EOF
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=$RUN_DIR/ca.pem
EOF
cat > "$RUN_DIR/sasl-client.properties" <<EOF
security.protocol=SASL_SSL
ssl.truststore.type=PEM
ssl.truststore.location=$RUN_DIR/ca.pem
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$KAFKA_SECURE_USERNAME" password="$KAFKA_SECURE_PASSWORD";
EOF
if [[ "$MTLS" == 1 ]]; then
  for config in ssl-client sasl-client; do
    cat >> "$RUN_DIR/$config.properties" <<EOF
ssl.keystore.type=PKCS12
ssl.keystore.location=$RUN_DIR/client.p12
ssl.keystore.password=$KAFKA_SECURE_STORE_PASSWORD
ssl.key.password=$KAFKA_SECURE_STORE_PASSWORD
EOF
  done
fi
STAGE=startup
CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
"$KAFKA_HOME/bin/kafka-storage.sh" format --standalone -t "$CLUSTER_ID" -c "$RUN_DIR/server.properties" > "$RUN_DIR/format.log" 2>&1
"$KAFKA_HOME/bin/kafka-server-start.sh" "$RUN_DIR/server.properties" > "$RUN_DIR/server.log" 2>&1 &
PID=$!
ready=false
for _ in {1..60}; do
  kill -0 "$PID" 2>/dev/null || { echo 'Secure broker exited before readiness' >&2; exit 1; }
  if grep -q 'Kafka Server started' "$RUN_DIR/server.log"; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { echo 'Secure broker startup timed out' >&2; exit 1; }
for topic in "$KAFKA_SECURE_TOPIC" "$KAFKA_SECURE_ORACLE_TOPIC"; do
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "127.0.0.1:$ADMIN_PORT" --create --topic "$topic" --partitions 1 --replication-factor 1 >> "$RUN_DIR/topics.log" 2>&1
done
STAGE=java-security-smoke
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "localhost:$KAFKA_SECURE_TLS_PORT" --command-config "$RUN_DIR/ssl-client.properties" --list > "$RUN_DIR/ssl-smoke.log" 2>&1
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "localhost:$KAFKA_SECURE_SASL_PORT" --command-config "$RUN_DIR/sasl-client.properties" --list > "$RUN_DIR/sasl-smoke.log" 2>&1
printf 'secure-java-to-dart-नमस्ते\n' | "$KAFKA_HOME/bin/kafka-console-producer.sh" --bootstrap-server "localhost:$KAFKA_SECURE_SASL_PORT" --producer.config "$RUN_DIR/sasl-client.properties" --topic "$KAFKA_SECURE_ORACLE_TOPIC" > "$RUN_DIR/oracle-producer.log" 2>&1
STAGE=tests
cd "$ROOT"
if [[ $# -gt 0 ]]; then "$@" > "$RUN_DIR/tests.log" 2>&1; else
  "$DART" test test/security_integration_test.dart --reporter expanded > "$RUN_DIR/tests.log" 2>&1
  STAGE=java-oracle
  # Newer CLIs warn on stdout for the legacy option; old 4.x CLIs still need it.
  # Kafka's normal --help exits 1. Accept it only with a recognized usage table.
  CONSUMER_HELP_STATUS=0
  "$KAFKA_HOME/bin/kafka-console-consumer.sh" --help > "$RUN_DIR/console-consumer-help.txt" 2>&1 || CONSUMER_HELP_STATUS=$?
  if [[ "$CONSUMER_HELP_STATUS" != 0 && "$CONSUMER_HELP_STATUS" != 1 ]]; then
    exit "$CONSUMER_HELP_STATUS"
  fi
  if ! grep -Eq -- '^Option[[:space:]]+Description[[:space:]]*$' "$RUN_DIR/console-consumer-help.txt"; then
    echo 'Console consumer did not return recognized help output' >&2; exit 1
  fi
  if grep -Eq -- '^[[:space:]]*--command-config([[:space:]]|$)' "$RUN_DIR/console-consumer-help.txt"; then
    CONSUMER_CONFIG_OPTION=--command-config
  elif grep -Eq -- '^[[:space:]]*--consumer[.]config([[:space:]]|$)' "$RUN_DIR/console-consumer-help.txt"; then
    CONSUMER_CONFIG_OPTION=--consumer.config
  else
    echo 'Console consumer help lists no supported config option' >&2; exit 1
  fi
  "$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "localhost:$KAFKA_SECURE_SASL_PORT" "$CONSUMER_CONFIG_OPTION" "$RUN_DIR/sasl-client.properties" --topic "$KAFKA_SECURE_ORACLE_TOPIC" --from-beginning --max-messages 2 --timeout-ms 10000 > "$RUN_DIR/oracle-consumer.txt" 2> "$RUN_DIR/oracle-consumer.log"
  printf 'secure-java-to-dart-नमस्ते\nsecure-dart-to-java-नमस्ते\n' > "$RUN_DIR/oracle-expected.txt"
  diff -u "$RUN_DIR/oracle-expected.txt" "$RUN_DIR/oracle-consumer.txt" >> "$RUN_DIR/tests.log"
  echo 'Official Java SASL_SSL consumer verified Dart-produced record' >> "$RUN_DIR/tests.log"
fi
STAGE=complete
