#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/.." && pwd)"
DATA_ROOT="${YAMA_JEPSEN_DATA:-/tmp/yama-jepsen}"
PORTS_FILE="$ROOT/cluster-ports.edn"
JAR="$REPO_ROOT/yama-example-raft/target/example-raft-0.0.1-SNAPSHOT.jar"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
PID_DIR="$DATA_ROOT/pids"
LOG_DIR="$DATA_ROOT/logs"

PORTS=(19001 19002 19003)
NODES=(n1 n2 n3)

usage() {
  echo "Usage: $0 {start|stop|status|write-ports}"
}

build_jar() {
  if [[ ! -f "$JAR" ]]; then
    echo "Building yama-example-raft jar..."
    (cd "$REPO_ROOT" && mvn -q package -pl yama-example-raft -am -DskipTests)
  fi
}

write_ports() {
  cat > "$PORTS_FILE" <<EOF
{:n1 ${PORTS[0]}, :n2 ${PORTS[1]}, :n3 ${PORTS[2]}}
EOF
  echo "Wrote $PORTS_FILE"
}

start_cluster() {
  build_jar
  mkdir -p "$DATA_ROOT" "$PID_DIR" "$LOG_DIR"
  write_ports

  local servers="127.0.0.1:${PORTS[0]};127.0.0.1:${PORTS[1]};127.0.0.1:${PORTS[2]}"

  for i in 0 1 2; do
    local node="${NODES[$i]}"
    local port="${PORTS[$i]}"
    local id=$((i + 1))
    local data_dir="$DATA_ROOT/node-$id"
    local pid_file="$PID_DIR/$node.pid"
    local log_file="$LOG_DIR/$node.log"

    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
      echo "$node already running (pid $(cat "$pid_file"))"
      continue
    fi

    rm -rf "$data_dir"
    mkdir -p "$data_dir"

    echo "Starting $node on port $port..."
    nohup "$JAVA_HOME/bin/java" -jar "$JAR" \
      --server.port="$port" \
      --com.song.yama.raft.id="$id" \
      --com.song.yama.raft.servers="$servers" \
      --com.song.yama.raft.join=false \
      --com.song.yama.raft.data-dir="$data_dir" \
      >"$log_file" 2>&1 &
    echo $! >"$pid_file"
  done

  echo "Waiting for cluster to elect leader..."
  sleep 8
  status_cluster
}

stop_cluster() {
  if [[ -d "$PID_DIR" ]]; then
    for pid_file in "$PID_DIR"/*.pid; do
      [[ -f "$pid_file" ]] || continue
      local pid
      pid="$(cat "$pid_file")"
      if kill -0 "$pid" 2>/dev/null; then
        echo "Stopping pid $pid"
        kill "$pid" || true
        sleep 1
        kill -9 "$pid" 2>/dev/null || true
      fi
      rm -f "$pid_file"
    done
  fi
  echo "Cluster stopped"
}

status_cluster() {
  for i in 0 1 2; do
    local node="${NODES[$i]}"
    local port="${PORTS[$i]}"
    local pid_file="$PID_DIR/$node.pid"
    local pid="stopped"
    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
      pid="$(cat "$pid_file")"
    fi
    if curl -sf "http://127.0.0.1:$port/yama/raft/api/v1/put?key=health&value=ok" >/dev/null; then
      echo "$node port=$port pid=$pid status=up"
    else
      echo "$node port=$port pid=$pid status=down"
    fi
  done
}

case "${1:-}" in
  start) start_cluster ;;
  stop) stop_cluster ;;
  status) status_cluster ;;
  write-ports) write_ports ;;
  *) usage; exit 1 ;;
esac
