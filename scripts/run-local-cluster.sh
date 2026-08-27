#!/usr/bin/env bash
set -euo pipefail

JAR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/server/target/server-0.1.0-SNAPSHOT.jar"
NODES=("$@")
if [ ${#NODES[@]} -eq 0 ]; then
    NODES=("node1:127.0.0.1:8090" "node2:127.0.0.1:8091" "node3:127.0.0.1:8092")
fi
PEERS=$(IFS=,; echo "${NODES[*]}")
SHUTDOWN_TIMEOUT=5

pids=()
cleaned_up=false

cleanup() {
    if [ "$cleaned_up" = true ]; then
        return
    fi
    cleaned_up=true

    echo "stopping all nodes..."
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done

    # give each process a chance to shut down gracefully, then force-kill stragglers
    deadline=$((SECONDS + SHUTDOWN_TIMEOUT))
    for pid in "${pids[@]}"; do
        while kill -0 "$pid" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; do
            sleep 0.2
        done
        if kill -0 "$pid" 2>/dev/null; then
            echo "node with pid $pid did not stop in time, killing"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup INT TERM
trap cleanup EXIT

JVM_FLAGS=(--sun-misc-unsafe-memory-access=allow -Djdk.tracePinnedThreads=full --add-opens java.base/java.util.concurrent=ALL-UNNAMED)

for entry in "${NODES[@]}"; do
    IFS=':' read -r nodeId host port <<< "$entry"
    NODE_ID="$nodeId" PORT="$port" PEERS="$PEERS" \
        java "${JVM_FLAGS[@]}" -jar "$JAR" &
    pids+=($!)
    echo "started $nodeId (pid $!)"
done

wait
