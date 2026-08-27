#!/usr/bin/env bash
set -euo pipefail

# Resolve relative to this script's own location, not the caller's cwd - Maelstrom
# launches this from a distinct per-node working directory, not from here.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec java -jar "$SCRIPT_DIR"/target/maelstrom-server-*.jar "$@"
