#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

OUT_DIR="${OUT_DIR:-${ROOT_DIR}/bin}"
mkdir -p "${OUT_DIR}"

echo "==> build pika_port_go"

(
  cd "${ROOT_DIR}"
  go build -o "${OUT_DIR}/pika_port_go" ./cmd/pika_port_go
)

echo "==> done: ${OUT_DIR}/pika_port_go"
