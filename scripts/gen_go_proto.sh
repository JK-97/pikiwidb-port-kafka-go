#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}/proto"

protoc -I . \
  --go_out="${REPO_ROOT}" \
  --go_opt=module=github.com/OpenAtomFoundation/pikiwidb-port-kafka-go \
  --go_opt=Mpika_inner_message.proto=github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/pb/innermessage \
  --go_opt=Mrsync_service.proto=github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/pb/rsyncservice \
  pika_inner_message.proto rsync_service.proto

