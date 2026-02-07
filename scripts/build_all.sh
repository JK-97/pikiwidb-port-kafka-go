#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/build_all.sh [options]

One-click build:
  1) Ensure /tmp/pikiwidb (clone if needed)
  2) Build PikiwiDB deps (rocksdb/glog/...)
  3) Build pika_port_go

Options:
  --pikiwidb-root PATH   PikiwiDB source root (default: /tmp/pikiwidb)
  --pikiwidb-repo URL    PikiwiDB repo URL (default: official repo)
  --pikiwidb-ref REF     PikiwiDB tag/branch/commit (default: v3.5.6)
  --clone                Clone if root does not exist
  --clean                Clean PikiwiDB build output before build
  --skip-pikiwidb-build  Skip PikiwiDB build step
  -h, --help             Show help
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PIKIWIDB_ROOT="${PIKIWIDB_ROOT:-/tmp/pikiwidb}"
PIKIWIDB_REPO="https://github.com/OpenAtomFoundation/pikiwidb.git"
PIKIWIDB_REF="v3.5.6"
CLONE=0
CLEAN=0
SKIP_PIKI=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pikiwidb-root) PIKIWIDB_ROOT="$2"; shift 2 ;;
    --pikiwidb-repo) PIKIWIDB_REPO="$2"; shift 2 ;;
    --pikiwidb-ref) PIKIWIDB_REF="$2"; shift 2 ;;
    --clone) CLONE=1; shift ;;
    --clean) CLEAN=1; shift ;;
    --skip-pikiwidb-build) SKIP_PIKI=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

if [[ -z "$PIKIWIDB_ROOT" ]]; then
  PIKIWIDB_ROOT="/tmp/pikiwidb"
fi

if [[ ! -d "$PIKIWIDB_ROOT" ]]; then
  if [[ "$CLONE" -eq 1 ]]; then
    git clone "$PIKIWIDB_REPO" "$PIKIWIDB_ROOT"
    (cd "$PIKIWIDB_ROOT" && git checkout "$PIKIWIDB_REF")
  else
    die "PIKIWIDB_ROOT not found: $PIKIWIDB_ROOT (use --clone)"
  fi
fi

# Cgo include paths are hardcoded to /tmp/pikiwidb
if [[ "$PIKIWIDB_ROOT" != "/tmp/pikiwidb" ]]; then
  ln -sfn "$PIKIWIDB_ROOT" /tmp/pikiwidb
fi

if [[ "$SKIP_PIKI" -eq 0 ]]; then
  if [[ "$CLEAN" -eq 1 ]]; then
    (cd "$PIKIWIDB_ROOT" && ./build.sh --clean)
  fi
  (cd "$PIKIWIDB_ROOT" && ./build.sh)
fi

(cd "$REPO_ROOT" && ./scripts/build_go.sh)
echo "==> build_all done"
