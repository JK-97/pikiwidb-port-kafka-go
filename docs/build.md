# Build (Go)

This repo builds a **PB-only** pika-port-kafka implementation:

- `bin/pika_port_go` (Go main program)

## Prerequisites

- PikiwiDB source tree at `/tmp/pikiwidb` (used by snapshot parser Cgo build).
- PikiwiDB deps at `/tmp/pikiwidb/deps` (rocksdb/glog/etc).
- `librdkafka` development files available for `confluent-kafka-go`.
- `protoc` + `protoc-gen-go` only needed when regenerating `proto/*.proto`.

## Build

```bash
cd /workspaces/pikiwidb-port-kafka-go
./scripts/build_go.sh
```

Artifacts:

- `bin/pika_port_go`

### Custom paths

The build script supports overriding default paths:

- `OUT_DIR` (default: `./bin`)

Example:

```bash
PIKIWIDB_ROOT=/tmp/pikiwidb OUT_DIR=/tmp/out ./scripts/build_go.sh
```

## Regenerate protobuf (optional)

```bash
./scripts/gen_go_proto.sh
```
